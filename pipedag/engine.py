"""
Core execution engine for pipeline orchestration.
"""

import asyncio
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional, cast

from pipedag.context import RunContext
from pipedag.errors import CancellationError, ExecutionError, TimeoutError
from pipedag.events import (
    Event,
    NodeFailedEvent,
    NodeFinishedEvent,
    NodeStartedEvent,
    RunFinishedEvent,
    RunStartedEvent,
)
from pipedag.graph import Graph
from pipedag.types import RunStatus


@dataclass
class RunResult:
    """Result of a pipeline run."""
    
    run_id: str
    status: RunStatus
    node_outputs: dict[str, dict[str, Any]]
    started_at: datetime
    finished_at: datetime
    duration_ms: float
    error: Optional[Exception] = None
    nodes_executed: int = 0
    nodes_failed: int = 0
    
    @property
    def success(self) -> bool:
        """Check if run succeeded."""
        return self.status == RunStatus.OK


class Engine:
    """
    Async execution engine for DAG pipelines.
    """
    
    def __init__(self, concurrency: Optional[int] = None) -> None:
        """
        Initialize the engine.
        
        Args:
            concurrency: Maximum concurrent node executions (default: CPU*2)
        """
        if concurrency is None:
            concurrency = os.cpu_count() or 1
            concurrency = concurrency * 2
        
        self.concurrency = concurrency
        self._semaphore: Optional[asyncio.Semaphore] = None
    
    async def run(
        self,
        graph: Graph,
        secrets: Optional[dict[str, Any]] = None,
        event_subscribers: Optional[list[Callable[[Event], None]]] = None,
        tags: Optional[list[str]] = None,
    ) -> RunResult:
        """
        Execute a graph pipeline.
        
        Args:
            graph: Graph to execute
            secrets: Secret values to pass to nodes
            event_subscribers: List of event callback functions
            tags: Optional tags for the run
            
        Returns:
            RunResult with execution details
            
        Raises:
            GraphError: If graph is invalid
            ExecutionError: If execution fails
        """
        # Validate graph before execution
        if not graph.is_validated():
            graph.validate()
        
        # Create run context
        run_ctx = RunContext(
            secrets=secrets or {},
            tags=tags or [],
        )
        
        # Subscribe event listeners
        if event_subscribers:
            for subscriber in event_subscribers:
                run_ctx.emitter.subscribe(subscriber)
        
        # Initialize semaphore
        self._semaphore = asyncio.Semaphore(self.concurrency)
        
        # Track execution state
        started_at = datetime.now(timezone.utc)
        node_outputs: dict[str, dict[str, Any]] = {}
        completed_nodes: set[str] = set()
        failed_nodes: set[str] = set()
        
        # Emit run started event
        run_ctx.emitter.emit(RunStartedEvent(
            run_id=run_ctx.run_id,
            meta={"total_nodes": len(graph.nodes), "tags": tags or []}
        ))
        
        try:
            # Execute graph in topological order with concurrency
            await self._execute_graph(
                graph,
                run_ctx,
                node_outputs,
                completed_nodes,
                failed_nodes,
            )
            
            # Determine final status
            if failed_nodes:
                status = RunStatus.ERROR
            elif run_ctx.cancelled:
                status = RunStatus.CANCELLED
            else:
                status = RunStatus.OK
            
            error = None
            
        except Exception as e:
            status = RunStatus.ERROR
            error = e
        
        finally:
            finished_at = datetime.now(timezone.utc)
            duration_ms = (finished_at - started_at).total_seconds() * 1000
            
            # Emit run finished event
            run_ctx.emitter.emit(RunFinishedEvent(
                run_id=run_ctx.run_id,
                meta={
                    "status": status.value,
                    "duration_ms": duration_ms,
                    "nodes_executed": len(completed_nodes),
                    "nodes_failed": len(failed_nodes),
                }
            ))
        
        return RunResult(
            run_id=run_ctx.run_id,
            status=status,
            node_outputs=node_outputs,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            error=error,
            nodes_executed=len(completed_nodes),
            nodes_failed=len(failed_nodes),
        )
    
    async def _execute_graph(
        self,
        graph: Graph,
        run_ctx: RunContext,
        node_outputs: dict[str, dict[str, Any]],
        completed_nodes: set[str],
        failed_nodes: set[str],
    ) -> None:
        """
        Execute graph nodes with concurrency control.
        
        Args:
            graph: Graph to execute
            run_ctx: Run context
            node_outputs: Dict to store outputs
            completed_nodes: Set to track completed nodes
            failed_nodes: Set to track failed nodes
        """
        # Get topological order
        topo_order = graph.get_topological_order()
        
        # Track ready nodes (all dependencies satisfied)
        pending_nodes = set(topo_order)
        running_tasks: dict[str, asyncio.Task] = {}
        
        while pending_nodes or running_tasks:
            # Check for cancellation
            if run_ctx.cancelled:
                # Cancel running tasks
                for task in running_tasks.values():
                    task.cancel()
                raise CancellationError("Run cancelled", run_id=run_ctx.run_id)
            
            # Find nodes that are ready to execute
            ready_nodes = []
            for node_id in list(pending_nodes):
                if self._is_node_ready(graph, node_id, completed_nodes, failed_nodes):
                    ready_nodes.append(node_id)
            
            # Start tasks for ready nodes
            for node_id in ready_nodes:
                pending_nodes.remove(node_id)
                
                # Skip if dependencies failed (fail-fast)
                if self._has_failed_dependency(graph, node_id, failed_nodes):
                    failed_nodes.add(node_id)
                    continue
                
                # Create task
                task = asyncio.create_task(
                    self._execute_node(
                        graph.nodes[node_id],
                        graph,
                        run_ctx,
                        node_outputs,
                    )
                )
                running_tasks[node_id] = task
            
            # Wait for at least one task to complete
            if running_tasks:
                done, _ = await asyncio.wait(
                    running_tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Process completed tasks
                for task in done:
                    # Find which node completed
                    node_id = next(
                        nid for nid, t in running_tasks.items() if t == task
                    )
                    del running_tasks[node_id]
                    
                    try:
                        result = await task
                        node_outputs[node_id] = result
                        completed_nodes.add(node_id)
                    except Exception:
                        failed_nodes.add(node_id)
                        # Fail-fast: cancel remaining tasks
                        for remaining_task in running_tasks.values():
                            remaining_task.cancel()
                        raise
            else:
                # No tasks running and no ready nodes - shouldn't happen
                # unless there's a logic error
                break
    
    def _is_node_ready(
        self,
        graph: Graph,
        node_id: str,
        completed_nodes: set[str],
        failed_nodes: set[str],
    ) -> bool:
        """
        Check if a node is ready to execute (all dependencies satisfied).
        
        Args:
            graph: Graph
            node_id: Node to check
            completed_nodes: Set of completed node IDs
            failed_nodes: Set of failed node IDs
            
        Returns:
            True if node can execute
        """
        dependencies = graph.get_node_dependencies(node_id)
        
        for edges in dependencies.values():
            for edge in edges:
                src_node_id = edge.src_node_id
                if src_node_id not in completed_nodes:
                    return False
        
        return True
    
    def _has_failed_dependency(
        self,
        graph: Graph,
        node_id: str,
        failed_nodes: set[str],
    ) -> bool:
        """
        Check if any of the node's dependencies failed.
        
        Args:
            graph: Graph
            node_id: Node to check
            failed_nodes: Set of failed node IDs
            
        Returns:
            True if any dependency failed
        """
        dependencies = graph.get_node_dependencies(node_id)
        
        for edges in dependencies.values():
            for edge in edges:
                if edge.src_node_id in failed_nodes:
                    return True
        
        return False
    
    async def _execute_node(
        self,
        node: Any,  # Node
        graph: Graph,
        run_ctx: RunContext,
        node_outputs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Execute a single node with retry and timeout logic.
        
        Args:
            node: Node to execute
            graph: Graph (for dependency lookup)
            run_ctx: Run context
            node_outputs: Dict of completed node outputs
            
        Returns:
            Node output dictionary
            
        Raises:
            ExecutionError: If execution fails after retries
            TimeoutError: If execution times out
        """
        assert self._semaphore is not None
        
        node_ctx = run_ctx.create_node_context(node.node_id)
        policy = node.spec.policy
        
        # Acquire semaphore for concurrency control
        async with self._semaphore:
            # Retry loop
            last_error: Optional[Exception] = None
            
            for attempt in range(policy.retry_count + 1):
                if run_ctx.cancelled:
                    raise CancellationError(
                        "Run cancelled during node execution",
                        run_id=run_ctx.run_id,
                        node_id=node.node_id
                    )
                
                try:
                    # Assemble inputs
                    inputs = self._assemble_node_inputs(
                        node.node_id,
                        graph,
                        node_outputs
                    )
                    
                    # Emit node started event
                    start_time = time.time()
                    run_ctx.emitter.emit(NodeStartedEvent(
                        run_id=run_ctx.run_id,
                        node_id=node.node_id,
                        meta={
                            "inputs_present": list(inputs.keys()),
                            "attempt": attempt + 1,
                        }
                    ))
                    
                    # Execute with timeout
                    if policy.timeout_seconds:
                        outputs = cast(
                            dict[str, Any],
                            await asyncio.wait_for(
                                node.execute(inputs, node_ctx),
                                timeout=policy.timeout_seconds
                            )
                        )
                    else:
                        outputs = cast(dict[str, Any], await node.execute(inputs, node_ctx))
                    
                    # Success - emit finished event
                    duration_ms = (time.time() - start_time) * 1000
                    run_ctx.emitter.emit(NodeFinishedEvent(
                        run_id=run_ctx.run_id,
                        node_id=node.node_id,
                        meta={
                            "outputs_present": list(outputs.keys()),
                            "duration_ms": duration_ms,
                            "attempt": attempt + 1,
                            "payload_sizes": {
                                k: len(str(v)) for k, v in outputs.items()
                            }
                        }
                    ))
                    
                    return outputs
                
                except asyncio.TimeoutError as e:
                    last_error = TimeoutError(
                        f"Node execution timed out after {policy.timeout_seconds}s",
                        run_id=run_ctx.run_id,
                        node_id=node.node_id,
                    )
                    
                    # Emit failed event
                    duration_ms = (time.time() - start_time) * 1000
                    run_ctx.emitter.emit(NodeFailedEvent(
                        run_id=run_ctx.run_id,
                        node_id=node.node_id,
                        meta={
                            "error_class": "TimeoutError",
                            "error_message": str(last_error),
                            "attempt": attempt + 1,
                            "retry_left": policy.retry_count - attempt,
                            "duration_ms": duration_ms,
                        }
                    ))
                    
                    # Retry if attempts remain
                    if attempt < policy.retry_count:
                        await self._backoff(attempt)
                        continue
                    else:
                        raise last_error
                
                except Exception as e:
                    last_error = e if isinstance(e, ExecutionError) else ExecutionError(
                        f"Node execution failed: {str(e)}",
                        run_id=run_ctx.run_id,
                        node_id=node.node_id,
                    )
                    
                    # Emit failed event
                    duration_ms = (time.time() - start_time) * 1000
                    run_ctx.emitter.emit(NodeFailedEvent(
                        run_id=run_ctx.run_id,
                        node_id=node.node_id,
                        meta={
                            "error_class": type(e).__name__,
                            "error_message": str(e),
                            "attempt": attempt + 1,
                            "retry_left": policy.retry_count - attempt,
                            "duration_ms": duration_ms,
                        }
                    ))
                    
                    # Retry if attempts remain
                    if attempt < policy.retry_count:
                        await self._backoff(attempt)
                        continue
                    else:
                        raise last_error
            
            # Should never reach here
            if last_error:
                raise last_error
            raise ExecutionError(
                "Node execution failed",
                run_id=run_ctx.run_id,
                node_id=node.node_id
            )
    
    def _assemble_node_inputs(
        self,
        node_id: str,
        graph: Graph,
        node_outputs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Assemble inputs for a node from upstream outputs.
        
        Args:
            node_id: Node to assemble inputs for
            graph: Graph
            node_outputs: Completed node outputs
            
        Returns:
            Dictionary of input values
        """
        inputs: dict[str, Any] = {}
        dependencies = graph.get_node_dependencies(node_id)
        
        for input_key, edges in dependencies.items():
            # Each input should have exactly one edge (validated earlier)
            if edges:
                edge = edges[0]
                src_outputs = node_outputs[edge.src_node_id]
                
                # Resolve field path
                inputs[input_key] = edge.src_field_path.resolve(src_outputs)
        
        # Add default values for unconnected optional inputs
        node = graph.nodes[node_id]
        for input_key, input_schema in node.spec.inputs.items():
            if input_key not in inputs and not input_schema.required:
                if input_schema.default is not None:
                    inputs[input_key] = input_schema.default
        
        return inputs
    
    async def _backoff(self, attempt: int) -> None:
        """
        Backoff before retry with jitter.
        
        Args:
            attempt: Current attempt number (0-indexed)
        """
        # Fixed backoff with jitter (MVP)
        base_delay = 0.5  # 500ms base
        jitter = random.uniform(0, 0.5)  # Up to 500ms jitter
        delay = base_delay * (attempt + 1) + jitter
        await asyncio.sleep(delay)

