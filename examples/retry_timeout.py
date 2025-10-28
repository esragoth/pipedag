"""
Retry and timeout example.

Demonstrates:
- Configuring retry policies
- Setting timeouts
- Handling transient failures
- Observing retry attempts via events
"""

import asyncio
import random

from pipedag import Engine, Graph, Node, NodeContext
from pipedag.events import Event, EventKind
from pipedag.types import NodeSpec, PolicySpec, PortSchema


def create_flaky_api_node() -> Node:
    """Create a node that fails intermittently."""
    spec = NodeSpec(
        name="flaky_api",
        inputs={},
        outputs={
            "data": PortSchema(type_hint="dict")
        },
        policy=PolicySpec(
            retry_count=5,  # Retry up to 5 times
            timeout_seconds=2.0,  # 2 second timeout per attempt
            idempotent=True  # Safe to retry
        )
    )
    
    # Track attempts across calls
    attempt_counter = {"count": 0}
    
    async def flaky_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        attempt_counter["count"] += 1
        current_attempt = attempt_counter["count"]
        
        ctx.logger.info(f"API call attempt #{current_attempt}")
        
        # Simulate network delay
        await asyncio.sleep(0.1)
        
        # Fail on first few attempts
        if current_attempt < 3:
            ctx.logger.warning(f"Attempt #{current_attempt} failed - temporary error")
            raise ConnectionError("Temporary network issue")
        
        # Success on 3rd attempt
        ctx.logger.info(f"Attempt #{current_attempt} succeeded!")
        return {"data": {"result": "success", "attempts": current_attempt}}
    
    return Node(
        node_id="flaky_api",
        spec=spec,
        runtime_callable=flaky_func,
        config={}
    )


def create_slow_computation_node() -> Node:
    """Create a node that might exceed timeout."""
    spec = NodeSpec(
        name="slow_compute",
        inputs={
            "timeout_config": PortSchema(type_hint="str", default="fast")
        },
        outputs={
            "result": PortSchema(type_hint="str")
        },
        policy=PolicySpec(
            timeout_seconds=0.5,  # 500ms timeout
            retry_count=2  # Try twice
        )
    )
    
    async def slow_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        mode = inputs.get("timeout_config", "fast")
        
        if mode == "slow":
            ctx.logger.info("Running in slow mode (will timeout)")
            await asyncio.sleep(1.0)  # Exceeds timeout
        else:
            ctx.logger.info("Running in fast mode")
            await asyncio.sleep(0.1)  # Within timeout
        
        return {"result": f"completed_{mode}"}
    
    return Node(
        node_id="slow_compute",
        spec=spec,
        runtime_callable=slow_func,
        config={}
    )


def create_config_source_node(mode: str = "fast") -> Node:
    """Create a source node that provides config."""
    spec = NodeSpec(
        name="config_source",
        inputs={},
        outputs={
            "timeout_config": PortSchema(type_hint="str")
        }
    )
    
    def source_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        return {"timeout_config": mode}
    
    return Node(
        node_id="config_source",
        spec=spec,
        runtime_callable=source_func,
        config={}
    )


def create_event_logger() -> callable:
    """Create an event logging callback."""
    def log_event(event: Event) -> None:
        if event.kind == EventKind.NODE_STARTED:
            attempt = event.meta.get("attempt", 1)
            print(f"  → Node '{event.node_id}' started (attempt {attempt})")
        
        elif event.kind == EventKind.NODE_FAILED:
            attempt = event.meta.get("attempt", 1)
            retry_left = event.meta.get("retry_left", 0)
            error = event.meta.get("error_message", "")
            print(f"  ✗ Node '{event.node_id}' failed (attempt {attempt}, "
                  f"{retry_left} retries left): {error}")
        
        elif event.kind == EventKind.NODE_FINISHED:
            duration = event.meta.get("duration_ms", 0)
            attempt = event.meta.get("attempt", 1)
            print(f"  ✓ Node '{event.node_id}' succeeded "
                  f"(attempt {attempt}, {duration:.1f}ms)")
    
    return log_event


async def demo_successful_retry() -> None:
    """Demonstrate successful retry after failures."""
    print("\n" + "="*70)
    print("DEMO 1: Successful Retry After Transient Failures")
    print("="*70)
    
    graph = Graph()
    flaky = create_flaky_api_node()
    graph.add_node(flaky)
    
    engine = Engine()
    result = await engine.run(
        graph,
        event_subscribers=[create_event_logger()]
    )
    
    print(f"\nResult: {result.status.value}")
    if result.success:
        data = result.node_outputs["flaky_api"]["data"]
        print(f"Success after {data['attempts']} attempts!")


async def demo_timeout() -> None:
    """Demonstrate timeout handling."""
    print("\n" + "="*70)
    print("DEMO 2: Timeout Handling")
    print("="*70)
    
    # Test with fast mode (should succeed)
    print("\nTest A: Fast mode (within timeout)")
    graph = Graph()
    source = create_config_source_node(mode="fast")
    compute = create_slow_computation_node()
    graph.add_node(source)
    graph.add_node(compute)
    graph.connect("config_source", "timeout_config", "slow_compute", "timeout_config")
    
    engine = Engine()
    result = await engine.run(graph, event_subscribers=[create_event_logger()])
    print(f"Result: {result.status.value}")
    
    # Test with slow mode (will timeout)
    print("\n\nTest B: Slow mode (exceeds timeout)")
    graph2 = Graph()
    source2 = create_config_source_node(mode="slow")
    compute2 = create_slow_computation_node()
    graph2.add_node(source2)
    graph2.add_node(compute2)
    graph2.connect("config_source", "timeout_config", "slow_compute", "timeout_config")
    
    result2 = await engine.run(graph2, event_subscribers=[create_event_logger()])
    print(f"Result: {result2.status.value}")


async def main() -> None:
    """Run all retry/timeout demos."""
    await demo_successful_retry()
    await demo_timeout()
    
    print("\n" + "="*70)
    print("All demos completed!")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())

