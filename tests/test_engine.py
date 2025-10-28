"""Tests for execution engine."""

import asyncio

import pytest

from pipedag.context import NodeContext
from pipedag.engine import Engine, RunResult
from pipedag.errors import ExecutionError, TimeoutError
from pipedag.events import Event, EventKind
from pipedag.graph import Graph
from pipedag.node import Node
from pipedag.types import NodeSpec, PolicySpec, PortSchema, RunStatus


def create_node(
    node_id: str,
    inputs: dict,
    outputs: dict,
    func_impl: callable,
    policy: PolicySpec = None,
) -> Node:
    """Helper to create a node with custom implementation."""
    spec = NodeSpec(
        name=node_id,
        inputs={k: PortSchema(**v) if isinstance(v, dict) else v for k, v in inputs.items()},
        outputs={k: PortSchema(**v) if isinstance(v, dict) else v for k, v in outputs.items()},
        policy=policy or PolicySpec(),
    )
    return Node(node_id=node_id, spec=spec, runtime_callable=func_impl, config={})


class TestEngine:
    """Test Engine execution."""
    
    async def test_simple_linear_pipeline(self) -> None:
        """Test executing a simple linear pipeline."""
        def add(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] + inputs["y"]}
        
        def multiply(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["value"] * 2}
        
        graph = Graph()
        node_a = create_node("add", {"x": {}, "y": {}}, {"result": {}}, add)
        node_b = create_node("multiply", {"value": {}}, {"result": {}}, multiply)
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("add", "result", "multiply", "value")
        
        # Manually set inputs for first node (in real scenarios, would use source nodes)
        # For testing, we'll modify the graph to have source node
        def source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"x": 5, "y": 3}
        
        node_source = create_node("source", {}, {"x": {}, "y": {}}, source)
        graph.nodes.clear()
        graph.edges.clear()
        graph.add_node(node_source)
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("source", "x", "add", "x")
        graph.connect("source", "y", "add", "y")
        graph.connect("add", "result", "multiply", "value")
        
        engine = Engine(concurrency=2)
        result = await engine.run(graph)
        
        assert result.success
        assert result.status == RunStatus.OK
        assert result.node_outputs["add"]["result"] == 8
        assert result.node_outputs["multiply"]["result"] == 16
    
    async def test_async_nodes(self) -> None:
        """Test executing async nodes."""
        async def async_fetch(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            await asyncio.sleep(0.01)
            return {"data": "fetched"}
        
        graph = Graph()
        node = create_node("fetch", {}, {"data": {}}, async_fetch)
        graph.add_node(node)
        
        engine = Engine()
        result = await engine.run(graph)
        
        assert result.success
        assert result.node_outputs["fetch"]["data"] == "fetched"
    
    async def test_branching_pipeline(self) -> None:
        """Test pipeline with branching (fan-out)."""
        def source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"value": 10}
        
        def double(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] * 2}
        
        def triple(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] * 3}
        
        graph = Graph()
        node_src = create_node("source", {}, {"value": {}}, source)
        node_a = create_node("double", {"x": {}}, {"result": {}}, double)
        node_b = create_node("triple", {"x": {}}, {"result": {}}, triple)
        
        graph.add_node(node_src)
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("source", "value", "double", "x")
        graph.connect("source", "value", "triple", "x")
        
        engine = Engine()
        result = await engine.run(graph)
        
        assert result.success
        assert result.node_outputs["double"]["result"] == 20
        assert result.node_outputs["triple"]["result"] == 30
    
    async def test_field_path_extraction(self) -> None:
        """Test extracting nested fields via field paths."""
        def source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {
                "data": {
                    "user": {"name": "Alice", "email": "alice@example.com"},
                    "status": "active"
                }
            }
        
        def use_email(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"message": f"Email: {inputs['email']}"}
        
        graph = Graph()
        node_src = create_node("source", {}, {"data": {}}, source)
        node_email = create_node("email_node", {"email": {}}, {"message": {}}, use_email)
        
        graph.add_node(node_src)
        graph.add_node(node_email)
        graph.connect("source", "data.user.email", "email_node", "email")
        
        engine = Engine()
        result = await engine.run(graph)
        
        assert result.success
        assert result.node_outputs["email_node"]["message"] == "Email: alice@example.com"
    
    async def test_node_failure(self) -> None:
        """Test handling node failure."""
        def failing_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            raise ValueError("Intentional failure")
        
        graph = Graph()
        node = create_node("fail", {}, {"result": {}}, failing_node)
        graph.add_node(node)
        
        engine = Engine()
        result = await engine.run(graph)
        
        assert not result.success
        assert result.status == RunStatus.ERROR
        assert result.nodes_failed == 1
    
    async def test_retry_logic(self) -> None:
        """Test retry mechanism."""
        attempts = []
        
        def flaky_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("Temporary failure")
            return {"result": "success"}
        
        policy = PolicySpec(retry_count=3)
        graph = Graph()
        node = create_node("flaky", {}, {"result": {}}, flaky_node, policy=policy)
        graph.add_node(node)
        
        engine = Engine()
        result = await engine.run(graph)
        
        assert result.success
        assert len(attempts) == 3  # Initial + 2 retries
        assert result.node_outputs["flaky"]["result"] == "success"
    
    async def test_timeout(self) -> None:
        """Test timeout enforcement."""
        async def slow_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            await asyncio.sleep(1.0)
            return {"result": "done"}
        
        policy = PolicySpec(timeout_seconds=0.1)
        graph = Graph()
        node = create_node("slow", {}, {"result": {}}, slow_node, policy=policy)
        graph.add_node(node)
        
        engine = Engine()
        result = await engine.run(graph)
        
        assert not result.success
        assert result.status == RunStatus.ERROR
    
    async def test_event_emission(self) -> None:
        """Test that events are emitted correctly."""
        events = []
        
        def collector(event: Event) -> None:
            events.append(event)
        
        def simple(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": 42}
        
        graph = Graph()
        node = create_node("simple", {}, {"result": {}}, simple)
        graph.add_node(node)
        
        engine = Engine()
        result = await engine.run(graph, event_subscribers=[collector])
        
        assert result.success
        
        # Check event sequence
        event_kinds = [e.kind for e in events]
        assert EventKind.RUN_STARTED in event_kinds
        assert EventKind.NODE_STARTED in event_kinds
        assert EventKind.NODE_FINISHED in event_kinds
        assert EventKind.RUN_FINISHED in event_kinds
    
    async def test_secrets_access(self) -> None:
        """Test that nodes can access secrets."""
        def use_secret(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            api_key = ctx.get_secret("api_key")
            return {"key": api_key}
        
        graph = Graph()
        node = create_node("secret_user", {}, {"key": {}}, use_secret)
        graph.add_node(node)
        
        engine = Engine()
        result = await engine.run(graph, secrets={"api_key": "secret123"})
        
        assert result.success
        assert result.node_outputs["secret_user"]["key"] == "secret123"
    
    async def test_concurrency_control(self) -> None:
        """Test that concurrency limit is enforced."""
        active_count = []
        max_concurrent = 0
        current_count = 0
        
        async def concurrent_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            nonlocal current_count, max_concurrent
            current_count += 1
            max_concurrent = max(max_concurrent, current_count)
            await asyncio.sleep(0.05)
            current_count -= 1
            return {"result": "done"}
        
        # Create 10 independent nodes
        graph = Graph()
        for i in range(10):
            node = create_node(f"node{i}", {}, {"result": {}}, concurrent_node)
            graph.add_node(node)
        
        engine = Engine(concurrency=3)
        result = await engine.run(graph)
        
        assert result.success
        assert max_concurrent <= 3  # Should respect concurrency limit
    
    async def test_diamond_pattern(self) -> None:
        """Test diamond dependency pattern."""
        def source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"value": 5}
        
        def branch_a(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] + 1}
        
        def branch_b(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] + 2}
        
        def merge(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["a"] + inputs["b"]}
        
        graph = Graph()
        node_src = create_node("source", {}, {"value": {}}, source)
        node_a = create_node("branch_a", {"x": {}}, {"result": {}}, branch_a)
        node_b = create_node("branch_b", {"x": {}}, {"result": {}}, branch_b)
        node_merge = create_node("merge", {"a": {}, "b": {}}, {"result": {}}, merge)
        
        graph.add_node(node_src)
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.add_node(node_merge)
        
        graph.connect("source", "value", "branch_a", "x")
        graph.connect("source", "value", "branch_b", "x")
        graph.connect("branch_a", "result", "merge", "a")
        graph.connect("branch_b", "result", "merge", "b")
        
        engine = Engine()
        result = await engine.run(graph)
        
        assert result.success
        assert result.node_outputs["branch_a"]["result"] == 6
        assert result.node_outputs["branch_b"]["result"] == 7
        assert result.node_outputs["merge"]["result"] == 13


class TestRunResult:
    """Test RunResult functionality."""
    
    def test_success_property(self) -> None:
        """Test success property."""
        from datetime import datetime, timezone
        
        result = RunResult(
            run_id="test",
            status=RunStatus.OK,
            node_outputs={},
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            duration_ms=100.0,
        )
        
        assert result.success is True
        
        result_failed = RunResult(
            run_id="test",
            status=RunStatus.ERROR,
            node_outputs={},
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            duration_ms=100.0,
        )
        
        assert result_failed.success is False

