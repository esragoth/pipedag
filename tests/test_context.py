"""Tests for context functionality."""

import pytest

from pipedag.context import NodeContext, RunContext
from pipedag.events import EventEmitter


class TestRunContext:
    """Test RunContext functionality."""
    
    def test_create_run_context(self) -> None:
        """Test creating a run context."""
        ctx = RunContext()
        
        assert ctx.run_id is not None
        assert isinstance(ctx.emitter, EventEmitter)
        assert isinstance(ctx.secrets, dict)
        assert isinstance(ctx.scratch, dict)
        assert isinstance(ctx.tags, list)
        assert not ctx.cancelled
    
    def test_custom_run_context(self) -> None:
        """Test creating context with custom values."""
        emitter = EventEmitter()
        secrets = {"key": "value"}
        tags = ["production", "critical"]
        
        ctx = RunContext(
            run_id="custom-id",
            emitter=emitter,
            secrets=secrets,
            tags=tags,
        )
        
        assert ctx.run_id == "custom-id"
        assert ctx.emitter is emitter
        assert ctx.secrets == secrets
        assert ctx.tags == tags
    
    def test_cancellation(self) -> None:
        """Test cancellation flag."""
        ctx = RunContext()
        
        assert not ctx.cancelled
        
        ctx.cancel()
        
        assert ctx.cancelled
    
    def test_create_node_context(self) -> None:
        """Test creating node context from run context."""
        run_ctx = RunContext()
        run_ctx.secrets = {"api_key": "secret"}
        run_ctx.scratch["shared"] = "data"
        
        node_ctx = run_ctx.create_node_context("test_node")
        
        assert node_ctx.run_id == run_ctx.run_id
        assert node_ctx.node_id == "test_node"
        assert node_ctx.secrets is run_ctx.secrets
        assert node_ctx.scratch is run_ctx.scratch
        assert node_ctx.run_context is run_ctx


class TestNodeContext:
    """Test NodeContext functionality."""
    
    def test_logger_initialization(self) -> None:
        """Test that logger is initialized correctly."""
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test_node")
        
        assert node_ctx.logger is not None
        # Logger should have run_id and node_id in extra
    
    def test_get_secret(self) -> None:
        """Test getting secrets."""
        run_ctx = RunContext(secrets={"key1": "value1", "key2": "value2"})
        node_ctx = run_ctx.create_node_context("test_node")
        
        assert node_ctx.get_secret("key1") == "value1"
        assert node_ctx.get_secret("key2") == "value2"
        assert node_ctx.get_secret("missing") is None
        assert node_ctx.get_secret("missing", "default") == "default"
    
    def test_cancelled_propagation(self) -> None:
        """Test that cancellation propagates to node context."""
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test_node")
        
        assert not node_ctx.cancelled
        
        run_ctx.cancel()
        
        assert node_ctx.cancelled
    
    def test_emit_custom_event(self) -> None:
        """Test emitting custom events from node."""
        from pipedag.events import Event
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test_node")
        
        events = []
        run_ctx.emitter.subscribe(lambda e: events.append(e))
        
        node_ctx.emit("custom_metric", {"value": 42})
        
        assert len(events) == 1
        assert events[0].meta["event_type"] == "custom_metric"
        assert events[0].meta["data"]["value"] == 42
    
    def test_scratch_space(self) -> None:
        """Test scratch space for cross-node communication."""
        run_ctx = RunContext()
        node_ctx1 = run_ctx.create_node_context("node1")
        node_ctx2 = run_ctx.create_node_context("node2")
        
        # Node 1 writes to scratch
        node_ctx1.scratch["shared_data"] = {"counter": 0}
        
        # Node 2 can read it
        assert node_ctx2.scratch["shared_data"]["counter"] == 0
        
        # Node 2 modifies it
        node_ctx2.scratch["shared_data"]["counter"] = 1
        
        # Node 1 sees the change
        assert node_ctx1.scratch["shared_data"]["counter"] == 1

