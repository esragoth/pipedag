"""Tests for event system."""

import pytest

from pipedag.events import (
    Event,
    EventEmitter,
    EventKind,
    NodeFailedEvent,
    NodeFinishedEvent,
    NodeStartedEvent,
    RunFinishedEvent,
    RunStartedEvent,
)


class TestEvent:
    """Test Event base class."""
    
    def test_create_event(self) -> None:
        """Test creating an event."""
        event = Event(
            run_id="test-run",
            kind=EventKind.RUN_STARTED,
            node_id="node1",
            meta={"key": "value"}
        )
        
        assert event.run_id == "test-run"
        assert event.kind == EventKind.RUN_STARTED
        assert event.node_id == "node1"
        assert event.meta["key"] == "value"
        assert event.timestamp is not None
    
    def test_to_dict(self) -> None:
        """Test converting event to dict."""
        event = Event(
            run_id="test-run",
            kind=EventKind.NODE_STARTED,
            node_id="node1",
        )
        
        d = event.to_dict()
        assert d["run_id"] == "test-run"
        assert d["kind"] == "node_started"
        assert d["node_id"] == "node1"
        assert "timestamp" in d


class TestSpecificEvents:
    """Test specific event types."""
    
    def test_run_started_event(self) -> None:
        """Test RunStartedEvent."""
        event = RunStartedEvent(run_id="test-run")
        
        assert event.kind == EventKind.RUN_STARTED
        assert event.run_id == "test-run"
    
    def test_node_started_event(self) -> None:
        """Test NodeStartedEvent."""
        event = NodeStartedEvent(
            run_id="test-run",
            node_id="node1",
            meta={"inputs_present": ["x", "y"]}
        )
        
        assert event.kind == EventKind.NODE_STARTED
        assert event.node_id == "node1"
        assert "inputs_present" in event.meta
    
    def test_node_finished_event(self) -> None:
        """Test NodeFinishedEvent."""
        event = NodeFinishedEvent(
            run_id="test-run",
            node_id="node1",
            meta={"duration_ms": 123.45}
        )
        
        assert event.kind == EventKind.NODE_FINISHED
        assert event.meta["duration_ms"] == 123.45
        assert "outputs_present" in event.meta
    
    def test_node_failed_event(self) -> None:
        """Test NodeFailedEvent."""
        event = NodeFailedEvent(
            run_id="test-run",
            node_id="node1",
            meta={"error_message": "Failed"}
        )
        
        assert event.kind == EventKind.NODE_FAILED
        assert event.meta["error_message"] == "Failed"
        assert "error_class" in event.meta
    
    def test_run_finished_event(self) -> None:
        """Test RunFinishedEvent."""
        event = RunFinishedEvent(
            run_id="test-run",
            meta={"status": "ok", "duration_ms": 1000}
        )
        
        assert event.kind == EventKind.RUN_FINISHED
        assert event.meta["status"] == "ok"


class TestEventEmitter:
    """Test EventEmitter functionality."""
    
    def test_subscribe_and_emit(self) -> None:
        """Test subscribing and emitting events."""
        emitter = EventEmitter()
        received = []
        
        def callback(event: Event) -> None:
            received.append(event)
        
        emitter.subscribe(callback)
        
        event = Event(run_id="test", kind=EventKind.RUN_STARTED)
        emitter.emit(event)
        
        assert len(received) == 1
        assert received[0].run_id == "test"
    
    def test_multiple_subscribers(self) -> None:
        """Test multiple subscribers."""
        emitter = EventEmitter()
        received1 = []
        received2 = []
        
        emitter.subscribe(lambda e: received1.append(e))
        emitter.subscribe(lambda e: received2.append(e))
        
        event = Event(run_id="test", kind=EventKind.RUN_STARTED)
        emitter.emit(event)
        
        assert len(received1) == 1
        assert len(received2) == 1
    
    def test_unsubscribe(self) -> None:
        """Test unsubscribing from events."""
        emitter = EventEmitter()
        received = []
        
        def callback(event: Event) -> None:
            received.append(event)
        
        emitter.subscribe(callback)
        emitter.unsubscribe(callback)
        
        event = Event(run_id="test", kind=EventKind.RUN_STARTED)
        emitter.emit(event)
        
        assert len(received) == 0
    
    def test_subscriber_error_does_not_break_pipeline(self) -> None:
        """Test that subscriber errors don't break emission."""
        emitter = EventEmitter()
        received = []
        
        def failing_callback(event: Event) -> None:
            raise ValueError("Subscriber error")
        
        def working_callback(event: Event) -> None:
            received.append(event)
        
        emitter.subscribe(failing_callback)
        emitter.subscribe(working_callback)
        
        event = Event(run_id="test", kind=EventKind.RUN_STARTED)
        emitter.emit(event)
        
        # Working callback should still receive event
        assert len(received) == 1
    
    def test_clear(self) -> None:
        """Test clearing all subscribers."""
        emitter = EventEmitter()
        received = []
        
        emitter.subscribe(lambda e: received.append(e))
        emitter.clear()
        
        event = Event(run_id="test", kind=EventKind.RUN_STARTED)
        emitter.emit(event)
        
        assert len(received) == 0

