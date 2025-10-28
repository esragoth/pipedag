"""
Event system for observable pipeline execution.
"""

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class EventKind(str, Enum):
    """Types of events emitted during execution."""
    
    RUN_STARTED = "run_started"
    NODE_STARTED = "node_started"
    NODE_FINISHED = "node_finished"
    NODE_FAILED = "node_failed"
    RUN_FINISHED = "run_finished"


@dataclass
class Event:
    """Base event structure for all pipeline events."""
    
    run_id: str
    kind: EventKind
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    node_id: Optional[str] = None
    meta: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary for serialization."""
        return {
            "run_id": self.run_id,
            "kind": self.kind.value,
            "timestamp": self.timestamp.isoformat(),
            "node_id": self.node_id,
            "meta": self.meta,
        }


@dataclass
class RunStartedEvent(Event):
    """Emitted when a run begins."""
    
    kind: EventKind = field(default=EventKind.RUN_STARTED, init=False)


@dataclass
class NodeStartedEvent(Event):
    """Emitted when a node begins execution."""
    
    kind: EventKind = field(default=EventKind.NODE_STARTED, init=False)
    
    def __post_init__(self) -> None:
        """Populate meta with node start info."""
        self.meta.setdefault("inputs_present", [])
        self.meta.setdefault("attempt", 1)


@dataclass
class NodeFinishedEvent(Event):
    """Emitted when a node completes successfully."""
    
    kind: EventKind = field(default=EventKind.NODE_FINISHED, init=False)
    
    def __post_init__(self) -> None:
        """Populate meta with node completion info."""
        self.meta.setdefault("outputs_present", [])
        self.meta.setdefault("duration_ms", 0)
        self.meta.setdefault("payload_sizes", {})


@dataclass
class NodeFailedEvent(Event):
    """Emitted when a node fails."""
    
    kind: EventKind = field(default=EventKind.NODE_FAILED, init=False)
    
    def __post_init__(self) -> None:
        """Populate meta with failure info."""
        self.meta.setdefault("error_class", "")
        self.meta.setdefault("error_message", "")
        self.meta.setdefault("attempt", 1)
        self.meta.setdefault("retry_left", 0)
        self.meta.setdefault("duration_ms", 0)


@dataclass
class RunFinishedEvent(Event):
    """Emitted when a run completes (success or failure)."""
    
    kind: EventKind = field(default=EventKind.RUN_FINISHED, init=False)
    
    def __post_init__(self) -> None:
        """Populate meta with run completion info."""
        self.meta.setdefault("status", "ok")
        self.meta.setdefault("duration_ms", 0)
        self.meta.setdefault("nodes_executed", 0)
        self.meta.setdefault("nodes_failed", 0)


class EventEmitter:
    """
    Thread-safe event emitter for in-process subscribers.
    """
    
    def __init__(self) -> None:
        """Initialize the event emitter."""
        self._subscribers: list[Callable[[Event], None]] = []
        self._lock = threading.Lock()
    
    def subscribe(self, callback: Callable[[Event], None]) -> None:
        """
        Subscribe to all events.
        
        Args:
            callback: Function called with each emitted event
        """
        with self._lock:
            if callback not in self._subscribers:
                self._subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable[[Event], None]) -> None:
        """
        Unsubscribe from events.
        
        Args:
            callback: Previously subscribed callback to remove
        """
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)
    
    def emit(self, event: Event) -> None:
        """
        Emit an event to all subscribers.
        
        Args:
            event: Event to emit
        """
        with self._lock:
            subscribers = self._subscribers.copy()
        
        # Call subscribers outside the lock to avoid deadlocks
        for callback in subscribers:
            try:
                callback(event)
            except Exception:
                # Don't let subscriber errors break the pipeline
                pass
    
    def clear(self) -> None:
        """Remove all subscribers."""
        with self._lock:
            self._subscribers.clear()

