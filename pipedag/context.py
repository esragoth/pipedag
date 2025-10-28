"""
Runtime context for runs and nodes.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from pipedag.events import Event, EventEmitter


@dataclass
class RunContext:
    """
    Context for an entire pipeline run.
    """
    
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    emitter: EventEmitter = field(default_factory=EventEmitter)
    secrets: dict[str, Any] = field(default_factory=dict)
    scratch: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    _cancelled: bool = field(default=False, init=False)
    
    def cancel(self) -> None:
        """Mark the run as cancelled."""
        self._cancelled = True
    
    @property
    def cancelled(self) -> bool:
        """Check if the run has been cancelled."""
        return self._cancelled
    
    def create_node_context(self, node_id: str) -> "NodeContext":
        """
        Create a NodeContext for a specific node.
        
        Args:
            node_id: Unique identifier for the node
            
        Returns:
            NodeContext instance
        """
        return NodeContext(
            run_id=self.run_id,
            node_id=node_id,
            emitter=self.emitter,
            secrets=self.secrets,
            scratch=self.scratch,
            run_context=self,
        )


@dataclass
class NodeContext:
    """
    Context exposed to nodes during execution.
    """
    
    run_id: str
    node_id: str
    emitter: EventEmitter
    secrets: dict[str, Any]
    scratch: dict[str, Any]
    run_context: RunContext
    logger: logging.LoggerAdapter[logging.Logger] = field(init=False)
    
    def __post_init__(self) -> None:
        """Initialize the node-specific logger."""
        base_logger = logging.getLogger(f"pipedag.node.{self.node_id}")
        self.logger = logging.LoggerAdapter(
            base_logger,
            {"run_id": self.run_id, "node_id": self.node_id}
        )
    
    def emit(
        self,
        event_type: str,
        data: Optional[dict[str, Any]] = None
    ) -> None:
        """
        Emit a custom metric or event.
        
        Args:
            event_type: Type of event/metric
            data: Optional data payload
        """
        from pipedag.events import Event, EventKind
        
        # Custom events use a generic kind
        event = Event(
            run_id=self.run_id,
            node_id=self.node_id,
            kind=EventKind.NODE_STARTED,  # Generic fallback
            meta={"event_type": event_type, "data": data or {}}
        )
        self.emitter.emit(event)
    
    def get_secret(self, key: str, default: Any = None) -> Any:
        """
        Safely retrieve a secret by key.
        
        Args:
            key: Secret key
            default: Default value if key not found
            
        Returns:
            Secret value or default
        """
        return self.secrets.get(key, default)
    
    @property
    def cancelled(self) -> bool:
        """Check if the run has been cancelled."""
        return self.run_context.cancelled

