"""
Exception hierarchy for PipeDag execution engine.
"""

from datetime import datetime, timezone
from typing import Any, Optional


class PipeDagError(Exception):
    """Base exception for all PipeDag errors."""

    def __init__(
        self,
        message: str,
        run_id: Optional[str] = None,
        node_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.run_id = run_id
        self.node_id = node_id
        self.metadata = metadata or {}
        self.timestamp = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        parts = [f"message={self.message!r}"]
        if self.run_id:
            parts.append(f"run_id={self.run_id!r}")
        if self.node_id:
            parts.append(f"node_id={self.node_id!r}")
        return f"{self.__class__.__name__}({', '.join(parts)})"


class GraphError(PipeDagError):
    """Raised when graph structure is invalid."""

    pass


class ValidationError(PipeDagError):
    """Raised when validation fails (schema, config, etc.)."""

    pass


class ExecutionError(PipeDagError):
    """Raised when node execution fails."""

    pass


class TimeoutError(PipeDagError):
    """Raised when a node execution exceeds its timeout."""

    pass


class CancellationError(PipeDagError):
    """Raised when execution is cancelled."""

    pass

