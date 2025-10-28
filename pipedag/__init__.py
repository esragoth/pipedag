"""
PipeDag: Lightweight, embeddable execution engine for DAG-style pipelines.
"""

__version__ = "0.1.0"

from pipedag.engine import Engine, RunResult
from pipedag.errors import (
    CancellationError,
    ExecutionError,
    GraphError,
    PipeDagError,
    TimeoutError,
    ValidationError,
)
from pipedag.events import Event, EventEmitter
from pipedag.graph import Graph
from pipedag.node import Node, NodeContext, node_spec
from pipedag.registry import NodeRegistry, NodeTypeInfo
from pipedag.types import (
    Edge,
    FieldPath,
    NodeSpec,
    PolicySpec,
    PortSchema,
    RunStatus,
)

__all__ = [
    # Core
    "Engine",
    "RunResult",
    "Graph",
    "Node",
    "NodeContext",
    "node_spec",
    # Registry
    "NodeRegistry",
    "NodeTypeInfo",
    # Types
    "NodeSpec",
    "PortSchema",
    "PolicySpec",
    "Edge",
    "FieldPath",
    "RunStatus",
    # Events
    "Event",
    "EventEmitter",
    # Errors
    "PipeDagError",
    "GraphError",
    "ValidationError",
    "ExecutionError",
    "TimeoutError",
    "CancellationError",
]

