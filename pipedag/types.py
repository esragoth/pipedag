"""
Core type system and data structures for PipeDag.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

try:
    from pydantic import BaseModel
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    BaseModel = None  # type: ignore


class RunStatus(str, Enum):
    """Status of a pipeline run."""
    OK = "ok"
    ERROR = "error"
    CANCELLED = "cancelled"


@dataclass
class PortSchema:
    """Schema definition for an input or output port."""
    
    description: Optional[str] = None
    type_hint: Optional[str] = None
    required: bool = True
    default: Any = None
    max_bytes: Optional[int] = None
    schema: Optional[Any] = None  # Optional pydantic model for introspection
    
    def __post_init__(self) -> None:
        """Validate schema configuration."""
        if self.schema and not PYDANTIC_AVAILABLE:
            raise ImportError("pydantic is required to use schema introspection")
        
        # Detect array types
        if self.type_hint:
            self.is_array = self.type_hint.startswith("list[") or self.type_hint.startswith("List[")
        else:
            self.is_array = False


@dataclass
class PolicySpec:
    """Runtime policy configuration for node execution."""
    
    retry_count: int = 0
    timeout_seconds: Optional[float] = None
    concurrency_class: Optional[str] = None
    idempotent: bool = False


@dataclass
class NodeSpec:
    """Declarative metadata for a node type."""
    
    name: str
    inputs: dict[str, PortSchema]
    outputs: dict[str, PortSchema]
    config_schema: Optional[dict[str, Any]] = None
    policy: Optional[PolicySpec] = None
    
    def __post_init__(self) -> None:
        """Initialize default policy if not provided."""
        if self.policy is None:
            self.policy = PolicySpec()


@dataclass
class FieldPath:
    """Parse and validate dot-notation field paths."""
    
    port: str
    path: list[str] = field(default_factory=list)
    
    @classmethod
    def parse(cls, path_str: str) -> "FieldPath":
        """
        Parse a field path string like 'result.user.email' into components.
        
        Args:
            path_str: Dot-notation path (e.g., "result.user.email")
            
        Returns:
            FieldPath instance with port and nested path
        """
        parts = path_str.split(".")
        port = parts[0]
        nested_path = parts[1:] if len(parts) > 1 else []
        return cls(port=port, path=nested_path)
    
    def __str__(self) -> str:
        """String representation of the path."""
        if self.path:
            return f"{self.port}.{'.'.join(self.path)}"
        return self.port
    
    def is_nested(self) -> bool:
        """Check if this is a nested field path."""
        return len(self.path) > 0
    
    def resolve(self, data: dict[str, Any]) -> Any:
        """
        Resolve the field path against actual data.
        
        Args:
            data: Dictionary containing the port data
            
        Returns:
            The value at the resolved path
            
        Raises:
            KeyError: If path does not exist in data
            TypeError: If intermediate value is not dict/list
        """
        if self.port not in data:
            raise KeyError(f"Port '{self.port}' not found in data")
        
        value = data[self.port]
        
        for part in self.path:
            if isinstance(value, dict):
                if part not in value:
                    raise KeyError(f"Key '{part}' not found in path {self}")
                value = value[part]
            elif isinstance(value, list):
                # Support array indexing
                try:
                    index = int(part)
                    value = value[index]
                except (ValueError, IndexError) as e:
                    raise KeyError(f"Invalid array access '{part}' in path {self}") from e
            else:
                raise TypeError(
                    f"Cannot traverse path {self}: "
                    f"'{part}' on non-dict/list type {type(value).__name__}"
                )
        
        return value
    
    def validate_against_schema(self, port_schema: PortSchema) -> None:
        """
        Validate that this field path exists in the port schema.
        
        Args:
            port_schema: The schema for the source port
            
        Raises:
            ValidationError: If path cannot be validated against schema
        """
        from pipedag.errors import ValidationError
        
        if not self.is_nested():
            # No nested path, just validate port exists
            return
        
        if not port_schema.schema:
            # No schema available for introspection, skip validation
            return
        
        if not PYDANTIC_AVAILABLE:
            return
        
        # Try to validate path against pydantic schema
        try:
            current_schema = port_schema.schema
            for part in self.path:
                if hasattr(current_schema, "model_fields"):
                    # Pydantic v2
                    if part not in current_schema.model_fields:
                        raise ValidationError(
                            f"Field '{part}' not found in schema for path {self}"
                        )
                    field_info = current_schema.model_fields[part]
                    current_schema = field_info.annotation
                elif hasattr(current_schema, "__fields__"):
                    # Pydantic v1
                    if part not in current_schema.__fields__:
                        raise ValidationError(
                            f"Field '{part}' not found in schema for path {self}"
                        )
                    current_schema = current_schema.__fields__[part].type_
                else:
                    # Can't traverse further
                    break
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            # If we can't validate, just warn but don't fail
            pass


@dataclass
class Edge:
    """Represents a connection between nodes."""
    
    src_node_id: str
    src_output_path: str  # Supports "port" or "port.field.nested"
    dst_node_id: str
    dst_input_key: str
    
    def __post_init__(self) -> None:
        """Parse the output path into a FieldPath."""
        self.src_field_path = FieldPath.parse(self.src_output_path)
    
    def __repr__(self) -> str:
        return (
            f"Edge({self.src_node_id}.{self.src_output_path} "
            f"-> {self.dst_node_id}.{self.dst_input_key})"
        )


@dataclass
class MapConfig:
    """
    Configuration for mapping operations over arrays.
    
    When an output is an array, this allows creating multiple instances
    of a downstream node (one per array element).
    """
    
    enabled: bool = False
    array_output_path: Optional[str] = None  # Which output to map over
    mapped_node_id: Optional[str] = None  # Which node to instantiate N times

