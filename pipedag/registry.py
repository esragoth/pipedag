"""
Node registry for dynamic node instantiation.

Allows UI to discover available node types and instantiate them dynamically.
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional

from pipedag.context import NodeContext
from pipedag.errors import ValidationError
from pipedag.node import Node, NodeCallable
from pipedag.types import NodeSpec


@dataclass
class NodeTypeInfo:
    """Information about a registered node type."""
    
    type_name: str
    spec: NodeSpec
    factory: NodeCallable
    category: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    
    def to_dict(self) -> dict[str, Any]:
        """Export to dictionary for UI consumption."""
        return {
            "type_name": self.type_name,
            "category": self.category,
            "description": self.description or self.spec.name,
            "icon": self.icon,
            "inputs": {
                key: {
                    "type_hint": schema.type_hint,
                    "description": schema.description,
                    "required": schema.required,
                    "default": schema.default,
                }
                for key, schema in self.spec.inputs.items()
            },
            "outputs": {
                key: {
                    "type_hint": schema.type_hint,
                    "description": schema.description,
                    "required": schema.required,
                }
                for key, schema in self.spec.outputs.items()
            },
            "config_schema": self.spec.config_schema or {},
            "policy": {
                "retry_count": self.spec.policy.retry_count if self.spec.policy else 0,
                "timeout_seconds": self.spec.policy.timeout_seconds if self.spec.policy else None,
                "idempotent": self.spec.policy.idempotent if self.spec.policy else False,
            }
        }


class NodeRegistry:
    """
    Registry for node types available to the UI.
    
    Developers register node implementations, UI queries available types,
    and engine instantiates nodes dynamically.
    """
    
    def __init__(self) -> None:
        """Initialize empty registry."""
        self._types: dict[str, NodeTypeInfo] = {}
    
    def register(
        self,
        type_name: str,
        spec: NodeSpec,
        factory: NodeCallable,
        category: Optional[str] = None,
        description: Optional[str] = None,
        icon: Optional[str] = None,
    ) -> None:
        """
        Register a node type.
        
        Args:
            type_name: Unique identifier for this node type (e.g., "http_request")
            spec: NodeSpec defining inputs/outputs/policy
            factory: Callable that implements the node logic
            category: Optional category for UI grouping (e.g., "HTTP", "Data", "AI")
            description: Human-readable description
            icon: Optional icon name/URL for UI
            
        Raises:
            ValidationError: If type_name already registered
            
        Example:
            >>> registry.register(
            ...     "http_request",
            ...     http_spec,
            ...     http_request_impl,
            ...     category="HTTP",
            ...     description="Make HTTP requests"
            ... )
        """
        if type_name in self._types:
            raise ValidationError(f"Node type '{type_name}' already registered")
        
        self._types[type_name] = NodeTypeInfo(
            type_name=type_name,
            spec=spec,
            factory=factory,
            category=category,
            description=description,
            icon=icon,
        )
    
    def register_from_decorator(
        self,
        type_name: str,
        category: Optional[str] = None,
        description: Optional[str] = None,
        icon: Optional[str] = None,
    ) -> Callable[[NodeCallable], NodeCallable]:
        """
        Decorator to register a node type.
        
        Args:
            type_name: Unique identifier for this node type
            category: Optional category for UI grouping
            description: Human-readable description
            icon: Optional icon name/URL for UI
            
        Returns:
            Decorator function
            
        Example:
            >>> @registry.register_from_decorator("http_request", category="HTTP")
            ... @node_spec(name="http", inputs={...}, outputs={...})
            ... async def http_request(inputs, config, ctx):
            ...     return {"response": ...}
        """
        def decorator(func: NodeCallable) -> NodeCallable:
            # Get spec from function (should have been decorated with @node_spec)
            if not hasattr(func, "__node_spec__"):
                raise ValidationError(
                    f"Function must be decorated with @node_spec before registering "
                    f"as node type '{type_name}'"
                )
            
            spec = func.__node_spec__  # type: ignore
            
            self.register(
                type_name=type_name,
                spec=spec,
                factory=func,
                category=category,
                description=description,
                icon=icon,
            )
            
            return func
        
        return decorator
    
    def get(self, type_name: str) -> NodeTypeInfo:
        """
        Get node type info.
        
        Args:
            type_name: Node type identifier
            
        Returns:
            NodeTypeInfo for the type
            
        Raises:
            ValidationError: If type not registered
        """
        if type_name not in self._types:
            raise ValidationError(f"Unknown node type: '{type_name}'")
        return self._types[type_name]
    
    def create_node(
        self,
        node_id: str,
        type_name: str,
        config: Optional[dict[str, Any]] = None,
    ) -> Node:
        """
        Create a node instance from a registered type.
        
        Args:
            node_id: Unique ID for this node instance
            type_name: Registered node type name
            config: Configuration values for this instance
            
        Returns:
            Node instance ready for graph
            
        Raises:
            ValidationError: If type not registered
            
        Example:
            >>> node = registry.create_node(
            ...     "fetch1",
            ...     "http_request",
            ...     {"url": "https://api.example.com", "timeout": 30}
            ... )
        """
        type_info = self.get(type_name)
        
        return Node(
            node_id=node_id,
            spec=type_info.spec,
            runtime_callable=type_info.factory,
            config=config or {},
        )
    
    def list_types(self, category: Optional[str] = None) -> list[dict[str, Any]]:
        """
        List all registered node types for UI consumption.
        
        Args:
            category: Optional filter by category
            
        Returns:
            List of node type definitions
            
        Example:
            >>> types = registry.list_types(category="HTTP")
            >>> # UI can render these as draggable palette items
        """
        types = self._types.values()
        
        if category:
            types = [t for t in types if t.category == category]
        
        return [t.to_dict() for t in types]
    
    def list_categories(self) -> list[str]:
        """
        List all categories for UI navigation.
        
        Returns:
            Sorted list of unique categories
        """
        categories = {t.category for t in self._types.values() if t.category}
        return sorted(categories)

