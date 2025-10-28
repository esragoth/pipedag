"""
Validation logic for graphs, schemas, and configurations.
"""

from typing import Any, Optional, Protocol

from pipedag.errors import GraphError, ValidationError
from pipedag.types import Edge, FieldPath, PortSchema


class PortCompatibilityMatcher(Protocol):
    """Protocol for pluggable port type compatibility checkers."""
    
    def are_compatible(
        self,
        src_port: PortSchema,
        dst_port: PortSchema,
        src_path: Optional[FieldPath] = None
    ) -> bool:
        """Check if source port can connect to destination port."""
        ...


class DefaultCompatibilityMatcher:
    """
    Default implementation of port compatibility checking.
    
    MVP: Check that both have type hints and they're not obviously incompatible.
    """
    
    def are_compatible(
        self,
        src_port: PortSchema,
        dst_port: PortSchema,
        src_path: Optional[FieldPath] = None
    ) -> bool:
        """
        Check basic type compatibility.
        
        Args:
            src_port: Source output port schema
            dst_port: Destination input port schema
            src_path: Optional field path if accessing nested data
            
        Returns:
            True if ports appear compatible
        """
        # If no type hints, assume compatible
        if not src_port.type_hint or not dst_port.type_hint:
            return True
        
        src_type = src_port.type_hint.lower()
        dst_type = dst_port.type_hint.lower()
        
        # Exact match
        if src_type == dst_type:
            return True
        
        # Any accepts anything
        if dst_type == "any":
            return True
        
        # Check for obvious incompatibilities
        numeric_types = {"int", "integer", "float", "number"}
        string_types = {"str", "string"}
        
        if src_type in numeric_types and dst_type in string_types:
            return False
        if src_type in string_types and dst_type in numeric_types:
            return False
        
        # Otherwise assume compatible (permissive for MVP)
        return True


class GraphValidator:
    """
    Validates graph structure and connectivity.
    """
    
    def __init__(
        self,
        compatibility_matcher: Optional[PortCompatibilityMatcher] = None
    ) -> None:
        """
        Initialize validator.
        
        Args:
            compatibility_matcher: Custom port compatibility checker
        """
        self.compatibility_matcher = compatibility_matcher or DefaultCompatibilityMatcher()
    
    def validate_graph(
        self,
        nodes: dict[str, Any],  # Dict[str, Node]
        edges: list[Edge]
    ) -> None:
        """
        Validate entire graph structure.
        
        Args:
            nodes: Dictionary of node_id -> Node
            edges: List of edges
            
        Raises:
            GraphError: If graph structure is invalid
            ValidationError: If validation fails
        """
        self._validate_acyclic(nodes, edges)
        self._validate_ports_exist(nodes, edges)
        self._validate_single_input_source(edges)
        self._validate_port_compatibility(nodes, edges)
    
    def _validate_acyclic(
        self,
        nodes: dict[str, Any],
        edges: list[Edge]
    ) -> None:
        """
        Ensure graph is acyclic using topological sort.
        
        Args:
            nodes: Dictionary of node_id -> Node
            edges: List of edges
            
        Raises:
            GraphError: If cycle detected
        """
        # Build adjacency list
        adjacency: dict[str, list[str]] = {node_id: [] for node_id in nodes}
        in_degree: dict[str, int] = {node_id: 0 for node_id in nodes}
        
        for edge in edges:
            adjacency[edge.src_node_id].append(edge.dst_node_id)
            in_degree[edge.dst_node_id] += 1
        
        # Kahn's algorithm for cycle detection
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        visited_count = 0
        
        while queue:
            current = queue.pop(0)
            visited_count += 1
            
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if visited_count != len(nodes):
            raise GraphError("Graph contains a cycle")
    
    def _validate_ports_exist(
        self,
        nodes: dict[str, Any],
        edges: list[Edge]
    ) -> None:
        """
        Validate that all referenced ports exist in node specs.
        
        Args:
            nodes: Dictionary of node_id -> Node
            edges: List of edges
            
        Raises:
            ValidationError: If port doesn't exist
        """
        for edge in edges:
            # Check source node and port
            if edge.src_node_id not in nodes:
                raise ValidationError(
                    f"Edge references unknown source node: {edge.src_node_id}"
                )
            
            src_node = nodes[edge.src_node_id]
            src_port = edge.src_field_path.port
            
            if src_port not in src_node.spec.outputs:
                raise ValidationError(
                    f"Edge references unknown output port '{src_port}' "
                    f"on node '{edge.src_node_id}'"
                )
            
            # Validate nested field path against schema if available
            if edge.src_field_path.is_nested():
                port_schema = src_node.spec.outputs[src_port]
                edge.src_field_path.validate_against_schema(port_schema)
            
            # Check destination node and port
            if edge.dst_node_id not in nodes:
                raise ValidationError(
                    f"Edge references unknown destination node: {edge.dst_node_id}"
                )
            
            dst_node = nodes[edge.dst_node_id]
            if edge.dst_input_key not in dst_node.spec.inputs:
                raise ValidationError(
                    f"Edge references unknown input port '{edge.dst_input_key}' "
                    f"on node '{edge.dst_node_id}'"
                )
    
    def _validate_single_input_source(self, edges: list[Edge]) -> None:
        """
        Ensure each input receives at most one edge (fan-in requires aggregator).
        
        Args:
            edges: List of edges
            
        Raises:
            ValidationError: If input receives multiple edges
        """
        input_sources: dict[tuple[str, str], str] = {}  # (node_id, input_key) -> src_node_id
        
        for edge in edges:
            key = (edge.dst_node_id, edge.dst_input_key)
            if key in input_sources:
                raise ValidationError(
                    f"Input '{edge.dst_input_key}' on node '{edge.dst_node_id}' "
                    f"receives multiple edges. Use an aggregator node for fan-in."
                )
            input_sources[key] = edge.src_node_id
    
    def _validate_port_compatibility(
        self,
        nodes: dict[str, Any],
        edges: list[Edge]
    ) -> None:
        """
        Validate type compatibility between connected ports.
        
        Args:
            nodes: Dictionary of node_id -> Node
            edges: List of edges
            
        Raises:
            ValidationError: If ports are incompatible
        """
        for edge in edges:
            src_node = nodes[edge.src_node_id]
            dst_node = nodes[edge.dst_node_id]
            
            src_port = edge.src_field_path.port
            src_port_schema = src_node.spec.outputs[src_port]
            dst_port_schema = dst_node.spec.inputs[edge.dst_input_key]
            
            if not self.compatibility_matcher.are_compatible(
                src_port_schema,
                dst_port_schema,
                edge.src_field_path if edge.src_field_path.is_nested() else None
            ):
                raise ValidationError(
                    f"Incompatible port types: {edge.src_node_id}.{edge.src_output_path} "
                    f"(type: {src_port_schema.type_hint}) -> "
                    f"{edge.dst_node_id}.{edge.dst_input_key} "
                    f"(type: {dst_port_schema.type_hint})"
                )


def validate_payload_size(
    data: Any,
    max_bytes: int,
    port_name: str,
    node_id: str
) -> None:
    """
    Validate payload size against max_bytes constraint.
    
    Args:
        data: Data to check
        max_bytes: Maximum allowed size
        port_name: Name of the port
        node_id: ID of the node
        
    Raises:
        ValidationError: If payload exceeds limit
    """
    if isinstance(data, (str, bytes)):
        size = len(data)
        if size > max_bytes:
            raise ValidationError(
                f"Payload for port '{port_name}' on node '{node_id}' "
                f"({size} bytes) exceeds max_bytes ({max_bytes})",
                node_id=node_id
            )

