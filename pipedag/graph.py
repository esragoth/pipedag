"""
DAG construction and validation.
"""

import hashlib
import json
from typing import Any, Optional, TYPE_CHECKING

from pipedag.errors import GraphError, ValidationError
from pipedag.node import Node
from pipedag.types import Edge
from pipedag.validation import GraphValidator, PortCompatibilityMatcher

if TYPE_CHECKING:
    from pipedag.registry import NodeRegistry


class Graph:
    """
    Directed Acyclic Graph representing a pipeline.
    """
    
    def __init__(
        self,
        compatibility_matcher: Optional[PortCompatibilityMatcher] = None
    ) -> None:
        """
        Initialize an empty graph.
        
        Args:
            compatibility_matcher: Custom port compatibility checker
        """
        self.nodes: dict[str, Node] = {}
        self.edges: list[Edge] = []
        self._validator = GraphValidator(compatibility_matcher)
        self._validated = False
    
    def add_node(self, node: Node) -> None:
        """
        Add a node to the graph.
        
        Args:
            node: Node instance to add
            
        Raises:
            GraphError: If node_id already exists
        """
        if node.node_id in self.nodes:
            raise GraphError(f"Node with id '{node.node_id}' already exists")
        
        self.nodes[node.node_id] = node
        self._validated = False
    
    def connect(
        self,
        src_node_id: str,
        src_output_path: str,
        dst_node_id: str,
        dst_input_key: str
    ) -> None:
        """
        Connect two nodes via an edge.
        
        Args:
            src_node_id: Source node ID
            src_output_path: Source output path (supports "port" or "port.field.nested")
            dst_node_id: Destination node ID
            dst_input_key: Destination input key
            
        Raises:
            GraphError: If nodes don't exist
        """
        if src_node_id not in self.nodes:
            raise GraphError(f"Source node '{src_node_id}' not found")
        if dst_node_id not in self.nodes:
            raise GraphError(f"Destination node '{dst_node_id}' not found")
        
        edge = Edge(
            src_node_id=src_node_id,
            src_output_path=src_output_path,
            dst_node_id=dst_node_id,
            dst_input_key=dst_input_key
        )
        
        self.edges.append(edge)
        self._validated = False
    
    def validate(self) -> None:
        """
        Validate the graph structure.
        
        Raises:
            GraphError: If graph structure is invalid
            ValidationError: If validation fails
        """
        if not self.nodes:
            raise GraphError("Graph has no nodes")
        
        self._validator.validate_graph(self.nodes, self.edges)
        self._validated = True
    
    def is_validated(self) -> bool:
        """Check if graph has been validated."""
        return self._validated
    
    def get_topological_order(self) -> list[str]:
        """
        Get nodes in topological order.
        
        Returns:
            List of node IDs in execution order
            
        Raises:
            GraphError: If graph is not validated or contains cycles
        """
        if not self._validated:
            raise GraphError("Graph must be validated before getting topological order")
        
        # Build adjacency list and in-degree map
        adjacency: dict[str, list[str]] = {node_id: [] for node_id in self.nodes}
        in_degree: dict[str, int] = {node_id: 0 for node_id in self.nodes}
        
        for edge in self.edges:
            adjacency[edge.src_node_id].append(edge.dst_node_id)
            in_degree[edge.dst_node_id] += 1
        
        # Kahn's algorithm
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # Sort for deterministic ordering
            queue.sort()
            current = queue.pop(0)
            result.append(current)
            
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        return result
    
    def get_node_dependencies(self, node_id: str) -> dict[str, list[Edge]]:
        """
        Get incoming edges for a node, grouped by input key.
        
        Args:
            node_id: Node to get dependencies for
            
        Returns:
            Dictionary mapping input_key to list of incoming edges
        """
        dependencies: dict[str, list[Edge]] = {}
        
        for edge in self.edges:
            if edge.dst_node_id == node_id:
                if edge.dst_input_key not in dependencies:
                    dependencies[edge.dst_input_key] = []
                dependencies[edge.dst_input_key].append(edge)
        
        return dependencies
    
    def get_node_dependents(self, node_id: str) -> list[Edge]:
        """
        Get outgoing edges from a node.
        
        Args:
            node_id: Node to get dependents for
            
        Returns:
            List of outgoing edges
        """
        return [edge for edge in self.edges if edge.src_node_id == node_id]
    
    def compute_graph_hash(self) -> str:
        """
        Compute a deterministic hash of the graph structure.
        
        Returns:
            SHA256 hash of the graph
        """
        graph_data = {
            "nodes": {
                node_id: {
                    "spec_name": node.spec.name,
                    "inputs": {k: v.type_hint for k, v in node.spec.inputs.items()},
                    "outputs": {k: v.type_hint for k, v in node.spec.outputs.items()},
                    "config": node.config,
                }
                for node_id, node in sorted(self.nodes.items())
            },
            "edges": [
                {
                    "src": edge.src_node_id,
                    "src_path": edge.src_output_path,
                    "dst": edge.dst_node_id,
                    "dst_key": edge.dst_input_key,
                }
                for edge in sorted(self.edges, key=lambda e: (e.src_node_id, e.dst_node_id))
            ]
        }
        
        graph_json = json.dumps(graph_data, sort_keys=True)
        return hashlib.sha256(graph_json.encode()).hexdigest()
    
    def to_dict(self) -> dict[str, Any]:
        """
        Export graph to dictionary for JSON serialization (UI rendering).
        
        Returns:
            Dictionary representation of the graph
        """
        return {
            "nodes": [
                {
                    "id": node_id,
                    "name": node.spec.name,
                    "inputs": {
                        k: {
                            "type_hint": v.type_hint,
                            "required": v.required,
                            "description": v.description,
                        }
                        for k, v in node.spec.inputs.items()
                    },
                    "outputs": {
                        k: {
                            "type_hint": v.type_hint,
                            "required": v.required,
                            "description": v.description,
                        }
                        for k, v in node.spec.outputs.items()
                    },
                    "config": node.config,
                    "policy": {
                        "retry_count": node.spec.policy.retry_count,
                        "timeout_seconds": node.spec.policy.timeout_seconds,
                        "concurrency_class": node.spec.policy.concurrency_class,
                        "idempotent": node.spec.policy.idempotent,
                    } if node.spec.policy else None,
                }
                for node_id, node in self.nodes.items()
            ],
            "edges": [
                {
                    "source_node": edge.src_node_id,
                    "source_path": edge.src_output_path,
                    "target_node": edge.dst_node_id,
                    "target_input": edge.dst_input_key,
                }
                for edge in self.edges
            ],
            "hash": self.compute_graph_hash(),
        }
    
    def to_json(self) -> str:
        """
        Export graph to JSON string.
        
        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=2)
    
    def to_definition(self) -> dict[str, Any]:
        """
        Export graph to a UI-compatible definition that can be re-loaded.
        
        This includes node types and configs needed for reconstruction.
        
        Returns:
            Dictionary with nodes, edges, and metadata
            
        Example:
            >>> definition = graph.to_definition()
            >>> # Save to file
            >>> with open("pipeline.json", "w") as f:
            ...     json.dump(definition, f)
        """
        return {
            "version": "1.0",
            "nodes": [
                {
                    "id": node_id,
                    "type": node.spec.name,  # Node type for registry lookup
                    "config": node.config,
                    "policy": {
                        "retry_count": node.spec.policy.retry_count,
                        "timeout_seconds": node.spec.policy.timeout_seconds,
                        "concurrency_class": node.spec.policy.concurrency_class,
                        "idempotent": node.spec.policy.idempotent,
                    } if node.spec.policy else None,
                }
                for node_id, node in self.nodes.items()
            ],
            "edges": [
                {
                    "source": {
                        "node": edge.src_node_id,
                        "output": edge.src_output_path,
                    },
                    "target": {
                        "node": edge.dst_node_id,
                        "input": edge.dst_input_key,
                    }
                }
                for edge in self.edges
            ],
            "metadata": {
                "hash": self.compute_graph_hash(),
            }
        }
    
    @classmethod
    def from_definition(
        cls,
        definition: dict[str, Any],
        registry: "NodeRegistry",
        compatibility_matcher: Optional[PortCompatibilityMatcher] = None,
    ) -> "Graph":
        """
        Load a graph from a definition (e.g., from UI).
        
        Args:
            definition: Graph definition dict (from to_definition())
            registry: NodeRegistry to resolve node types
            compatibility_matcher: Optional custom port matcher
            
        Returns:
            Constructed and validated Graph
            
        Raises:
            GraphError: If definition is invalid
            ValidationError: If nodes/edges are invalid
            
        Example:
            >>> # Load from file saved by UI
            >>> with open("pipeline.json") as f:
            ...     definition = json.load(f)
            >>> graph = Graph.from_definition(definition, registry)
            >>> result = await engine.run(graph)
        """
        graph = cls(compatibility_matcher=compatibility_matcher)
        
        # Validate definition structure
        if "nodes" not in definition:
            raise GraphError("Definition missing 'nodes' field")
        if "edges" not in definition:
            raise GraphError("Definition missing 'edges' field")
        
        # Create nodes
        for node_def in definition["nodes"]:
            node_id = node_def.get("id")
            node_type = node_def.get("type")
            node_config = node_def.get("config", {})
            
            if not node_id:
                raise GraphError("Node definition missing 'id'")
            if not node_type:
                raise GraphError(f"Node '{node_id}' missing 'type'")
            
            # Create node from registry
            node = registry.create_node(node_id, node_type, node_config)
            graph.add_node(node)
        
        # Create edges
        for edge_def in definition["edges"]:
            source = edge_def.get("source", {})
            target = edge_def.get("target", {})
            
            src_node = source.get("node")
            src_output = source.get("output")
            dst_node = target.get("node")
            dst_input = target.get("input")
            
            if not all([src_node, src_output, dst_node, dst_input]):
                raise GraphError(f"Invalid edge definition: {edge_def}")
            
            graph.connect(src_node, src_output, dst_node, dst_input)
        
        # Validate the constructed graph
        graph.validate()
        
        return graph
    
    @classmethod
    def from_json(
        cls,
        json_str: str,
        registry: "NodeRegistry",
        compatibility_matcher: Optional[PortCompatibilityMatcher] = None,
    ) -> "Graph":
        """
        Load a graph from JSON string.
        
        Args:
            json_str: JSON string containing graph definition
            registry: NodeRegistry to resolve node types
            compatibility_matcher: Optional custom port matcher
            
        Returns:
            Constructed and validated Graph
            
        Example:
            >>> json_str = open("pipeline.json").read()
            >>> graph = Graph.from_json(json_str, registry)
        """
        definition = json.loads(json_str)
        return cls.from_definition(definition, registry, compatibility_matcher)
    
    @classmethod
    def from_file(
        cls,
        filepath: str,
        registry: "NodeRegistry",
        compatibility_matcher: Optional[PortCompatibilityMatcher] = None,
    ) -> "Graph":
        """
        Load a graph from a file.
        
        Args:
            filepath: Path to JSON file
            registry: NodeRegistry to resolve node types
            compatibility_matcher: Optional custom port matcher
            
        Returns:
            Constructed and validated Graph
            
        Example:
            >>> graph = Graph.from_file("pipeline.json", registry)
        """
        with open(filepath, "r") as f:
            return cls.from_json(f.read(), registry, compatibility_matcher)

