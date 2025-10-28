"""Tests for graph functionality."""

import json

import pytest

from pipedag.context import NodeContext
from pipedag.errors import GraphError, ValidationError
from pipedag.graph import Graph
from pipedag.node import Node
from pipedag.types import NodeSpec, PortSchema


def create_simple_node(node_id: str, inputs: dict, outputs: dict) -> Node:
    """Helper to create a simple node."""
    spec = NodeSpec(
        name=node_id,
        inputs={k: PortSchema(**v) if isinstance(v, dict) else v for k, v in inputs.items()},
        outputs={k: PortSchema(**v) if isinstance(v, dict) else v for k, v in outputs.items()},
    )
    
    def func(inp: dict, config: dict, ctx: NodeContext) -> dict:
        return {k: inp.get(k, None) for k in outputs.keys()}
    
    return Node(node_id=node_id, spec=spec, runtime_callable=func, config={})


class TestGraph:
    """Test Graph class."""
    
    def test_add_node(self) -> None:
        """Test adding nodes to graph."""
        graph = Graph()
        node = create_simple_node("node1", {}, {"result": {}})
        
        graph.add_node(node)
        
        assert "node1" in graph.nodes
        assert graph.nodes["node1"] == node
    
    def test_add_duplicate_node(self) -> None:
        """Test error on duplicate node ID."""
        graph = Graph()
        node1 = create_simple_node("node1", {}, {"result": {}})
        node2 = create_simple_node("node1", {}, {"result": {}})
        
        graph.add_node(node1)
        
        with pytest.raises(GraphError, match="already exists"):
            graph.add_node(node2)
    
    def test_connect_nodes(self) -> None:
        """Test connecting nodes."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {}})
        node_b = create_simple_node("b", {"in": {}}, {"result": {}})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "out", "b", "in")
        
        assert len(graph.edges) == 1
        assert graph.edges[0].src_node_id == "a"
        assert graph.edges[0].dst_node_id == "b"
    
    def test_connect_nonexistent_nodes(self) -> None:
        """Test error on connecting non-existent nodes."""
        graph = Graph()
        
        with pytest.raises(GraphError, match="Source node .* not found"):
            graph.connect("a", "out", "b", "in")
    
    def test_nested_path_connection(self) -> None:
        """Test connecting with nested field path."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"data": {}})
        node_b = create_simple_node("b", {"email": {}}, {})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "data.user.email", "b", "email")
        
        edge = graph.edges[0]
        assert edge.src_output_path == "data.user.email"
        assert edge.src_field_path.port == "data"
        assert edge.src_field_path.path == ["user", "email"]
    
    def test_validate_empty_graph(self) -> None:
        """Test error on validating empty graph."""
        graph = Graph()
        
        with pytest.raises(GraphError, match="Graph has no nodes"):
            graph.validate()
    
    def test_validate_simple_graph(self) -> None:
        """Test validating a simple valid graph."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {}})
        node_b = create_simple_node("b", {"in": {}}, {"result": {}})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "out", "b", "in")
        
        graph.validate()
        assert graph.is_validated()
    
    def test_detect_cycle(self) -> None:
        """Test cycle detection."""
        graph = Graph()
        node_a = create_simple_node("a", {"in": {}}, {"out": {}})
        node_b = create_simple_node("b", {"in": {}}, {"out": {}})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "out", "b", "in")
        graph.connect("b", "out", "a", "in")  # Creates cycle
        
        with pytest.raises(GraphError, match="cycle"):
            graph.validate()
    
    def test_topological_order(self) -> None:
        """Test getting topological order."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {}})
        node_b = create_simple_node("b", {"in": {}}, {"out": {}})
        node_c = create_simple_node("c", {"in": {}}, {"result": {}})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.add_node(node_c)
        graph.connect("a", "out", "b", "in")
        graph.connect("b", "out", "c", "in")
        
        graph.validate()
        order = graph.get_topological_order()
        
        assert order.index("a") < order.index("b")
        assert order.index("b") < order.index("c")
    
    def test_get_node_dependencies(self) -> None:
        """Test getting node dependencies."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {}})
        node_b = create_simple_node("b", {"in": {}}, {"result": {}})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "out", "b", "in")
        
        deps = graph.get_node_dependencies("b")
        assert "in" in deps
        assert len(deps["in"]) == 1
        assert deps["in"][0].src_node_id == "a"
    
    def test_get_node_dependents(self) -> None:
        """Test getting node dependents."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {}})
        node_b = create_simple_node("b", {"in": {}}, {})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "out", "b", "in")
        
        dependents = graph.get_node_dependents("a")
        assert len(dependents) == 1
        assert dependents[0].dst_node_id == "b"
    
    def test_compute_graph_hash(self) -> None:
        """Test computing graph hash."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {}})
        node_b = create_simple_node("b", {"in": {}}, {})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "out", "b", "in")
        
        hash1 = graph.compute_graph_hash()
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # SHA256
        
        # Same graph should produce same hash
        hash2 = graph.compute_graph_hash()
        assert hash1 == hash2
    
    def test_to_dict(self) -> None:
        """Test exporting graph to dict."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {"type_hint": "str"}})
        node_b = create_simple_node("b", {"in": {"type_hint": "str"}}, {})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "out", "b", "in")
        
        graph_dict = graph.to_dict()
        
        assert "nodes" in graph_dict
        assert "edges" in graph_dict
        assert "hash" in graph_dict
        assert len(graph_dict["nodes"]) == 2
        assert len(graph_dict["edges"]) == 1
    
    def test_to_json(self) -> None:
        """Test exporting graph to JSON."""
        graph = Graph()
        node_a = create_simple_node("a", {}, {"out": {}})
        
        graph.add_node(node_a)
        
        json_str = graph.to_json()
        parsed = json.loads(json_str)
        
        assert "nodes" in parsed
        assert len(parsed["nodes"]) == 1
    
    def test_to_definition(self) -> None:
        """Test exporting graph to UI-compatible definition."""
        from pipedag.types import PolicySpec
        
        graph = Graph()
        
        # Create nodes with policies
        spec_a = NodeSpec(
            name="node_a",
            inputs={},
            outputs={"result": PortSchema()},
            policy=PolicySpec(retry_count=3, timeout_seconds=10.0),
        )
        node_a = Node("a", spec_a, lambda i, c, ctx: {"result": 1}, {"key": "value"})
        
        spec_b = NodeSpec(
            name="node_b",
            inputs={"x": PortSchema()},
            outputs={"y": PortSchema()},
        )
        node_b = Node("b", spec_b, lambda i, c, ctx: {"y": i["x"]}, {})
        
        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.connect("a", "result", "b", "x")
        
        definition = graph.to_definition()
        
        # Check structure
        assert definition["version"] == "1.0"
        assert "nodes" in definition
        assert "edges" in definition
        assert "metadata" in definition
        
        # Check nodes
        assert len(definition["nodes"]) == 2
        node_a_def = next(n for n in definition["nodes"] if n["id"] == "a")
        assert node_a_def["type"] == "node_a"
        assert node_a_def["config"] == {"key": "value"}
        assert node_a_def["policy"]["retry_count"] == 3
        assert node_a_def["policy"]["timeout_seconds"] == 10.0
        
        node_b_def = next(n for n in definition["nodes"] if n["id"] == "b")
        # When no policy is set, it still creates a policy dict with defaults
        assert node_b_def["policy"] is not None
        
        # Check edges
        assert len(definition["edges"]) == 1
        edge = definition["edges"][0]
        assert edge["source"]["node"] == "a"
        assert edge["source"]["output"] == "result"
        assert edge["target"]["node"] == "b"
        assert edge["target"]["input"] == "x"
    
    def test_from_definition(self) -> None:
        """Test loading graph from definition."""
        from pipedag.registry import NodeRegistry
        from pipedag.node import node_spec
        
        # Create registry with test nodes
        registry = NodeRegistry()
        
        @node_spec(
            name="source",
            inputs={},
            outputs={"value": PortSchema()},
        )
        def source_node(inputs, config, ctx):
            return {"value": config.get("value", 0)}
        
        @node_spec(
            name="processor",
            inputs={"x": PortSchema()},
            outputs={"result": PortSchema()},
        )
        def processor_node(inputs, config, ctx):
            return {"result": inputs["x"] * 2}
        
        registry.register("source_type", source_node.__node_spec__, source_node)
        registry.register("processor_type", processor_node.__node_spec__, processor_node)
        
        # Create definition
        definition = {
            "version": "1.0",
            "nodes": [
                {"id": "src", "type": "source_type", "config": {"value": 5}},
                {"id": "proc", "type": "processor_type", "config": {}},
            ],
            "edges": [
                {
                    "source": {"node": "src", "output": "value"},
                    "target": {"node": "proc", "input": "x"},
                }
            ],
        }
        
        # Load graph
        graph = Graph.from_definition(definition, registry)
        
        # Check structure
        assert len(graph.nodes) == 2
        assert "src" in graph.nodes
        assert "proc" in graph.nodes
        assert len(graph.edges) == 1
        
        # Check nodes
        assert graph.nodes["src"].config == {"value": 5}
        assert graph.nodes["proc"].config == {}
    
    def test_from_definition_missing_nodes_field(self) -> None:
        """Test error when definition missing nodes field."""
        from pipedag.registry import NodeRegistry
        
        registry = NodeRegistry()
        definition = {"edges": []}
        
        with pytest.raises(GraphError, match="missing 'nodes' field"):
            Graph.from_definition(definition, registry)
    
    def test_from_definition_missing_edges_field(self) -> None:
        """Test error when definition missing edges field."""
        from pipedag.registry import NodeRegistry
        
        registry = NodeRegistry()
        definition = {"nodes": []}
        
        with pytest.raises(GraphError, match="missing 'edges' field"):
            Graph.from_definition(definition, registry)
    
    def test_from_definition_node_missing_id(self) -> None:
        """Test error when node definition missing id."""
        from pipedag.registry import NodeRegistry
        
        registry = NodeRegistry()
        definition = {
            "nodes": [{"type": "some_type", "config": {}}],
            "edges": [],
        }
        
        with pytest.raises(GraphError, match="missing 'id'"):
            Graph.from_definition(definition, registry)
    
    def test_from_definition_node_missing_type(self) -> None:
        """Test error when node definition missing type."""
        from pipedag.registry import NodeRegistry
        
        registry = NodeRegistry()
        definition = {
            "nodes": [{"id": "node1", "config": {}}],
            "edges": [],
        }
        
        with pytest.raises(GraphError, match="missing 'type'"):
            Graph.from_definition(definition, registry)
    
    def test_from_definition_unknown_node_type(self) -> None:
        """Test error when referencing unknown node type."""
        from pipedag.registry import NodeRegistry
        
        registry = NodeRegistry()
        definition = {
            "nodes": [{"id": "node1", "type": "unknown_type", "config": {}}],
            "edges": [],
        }
        
        with pytest.raises(ValidationError, match="Unknown node type"):
            Graph.from_definition(definition, registry)
    
    def test_from_definition_invalid_edge(self) -> None:
        """Test error when edge definition is invalid."""
        from pipedag.registry import NodeRegistry
        from pipedag.node import node_spec
        
        registry = NodeRegistry()
        
        @node_spec(name="test", inputs={}, outputs={"out": PortSchema()})
        def test_node(inputs, config, ctx):
            return {"out": 1}
        
        registry.register("test_type", test_node.__node_spec__, test_node)
        
        definition = {
            "nodes": [{"id": "n1", "type": "test_type", "config": {}}],
            "edges": [
                {
                    "source": {"node": "n1"},  # Missing output
                    "target": {"node": "n2", "input": "in"},
                }
            ],
        }
        
        with pytest.raises(GraphError, match="Invalid edge definition"):
            Graph.from_definition(definition, registry)
    
    def test_from_json(self) -> None:
        """Test loading graph from JSON string."""
        from pipedag.registry import NodeRegistry
        from pipedag.node import node_spec
        
        registry = NodeRegistry()
        
        @node_spec(name="test", inputs={}, outputs={"out": PortSchema()})
        def test_node(inputs, config, ctx):
            return {"out": 1}
        
        registry.register("test_type", test_node.__node_spec__, test_node)
        
        json_str = json.dumps({
            "nodes": [{"id": "n1", "type": "test_type", "config": {}}],
            "edges": [],
        })
        
        graph = Graph.from_json(json_str, registry)
        
        assert len(graph.nodes) == 1
        assert "n1" in graph.nodes
    
    def test_from_file(self, tmp_path) -> None:
        """Test loading graph from file."""
        from pipedag.registry import NodeRegistry
        from pipedag.node import node_spec
        
        registry = NodeRegistry()
        
        @node_spec(name="test", inputs={}, outputs={"out": PortSchema()})
        def test_node(inputs, config, ctx):
            return {"out": 1}
        
        registry.register("test_type", test_node.__node_spec__, test_node)
        
        # Create temp file
        file_path = tmp_path / "pipeline.json"
        definition = {
            "nodes": [{"id": "n1", "type": "test_type", "config": {"x": 5}}],
            "edges": [],
        }
        file_path.write_text(json.dumps(definition))
        
        # Load from file
        graph = Graph.from_file(str(file_path), registry)
        
        assert len(graph.nodes) == 1
        assert "n1" in graph.nodes
        assert graph.nodes["n1"].config == {"x": 5}
    
    def test_roundtrip_to_definition_and_back(self) -> None:
        """Test that graph can be exported and re-imported."""
        from pipedag.registry import NodeRegistry
        from pipedag.node import node_spec
        
        registry = NodeRegistry()
        
        @node_spec(
            name="add",
            inputs={"x": PortSchema(), "y": PortSchema()},
            outputs={"sum": PortSchema()},
        )
        def add_node(inputs, config, ctx):
            return {"sum": inputs["x"] + inputs["y"]}
        
        registry.register("add", add_node.__node_spec__, add_node)
        
        # Create original graph
        graph1 = Graph()
        node1 = registry.create_node("add1", "add", {"offset": 0})
        node2 = registry.create_node("add2", "add", {"offset": 10})
        graph1.add_node(node1)
        graph1.add_node(node2)
        graph1.connect("add1", "sum", "add2", "x")
        
        # Export to definition
        definition = graph1.to_definition()
        
        # Import from definition
        graph2 = Graph.from_definition(definition, registry)
        
        # Should have same structure
        assert len(graph2.nodes) == len(graph1.nodes)
        assert len(graph2.edges) == len(graph1.edges)
        assert set(graph2.nodes.keys()) == set(graph1.nodes.keys())
        
        # Configs should match
        assert graph2.nodes["add1"].config == {"offset": 0}
        assert graph2.nodes["add2"].config == {"offset": 10}

