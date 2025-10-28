"""Tests for validation logic."""

import pytest

from pipedag.context import NodeContext
from pipedag.errors import GraphError, ValidationError
from pipedag.graph import Graph
from pipedag.node import Node
from pipedag.types import Edge, NodeSpec, PortSchema
from pipedag.validation import DefaultCompatibilityMatcher, GraphValidator


def create_test_node(node_id: str, inputs: dict, outputs: dict) -> Node:
    """Helper to create a test node."""
    spec = NodeSpec(
        name=node_id,
        inputs={k: PortSchema(**v) if isinstance(v, dict) else v for k, v in inputs.items()},
        outputs={k: PortSchema(**v) if isinstance(v, dict) else v for k, v in outputs.items()},
    )
    
    def func(inp: dict, config: dict, ctx: NodeContext) -> dict:
        return {}
    
    return Node(node_id=node_id, spec=spec, runtime_callable=func, config={})


class TestDefaultCompatibilityMatcher:
    """Test default port compatibility matcher."""
    
    def test_exact_match(self) -> None:
        """Test exact type match."""
        matcher = DefaultCompatibilityMatcher()
        src = PortSchema(type_hint="str")
        dst = PortSchema(type_hint="str")
        
        assert matcher.are_compatible(src, dst) is True
    
    def test_no_type_hints(self) -> None:
        """Test compatibility when no type hints present."""
        matcher = DefaultCompatibilityMatcher()
        src = PortSchema()
        dst = PortSchema()
        
        assert matcher.are_compatible(src, dst) is True
    
    def test_any_destination(self) -> None:
        """Test that 'any' destination accepts anything."""
        matcher = DefaultCompatibilityMatcher()
        src = PortSchema(type_hint="str")
        dst = PortSchema(type_hint="any")
        
        assert matcher.are_compatible(src, dst) is True
    
    def test_numeric_to_string_incompatible(self) -> None:
        """Test that numeric to string is incompatible."""
        matcher = DefaultCompatibilityMatcher()
        src = PortSchema(type_hint="int")
        dst = PortSchema(type_hint="str")
        
        assert matcher.are_compatible(src, dst) is False
    
    def test_string_to_numeric_incompatible(self) -> None:
        """Test that string to numeric is incompatible."""
        matcher = DefaultCompatibilityMatcher()
        src = PortSchema(type_hint="str")
        dst = PortSchema(type_hint="int")
        
        assert matcher.are_compatible(src, dst) is False


class TestGraphValidator:
    """Test graph validator."""
    
    def test_validate_acyclic_simple(self) -> None:
        """Test validating simple acyclic graph."""
        validator = GraphValidator()
        
        node_a = create_test_node("a", {}, {"out": {}})
        node_b = create_test_node("b", {"in": {}}, {})
        nodes = {"a": node_a, "b": node_b}
        
        edges = [Edge("a", "out", "b", "in")]
        
        # Should not raise
        validator.validate_graph(nodes, edges)
    
    def test_validate_cycle_detection(self) -> None:
        """Test cycle detection."""
        validator = GraphValidator()
        
        node_a = create_test_node("a", {"in": {}}, {"out": {}})
        node_b = create_test_node("b", {"in": {}}, {"out": {}})
        nodes = {"a": node_a, "b": node_b}
        
        edges = [
            Edge("a", "out", "b", "in"),
            Edge("b", "out", "a", "in"),
        ]
        
        with pytest.raises(GraphError, match="cycle"):
            validator.validate_graph(nodes, edges)
    
    def test_validate_unknown_port(self) -> None:
        """Test error on unknown port."""
        validator = GraphValidator()
        
        node_a = create_test_node("a", {}, {"out": {}})
        node_b = create_test_node("b", {"in": {}}, {})
        nodes = {"a": node_a, "b": node_b}
        
        # Reference non-existent output port
        edges = [Edge("a", "missing", "b", "in")]
        
        with pytest.raises(ValidationError, match="unknown output port"):
            validator.validate_graph(nodes, edges)
    
    def test_validate_single_input_source(self) -> None:
        """Test that each input can only have one source."""
        validator = GraphValidator()
        
        node_a = create_test_node("a", {}, {"out": {}})
        node_b = create_test_node("b", {}, {"out": {}})
        node_c = create_test_node("c", {"in": {}}, {})
        nodes = {"a": node_a, "b": node_b, "c": node_c}
        
        # Two edges to same input
        edges = [
            Edge("a", "out", "c", "in"),
            Edge("b", "out", "c", "in"),
        ]
        
        with pytest.raises(ValidationError, match="receives multiple edges"):
            validator.validate_graph(nodes, edges)
    
    def test_validate_port_compatibility(self) -> None:
        """Test port type compatibility validation."""
        validator = GraphValidator()
        
        node_a = create_test_node("a", {}, {"out": {"type_hint": "int"}})
        node_b = create_test_node("b", {"in": {"type_hint": "str"}}, {})
        nodes = {"a": node_a, "b": node_b}
        
        edges = [Edge("a", "out", "b", "in")]
        
        with pytest.raises(ValidationError, match="Incompatible port types"):
            validator.validate_graph(nodes, edges)
    
    def test_diamond_pattern(self) -> None:
        """Test diamond pattern is valid (fan-out then fan-in with aggregator)."""
        validator = GraphValidator()
        
        node_a = create_test_node("a", {}, {"out": {}})
        node_b = create_test_node("b", {"in": {}}, {"out": {}})
        node_c = create_test_node("c", {"in": {}}, {"out": {}})
        node_d = create_test_node("d", {"in1": {}, "in2": {}}, {})
        
        nodes = {"a": node_a, "b": node_b, "c": node_c, "d": node_d}
        
        edges = [
            Edge("a", "out", "b", "in"),
            Edge("a", "out", "c", "in"),
            Edge("b", "out", "d", "in1"),
            Edge("c", "out", "d", "in2"),
        ]
        
        # Should validate successfully
        validator.validate_graph(nodes, edges)

