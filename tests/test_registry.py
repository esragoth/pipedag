"""
Tests for NodeRegistry.

Covers node registration, discovery, and dynamic instantiation.
"""

import pytest
from pipedag.errors import ValidationError
from pipedag.node import NodeContext, node_spec
from pipedag.registry import NodeRegistry, NodeTypeInfo
from pipedag.types import NodeSpec, PortSchema, PolicySpec


# Sample node implementations for testing
def simple_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Simple test node."""
    return {"result": inputs.get("x", 0) + 1}


async def async_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Async test node."""
    return {"result": inputs.get("x", 0) * 2}


@node_spec(
    name="decorated_node",
    inputs={"a": PortSchema(type_hint="int")},
    outputs={"b": PortSchema(type_hint="int")},
)
def decorated_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Node with decorator."""
    return {"b": inputs["a"] + config.get("offset", 0)}


class TestNodeRegistry:
    """Test suite for NodeRegistry."""
    
    def test_initialization(self):
        """Test registry initialization."""
        registry = NodeRegistry()
        assert len(registry._types) == 0
    
    def test_register_basic(self):
        """Test basic node registration."""
        registry = NodeRegistry()
        
        spec = NodeSpec(
            name="test_node",
            inputs={"x": PortSchema()},
            outputs={"result": PortSchema()},
        )
        
        registry.register("test_type", spec, simple_node)
        
        # Should be registered
        assert "test_type" in registry._types
        
        # Get type info
        info = registry.get("test_type")
        assert info.type_name == "test_type"
        assert info.spec == spec
        assert info.factory == simple_node
        assert info.category is None
    
    def test_register_with_metadata(self):
        """Test registration with full metadata."""
        registry = NodeRegistry()
        
        spec = NodeSpec(
            name="http_request",
            inputs={"url": PortSchema()},
            outputs={"response": PortSchema()},
        )
        
        registry.register(
            "http_request",
            spec,
            simple_node,
            category="HTTP",
            description="Make HTTP requests",
            icon="globe",
        )
        
        info = registry.get("http_request")
        assert info.category == "HTTP"
        assert info.description == "Make HTTP requests"
        assert info.icon == "globe"
    
    def test_register_duplicate_fails(self):
        """Test that registering duplicate type fails."""
        registry = NodeRegistry()
        
        spec = NodeSpec(
            name="test",
            inputs={},
            outputs={},
        )
        
        registry.register("test_type", spec, simple_node)
        
        # Try to register again
        with pytest.raises(ValidationError, match="already registered"):
            registry.register("test_type", spec, simple_node)
    
    def test_get_unknown_type_fails(self):
        """Test getting unknown type raises error."""
        registry = NodeRegistry()
        
        with pytest.raises(ValidationError, match="Unknown node type"):
            registry.get("nonexistent")
    
    def test_decorator_registration(self):
        """Test decorator-based registration."""
        registry = NodeRegistry()
        
        # Register using decorator
        decorated = registry.register_from_decorator(
            "decorated_type",
            category="Test",
            description="Test node",
        )(decorated_node)
        
        # Should still be the same function
        assert decorated == decorated_node
        
        # Should be registered
        info = registry.get("decorated_type")
        assert info.type_name == "decorated_type"
        assert info.category == "Test"
        assert info.spec.name == "decorated_node"
    
    def test_decorator_without_node_spec_fails(self):
        """Test decorator fails if function not decorated with @node_spec."""
        registry = NodeRegistry()
        
        def undecorated(inputs, config, ctx):
            return {}
        
        with pytest.raises(ValidationError, match="must be decorated with @node_spec"):
            registry.register_from_decorator("test")(undecorated)
    
    def test_create_node(self):
        """Test creating node instance from registry."""
        registry = NodeRegistry()
        
        spec = NodeSpec(
            name="adder",
            inputs={"x": PortSchema(), "y": PortSchema()},
            outputs={"sum": PortSchema()},
        )
        
        registry.register("add", spec, simple_node)
        
        # Create node
        node = registry.create_node("add_1", "add", {"offset": 5})
        
        assert node.node_id == "add_1"
        assert node.spec == spec
        assert node.config == {"offset": 5}
        assert node.runtime_callable == simple_node
    
    def test_create_node_unknown_type_fails(self):
        """Test creating node with unknown type fails."""
        registry = NodeRegistry()
        
        with pytest.raises(ValidationError, match="Unknown node type"):
            registry.create_node("node1", "unknown_type")
    
    def test_list_types_all(self):
        """Test listing all registered types."""
        registry = NodeRegistry()
        
        spec1 = NodeSpec(name="n1", inputs={}, outputs={})
        spec2 = NodeSpec(name="n2", inputs={}, outputs={})
        
        registry.register("type1", spec1, simple_node, category="Math")
        registry.register("type2", spec2, async_node, category="HTTP")
        
        types = registry.list_types()
        
        assert len(types) == 2
        assert any(t["type_name"] == "type1" for t in types)
        assert any(t["type_name"] == "type2" for t in types)
    
    def test_list_types_filtered(self):
        """Test listing types filtered by category."""
        registry = NodeRegistry()
        
        spec1 = NodeSpec(name="n1", inputs={}, outputs={})
        spec2 = NodeSpec(name="n2", inputs={}, outputs={})
        spec3 = NodeSpec(name="n3", inputs={}, outputs={})
        
        registry.register("add", spec1, simple_node, category="Math")
        registry.register("multiply", spec2, simple_node, category="Math")
        registry.register("fetch", spec3, async_node, category="HTTP")
        
        math_types = registry.list_types(category="Math")
        
        assert len(math_types) == 2
        assert all(t["category"] == "Math" for t in math_types)
    
    def test_list_categories(self):
        """Test listing all categories."""
        registry = NodeRegistry()
        
        spec = NodeSpec(name="n", inputs={}, outputs={})
        
        registry.register("t1", spec, simple_node, category="Math")
        registry.register("t2", spec, simple_node, category="HTTP")
        registry.register("t3", spec, simple_node, category="Math")  # duplicate category
        registry.register("t4", spec, simple_node)  # no category
        
        categories = registry.list_categories()
        
        assert set(categories) == {"HTTP", "Math"}
        assert categories == sorted(categories)  # Should be sorted
    
    def test_node_type_info_to_dict(self):
        """Test NodeTypeInfo serialization to dict."""
        spec = NodeSpec(
            name="test",
            inputs={
                "x": PortSchema(
                    type_hint="int",
                    description="Input value",
                    required=True,
                    default=5,
                )
            },
            outputs={
                "result": PortSchema(
                    type_hint="int",
                    description="Output value",
                    required=True,
                )
            },
            policy=PolicySpec(
                retry_count=3,
                timeout_seconds=10.0,
            ),
        )
        
        info = NodeTypeInfo(
            type_name="test_type",
            spec=spec,
            factory=simple_node,
            category="Test",
            description="Test description",
            icon="test-icon",
        )
        
        data = info.to_dict()
        
        assert data["type_name"] == "test_type"
        assert data["category"] == "Test"
        assert data["description"] == "Test description"
        assert data["icon"] == "test-icon"
        
        # Check inputs
        assert "x" in data["inputs"]
        assert data["inputs"]["x"]["type_hint"] == "int"
        assert data["inputs"]["x"]["description"] == "Input value"
        assert data["inputs"]["x"]["required"] is True
        assert data["inputs"]["x"]["default"] == 5
        
        # Check outputs
        assert "result" in data["outputs"]
        assert data["outputs"]["result"]["type_hint"] == "int"
        assert data["outputs"]["result"]["description"] == "Output value"
        
        # Check policy
        assert data["policy"]["retry_count"] == 3
        assert data["policy"]["timeout_seconds"] == 10.0
    
    def test_multiple_registrations(self):
        """Test registering multiple node types."""
        registry = NodeRegistry()
        
        # Register 5 different types
        for i in range(5):
            spec = NodeSpec(
                name=f"node_{i}",
                inputs={"x": PortSchema()},
                outputs={"y": PortSchema()},
            )
            registry.register(f"type_{i}", spec, simple_node)
        
        # All should be registered
        for i in range(5):
            assert f"type_{i}" in registry._types
        
        types = registry.list_types()
        assert len(types) == 5
    
    def test_async_node_registration(self):
        """Test registering async nodes."""
        registry = NodeRegistry()
        
        spec = NodeSpec(
            name="async_test",
            inputs={"x": PortSchema()},
            outputs={"result": PortSchema()},
        )
        
        registry.register("async_type", spec, async_node)
        
        # Should work the same
        info = registry.get("async_type")
        assert info.factory == async_node
        
        # Can create nodes
        node = registry.create_node("async_1", "async_type")
        assert node.node_id == "async_1"

