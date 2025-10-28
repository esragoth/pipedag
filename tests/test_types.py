"""Tests for core type system."""

import pytest

from pipedag.errors import ValidationError
from pipedag.types import (
    Edge,
    FieldPath,
    MapConfig,
    NodeSpec,
    PolicySpec,
    PortSchema,
    RunStatus,
    PYDANTIC_AVAILABLE,
)


class TestPortSchema:
    """Test PortSchema functionality."""
    
    def test_basic_port_schema(self) -> None:
        """Test creating a basic port schema."""
        port = PortSchema(
            description="Test port",
            type_hint="str",
            required=True,
        )
        
        assert port.description == "Test port"
        assert port.type_hint == "str"
        assert port.required is True
        assert port.default is None
    
    def test_port_schema_with_default(self) -> None:
        """Test port schema with default value."""
        port = PortSchema(
            type_hint="int",
            required=False,
            default=42,
        )
        
        assert port.required is False
        assert port.default == 42
    
    def test_port_schema_with_max_bytes(self) -> None:
        """Test port schema with max_bytes constraint."""
        port = PortSchema(
            type_hint="bytes",
            max_bytes=1024,
        )
        
        assert port.max_bytes == 1024
    
    def test_port_schema_no_type_hint(self) -> None:
        """Test port schema without type_hint."""
        port = PortSchema(description="No type")
        
        assert port.type_hint is None
        assert port.is_array is False
    
    def test_array_detection(self) -> None:
        """Test array type detection."""
        port_list = PortSchema(type_hint="list[str]")
        assert port_list.is_array is True
        
        port_List = PortSchema(type_hint="List[int]")
        assert port_List.is_array is True
        
        port_str = PortSchema(type_hint="str")
        assert port_str.is_array is False
    
    def test_port_schema_with_pydantic_schema(self) -> None:
        """Test port schema with pydantic model."""
        if not PYDANTIC_AVAILABLE:
            pytest.skip("Pydantic not available")
        
        from pydantic import BaseModel
        
        class UserModel(BaseModel):
            name: str
            age: int
        
        port = PortSchema(
            type_hint="UserModel",
            schema=UserModel,
        )
        
        assert port.schema == UserModel
    
    def test_port_schema_pydantic_required_error(self) -> None:
        """Test error when using schema without pydantic."""
        # Temporarily mock PYDANTIC_AVAILABLE
        import pipedag.types as types_module
        
        original = types_module.PYDANTIC_AVAILABLE
        try:
            types_module.PYDANTIC_AVAILABLE = False
            
            class MockModel:
                pass
            
            with pytest.raises(ImportError, match="pydantic is required"):
                PortSchema(schema=MockModel)
        finally:
            types_module.PYDANTIC_AVAILABLE = original


class TestPolicySpec:
    """Test PolicySpec functionality."""
    
    def test_default_policy(self) -> None:
        """Test default policy values."""
        policy = PolicySpec()
        
        assert policy.retry_count == 0
        assert policy.timeout_seconds is None
        assert policy.concurrency_class is None
        assert policy.idempotent is False
    
    def test_custom_policy(self) -> None:
        """Test custom policy values."""
        policy = PolicySpec(
            retry_count=3,
            timeout_seconds=30.0,
            concurrency_class="http",
            idempotent=True,
        )
        
        assert policy.retry_count == 3
        assert policy.timeout_seconds == 30.0
        assert policy.concurrency_class == "http"
        assert policy.idempotent is True


class TestFieldPath:
    """Test FieldPath parsing and resolution."""
    
    def test_simple_path(self) -> None:
        """Test parsing a simple port name."""
        path = FieldPath.parse("result")
        
        assert path.port == "result"
        assert path.path == []
        assert not path.is_nested()
        assert str(path) == "result"
    
    def test_nested_path(self) -> None:
        """Test parsing nested field path."""
        path = FieldPath.parse("result.user.email")
        
        assert path.port == "result"
        assert path.path == ["user", "email"]
        assert path.is_nested()
        assert str(path) == "result.user.email"
    
    def test_field_path_direct_construction(self) -> None:
        """Test constructing FieldPath directly."""
        path = FieldPath(port="result", path=["user", "email"])
        
        assert path.port == "result"
        assert path.path == ["user", "email"]
        assert path.is_nested()
    
    def test_resolve_simple(self) -> None:
        """Test resolving a simple path."""
        path = FieldPath.parse("data")
        data = {"data": {"value": 123}}
        
        result = path.resolve(data)
        assert result == {"value": 123}
    
    def test_resolve_nested(self) -> None:
        """Test resolving nested path."""
        path = FieldPath.parse("result.user.name")
        data = {
            "result": {
                "user": {"name": "Alice", "age": 30},
                "status": "ok"
            }
        }
        
        result = path.resolve(data)
        assert result == "Alice"
    
    def test_resolve_deeply_nested(self) -> None:
        """Test resolving deeply nested path."""
        path = FieldPath.parse("a.b.c.d")
        data = {"a": {"b": {"c": {"d": "deep_value"}}}}
        
        result = path.resolve(data)
        assert result == "deep_value"
    
    def test_resolve_missing_port(self) -> None:
        """Test error when port is missing."""
        path = FieldPath.parse("missing")
        data = {"other": "value"}
        
        with pytest.raises(KeyError, match="Port 'missing' not found"):
            path.resolve(data)
    
    def test_resolve_missing_nested(self) -> None:
        """Test error when nested key is missing."""
        path = FieldPath.parse("result.missing")
        data = {"result": {"other": "value"}}
        
        with pytest.raises(KeyError, match="Key 'missing' not found"):
            path.resolve(data)
    
    def test_resolve_array_index(self) -> None:
        """Test resolving array index."""
        path = FieldPath.parse("items.0")
        data = {"items": ["first", "second", "third"]}
        
        result = path.resolve(data)
        assert result == "first"
    
    def test_resolve_array_index_out_of_bounds(self) -> None:
        """Test error when array index is out of bounds."""
        path = FieldPath.parse("items.10")
        data = {"items": ["first", "second"]}
        
        with pytest.raises(KeyError, match="Invalid array access"):
            path.resolve(data)
    
    def test_resolve_array_invalid_index(self) -> None:
        """Test error when array index is not numeric."""
        path = FieldPath.parse("items.invalid")
        data = {"items": ["first", "second"]}
        
        with pytest.raises(KeyError, match="Invalid array access"):
            path.resolve(data)
    
    def test_resolve_array_nested_in_dict(self) -> None:
        """Test resolving array index in nested structure."""
        path = FieldPath.parse("data.items.1")
        data = {"data": {"items": ["first", "second", "third"]}}
        
        result = path.resolve(data)
        assert result == "second"
    
    def test_resolve_invalid_traversal(self) -> None:
        """Test error on invalid path traversal."""
        path = FieldPath.parse("result.nested")
        data = {"result": "string_value"}
        
        with pytest.raises(TypeError, match="Cannot traverse path"):
            path.resolve(data)
    
    def test_resolve_invalid_traversal_on_number(self) -> None:
        """Test error when trying to traverse a number."""
        path = FieldPath.parse("result.nested")
        data = {"result": 42}
        
        with pytest.raises(TypeError, match="Cannot traverse path"):
            path.resolve(data)
    
    def test_validate_against_schema_no_nested(self) -> None:
        """Test validation when path is not nested."""
        path = FieldPath.parse("result")
        schema = PortSchema(type_hint="dict")
        
        # Should not raise
        path.validate_against_schema(schema)
    
    def test_validate_against_schema_no_schema(self) -> None:
        """Test validation when port schema has no schema."""
        path = FieldPath.parse("result.nested")
        schema = PortSchema(type_hint="dict")  # No .schema attribute
        
        # Should not raise (skips validation)
        path.validate_against_schema(schema)
    
    def test_validate_against_schema_pydantic_v2(self) -> None:
        """Test validation against Pydantic v2 schema."""
        if not PYDANTIC_AVAILABLE:
            pytest.skip("Pydantic not available")
        
        from pydantic import BaseModel
        
        class UserModel(BaseModel):
            name: str
            email: str
        
        class ResponseModel(BaseModel):
            user: UserModel
            status: str
        
        path = FieldPath.parse("result.user.name")
        schema = PortSchema(type_hint="ResponseModel", schema=ResponseModel)
        
        # Should validate successfully
        path.validate_against_schema(schema)
        
        # Test invalid field
        invalid_path = FieldPath.parse("result.user.invalid_field")
        with pytest.raises(ValidationError, match="Field 'invalid_field' not found"):
            invalid_path.validate_against_schema(schema)
    
    def test_validate_against_schema_pydantic_v1(self) -> None:
        """Test validation against Pydantic v1 schema if available."""
        if not PYDANTIC_AVAILABLE:
            pytest.skip("Pydantic not available")
        
        try:
            from pydantic import BaseModel
            
            # Create a mock v1-style model (with __fields__)
            class MockV1Model:
                def __init__(self):
                    # Simulate v1 __fields__ structure
                    self.__fields__ = {
                        "name": type("FieldInfo", (), {"type_": str})()
                    }
            
            path = FieldPath.parse("result.name")
            schema = PortSchema(type_hint="MockV1Model", schema=MockV1Model())
            
            # Should not raise
            path.validate_against_schema(schema)
            
            # Test invalid field
            invalid_path = FieldPath.parse("result.invalid")
            with pytest.raises(ValidationError, match="Field 'invalid' not found"):
                invalid_path.validate_against_schema(schema)
        except (ImportError, AttributeError):
            pytest.skip("Pydantic v1 structure not available")


class TestNodeSpec:
    """Test NodeSpec functionality."""
    
    def test_basic_node_spec(self) -> None:
        """Test creating a basic node spec."""
        spec = NodeSpec(
            name="test_node",
            inputs={
                "x": PortSchema(type_hint="int"),
            },
            outputs={
                "result": PortSchema(type_hint="int"),
            },
        )
        
        assert spec.name == "test_node"
        assert "x" in spec.inputs
        assert "result" in spec.outputs
        assert spec.policy is not None
        assert spec.policy.retry_count == 0
    
    def test_node_spec_with_policy(self) -> None:
        """Test node spec with custom policy."""
        policy = PolicySpec(retry_count=3, timeout_seconds=10.0)
        spec = NodeSpec(
            name="test_node",
            inputs={},
            outputs={},
            policy=policy,
        )
        
        assert spec.policy.retry_count == 3
        assert spec.policy.timeout_seconds == 10.0
    
    def test_node_spec_with_config_schema(self) -> None:
        """Test node spec with config schema."""
        config_schema = {
            "url": {"type": "string", "required": True},
            "timeout": {"type": "integer", "default": 30},
        }
        spec = NodeSpec(
            name="test_node",
            inputs={},
            outputs={},
            config_schema=config_schema,
        )
        
        assert spec.config_schema == config_schema
    
    def test_node_spec_default_policy_creation(self) -> None:
        """Test that default policy is created if not provided."""
        spec = NodeSpec(
            name="test_node",
            inputs={},
            outputs={},
            policy=None,  # Explicitly None
        )
        
        # Should have default policy after __post_init__
        assert spec.policy is not None
        assert isinstance(spec.policy, PolicySpec)
        assert spec.policy.retry_count == 0


class TestEdge:
    """Test Edge functionality."""
    
    def test_simple_edge(self) -> None:
        """Test creating a simple edge."""
        edge = Edge(
            src_node_id="node_a",
            src_output_path="result",
            dst_node_id="node_b",
            dst_input_key="input",
        )
        
        assert edge.src_node_id == "node_a"
        assert edge.src_output_path == "result"
        assert edge.dst_node_id == "node_b"
        assert edge.dst_input_key == "input"
        assert edge.src_field_path.port == "result"
        assert not edge.src_field_path.is_nested()
    
    def test_nested_path_edge(self) -> None:
        """Test edge with nested field path."""
        edge = Edge(
            src_node_id="node_a",
            src_output_path="result.user.email",
            dst_node_id="node_b",
            dst_input_key="email",
        )
        
        assert edge.src_field_path.port == "result"
        assert edge.src_field_path.path == ["user", "email"]
        assert edge.src_field_path.is_nested()
    
    def test_edge_repr(self) -> None:
        """Test edge string representation."""
        edge = Edge(
            src_node_id="node_a",
            src_output_path="result",
            dst_node_id="node_b",
            dst_input_key="input",
        )
        
        repr_str = repr(edge)
        assert "node_a" in repr_str
        assert "node_b" in repr_str
        assert "result" in repr_str
        assert "input" in repr_str


class TestRunStatus:
    """Test RunStatus enum."""
    
    def test_run_status_values(self) -> None:
        """Test all RunStatus enum values."""
        assert RunStatus.OK == "ok"
        assert RunStatus.ERROR == "error"
        assert RunStatus.CANCELLED == "cancelled"
    
    def test_run_status_usage(self) -> None:
        """Test using RunStatus in practice."""
        status = RunStatus.OK
        assert status == "ok"  # Can compare directly to string
        assert status.value == "ok"
        # str() returns the enum name, but value is the string
        assert str(status) in ["RunStatus.OK", "ok"]  # Both valid


class TestMapConfig:
    """Test MapConfig dataclass."""
    
    def test_map_config_defaults(self) -> None:
        """Test MapConfig default values."""
        config = MapConfig()
        
        assert config.enabled is False
        assert config.array_output_path is None
        assert config.mapped_node_id is None
    
    def test_map_config_custom(self) -> None:
        """Test MapConfig with custom values."""
        config = MapConfig(
            enabled=True,
            array_output_path="result.items",
            mapped_node_id="process_item",
        )
        
        assert config.enabled is True
        assert config.array_output_path == "result.items"
        assert config.mapped_node_id == "process_item"

