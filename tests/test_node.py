"""Tests for node functionality."""

import pytest

from pipedag.context import NodeContext, RunContext
from pipedag.errors import ExecutionError, ValidationError
from pipedag.node import Node, node_spec
from pipedag.types import NodeSpec, PortSchema


class TestNode:
    """Test Node class."""
    
    async def test_sync_node_execution(self) -> None:
        """Test executing a synchronous node."""
        spec = NodeSpec(
            name="add",
            inputs={"x": PortSchema(type_hint="int"), "y": PortSchema(type_hint="int")},
            outputs={"result": PortSchema(type_hint="int")},
        )
        
        def add_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] + inputs["y"]}
        
        node = Node(
            node_id="add1",
            spec=spec,
            runtime_callable=add_func,
            config={},
        )
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("add1")
        
        result = await node.execute({"x": 5, "y": 3}, node_ctx)
        assert result["result"] == 8
    
    async def test_async_node_execution(self) -> None:
        """Test executing an async node."""
        spec = NodeSpec(
            name="multiply",
            inputs={"x": PortSchema(type_hint="int"), "y": PortSchema(type_hint="int")},
            outputs={"result": PortSchema(type_hint="int")},
        )
        
        async def multiply_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] * inputs["y"]}
        
        node = Node(
            node_id="mult1",
            spec=spec,
            runtime_callable=multiply_func,
            config={},
        )
        
        assert node.is_async() is True
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("mult1")
        
        result = await node.execute({"x": 4, "y": 7}, node_ctx)
        assert result["result"] == 28
    
    async def test_missing_required_input(self) -> None:
        """Test error on missing required input."""
        spec = NodeSpec(
            name="test",
            inputs={"x": PortSchema(required=True)},
            outputs={"result": PortSchema()},
        )
        
        def func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"]}
        
        node = Node(node_id="test1", spec=spec, runtime_callable=func, config={})
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test1")
        
        with pytest.raises(ValidationError, match="Required input 'x' missing"):
            await node.execute({}, node_ctx)
    
    async def test_missing_required_output(self) -> None:
        """Test error on missing required output."""
        spec = NodeSpec(
            name="test",
            inputs={},
            outputs={"result": PortSchema(required=True)},
        )
        
        def func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {}  # Missing required output
        
        node = Node(node_id="test1", spec=spec, runtime_callable=func, config={})
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test1")
        
        with pytest.raises(ValidationError, match="Required output 'result' not returned"):
            await node.execute({}, node_ctx)
    
    async def test_undeclared_output(self) -> None:
        """Test error on undeclared output."""
        spec = NodeSpec(
            name="test",
            inputs={},
            outputs={"result": PortSchema()},
        )
        
        def func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": 1, "extra": 2}  # Extra undeclared output
        
        node = Node(node_id="test1", spec=spec, runtime_callable=func, config={})
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test1")
        
        with pytest.raises(ValidationError, match="Undeclared output 'extra'"):
            await node.execute({}, node_ctx)
    
    async def test_max_bytes_validation(self) -> None:
        """Test max_bytes validation on inputs."""
        spec = NodeSpec(
            name="test",
            inputs={"data": PortSchema(max_bytes=10)},
            outputs={"result": PortSchema()},
        )
        
        def func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["data"]}
        
        node = Node(node_id="test1", spec=spec, runtime_callable=func, config={})
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test1")
        
        # Should work with small data
        result = await node.execute({"data": "small"}, node_ctx)
        assert result["result"] == "small"
        
        # Should fail with large data
        with pytest.raises(ValidationError, match="exceeds max_bytes"):
            await node.execute({"data": "x" * 100}, node_ctx)
    
    async def test_execution_error_wrapping(self) -> None:
        """Test that execution errors are properly wrapped."""
        spec = NodeSpec(
            name="test",
            inputs={},
            outputs={"result": PortSchema()},
        )
        
        def func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            raise ValueError("Something went wrong")
        
        node = Node(node_id="test1", spec=spec, runtime_callable=func, config={})
        
        run_ctx = RunContext()
        node_ctx = run_ctx.create_node_context("test1")
        
        with pytest.raises(ExecutionError, match="Node execution failed"):
            await node.execute({}, node_ctx)


class TestNodeSpecDecorator:
    """Test node_spec decorator."""
    
    def test_decorator_basic(self) -> None:
        """Test basic decorator usage."""
        @node_spec(
            name="add",
            inputs={"x": PortSchema(type_hint="int"), "y": PortSchema(type_hint="int")},
            outputs={"result": PortSchema(type_hint="int")},
        )
        def add(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": inputs["x"] + inputs["y"]}
        
        assert hasattr(add, "__node_spec__")
        spec = add.__node_spec__
        assert spec.name == "add"
        assert "x" in spec.inputs
        assert "y" in spec.inputs
        assert "result" in spec.outputs
    
    def test_decorator_with_dict_ports(self) -> None:
        """Test decorator with dict-based port definitions."""
        @node_spec(
            name="test",
            inputs={"x": {"type_hint": "int", "required": True}},
            outputs={"result": {"type_hint": "str"}},
        )
        def test_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
            return {"result": str(inputs["x"])}
        
        spec = test_func.__node_spec__
        assert isinstance(spec.inputs["x"], PortSchema)
        assert spec.inputs["x"].type_hint == "int"
        assert spec.inputs["x"].required is True

