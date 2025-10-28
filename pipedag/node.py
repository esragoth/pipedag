"""
Node definition and runtime interface.
"""

import asyncio
import inspect
import sys
from dataclasses import dataclass
from typing import Any, Callable, Optional, Union, cast

from pipedag.context import NodeContext
from pipedag.errors import ValidationError
from pipedag.types import NodeSpec, PortSchema

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

P = ParamSpec("P")

# Node callable signature: (inputs, config, ctx) -> outputs
NodeCallable = Callable[[dict[str, Any], dict[str, Any], NodeContext], dict[str, Any]]


@dataclass
class Node:
    """
    Runtime instance of a node in a graph.
    """
    
    node_id: str
    spec: NodeSpec
    runtime_callable: NodeCallable
    config: dict[str, Any]
    
    def __post_init__(self) -> None:
        """Validate node configuration."""
        self._validate_config()
        self._is_async = asyncio.iscoroutinefunction(self.runtime_callable)
    
    def _validate_config(self) -> None:
        """Validate config against spec's config_schema."""
        if self.spec.config_schema:
            # Basic validation: check required keys exist
            for key, schema in self.spec.config_schema.items():
                if schema.get("required", False) and key not in self.config:
                    raise ValidationError(
                        f"Required config key '{key}' missing for node '{self.node_id}'",
                        node_id=self.node_id
                    )
    
    def is_async(self) -> bool:
        """Check if the node's callable is async."""
        return self._is_async
    
    async def execute(
        self,
        inputs: dict[str, Any],
        ctx: NodeContext
    ) -> dict[str, Any]:
        """
        Execute the node with given inputs and context.
        
        Args:
            inputs: Dictionary of input values
            ctx: Node execution context
            
        Returns:
            Dictionary of output values
            
        Raises:
            ValidationError: If inputs or outputs don't match spec
            ExecutionError: If execution fails
        """
        from pipedag.errors import ExecutionError
        
        # Validate inputs against spec
        self._validate_inputs(inputs)
        
        try:
            # Execute callable (handle both sync and async)
            if self._is_async:
                result = self.runtime_callable(inputs, self.config, ctx)
                outputs = cast(dict[str, Any], await result)  # type: ignore[misc]
            else:
                # Run sync callable in thread pool to avoid blocking
                outputs = cast(
                    dict[str, Any],
                    await asyncio.to_thread(
                        self.runtime_callable, inputs, self.config, ctx
                    )
                )
            
            # Validate outputs against spec
            self._validate_outputs(outputs)
            
            return outputs
            
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            raise ExecutionError(
                f"Node execution failed: {str(e)}",
                run_id=ctx.run_id,
                node_id=self.node_id,
                metadata={"error_type": type(e).__name__}
            ) from e
    
    def _validate_inputs(self, inputs: dict[str, Any]) -> None:
        """Validate inputs match the node spec."""
        for input_key, input_schema in self.spec.inputs.items():
            if input_schema.required and input_key not in inputs:
                raise ValidationError(
                    f"Required input '{input_key}' missing for node '{self.node_id}'",
                    node_id=self.node_id
                )
            
            # Check max_bytes if specified
            if input_key in inputs and input_schema.max_bytes:
                value = inputs[input_key]
                if isinstance(value, (str, bytes)):
                    size = len(value)
                    if size > input_schema.max_bytes:
                        raise ValidationError(
                            f"Input '{input_key}' size {size} exceeds max_bytes "
                            f"{input_schema.max_bytes} for node '{self.node_id}'",
                            node_id=self.node_id
                        )
    
    def _validate_outputs(self, outputs: dict[str, Any]) -> None:
        """Validate outputs match the node spec."""
        # Check that all output keys are declared in spec
        for output_key in outputs.keys():
            if output_key not in self.spec.outputs:
                raise ValidationError(
                    f"Undeclared output '{output_key}' returned by node '{self.node_id}'",
                    node_id=self.node_id
                )
        
        # Check required outputs are present
        for output_key, output_schema in self.spec.outputs.items():
            if output_schema.required and output_key not in outputs:
                raise ValidationError(
                    f"Required output '{output_key}' not returned by node '{self.node_id}'",
                    node_id=self.node_id
                )


def node_spec(
    name: str,
    inputs: Optional[dict[str, Union[PortSchema, dict[str, Any]]]] = None,
    outputs: Optional[dict[str, Union[PortSchema, dict[str, Any]]]] = None,
    config_schema: Optional[dict[str, Any]] = None,
    policy: Optional[dict[str, Any]] = None,
) -> Callable[[NodeCallable], NodeCallable]:
    """
    Decorator to attach NodeSpec metadata to a node callable.
    
    Args:
        name: Node type name
        inputs: Input port definitions
        outputs: Output port definitions
        config_schema: Configuration schema
        policy: Runtime policy
        
    Returns:
        Decorated function with attached spec
        
    Example:
        @node_spec(
            name="fetch_data",
            inputs={"url": PortSchema(type_hint="str")},
            outputs={"data": PortSchema(type_hint="dict")}
        )
        async def fetch_data(inputs, config, ctx):
            return {"data": {...}}
    """
    from pipedag.types import PolicySpec
    
    # Convert dict inputs/outputs to PortSchema if needed
    def _normalize_ports(
        ports: Optional[dict[str, Union[PortSchema, dict[str, Any]]]]
    ) -> dict[str, PortSchema]:
        if not ports:
            return {}
        
        result = {}
        for key, value in ports.items():
            if isinstance(value, PortSchema):
                result[key] = value
            elif isinstance(value, dict):
                result[key] = PortSchema(**value)
            else:
                raise ValueError(f"Invalid port definition: {value}")
        return result
    
    normalized_inputs = _normalize_ports(inputs)
    normalized_outputs = _normalize_ports(outputs)
    
    # Create policy if provided
    policy_obj = PolicySpec(**policy) if policy else None
    
    # Create the spec
    spec = NodeSpec(
        name=name,
        inputs=normalized_inputs,
        outputs=normalized_outputs,
        config_schema=config_schema,
        policy=policy_obj,
    )
    
    def decorator(func: NodeCallable) -> NodeCallable:
        """Attach spec to the function."""
        func.__node_spec__ = spec  # type: ignore
        return func
    
    return decorator

