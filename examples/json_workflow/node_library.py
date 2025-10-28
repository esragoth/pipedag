"""
Node Library - Collection of reusable node implementations.

These nodes are defined in code by developers and can be instantiated
from JSON definitions (e.g., by a UI).
"""

import asyncio
import json
from datetime import datetime

from pipedag import NodeRegistry, node_spec, NodeContext
from pipedag.types import PortSchema, PolicySpec


# Initialize the registry
registry = NodeRegistry()


# ============================================================================
# DATA SOURCE NODES
# ============================================================================

@registry.register_from_decorator("constant_source", category="Sources")
@node_spec(
    name="constant_source",
    inputs={},
    outputs={"value": PortSchema(type_hint="any", description="Constant value")},
)
def constant_source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Output a constant value from config."""
    value = config.get("value", None)
    ctx.logger.info(f"Emitting constant: {value}")
    return {"value": value}


@registry.register_from_decorator("range_source", category="Sources")
@node_spec(
    name="range_source",
    inputs={},
    outputs={"numbers": PortSchema(type_hint="list[int]", description="List of numbers")},
)
def range_source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Generate a range of numbers."""
    start = config.get("start", 0)
    end = config.get("end", 10)
    step = config.get("step", 1)
    
    numbers = list(range(start, end, step))
    ctx.logger.info(f"Generated {len(numbers)} numbers")
    return {"numbers": numbers}


@registry.register_from_decorator("timestamp_source", category="Sources")
@node_spec(
    name="timestamp_source",
    inputs={},
    outputs={
        "timestamp": PortSchema(type_hint="str", description="ISO timestamp"),
        "unix_time": PortSchema(type_hint="int", description="Unix epoch time"),
    },
)
def timestamp_source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Generate current timestamp."""
    now = datetime.now()
    return {
        "timestamp": now.isoformat(),
        "unix_time": int(now.timestamp()),
    }


# ============================================================================
# MATH NODES
# ============================================================================

@registry.register_from_decorator("add", category="Math")
@node_spec(
    name="add",
    inputs={
        "x": PortSchema(type_hint="number", required=True),
        "y": PortSchema(type_hint="number", required=True),
    },
    outputs={"result": PortSchema(type_hint="number")},
)
def add(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Add two numbers."""
    result = inputs["x"] + inputs["y"]
    ctx.logger.info(f"Add: {inputs['x']} + {inputs['y']} = {result}")
    return {"result": result}


@registry.register_from_decorator("multiply", category="Math")
@node_spec(
    name="multiply",
    inputs={
        "x": PortSchema(type_hint="number", required=True),
        "y": PortSchema(type_hint="number", required=True),
    },
    outputs={"result": PortSchema(type_hint="number")},
)
def multiply(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Multiply two numbers."""
    result = inputs["x"] * inputs["y"]
    ctx.logger.info(f"Multiply: {inputs['x']} * {inputs['y']} = {result}")
    return {"result": result}


@registry.register_from_decorator("sum_list", category="Math")
@node_spec(
    name="sum_list",
    inputs={"numbers": PortSchema(type_hint="list[number]", required=True)},
    outputs={
        "sum": PortSchema(type_hint="number"),
        "count": PortSchema(type_hint="int"),
        "average": PortSchema(type_hint="number"),
    },
)
def sum_list(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Sum a list of numbers and calculate statistics."""
    numbers = inputs["numbers"]
    total = sum(numbers)
    count = len(numbers)
    avg = total / count if count > 0 else 0
    
    ctx.logger.info(f"Sum of {count} numbers: {total}, avg: {avg:.2f}")
    
    return {
        "sum": total,
        "count": count,
        "average": avg,
    }


# ============================================================================
# DATA TRANSFORMATION NODES
# ============================================================================

@registry.register_from_decorator("filter_list", category="Transform")
@node_spec(
    name="filter_list",
    inputs={"items": PortSchema(type_hint="list", required=True)},
    outputs={"filtered": PortSchema(type_hint="list")},
)
def filter_list(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Filter list items based on a condition."""
    items = inputs["items"]
    condition = config.get("condition", "all")  # "all", "even", "odd", "positive"
    
    if condition == "even":
        filtered = [x for x in items if isinstance(x, int) and x % 2 == 0]
    elif condition == "odd":
        filtered = [x for x in items if isinstance(x, int) and x % 2 != 0]
    elif condition == "positive":
        filtered = [x for x in items if isinstance(x, (int, float)) and x > 0]
    else:
        filtered = items
    
    ctx.logger.info(f"Filtered {len(items)} items to {len(filtered)} ({condition})")
    return {"filtered": filtered}


@registry.register_from_decorator("map_transform", category="Transform")
@node_spec(
    name="map_transform",
    inputs={"items": PortSchema(type_hint="list", required=True)},
    outputs={"transformed": PortSchema(type_hint="list")},
)
def map_transform(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Apply a transformation to each item."""
    items = inputs["items"]
    operation = config.get("operation", "identity")  # "square", "double", "increment"
    
    if operation == "square":
        transformed = [x * x for x in items]
    elif operation == "double":
        transformed = [x * 2 for x in items]
    elif operation == "increment":
        transformed = [x + 1 for x in items]
    else:
        transformed = items
    
    ctx.logger.info(f"Transformed {len(items)} items using '{operation}'")
    return {"transformed": transformed}


@registry.register_from_decorator("format_string", category="Transform")
@node_spec(
    name="format_string",
    inputs={
        "template": PortSchema(type_hint="str", required=True),
        "value": PortSchema(type_hint="any", required=True),
    },
    outputs={"formatted": PortSchema(type_hint="str")},
)
def format_string(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Format a string with a value."""
    template = inputs["template"]
    value = inputs["value"]
    
    formatted = template.format(value=value)
    ctx.logger.info(f"Formatted: '{formatted}'")
    
    return {"formatted": formatted}


# ============================================================================
# LOGIC & CONTROL NODES
# ============================================================================

@registry.register_from_decorator("compare", category="Logic")
@node_spec(
    name="compare",
    inputs={
        "a": PortSchema(type_hint="any", required=True),
        "b": PortSchema(type_hint="any", required=True),
    },
    outputs={
        "equal": PortSchema(type_hint="bool"),
        "greater": PortSchema(type_hint="bool"),
        "less": PortSchema(type_hint="bool"),
    },
)
def compare(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Compare two values."""
    a = inputs["a"]
    b = inputs["b"]
    
    return {
        "equal": a == b,
        "greater": a > b if isinstance(a, (int, float)) and isinstance(b, (int, float)) else False,
        "less": a < b if isinstance(a, (int, float)) and isinstance(b, (int, float)) else False,
    }


@registry.register_from_decorator("aggregate", category="Logic")
@node_spec(
    name="aggregate",
    inputs={
        "input1": PortSchema(type_hint="any", required=False),
        "input2": PortSchema(type_hint="any", required=False),
        "input3": PortSchema(type_hint="any", required=False),
    },
    outputs={"combined": PortSchema(type_hint="dict")},
)
def aggregate(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Aggregate multiple inputs into a dict."""
    combined = {k: v for k, v in inputs.items() if v is not None}
    ctx.logger.info(f"Aggregated {len(combined)} inputs")
    return {"combined": combined}


# ============================================================================
# OUTPUT NODES
# ============================================================================

@registry.register_from_decorator("logger", category="Output")
@node_spec(
    name="logger",
    inputs={"message": PortSchema(type_hint="any", required=True)},
    outputs={"logged": PortSchema(type_hint="bool")},
)
def logger(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Log a message."""
    message = inputs["message"]
    level = config.get("level", "info")
    prefix = config.get("prefix", "")
    
    formatted = f"{prefix}{message}" if prefix else str(message)
    
    if level == "info":
        ctx.logger.info(formatted)
    elif level == "warning":
        ctx.logger.warning(formatted)
    elif level == "error":
        ctx.logger.error(formatted)
    
    return {"logged": True}


@registry.register_from_decorator("collect_results", category="Output")
@node_spec(
    name="collect_results",
    inputs={
        "data": PortSchema(type_hint="any", required=True),
    },
    outputs={
        "results": PortSchema(type_hint="dict"),
        "summary": PortSchema(type_hint="str"),
    },
)
def collect_results(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Collect and summarize results."""
    data = inputs["data"]
    
    results = {
        "data": data,
        "type": type(data).__name__,
        "timestamp": datetime.now().isoformat(),
    }
    
    if isinstance(data, (list, dict)):
        results["size"] = len(data)
    
    summary = f"Collected {type(data).__name__} at {results['timestamp']}"
    ctx.logger.info(summary)
    
    return {
        "results": results,
        "summary": summary,
    }


# ============================================================================
# ASYNC NODES (simulate external calls)
# ============================================================================

@registry.register_from_decorator("async_delay", category="Async", description="Simulate async operation")
@node_spec(
    name="async_delay",
    inputs={"value": PortSchema(type_hint="any", required=True)},
    outputs={"value": PortSchema(type_hint="any")},
    policy={"timeout_seconds": 5.0},
)
async def async_delay(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    """Simulate an async operation with delay."""
    delay = config.get("delay_ms", 100) / 1000.0
    value = inputs["value"]
    
    ctx.logger.info(f"Starting async operation (delay: {delay}s)")
    await asyncio.sleep(delay)
    ctx.logger.info("Async operation completed")
    
    return {"value": value}

