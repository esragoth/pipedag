"""
Basic linear pipeline example.

Demonstrates:
- Creating simple nodes
- Connecting nodes in sequence
- Executing a pipeline
- Accessing results
"""

import asyncio

from pipedag import Engine, Graph, Node, NodeContext
from pipedag.types import NodeSpec, PortSchema


def create_source_node() -> Node:
    """Create a source node that generates initial data."""
    spec = NodeSpec(
        name="data_source",
        inputs={},
        outputs={
            "numbers": PortSchema(
                type_hint="list[int]",
                description="List of numbers to process"
            )
        }
    )
    
    def source_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        ctx.logger.info("Generating source data")
        return {"numbers": [1, 2, 3, 4, 5]}
    
    return Node(
        node_id="source",
        spec=spec,
        runtime_callable=source_func,
        config={}
    )


def create_sum_node() -> Node:
    """Create a node that sums numbers."""
    spec = NodeSpec(
        name="sum_numbers",
        inputs={
            "numbers": PortSchema(
                type_hint="list[int]",
                required=True,
                description="Numbers to sum"
            )
        },
        outputs={
            "total": PortSchema(
                type_hint="int",
                description="Sum of all numbers"
            )
        }
    )
    
    def sum_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        numbers = inputs["numbers"]
        total = sum(numbers)
        ctx.logger.info(f"Summed {len(numbers)} numbers to get {total}")
        return {"total": total}
    
    return Node(
        node_id="sum",
        spec=spec,
        runtime_callable=sum_func,
        config={}
    )


def create_double_node() -> Node:
    """Create a node that doubles a number."""
    spec = NodeSpec(
        name="double_value",
        inputs={
            "value": PortSchema(
                type_hint="int",
                required=True,
                description="Value to double"
            )
        },
        outputs={
            "result": PortSchema(
                type_hint="int",
                description="Doubled value"
            )
        }
    )
    
    def double_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        value = inputs["value"]
        result = value * 2
        ctx.logger.info(f"Doubled {value} to get {result}")
        return {"result": result}
    
    return Node(
        node_id="double",
        spec=spec,
        runtime_callable=double_func,
        config={}
    )


async def main() -> None:
    """Run the basic pipeline."""
    # Create graph
    graph = Graph()
    
    # Add nodes
    source = create_source_node()
    sum_node = create_sum_node()
    double = create_double_node()
    
    graph.add_node(source)
    graph.add_node(sum_node)
    graph.add_node(double)
    
    # Connect nodes: source -> sum -> double
    graph.connect("source", "numbers", "sum", "numbers")
    graph.connect("sum", "total", "double", "value")
    
    # Validate graph
    print("Validating graph...")
    graph.validate()
    print(f"✓ Graph is valid (hash: {graph.compute_graph_hash()[:8]}...)")
    
    # Execute pipeline
    print("\nExecuting pipeline...")
    engine = Engine(concurrency=4)
    result = await engine.run(graph)
    
    # Display results
    print(f"\n{'='*50}")
    print(f"Pipeline Status: {result.status.value}")
    print(f"Duration: {result.duration_ms:.2f}ms")
    print(f"Nodes Executed: {result.nodes_executed}")
    print(f"{'='*50}")
    
    if result.success:
        print("\nResults:")
        print(f"  Source generated: {result.node_outputs['source']['numbers']}")
        print(f"  Sum calculated: {result.node_outputs['sum']['total']}")
        print(f"  Final result: {result.node_outputs['double']['result']}")
    else:
        print(f"\n✗ Pipeline failed: {result.error}")


if __name__ == "__main__":
    asyncio.run(main())

