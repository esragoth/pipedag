"""
Event monitoring example.

Demonstrates:
- Subscribing to pipeline events
- Real-time monitoring of execution
- Building custom event handlers
- Extracting execution metrics
"""

import asyncio
from datetime import datetime

from pipedag import Engine, Graph, Node, NodeContext
from pipedag.events import Event, EventKind
from pipedag.types import NodeSpec, PortSchema


class ExecutionMonitor:
    """Custom monitor that tracks pipeline execution."""
    
    def __init__(self) -> None:
        """Initialize the monitor."""
        self.events = []
        self.node_timings = {}
        self.run_start_time = None
        self.run_end_time = None
    
    def handle_event(self, event: Event) -> None:
        """Handle incoming events."""
        self.events.append(event)
        
        if event.kind == EventKind.RUN_STARTED:
            self.run_start_time = event.timestamp
            print(f"\n🚀 Run started: {event.run_id}")
            print(f"   Nodes: {event.meta.get('total_nodes', 0)}")
            print(f"   Tags: {event.meta.get('tags', [])}")
        
        elif event.kind == EventKind.NODE_STARTED:
            print(f"\n⏱  Node '{event.node_id}' started")
            print(f"   Inputs: {event.meta.get('inputs_present', [])}")
            self.node_timings[event.node_id] = {
                "start": event.timestamp,
                "attempts": event.meta.get("attempt", 1)
            }
        
        elif event.kind == EventKind.NODE_FINISHED:
            duration = event.meta.get("duration_ms", 0)
            outputs = event.meta.get("outputs_present", [])
            print(f"✓  Node '{event.node_id}' finished in {duration:.1f}ms")
            print(f"   Outputs: {outputs}")
            
            if event.node_id in self.node_timings:
                self.node_timings[event.node_id]["duration_ms"] = duration
        
        elif event.kind == EventKind.NODE_FAILED:
            error = event.meta.get("error_message", "")
            retry_left = event.meta.get("retry_left", 0)
            print(f"✗  Node '{event.node_id}' failed: {error}")
            if retry_left > 0:
                print(f"   Retries remaining: {retry_left}")
        
        elif event.kind == EventKind.RUN_FINISHED:
            self.run_end_time = event.timestamp
            status = event.meta.get("status", "unknown")
            duration = event.meta.get("duration_ms", 0)
            nodes_executed = event.meta.get("nodes_executed", 0)
            nodes_failed = event.meta.get("nodes_failed", 0)
            
            print(f"\n{'='*60}")
            print(f"🏁 Run finished: {status.upper()}")
            print(f"   Duration: {duration:.1f}ms")
            print(f"   Nodes executed: {nodes_executed}")
            print(f"   Nodes failed: {nodes_failed}")
            print(f"{'='*60}")
    
    def print_summary(self) -> None:
        """Print execution summary."""
        print("\n" + "="*60)
        print("EXECUTION SUMMARY")
        print("="*60)
        
        print(f"\nTotal events: {len(self.events)}")
        
        event_counts = {}
        for event in self.events:
            kind = event.kind.value
            event_counts[kind] = event_counts.get(kind, 0) + 1
        
        print("\nEvent breakdown:")
        for kind, count in sorted(event_counts.items()):
            print(f"  {kind}: {count}")
        
        if self.node_timings:
            print("\nNode timing details:")
            for node_id, timing in sorted(self.node_timings.items()):
                duration = timing.get("duration_ms", "N/A")
                attempts = timing.get("attempts", 1)
                print(f"  {node_id}: {duration}ms (attempts: {attempts})")


def create_data_pipeline() -> Graph:
    """Create a sample data processing pipeline."""
    graph = Graph()
    
    # Source node
    def source(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        ctx.logger.info("Loading data")
        return {"data": [1, 2, 3, 4, 5]}
    
    source_node = Node(
        node_id="load_data",
        spec=NodeSpec(
            name="data_source",
            inputs={},
            outputs={"data": PortSchema(type_hint="list[int]")}
        ),
        runtime_callable=source,
        config={}
    )
    
    # Transform node
    async def transform(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        ctx.logger.info("Transforming data")
        await asyncio.sleep(0.1)  # Simulate work
        
        data = inputs["data"]
        transformed = [x * 2 for x in data]
        
        # Emit custom metric
        ctx.emit("data_transformed", {"count": len(transformed)})
        
        return {"transformed": transformed}
    
    transform_node = Node(
        node_id="transform_data",
        spec=NodeSpec(
            name="transformer",
            inputs={"data": PortSchema(type_hint="list[int]")},
            outputs={"transformed": PortSchema(type_hint="list[int]")}
        ),
        runtime_callable=transform,
        config={}
    )
    
    # Aggregate node
    def aggregate(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        ctx.logger.info("Aggregating results")
        
        data = inputs["data"]
        result = {
            "sum": sum(data),
            "count": len(data),
            "avg": sum(data) / len(data) if data else 0
        }
        
        ctx.emit("aggregation_complete", result)
        
        return {"stats": result}
    
    aggregate_node = Node(
        node_id="aggregate",
        spec=NodeSpec(
            name="aggregator",
            inputs={"data": PortSchema(type_hint="list[int]")},
            outputs={"stats": PortSchema(type_hint="dict")}
        ),
        runtime_callable=aggregate,
        config={}
    )
    
    # Build graph
    graph.add_node(source_node)
    graph.add_node(transform_node)
    graph.add_node(aggregate_node)
    
    graph.connect("load_data", "data", "transform_data", "data")
    graph.connect("transform_data", "transformed", "aggregate", "data")
    
    return graph


async def main() -> None:
    """Run the monitored pipeline."""
    print("="*60)
    print("EVENT MONITORING EXAMPLE")
    print("="*60)
    
    # Create monitor
    monitor = ExecutionMonitor()
    
    # Create and validate graph
    graph = create_data_pipeline()
    graph.validate()
    
    # Run with monitoring
    engine = Engine(concurrency=4)
    result = await engine.run(
        graph,
        event_subscribers=[monitor.handle_event],
        tags=["example", "monitored"]
    )
    
    # Print detailed summary
    monitor.print_summary()
    
    # Show final outputs if successful
    if result.success:
        print("\nFinal outputs:")
        stats = result.node_outputs["aggregate"]["stats"]
        print(f"  Sum: {stats['sum']}")
        print(f"  Count: {stats['count']}")
        print(f"  Average: {stats['avg']:.2f}")


if __name__ == "__main__":
    asyncio.run(main())

