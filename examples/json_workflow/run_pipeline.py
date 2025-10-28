#!/usr/bin/env python3
"""
Execute a pipeline defined in JSON.

This demonstrates the complete workflow:
1. Node types defined in Python (node_library.py)
2. Pipeline defined in JSON (pipeline.json)
3. Loaded and executed by the engine

Usage:
    python run_pipeline.py
    python run_pipeline.py pipeline.json
"""

import asyncio
import json
import sys
from pathlib import Path

from pipedag import Engine, Graph
from pipedag.events import Event, EventKind

# Import the node library (registers all node types)
from node_library import registry


def create_event_monitor() -> callable:
    """Create a simple event monitor for execution feedback."""
    
    def monitor(event: Event) -> None:
        """Monitor execution events."""
        if event.kind == EventKind.RUN_STARTED:
            total_nodes = event.meta.get("total_nodes", 0)
            print(f"\n{'='*70}")
            print(f"🚀 Starting pipeline execution")
            print(f"   Total nodes: {total_nodes}")
            print(f"   Run ID: {event.run_id}")
            print(f"{'='*70}\n")
        
        elif event.kind == EventKind.NODE_FINISHED:
            duration = event.meta.get("duration_ms", 0)
            print(f"  ✓ {event.node_id} completed in {duration:.1f}ms")
        
        elif event.kind == EventKind.NODE_FAILED:
            error = event.meta.get("error_message", "")
            print(f"  ✗ {event.node_id} failed: {error}")
        
        elif event.kind == EventKind.RUN_FINISHED:
            status = event.meta.get("status", "unknown")
            duration = event.meta.get("duration_ms", 0)
            nodes_executed = event.meta.get("nodes_executed", 0)
            
            print(f"\n{'='*70}")
            if status == "ok":
                print(f"✅ Pipeline completed successfully!")
            else:
                print(f"❌ Pipeline failed with status: {status}")
            print(f"   Duration: {duration:.1f}ms")
            print(f"   Nodes executed: {nodes_executed}")
            print(f"{'='*70}\n")
    
    return monitor


def print_available_nodes() -> None:
    """Print all available node types from the registry."""
    print("\n📚 Available Node Types:")
    print("="*70)
    
    categories = registry.list_categories()
    for category in categories:
        types = registry.list_types(category=category)
        print(f"\n{category}:")
        for node_type in types:
            desc = node_type.get("description", "")
            inputs = len(node_type.get("inputs", {}))
            outputs = len(node_type.get("outputs", {}))
            print(f"  • {node_type['type_name']}: {desc}")
            print(f"    Inputs: {inputs}, Outputs: {outputs}")
    
    print(f"\n{'='*70}\n")


def print_graph_structure(definition: dict) -> None:
    """Print the loaded graph structure."""
    print(f"\n📊 Graph Structure:")
    print("="*70)
    
    nodes = definition.get("nodes", [])
    edges = definition.get("edges", [])
    metadata = definition.get("metadata", {})
    
    if metadata:
        print(f"\nMetadata:")
        for key, value in metadata.items():
            print(f"  {key}: {value}")
    
    print(f"\nNodes ({len(nodes)}):")
    for node in nodes:
        node_id = node.get("id")
        node_type = node.get("type")
        config = node.get("config", {})
        config_str = json.dumps(config) if config else "{}"
        print(f"  • {node_id} ({node_type})")
        if config:
            print(f"    Config: {config_str}")
    
    print(f"\nEdges ({len(edges)}):")
    for edge in edges:
        src = edge.get("source", {})
        tgt = edge.get("target", {})
        print(f"  • {src.get('node')}.{src.get('output')} → {tgt.get('node')}.{tgt.get('input')}")
    
    print(f"\n{'='*70}\n")


async def main() -> None:
    """Main execution function."""
    
    # Get pipeline file from command line or use default
    if len(sys.argv) > 1:
        pipeline_file = sys.argv[1]
    else:
        pipeline_file = Path(__file__).parent / "pipeline.json"
    
    print(f"\n🔧 PipeDag JSON Pipeline Executor")
    print(f"Pipeline file: {pipeline_file}")
    
    # Show available node types
    print_available_nodes()
    
    # Load pipeline definition
    print(f"📖 Loading pipeline from {pipeline_file}...")
    try:
        with open(pipeline_file, "r") as f:
            definition = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: Pipeline file not found: {pipeline_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in pipeline file: {e}")
        sys.exit(1)
    
    # Print graph structure
    print_graph_structure(definition)
    
    # Load graph from definition
    print("🏗️  Building graph from definition...")
    try:
        graph = Graph.from_definition(definition, registry)
        print(f"✓ Graph loaded successfully")
        print(f"  Nodes: {len(graph.nodes)}")
        print(f"  Edges: {len(graph.edges)}")
        print(f"  Hash: {graph.compute_graph_hash()[:12]}...")
    except Exception as e:
        print(f"❌ Error building graph: {e}")
        sys.exit(1)
    
    # Create engine and execute
    print("\n⚙️  Initializing execution engine...")
    engine = Engine(concurrency=8)
    
    monitor = create_event_monitor()
    
    print("▶️  Executing pipeline...")
    result = await engine.run(
        graph,
        event_subscribers=[monitor],
        tags=["json_workflow", "example"]
    )
    
    # Show results
    if result.success:
        print("\n📋 Pipeline Results:")
        print("="*70)
        
        # Show final results node output
        if "final_results" in result.node_outputs:
            final = result.node_outputs["final_results"]
            print(f"\nFinal Results:")
            print(json.dumps(final, indent=2, default=str))
        
        # Show key intermediate results
        print(f"\nKey Intermediate Results:")
        for node_id in ["sum_even", "sum_odd", "add_totals", "multiply_result"]:
            if node_id in result.node_outputs:
                output = result.node_outputs[node_id]
                print(f"  {node_id}: {output}")
        
        print(f"\n{'='*70}")
        print("✅ All done!")
    else:
        print("\n❌ Pipeline execution failed!")
        if result.error:
            print(f"Error: {result.error}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

