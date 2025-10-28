# PipeDag

**Lightweight, embeddable execution engine for DAG-style pipelines**

PipeDag is a Python package that provides a clean, observable, and robust execution engine for directed acyclic graph (DAG) pipelines. Unlike workflow orchestrators that run as separate services, PipeDag is designed to be embedded directly into your Python applications.

## Features

- 🔗 **Flexible Node Wiring**: Connect nodes using field paths to extract nested data (`result.user.email`)
- ⚡ **Async-Native**: Full async/await support with configurable concurrency
- 🔄 **Retry & Timeout**: Built-in retry logic with backoff and per-node timeout enforcement
- 📊 **Observable**: Rich event stream for monitoring and debugging
- 🎯 **Type-Safe**: Optional schema validation with Pydantic integration
- 🧩 **Embeddable**: Zero infrastructure - runs in your process
- 🔍 **Validated**: Graph validation catches errors before execution
- 🎨 **UI-Ready**: Node registry + JSON serialization for visual graph editors
- 💾 **Dual-Mode**: Build graphs in Python code OR load from JSON definitions

## Installation

```bash
# Basic installation (stdlib only)
pip install pipedag

# With schema validation support
pip install pipedag[schemas]

# Development installation
pip install pipedag[dev]
```

## Quick Start

### Option 1: Pure Python (Full Control)

```python
import asyncio
from pipedag import Engine, Graph, Node, NodeContext
from pipedag.types import NodeSpec, PortSchema

# Define a node
def add_numbers(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    return {"result": inputs["x"] + inputs["y"]}

# Create node instance
spec = NodeSpec(
    name="adder",
    inputs={
        "x": PortSchema(type_hint="int"),
        "y": PortSchema(type_hint="int")
    },
    outputs={"result": PortSchema(type_hint="int")}
)
node = Node(node_id="add", spec=spec, runtime_callable=add_numbers, config={})

# Build and execute graph
graph = Graph()
graph.add_node(node)
graph.validate()

engine = Engine()
result = await engine.run(graph)
```

### Option 2: JSON Definitions (UI-Friendly)

```python
from pipedag import NodeRegistry, Graph, Engine, node_spec
from pipedag.types import PortSchema

# Step 1: Developer registers reusable node types
registry = NodeRegistry()

@registry.register_from_decorator("add_numbers", category="Math")
@node_spec(
    name="add",
    inputs={"x": PortSchema(), "y": PortSchema()},
    outputs={"result": PortSchema()}
)
def add_numbers(inputs, config, ctx):
    return {"result": inputs["x"] + inputs["y"]}

# Step 2: Load pipeline from JSON (created by UI or manually)
graph = Graph.from_file("pipeline.json", registry)

# Step 3: Execute as normal
engine = Engine()
result = await engine.run(graph)
```

**Complete working example with 14 node types:** [`examples/json_workflow/`](examples/json_workflow/)

## Core Concepts

### Nodes

Nodes are the building blocks of your pipeline. Each node has:
- **Inputs**: Named input ports with optional schemas
- **Outputs**: Named output ports with optional schemas
- **Runtime**: Sync or async callable that processes inputs
- **Policy**: Retry, timeout, and concurrency settings

```python
from pipedag.types import NodeSpec, PortSchema, PolicySpec

spec = NodeSpec(
    name="fetch_data",
    inputs={"url": PortSchema(type_hint="str", required=True)},
    outputs={"data": PortSchema(type_hint="dict")},
    policy=PolicySpec(
        retry_count=3,
        timeout_seconds=30.0,
        concurrency_class="http"
    )
)
```

### Field Paths

Access nested fields in node outputs using dot notation:

```python
# Node outputs complex data
def api_call(inputs, config, ctx):
    return {
        "response": {
            "user": {"name": "Alice", "email": "alice@example.com"},
            "metadata": {"timestamp": "2024-01-15"}
        }
    }

# Extract nested fields
graph.connect("api", "response.user.email", "email_sender", "recipient")
graph.connect("api", "response.metadata.timestamp", "logger", "time")
```

### Graph

Build your pipeline by adding nodes and connecting them:

```python
graph = Graph()

# Add nodes
graph.add_node(node_a)
graph.add_node(node_b)

# Connect outputs to inputs
graph.connect("node_a", "output_key", "node_b", "input_key")

# Validate structure
graph.validate()  # Checks for cycles, unknown ports, type compatibility

# Save to JSON for UI
import json
definition = graph.to_definition()
with open("pipeline.json", "w") as f:
    json.dump(definition, f)

# Load from JSON
from pipedag import NodeRegistry
registry = NodeRegistry()  # with your registered nodes
graph = Graph.from_file("pipeline.json", registry)
```

### Execution

Run your graph with the async engine:

```python
engine = Engine(concurrency=8)

result = await engine.run(
    graph,
    secrets={"api_key": "secret"},  # Pass secrets to nodes
    event_subscribers=[my_logger],   # Monitor execution
    tags=["production", "v2"]        # Tag the run
)

# Check results
if result.success:
    outputs = result.node_outputs["node_id"]["output_key"]
```

### Events

Subscribe to execution events for monitoring:

```python
from pipedag.events import Event, EventKind

def monitor(event: Event):
    if event.kind == EventKind.NODE_STARTED:
        print(f"Node {event.node_id} started")
    elif event.kind == EventKind.NODE_FINISHED:
        duration = event.meta["duration_ms"]
        print(f"Node {event.node_id} finished in {duration}ms")

result = await engine.run(graph, event_subscribers=[monitor])
```

## Examples

See the `examples/` directory for complete working examples:

- **basic_pipeline.py**: Simple linear pipeline
- **field_paths.py**: Nested data extraction with field paths
- **retry_timeout.py**: Retry and timeout configuration
- **event_monitoring.py**: Custom event monitoring
- **json_workflow/**: Complete UI-ready workflow system
  - 14 reusable node types (sources, math, transforms, logic, output, async)
  - JSON pipeline definition
  - Full execution with real-time monitoring

Run an example:

```bash
python examples/basic_pipeline.py
python examples/json_workflow/run_pipeline.py
```

## Architecture

### Graph Validation

Before execution, the engine validates:
- ✓ Graph is acyclic (DAG)
- ✓ All referenced ports exist
- ✓ Each input has at most one source
- ✓ Port types are compatible
- ✓ Field paths are valid (if schemas provided)

### Execution Model

- **Topological scheduling**: Nodes execute when dependencies are satisfied
- **Concurrency control**: Semaphore-based limits prevent overload
- **Async-first**: Native async/await with sync node support via thread pool
- **Fail-fast**: By default, first failure stops the pipeline
- **Retry with backoff**: Configurable retries with jitter

### Runtime Semantics

Each node execution:
1. Waits for concurrency semaphore
2. Assembles inputs from upstream outputs
3. Applies timeout wrapper if configured
4. Executes with retry logic on failure
5. Validates outputs against schema
6. Routes outputs to downstream nodes
7. Emits events at each transition

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/pipedag.git
cd pipedag

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install with dev dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# With coverage
pytest --cov=pipedag --cov-report=html

# Run specific test file
pytest tests/test_engine.py

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Type checking
mypy pipedag

# Formatting
black pipedag tests examples

# Linting
ruff check pipedag tests examples
```

## API Reference

### Core Classes

- **`Engine`**: Async execution orchestrator
- **`Graph`**: DAG builder and validator
- **`Node`**: Runtime node instance
- **`NodeSpec`**: Declarative node metadata
- **`RunResult`**: Execution results and metrics

### Types

- **`PortSchema`**: Input/output port definition
- **`PolicySpec`**: Retry, timeout, concurrency policy
- **`Edge`**: Connection between nodes
- **`FieldPath`**: Nested field path parser
- **`RunStatus`**: Enum: OK, ERROR, CANCELLED

### Events

- **`EventEmitter`**: Thread-safe event dispatcher
- **`Event`**: Base event with run_id, node_id, timestamp, meta
- **`NodeStartedEvent`, `NodeFinishedEvent`, `NodeFailedEvent`**
- **`RunStartedEvent`, `RunFinishedEvent`**

## Performance

Expected performance characteristics:

- **Latency overhead**: < 3ms per node (excluding user code)
- **Throughput**: Hundreds of concurrent I/O-bound tasks
- **Memory**: Bounded by in-memory payloads (~10MB per port recommended)
- **Graph size**: Thousands of nodes per graph

## Roadmap

### MVP (Current)
- ✅ Graph validation + async executor
- ✅ Retries + timeouts + events
- ✅ Field path support
- ✅ Basic schema validation
- ✅ Node registry + JSON serialization
- ✅ UI-ready event system

### Phase 2 (Planned)
- Resource classes & rate limits
- Artifact stores for large payloads
- Subprocess sandbox mode
- OpenTelemetry exporters
- Map/join helpers for arrays

### Phase 3 (Future)
- Distributed execution option
- Checkpoints & resumable runs
- Rich type system with converters
- Deterministic replay tools

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Support

- **Documentation**: [https://pipedag.readthedocs.io](https://pipedag.readthedocs.io) (coming soon)
- **Issues**: [GitHub Issues](https://github.com/yourusername/pipedag/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/pipedag/discussions)

