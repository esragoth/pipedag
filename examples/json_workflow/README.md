# JSON Workflow Example

This example demonstrates the complete workflow for UI-driven pipeline execution:

1. **Node types defined in Python** (`node_library.py`)
2. **Pipeline defined in JSON** (`pipeline.json`)
3. **Loaded and executed** (`run_pipeline.py`)

## Overview

This showcases how a visual UI would work with PipeDag:
- Developers define reusable node types in code
- Users compose pipelines visually (saved as JSON)
- Engine loads and executes the JSON definition

## Node Library

The `node_library.py` contains **14 node types** across 5 categories:

### Sources (3 nodes)
- `constant_source` - Output a constant value
- `range_source` - Generate a range of numbers
- `timestamp_source` - Generate current timestamp

### Math (3 nodes)
- `add` - Add two numbers
- `multiply` - Multiply two numbers
- `sum_list` - Sum list and calculate statistics

### Transform (3 nodes)
- `filter_list` - Filter items by condition
- `map_transform` - Transform each item
- `format_string` - Format string with value

### Logic (2 nodes)
- `compare` - Compare two values
- `aggregate` - Combine multiple inputs

### Output (2 nodes)
- `logger` - Log a message
- `collect_results` - Collect and summarize results

### Async (1 node)
- `async_delay` - Simulate async operation

## Pipeline Definition

The `pipeline.json` defines a complete data processing pipeline:

```
range_source (1-10)
    ├─> filter_even ─> square ─> sum_even ─┐
    │                                        ├─> add ─> multiply ─> async ─> aggregate ─> results ─> log
    └─> filter_odd ─> double ─> sum_odd ───┘
```

The pipeline:
1. Generates numbers 1-10
2. Splits into even/odd branches
3. Transforms each branch (square vs double)
4. Sums each branch
5. Adds the totals together
6. Multiplies by a constant
7. Processes asynchronously
8. Aggregates statistics
9. Collects and logs results

## Running the Example

```bash
# From the examples/json_workflow directory
python run_pipeline.py

# Or specify a custom pipeline file
python run_pipeline.py custom_pipeline.json
```

## Expected Output

```
🔧 PipeDag JSON Pipeline Executor
Pipeline file: pipeline.json

📚 Available Node Types:
====================================================================================

Sources:
  • constant_source: Output a constant value from config.
    Inputs: 0, Outputs: 1
  ...

📊 Graph Structure:
====================================================================================

Nodes (18):
  • source1 (range_source)
    Config: {"start": 1, "end": 11, "step": 1}
  ...

Edges (19):
  • source1.numbers → filter_even.items
  ...

====================================================================================
🚀 Starting pipeline execution
   Total nodes: 18
   Run ID: ...
====================================================================================

  ✓ source1 completed in 0.3ms
  ✓ filter_even completed in 0.2ms
  ...

====================================================================================
✅ Pipeline completed successfully!
   Duration: 55.2ms
   Nodes executed: 18
====================================================================================

📋 Pipeline Results:
====================================================================================

Final Results:
{
  "results": {
    "data": {
      "input1": 650,
      "input2": 34.0,
      "input3": 5
    },
    "type": "dict",
    "timestamp": "2024-...",
    "size": 3
  },
  "summary": "Collected dict at 2024-..."
}

Key Intermediate Results:
  sum_even: {'sum': 220, 'count': 5, 'average': 44.0}
  sum_odd: {'sum': 105, 'count': 5, 'average': 21.0}
  add_totals: {'result': 325}
  multiply_result: {'result': 650}

====================================================================================
✅ All done!
```

## How to Extend

### Add a New Node Type

1. **Define the node in `node_library.py`:**

```python
@registry.register_from_decorator("my_node", category="Custom")
@node_spec(
    name="my_node",
    inputs={"input": PortSchema(type_hint="str")},
    outputs={"output": PortSchema(type_hint="str")},
)
def my_node(inputs: dict, config: dict, ctx: NodeContext) -> dict:
    # Your logic here
    return {"output": inputs["input"].upper()}
```

2. **Use it in JSON:**

```json
{
  "nodes": [
    {
      "id": "uppercase",
      "type": "my_node",
      "config": {}
    }
  ],
  "edges": [
    {
      "source": {"node": "some_source", "output": "text"},
      "target": {"node": "uppercase", "input": "input"}
    }
  ]
}
```

## UI Integration

A visual UI would:

1. **Query available nodes:**
   ```python
   types = registry.list_types()
   # Display as draggable palette
   ```

2. **Let users drag/drop nodes:**
   ```json
   {
     "id": "user_generated_id",
     "type": "selected_node_type",
     "config": {...}  // From UI form
   }
   ```

3. **Let users connect nodes visually:**
   ```json
   {
     "source": {"node": "src", "output": "out"},
     "target": {"node": "dst", "input": "in"}
   }
   ```

4. **Save to JSON and execute:**
   ```python
   graph = Graph.from_definition(ui_json, registry)
   result = await engine.run(graph)
   ```

## Key Takeaways

✅ **Node types** defined once in Python
✅ **Pipelines** composed in JSON (or UI)
✅ **Full field path support** for nested data
✅ **Validation** happens at load time
✅ **Events** provide real-time execution feedback
✅ **Both approaches** work: code or JSON

