# Contributing to PipeDag

Thank you for your interest in contributing to PipeDag! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- Git

### Initial Setup

1. **Fork and clone the repository**

```bash
git clone https://github.com/yourusername/pipedag.git
cd pipedag
```

2. **Create a virtual environment**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install in development mode**

```bash
pip install -e ".[dev]"
```

This installs the package in editable mode with all development dependencies.

## Development Workflow

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=pipedag --cov-report=html

# Run specific test file
pytest tests/test_engine.py

# Run specific test
pytest tests/test_engine.py::TestEngine::test_simple_linear_pipeline

# Run with verbose output
pytest -v
```

### Code Quality

Before submitting a PR, ensure your code passes all quality checks:

```bash
# Type checking with mypy
mypy pipedag

# Format code with black
black pipedag tests examples

# Lint with ruff
ruff check pipedag tests examples

# Fix auto-fixable lint issues
ruff check --fix pipedag tests examples
```

### Coverage

We aim for >80% test coverage. Check coverage after adding new features:

```bash
pytest --cov=pipedag --cov-report=term-missing
```

View detailed HTML coverage report:

```bash
pytest --cov=pipedag --cov-report=html
open htmlcov/index.html  # or start htmlcov/index.html on Windows
```

## Making Changes

### Branch Naming

Use descriptive branch names:

- `feature/add-array-mapping` - New features
- `fix/retry-timeout-race` - Bug fixes
- `docs/improve-examples` - Documentation
- `refactor/simplify-validation` - Code refactoring

### Commit Messages

Write clear, descriptive commit messages:

```
Add support for array mapping in field paths

- Implement FieldPath.resolve() for list indexing
- Add tests for array access patterns
- Update documentation with examples

Fixes #42
```

### Code Style

- **Formatting**: Use Black with default settings (100 char line length)
- **Type hints**: Required for all functions (use `mypy --strict`)
- **Docstrings**: Google-style docstrings for public APIs
- **Imports**: Organized (stdlib, third-party, local)

Example:

```python
def create_node(
    node_id: str,
    spec: NodeSpec,
    config: dict[str, Any],
) -> Node:
    """
    Create a node instance.
    
    Args:
        node_id: Unique identifier for the node
        spec: Node specification with inputs/outputs
        config: Configuration values for the node
        
    Returns:
        Configured Node instance
        
    Raises:
        ValidationError: If config doesn't match spec schema
    """
    ...
```

## Testing Guidelines

### Test Structure

Organize tests to mirror the source structure:

```
pipedag/
  engine.py
  graph.py
  node.py
tests/
  test_engine.py
  test_graph.py
  test_node.py
```

### Test Categories

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test component interactions
3. **Edge Cases**: Test boundary conditions and error handling

### Writing Good Tests

```python
class TestNodeExecution:
    """Test node execution functionality."""
    
    async def test_sync_node_execution(self) -> None:
        """Test executing a synchronous node."""
        # Arrange
        spec = NodeSpec(...)
        node = Node(...)
        
        # Act
        result = await node.execute(inputs, ctx)
        
        # Assert
        assert result["output"] == expected_value
    
    async def test_missing_required_input(self) -> None:
        """Test error on missing required input."""
        node = create_test_node()
        
        with pytest.raises(ValidationError, match="Required input"):
            await node.execute({}, ctx)
```

### Test Fixtures

Use pytest fixtures for common setup:

```python
@pytest.fixture
def simple_graph() -> Graph:
    """Create a simple test graph."""
    graph = Graph()
    # ... setup
    return graph

def test_something(simple_graph: Graph) -> None:
    """Test using the fixture."""
    assert len(simple_graph.nodes) > 0
```

## Pull Request Process

### Before Submitting

1. ✅ All tests pass locally
2. ✅ Code is formatted with Black
3. ✅ No linting errors from Ruff
4. ✅ Type checking passes with mypy
5. ✅ Coverage hasn't decreased
6. ✅ Documentation is updated if needed
7. ✅ CHANGELOG is updated (if applicable)

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No new warnings
```

### Review Process

1. Automated checks must pass (CI)
2. At least one maintainer approval required
3. Address review feedback
4. Squash commits if requested
5. Maintainer will merge when ready

## Documentation

### Docstring Style

Use Google-style docstrings:

```python
def connect(
    self,
    src_node_id: str,
    src_output_path: str,
    dst_node_id: str,
    dst_input_key: str
) -> None:
    """
    Connect two nodes via an edge.
    
    The source output path supports nested field access using dot notation
    (e.g., "response.user.email").
    
    Args:
        src_node_id: Source node ID
        src_output_path: Source output path, supports "port" or "port.field.nested"
        dst_node_id: Destination node ID
        dst_input_key: Destination input key
        
    Raises:
        GraphError: If nodes don't exist
        
    Example:
        >>> graph.connect("api", "response.user.email", "emailer", "recipient")
    """
```

### Adding Examples

When adding new features, include:

1. **Code example** in docstring
2. **Test** demonstrating usage
3. **Example script** in `examples/` directory (for major features)
4. **README section** update (if user-facing)

## Release Process

(For maintainers)

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create git tag: `git tag v0.2.0`
4. Push tag: `git push origin v0.2.0`
5. GitHub Actions will build and publish to PyPI

## Getting Help

- **Questions**: Open a [GitHub Discussion](https://github.com/yourusername/pipedag/discussions)
- **Bugs**: Open a [GitHub Issue](https://github.com/yourusername/pipedag/issues)
- **Security**: Email security@pipedag.dev

## Code of Conduct

Be respectful, inclusive, and constructive. We're all here to build something great together.

## License

By contributing to PipeDag, you agree that your contributions will be licensed under the MIT License.

