"""
Field path extraction example.

Demonstrates:
- Accessing nested fields from complex outputs
- Using dot-notation field paths
- Flexible data wiring
"""

import asyncio

from pipedag import Engine, Graph, Node, NodeContext
from pipedag.types import NodeSpec, PortSchema


def create_api_response_node() -> Node:
    """Create a node that simulates an API response with nested data."""
    spec = NodeSpec(
        name="api_fetch",
        inputs={},
        outputs={
            "response": PortSchema(
                type_hint="dict",
                description="Complex API response"
            )
        }
    )
    
    def fetch_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        # Simulate complex nested API response
        response = {
            "response": {
                "user": {
                    "id": 123,
                    "name": "Alice Johnson",
                    "email": "alice@example.com",
                    "profile": {
                        "bio": "Software engineer",
                        "location": "San Francisco"
                    }
                },
                "metadata": {
                    "timestamp": "2024-01-15T10:30:00Z",
                    "version": "v2",
                    "status_code": 200
                },
                "settings": {
                    "notifications": True,
                    "theme": "dark"
                }
            }
        }
        ctx.logger.info("Fetched API data")
        return response
    
    return Node(
        node_id="api",
        spec=spec,
        runtime_callable=fetch_func,
        config={}
    )


def create_email_node() -> Node:
    """Create a node that uses email from nested data."""
    spec = NodeSpec(
        name="send_email",
        inputs={
            "email": PortSchema(type_hint="str", required=True),
            "name": PortSchema(type_hint="str", required=True)
        },
        outputs={
            "sent": PortSchema(type_hint="bool")
        }
    )
    
    def email_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        email = inputs["email"]
        name = inputs["name"]
        ctx.logger.info(f"Sending email to {name} <{email}>")
        return {"sent": True}
    
    return Node(
        node_id="email",
        spec=spec,
        runtime_callable=email_func,
        config={}
    )


def create_location_node() -> Node:
    """Create a node that processes location."""
    spec = NodeSpec(
        name="process_location",
        inputs={
            "location": PortSchema(type_hint="str", required=True)
        },
        outputs={
            "timezone": PortSchema(type_hint="str")
        }
    )
    
    def location_func(inputs: dict, config: dict, ctx: NodeContext) -> dict:
        location = inputs["location"]
        # Simple mock timezone lookup
        timezones = {
            "San Francisco": "America/Los_Angeles",
            "New York": "America/New_York",
            "London": "Europe/London"
        }
        timezone = timezones.get(location, "UTC")
        ctx.logger.info(f"Location {location} -> Timezone {timezone}")
        return {"timezone": timezone}
    
    return Node(
        node_id="location",
        spec=spec,
        runtime_callable=location_func,
        config={}
    )


async def main() -> None:
    """Run the field path extraction pipeline."""
    # Create graph
    graph = Graph()
    
    # Add nodes
    api = create_api_response_node()
    email = create_email_node()
    location = create_location_node()
    
    graph.add_node(api)
    graph.add_node(email)
    graph.add_node(location)
    
    # Connect using field paths to extract nested data
    # Extract email and name from nested user object
    graph.connect("api", "response.user.email", "email", "email")
    graph.connect("api", "response.user.name", "email", "name")
    
    # Extract location from nested profile
    graph.connect("api", "response.user.profile.location", "location", "location")
    
    # Validate and execute
    print("Validating graph...")
    graph.validate()
    print("✓ Graph validated with field path connections")
    
    print("\nExecuting pipeline with field paths...")
    engine = Engine()
    result = await engine.run(graph)
    
    # Display results
    print(f"\n{'='*60}")
    print(f"Pipeline Status: {result.status.value}")
    print(f"Duration: {result.duration_ms:.2f}ms")
    print(f"{'='*60}")
    
    if result.success:
        print("\nExtracted and processed nested fields:")
        print(f"  Email sent: {result.node_outputs['email']['sent']}")
        print(f"  Timezone calculated: {result.node_outputs['location']['timezone']}")
        
        # Show the original complex data
        print("\nOriginal API response structure:")
        api_response = result.node_outputs['api']['response']
        print(f"  User: {api_response['user']['name']}")
        print(f"  Email: {api_response['user']['email']}")
        print(f"  Location: {api_response['user']['profile']['location']}")


if __name__ == "__main__":
    asyncio.run(main())

