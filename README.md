# blackroad-graphql-engine

> GraphQL API engine with schema generation, resolvers, introspection, and query execution — part of the BlackRoad OS developer platform.

## Features

- 📐 **Schema Definition** — Programmatic type/query/mutation/subscription registration
- 🔍 **SDL Generation** — Auto-generate valid GraphQL Schema Definition Language
- ⚡ **Query Execution** — Simple query parser with variable substitution
- 🔎 **Introspection** — Full `__schema` introspection response
- ✅ **Validation** — Query validation with detailed error messages
- 📄 **Docs Generation** — Auto-generate Markdown API docs
- 🗄️ **Schema Registry** — SQLite-backed schema versioning and execution logging

## Quick Start

```python
from graphql_engine import GraphQLEngine

engine = GraphQLEngine()

# Define types
engine.add_type("User", [
    {"name": "id", "type": "ID", "nullable": False},
    {"name": "name", "type": "String", "nullable": False},
    {"name": "email", "type": "String"},
])

# Define queries with resolvers
users_db = [{"id": "1", "name": "Alice", "email": "alice@example.com"}]
engine.add_query("users", "[User]", resolver=lambda args, ctx: users_db)

# Execute queries
result = engine.execute("{ users }")
print(result)  # {"data": {"users": [...]}}

# Generate SDL
sdl = engine.generate_schema_sdl()
print(sdl)

# Introspect
schema_info = engine.introspect()

# Generate docs
docs = engine.generate_docs()
```

## API Reference

| Method | Description |
|--------|-------------|
| `add_type(name, fields)` | Register a GraphQL object type |
| `add_query(name, return_type, args, resolver)` | Register a query field |
| `add_mutation(name, input_type, return_type, resolver)` | Register a mutation |
| `add_subscription(name, return_type, resolver)` | Register a subscription |
| `generate_schema_sdl()` | Generate SDL string |
| `execute(query_str, variables, context)` | Execute a query |
| `introspect()` | Return `__schema` introspection |
| `validate_query(query_str)` | Validate query, return errors |
| `generate_docs()` | Generate Markdown documentation |

## Running Tests

```bash
pip install pytest pytest-cov
pytest tests/ -v --cov=graphql_engine
```

## License

Proprietary — © BlackRoad OS, Inc.
