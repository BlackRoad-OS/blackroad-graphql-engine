"""Tests for BlackRoad GraphQL Engine"""
import pytest
from graphql_engine import (
    GraphQLEngine, Field, Type, QueryDef, MutationDef,
    Argument, Schema, SimpleQueryParser, SchemaRegistry
)


@pytest.fixture
def engine():
    e = GraphQLEngine(db_path=":memory:")
    return e


@pytest.fixture
def populated_engine(engine):
    engine.add_type("User", [
        {"name": "id", "type": "ID", "nullable": False},
        {"name": "name", "type": "String", "nullable": False},
        {"name": "email", "type": "String"},
        {"name": "age", "type": "Int"},
    ])
    engine.add_type("Post", [
        {"name": "id", "type": "ID", "nullable": False},
        {"name": "title", "type": "String", "nullable": False},
        {"name": "content", "type": "String"},
        {"name": "authorId", "type": "ID"},
    ])

    _users = [{"id": "1", "name": "Alice", "email": "alice@example.com", "age": 30}]
    _posts = [{"id": "1", "title": "Hello", "content": "World", "authorId": "1"}]

    engine.add_query(
        "users", "[User]",
        args=[],
        resolver=lambda args, ctx: _users,
    )
    engine.add_query(
        "user", "User",
        args=[{"name": "id", "type": "ID", "nullable": False}],
        resolver=lambda args, ctx: next((u for u in _users if u["id"] == args.get("id")), None),
    )
    engine.add_query(
        "posts", "[Post]",
        resolver=lambda args, ctx: _posts,
    )
    engine.add_mutation(
        "createUser", "UserInput", "User",
        resolver=lambda args, ctx: {"id": "99", **args.get("input", args)},
    )
    return engine


class TestFieldSDL:
    def test_nullable_field(self):
        f = Field("name", "String", nullable=True)
        assert "name: String" in f.to_sdl()
        assert "!" not in f.to_sdl()

    def test_non_nullable_field(self):
        f = Field("id", "ID", nullable=False)
        assert "id: ID!" in f.to_sdl()

    def test_field_with_description(self):
        f = Field("email", "String", description="User email")
        assert f.description == "User email"


class TestTypeSDL:
    def test_type_sdl_generation(self):
        t = Type("User", fields=[
            Field("id", "ID", nullable=False),
            Field("name", "String"),
        ])
        sdl = t.to_sdl()
        assert "type User" in sdl
        assert "id: ID!" in sdl
        assert "name: String" in sdl

    def test_type_with_description(self):
        t = Type("User", fields=[], description="A user")
        sdl = t.to_sdl()
        assert "A user" in sdl


class TestAddType:
    def test_add_type(self, engine):
        t = engine.add_type("Product", [
            {"name": "id", "type": "ID"},
            {"name": "name", "type": "String"},
        ])
        assert t.name == "Product"
        assert len(t.fields) == 2

    def test_add_multiple_types(self, engine):
        engine.add_type("TypeA", [{"name": "x", "type": "String"}])
        engine.add_type("TypeB", [{"name": "y", "type": "Int"}])
        assert len(engine.schema.types) == 2


class TestAddQuery:
    def test_add_query(self, engine):
        engine.add_type("Item", [{"name": "id", "type": "ID"}])
        q = engine.add_query("items", "[Item]", args=[], resolver=lambda a, c: [])
        assert q.name == "items"
        assert q.return_type == "[Item]"

    def test_add_query_with_args(self, engine):
        q = engine.add_query(
            "item", "Item",
            args=[{"name": "id", "type": "ID", "nullable": False}],
            resolver=lambda a, c: None,
        )
        assert len(q.args) == 1
        assert q.args[0].name == "id"


class TestSDLGeneration:
    def test_full_sdl_has_type_query(self, populated_engine):
        sdl = populated_engine.generate_schema_sdl()
        assert "type User" in sdl
        assert "type Post" in sdl
        assert "type Query" in sdl

    def test_sdl_has_mutation(self, populated_engine):
        sdl = populated_engine.generate_schema_sdl()
        assert "type Mutation" in sdl

    def test_sdl_non_null_fields(self, populated_engine):
        sdl = populated_engine.generate_schema_sdl()
        assert "id: ID!" in sdl

    def test_sdl_saved_to_registry(self, populated_engine):
        sdl = populated_engine.generate_schema_sdl()
        stored = populated_engine.registry.get_latest_schema()
        assert stored == sdl


class TestQueryExecution:
    def test_execute_query(self, populated_engine):
        result = populated_engine.execute("{ users }")
        assert "data" in result
        assert "users" in result["data"]

    def test_execute_returns_data(self, populated_engine):
        result = populated_engine.execute("{ users }")
        assert result["data"]["users"] == [{"id": "1", "name": "Alice", "email": "alice@example.com", "age": 30}]

    def test_execute_unknown_field_returns_error(self, populated_engine):
        result = populated_engine.execute("{ unknownField }")
        assert "errors" in result

    def test_execute_with_variables(self, populated_engine):
        result = populated_engine.execute(
            'query GetUser($id: ID) { user(id: $id) }',
            variables={"id": "1"},
        )
        assert result["data"]["user"] is not None

    def test_execute_mutation(self, populated_engine):
        result = populated_engine.execute('mutation { createUser(input: {name: "Bob"}) }')
        assert "data" in result

    def test_execute_logs_to_registry(self, populated_engine):
        populated_engine.execute("{ users }")
        stats = populated_engine.registry.get_execution_stats()
        assert "ok" in stats


class TestIntrospection:
    def test_introspect_returns_schema(self, populated_engine):
        result = populated_engine.introspect()
        assert "__schema" in result["data"]

    def test_introspect_has_types(self, populated_engine):
        result = populated_engine.introspect()
        type_names = [t["name"] for t in result["data"]["__schema"]["types"]]
        assert "User" in type_names
        assert "String" in type_names

    def test_introspect_has_query_type(self, populated_engine):
        result = populated_engine.introspect()
        assert result["data"]["__schema"]["queryType"] == {"name": "Query"}


class TestValidation:
    def test_validate_valid_query(self, populated_engine):
        errors = populated_engine.validate_query("{ users }")
        assert errors == []

    def test_validate_empty_query(self, engine):
        errors = engine.validate_query("")
        assert len(errors) > 0

    def test_validate_mismatched_braces(self, engine):
        errors = engine.validate_query("{ users")
        assert any("brace" in e.lower() or "mismatch" in e.lower() for e in errors)

    def test_validate_unknown_field(self, populated_engine):
        errors = populated_engine.validate_query("{ nonExistent }")
        assert len(errors) > 0


class TestDocsGeneration:
    def test_generate_docs(self, populated_engine):
        docs = populated_engine.generate_docs()
        assert "GraphQL API Documentation" in docs
        assert "User" in docs
        assert "users" in docs

    def test_docs_has_types_section(self, populated_engine):
        docs = populated_engine.generate_docs()
        assert "## Types" in docs

    def test_docs_has_queries_section(self, populated_engine):
        docs = populated_engine.generate_docs()
        assert "## Queries" in docs


class TestQueryParser:
    def test_parse_simple_query(self):
        items = SimpleQueryParser.parse("{ users }")
        assert len(items) >= 1
        assert items[0]["field"] == "users"

    def test_parse_query_with_args(self):
        items = SimpleQueryParser.parse('{ user(id: "1") }')
        assert items[0]["args"].get("id") == "1"


class TestSchemaRegistry:
    def test_save_and_retrieve_schema(self):
        reg = SchemaRegistry(":memory:")
        reg.save_schema("type Query { hello: String }")
        stored = reg.get_latest_schema()
        assert "hello" in stored

    def test_log_and_get_stats(self):
        reg = SchemaRegistry(":memory:")
        reg.log_execution("abc", "ok", 12.5)
        reg.log_execution("def", "error", 5.0)
        stats = reg.get_execution_stats()
        assert "ok" in stats
        assert stats["ok"]["count"] == 1
