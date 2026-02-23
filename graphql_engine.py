"""
BlackRoad GraphQL Engine - Schema generation, resolvers, and query execution
"""
from __future__ import annotations
import re
import json
import sqlite3
import hashlib
import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data-model
# ---------------------------------------------------------------------------

@dataclass
class Field:
    name: str
    type: str
    nullable: bool = True
    description: str = ""
    resolver_fn: Optional[Callable] = None

    def to_sdl(self) -> str:
        t = self.type if self.nullable else f"{self.type}!"
        return f"  {self.name}: {t}"


@dataclass
class Type:
    name: str
    fields: List[Field] = field(default_factory=list)
    description: str = ""

    def to_sdl(self) -> str:
        lines = []
        if self.description:
            lines.append(f'"""\n{self.description}\n"""')
        lines.append(f"type {self.name} {{")
        for f in self.fields:
            if f.description:
                lines.append(f'  """{f.description}"""')
            lines.append(f.to_sdl())
        lines.append("}")
        return "\n".join(lines)


@dataclass
class Argument:
    name: str
    type: str
    nullable: bool = True
    default: Any = None

    def to_sdl(self) -> str:
        t = self.type if self.nullable else f"{self.type}!"
        if self.default is not None:
            return f"{self.name}: {t} = {json.dumps(self.default)}"
        return f"{self.name}: {t}"


@dataclass
class QueryDef:
    name: str
    return_type: str
    args: List[Argument] = field(default_factory=list)
    resolver: Optional[Callable] = None
    description: str = ""

    def to_sdl(self) -> str:
        args_str = ""
        if self.args:
            args_str = "(" + ", ".join(a.to_sdl() for a in self.args) + ")"
        return f"  {self.name}{args_str}: {self.return_type}"


@dataclass
class MutationDef:
    name: str
    input_type: str
    return_type: str
    resolver: Optional[Callable] = None
    description: str = ""

    def to_sdl(self) -> str:
        return f"  {self.name}(input: {self.input_type}): {self.return_type}"


@dataclass
class SubscriptionDef:
    name: str
    return_type: str
    resolver: Optional[Callable] = None
    description: str = ""

    def to_sdl(self) -> str:
        return f"  {self.name}: {self.return_type}"


@dataclass
class Schema:
    types: List[Type] = field(default_factory=list)
    queries: List[QueryDef] = field(default_factory=list)
    mutations: List[MutationDef] = field(default_factory=list)
    subscriptions: List[SubscriptionDef] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Query parsing helpers
# ---------------------------------------------------------------------------

class SimpleQueryParser:
    """Minimal GraphQL query parser — supports flat queries with variables."""

    QUERY_RE = re.compile(
        r'(?:query|mutation)\s*\w*\s*(?:\(([^)]*)\))?\s*\{([^}]+)\}',
        re.S,
    )
    FIELD_RE = re.compile(r'(\w+)\s*(?:\(([^)]*)\))?', re.S)
    ARG_RE = re.compile(r'(\w+)\s*:\s*(\$\w+|"[^"]*"|\d+|true|false|null)')

    @classmethod
    def parse(cls, query_str: str) -> List[Dict[str, Any]]:
        """Return list of {field, args} dicts."""
        m = cls.QUERY_RE.search(query_str)
        if not m:
            body = query_str.strip().strip('{}')
        else:
            body = m.group(2)
        results = []
        for fm in cls.FIELD_RE.finditer(body):
            fname = fm.group(1)
            raw_args = fm.group(2) or ""
            args = {}
            for am in cls.ARG_RE.finditer(raw_args):
                k, v = am.group(1), am.group(2)
                if v.startswith('"'):
                    args[k] = v.strip('"')
                elif v == 'true':
                    args[k] = True
                elif v == 'false':
                    args[k] = False
                elif v == 'null':
                    args[k] = None
                else:
                    try:
                        args[k] = int(v)
                    except ValueError:
                        args[k] = v
            results.append({"field": fname, "args": args})
        return results

    @classmethod
    def extract_variables(cls, query_str: str, variables: Dict) -> Dict:
        """Substitute $var references with actual variable values."""
        result = {}
        for item in cls.parse(query_str):
            for k, v in item["args"].items():
                if isinstance(v, str) and v.startswith("$"):
                    var_name = v[1:]
                    result[k] = variables.get(var_name, v)
                else:
                    result[k] = v
        return result


# ---------------------------------------------------------------------------
# Schema Registry (SQLite)
# ---------------------------------------------------------------------------

class SchemaRegistry:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()

    def _init_db(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS schema_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version TEXT NOT NULL,
                sdl TEXT NOT NULL,
                checksum TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS type_registry (
                name TEXT PRIMARY KEY,
                definition TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS query_registry (
                name TEXT PRIMARY KEY,
                return_type TEXT NOT NULL,
                args TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS execution_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                duration_ms REAL,
                executed_at TEXT NOT NULL
            );
        """)
        self.conn.commit()

    def save_schema(self, sdl: str) -> str:
        checksum = hashlib.sha256(sdl.encode()).hexdigest()[:16]
        version = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        self.conn.execute(
            "INSERT INTO schema_versions (version, sdl, checksum, created_at) VALUES (?,?,?,?)",
            (version, sdl, checksum, datetime.utcnow().isoformat()),
        )
        self.conn.commit()
        return version

    def get_latest_schema(self) -> Optional[str]:
        row = self.conn.execute(
            "SELECT sdl FROM schema_versions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    def register_type(self, name: str, definition: dict):
        self.conn.execute(
            "INSERT OR REPLACE INTO type_registry (name, definition, updated_at) VALUES (?,?,?)",
            (name, json.dumps(definition), datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def register_query(self, name: str, return_type: str, args: list):
        self.conn.execute(
            "INSERT OR REPLACE INTO query_registry (name, return_type, args, updated_at) VALUES (?,?,?,?)",
            (name, return_type, json.dumps(args), datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def log_execution(self, query_hash: str, status: str, duration_ms: float):
        self.conn.execute(
            "INSERT INTO execution_log (query_hash, status, duration_ms, executed_at) VALUES (?,?,?,?)",
            (query_hash, status, duration_ms, datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def get_execution_stats(self) -> Dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt, AVG(duration_ms) as avg_ms FROM execution_log GROUP BY status"
        ).fetchall()
        return {r[0]: {"count": r[1], "avg_ms": round(r[2] or 0, 2)} for r in rows}


# ---------------------------------------------------------------------------
# GraphQL Engine
# ---------------------------------------------------------------------------

class GraphQLEngine:
    """
    Lightweight GraphQL engine with schema generation, query execution,
    introspection, validation, and documentation generation.
    """

    SCALAR_TYPES = {"String", "Int", "Float", "Boolean", "ID"}

    def __init__(self, db_path: str = ":memory:"):
        self.schema = Schema()
        self._resolvers: Dict[str, Callable] = {}
        self._mutation_resolvers: Dict[str, Callable] = {}
        self._subscription_resolvers: Dict[str, Callable] = {}
        self.registry = SchemaRegistry(db_path)
        self._scalar_coercers: Dict[str, Callable] = {
            "Int": int,
            "Float": float,
            "Boolean": lambda v: v if isinstance(v, bool) else str(v).lower() == "true",
            "String": str,
            "ID": str,
        }

    # ------------------------------------------------------------------
    # Schema definition API
    # ------------------------------------------------------------------

    def add_type(self, name: str, fields: List[Dict]) -> Type:
        """Add a GraphQL type to the schema."""
        type_fields = []
        for f in fields:
            type_fields.append(Field(
                name=f["name"],
                type=f["type"],
                nullable=f.get("nullable", True),
                description=f.get("description", ""),
                resolver_fn=f.get("resolver_fn"),
            ))
        t = Type(name=name, fields=type_fields)
        self.schema.types.append(t)
        self.registry.register_type(name, {"fields": [{"name": fi.name, "type": fi.type} for fi in type_fields]})
        logger.debug("Added type: %s", name)
        return t

    def add_query(
        self,
        name: str,
        return_type: str,
        args: Optional[List[Dict]] = None,
        resolver: Optional[Callable] = None,
        description: str = "",
    ) -> QueryDef:
        """Add a query to the schema."""
        query_args = []
        for a in (args or []):
            query_args.append(Argument(
                name=a["name"],
                type=a["type"],
                nullable=a.get("nullable", True),
                default=a.get("default"),
            ))
        q = QueryDef(name=name, return_type=return_type, args=query_args, resolver=resolver, description=description)
        self.schema.queries.append(q)
        if resolver:
            self._resolvers[name] = resolver
        self.registry.register_query(name, return_type, [a["name"] for a in (args or [])])
        logger.debug("Added query: %s -> %s", name, return_type)
        return q

    def add_mutation(
        self,
        name: str,
        input_type: str,
        return_type: str,
        resolver: Optional[Callable] = None,
        description: str = "",
    ) -> MutationDef:
        """Add a mutation to the schema."""
        m = MutationDef(name=name, input_type=input_type, return_type=return_type, resolver=resolver, description=description)
        self.schema.mutations.append(m)
        if resolver:
            self._mutation_resolvers[name] = resolver
        logger.debug("Added mutation: %s", name)
        return m

    def add_subscription(
        self,
        name: str,
        return_type: str,
        resolver: Optional[Callable] = None,
        description: str = "",
    ) -> SubscriptionDef:
        """Add a subscription to the schema."""
        s = SubscriptionDef(name=name, return_type=return_type, resolver=resolver, description=description)
        self.schema.subscriptions.append(s)
        if resolver:
            self._subscription_resolvers[name] = resolver
        return s

    # ------------------------------------------------------------------
    # SDL generation
    # ------------------------------------------------------------------

    def generate_schema_sdl(self) -> str:
        """Generate a valid GraphQL SDL string from the current schema."""
        parts = []

        # Built-in scalars comment
        parts.append('"""BlackRoad GraphQL Engine - Auto-generated Schema"""')
        parts.append("")

        # Scalar declarations for non-standard scalars
        custom_scalars = set()
        for t in self.schema.types:
            for f in t.fields:
                base_type = f.type.strip("[]!").strip()
                if base_type not in self.SCALAR_TYPES and base_type not in {tt.name for tt in self.schema.types}:
                    custom_scalars.add(base_type)
        for s in sorted(custom_scalars):
            parts.append(f"scalar {s}")
        if custom_scalars:
            parts.append("")

        # Types
        for t in self.schema.types:
            parts.append(t.to_sdl())
            parts.append("")

        # Query type
        if self.schema.queries:
            parts.append("type Query {")
            for q in self.schema.queries:
                if q.description:
                    parts.append(f'  """{q.description}"""')
                parts.append(q.to_sdl())
            parts.append("}")
            parts.append("")

        # Mutation type
        if self.schema.mutations:
            parts.append("type Mutation {")
            for m in self.schema.mutations:
                if m.description:
                    parts.append(f'  """{m.description}"""')
                parts.append(m.to_sdl())
            parts.append("}")
            parts.append("")

        # Subscription type
        if self.schema.subscriptions:
            parts.append("type Subscription {")
            for s in self.schema.subscriptions:
                if s.description:
                    parts.append(f'  """{s.description}"""')
                parts.append(s.to_sdl())
            parts.append("}")
            parts.append("")

        sdl = "\n".join(parts).rstrip() + "\n"
        self.registry.save_schema(sdl)
        return sdl

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(
        self,
        query_str: str,
        variables: Optional[Dict] = None,
        context: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Execute a GraphQL query string and return {data, errors}."""
        import time
        variables = variables or {}
        context = context or {}
        start = time.monotonic()
        errors = []
        data = {}

        query_hash = hashlib.md5(query_str.encode()).hexdigest()[:8]

        try:
            parsed = SimpleQueryParser.parse(query_str)
            is_mutation = query_str.strip().startswith("mutation")

            resolver_map = self._mutation_resolvers if is_mutation else self._resolvers

            for item in parsed:
                fname = item["field"]
                raw_args = item["args"]
                # Substitute variables
                resolved_args = {}
                for k, v in raw_args.items():
                    if isinstance(v, str) and v.startswith("$"):
                        resolved_args[k] = variables.get(v[1:], v)
                    else:
                        resolved_args[k] = v

                if fname in resolver_map:
                    try:
                        result = resolver_map[fname](resolved_args, context)
                        data[fname] = result
                    except Exception as exc:
                        errors.append({"message": str(exc), "path": [fname]})
                        data[fname] = None
                else:
                    errors.append({"message": f"No resolver for field '{fname}'", "path": [fname]})
                    data[fname] = None

        except Exception as exc:
            errors.append({"message": f"Parse error: {exc}"})

        duration_ms = (time.monotonic() - start) * 1000
        status = "error" if errors else "ok"
        self.registry.log_execution(query_hash, status, duration_ms)

        response: Dict[str, Any] = {"data": data}
        if errors:
            response["errors"] = errors
        return response

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def introspect(self) -> Dict[str, Any]:
        """Return a full __schema introspection response."""
        type_list = []

        # Built-in scalars
        for scalar in self.SCALAR_TYPES:
            type_list.append({
                "kind": "SCALAR",
                "name": scalar,
                "fields": None,
                "inputFields": None,
                "interfaces": [],
                "enumValues": None,
                "possibleTypes": None,
            })

        # User-defined types
        for t in self.schema.types:
            type_list.append({
                "kind": "OBJECT",
                "name": t.name,
                "description": t.description,
                "fields": [
                    {
                        "name": f.name,
                        "type": {
                            "kind": "SCALAR" if f.type in self.SCALAR_TYPES else "OBJECT",
                            "name": f.type,
                            "ofType": None,
                        },
                        "isDeprecated": False,
                        "deprecationReason": None,
                        "description": f.description,
                    }
                    for f in t.fields
                ],
                "inputFields": None,
                "interfaces": [],
                "enumValues": None,
                "possibleTypes": None,
            })

        # Query type
        query_fields = []
        for q in self.schema.queries:
            query_fields.append({
                "name": q.name,
                "args": [{"name": a.name, "type": {"name": a.type}} for a in q.args],
                "type": {"kind": "OBJECT", "name": q.return_type, "ofType": None},
                "isDeprecated": False,
                "description": q.description,
            })

        if query_fields:
            type_list.append({
                "kind": "OBJECT",
                "name": "Query",
                "fields": query_fields,
                "inputFields": None,
                "interfaces": [],
                "enumValues": None,
                "possibleTypes": None,
            })

        # Mutation type
        mutation_fields = []
        for m in self.schema.mutations:
            mutation_fields.append({
                "name": m.name,
                "args": [{"name": "input", "type": {"name": m.input_type}}],
                "type": {"kind": "OBJECT", "name": m.return_type, "ofType": None},
                "isDeprecated": False,
                "description": m.description,
            })

        if mutation_fields:
            type_list.append({
                "kind": "OBJECT",
                "name": "Mutation",
                "fields": mutation_fields,
                "inputFields": None,
                "interfaces": [],
                "enumValues": None,
                "possibleTypes": None,
            })

        return {
            "data": {
                "__schema": {
                    "queryType": {"name": "Query"} if self.schema.queries else None,
                    "mutationType": {"name": "Mutation"} if self.schema.mutations else None,
                    "subscriptionType": {"name": "Subscription"} if self.schema.subscriptions else None,
                    "types": type_list,
                    "directives": [
                        {"name": "skip", "locations": ["FIELD", "FRAGMENT_SPREAD"]},
                        {"name": "include", "locations": ["FIELD", "FRAGMENT_SPREAD"]},
                        {"name": "deprecated", "locations": ["FIELD_DEFINITION", "ENUM_VALUE"]},
                    ],
                }
            }
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_query(self, query_str: str) -> List[str]:
        """Validate a query string and return a list of error messages."""
        errors = []

        if not query_str or not query_str.strip():
            errors.append("Query string is empty")
            return errors

        # Check basic syntax
        stripped = query_str.strip()
        if not (stripped.startswith("{") or stripped.startswith("query") or stripped.startswith("mutation") or stripped.startswith("subscription")):
            errors.append("Query must start with '{', 'query', 'mutation', or 'subscription'")

        # Count braces
        opens = query_str.count("{")
        closes = query_str.count("}")
        if opens != closes:
            errors.append(f"Mismatched braces: {opens} opening, {closes} closing")

        # Check requested fields exist
        try:
            parsed = SimpleQueryParser.parse(query_str)
            is_mutation = stripped.startswith("mutation")
            is_subscription = stripped.startswith("subscription")

            if is_mutation:
                known = {m.name for m in self.schema.mutations}
            elif is_subscription:
                known = {s.name for s in self.schema.subscriptions}
            else:
                known = {q.name for q in self.schema.queries}

            for item in parsed:
                fname = item["field"]
                if fname and fname not in known:
                    errors.append(f"Unknown field '{fname}' in {'mutation' if is_mutation else 'query'}")
        except Exception as exc:
            errors.append(f"Parse error: {exc}")

        return errors

    # ------------------------------------------------------------------
    # Documentation generation
    # ------------------------------------------------------------------

    def generate_docs(self) -> str:
        """Generate Markdown documentation for the schema."""
        lines = ["# GraphQL API Documentation", "", "## Overview", ""]
        lines.append(f"- **Types**: {len(self.schema.types)}")
        lines.append(f"- **Queries**: {len(self.schema.queries)}")
        lines.append(f"- **Mutations**: {len(self.schema.mutations)}")
        lines.append(f"- **Subscriptions**: {len(self.schema.subscriptions)}")
        lines.append("")

        if self.schema.types:
            lines.append("## Types")
            lines.append("")
            for t in self.schema.types:
                lines.append(f"### `{t.name}`")
                if t.description:
                    lines.append(f"> {t.description}")
                lines.append("")
                lines.append("| Field | Type | Nullable | Description |")
                lines.append("|-------|------|----------|-------------|")
                for f in t.fields:
                    lines.append(f"| `{f.name}` | `{f.type}` | {'✓' if f.nullable else '✗'} | {f.description or '-'} |")
                lines.append("")

        if self.schema.queries:
            lines.append("## Queries")
            lines.append("")
            for q in self.schema.queries:
                lines.append(f"### `{q.name}`")
                if q.description:
                    lines.append(f"> {q.description}")
                lines.append(f"- **Returns**: `{q.return_type}`")
                if q.args:
                    lines.append("- **Arguments**:")
                    for a in q.args:
                        req = "" if a.nullable else " *(required)*"
                        lines.append(f"  - `{a.name}: {a.type}`{req}")
                lines.append("")

        if self.schema.mutations:
            lines.append("## Mutations")
            lines.append("")
            for m in self.schema.mutations:
                lines.append(f"### `{m.name}`")
                if m.description:
                    lines.append(f"> {m.description}")
                lines.append(f"- **Input**: `{m.input_type}`")
                lines.append(f"- **Returns**: `{m.return_type}`")
                lines.append("")

        if self.schema.subscriptions:
            lines.append("## Subscriptions")
            lines.append("")
            for s in self.schema.subscriptions:
                lines.append(f"### `{s.name}`")
                if s.description:
                    lines.append(f"> {s.description}")
                lines.append(f"- **Returns**: `{s.return_type}`")
                lines.append("")

        lines.append("## Execution Stats")
        lines.append("")
        stats = self.registry.get_execution_stats()
        if stats:
            lines.append("| Status | Count | Avg (ms) |")
            lines.append("|--------|-------|----------|")
            for status, info in stats.items():
                lines.append(f"| {status} | {info['count']} | {info['avg_ms']} |")
        else:
            lines.append("*No executions recorded yet.*")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Coercion helpers
    # ------------------------------------------------------------------

    def coerce_arg(self, value: Any, type_name: str) -> Any:
        """Coerce a raw argument value to the expected GraphQL scalar type."""
        coercer = self._scalar_coercers.get(type_name)
        if coercer and value is not None:
            try:
                return coercer(value)
            except (ValueError, TypeError):
                return value
        return value

    # ------------------------------------------------------------------
    # Decorator API
    # ------------------------------------------------------------------

    def resolver(self, query_name: str):
        """Decorator to register a resolver for a query field."""
        def decorator(fn: Callable) -> Callable:
            self._resolvers[query_name] = fn
            return fn
        return decorator

    def mutation_resolver(self, mutation_name: str):
        """Decorator to register a resolver for a mutation field."""
        def decorator(fn: Callable) -> Callable:
            self._mutation_resolvers[mutation_name] = fn
            return fn
        return decorator

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def get_schema_stats(self) -> Dict:
        return {
            "types": len(self.schema.types),
            "queries": len(self.schema.queries),
            "mutations": len(self.schema.mutations),
            "subscriptions": len(self.schema.subscriptions),
        }

    def reset(self):
        """Clear the schema (useful for testing)."""
        self.schema = Schema()
        self._resolvers.clear()
        self._mutation_resolvers.clear()
        self._subscription_resolvers.clear()
