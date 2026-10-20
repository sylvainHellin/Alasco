"""Minimal OpenAPI schema loader + validator for offline body-shape tests.

Loads ``alasco/openapi/openapi.json`` (FinCon, the default) or
``alasco/openapi/capex-openapi.json`` (CapEx, ``spec=CAPEX``) and validates a
request body dict against a named component schema, following ``$ref``, ``anyOf`` and ``required`` /
property constraints. This is deliberately small (no jsonschema dependency): it
checks that generated request bodies match the verified write contract.
"""

from __future__ import annotations

import json
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

OPENAPI_DIR = Path(__file__).resolve().parent.parent / "alasco" / "openapi"
FINCON = "openapi.json"
CAPEX = "capex-openapi.json"
OPENAPI_PATH = OPENAPI_DIR / FINCON


@lru_cache(maxsize=2)
def load_spec(spec: str = FINCON) -> dict[str, Any]:
    return json.loads((OPENAPI_DIR / spec).read_text())


def _schemas(spec: str = FINCON) -> dict[str, Any]:
    return load_spec(spec)["components"]["schemas"]


def resolve(schema: dict[str, Any], spec: str = FINCON) -> dict[str, Any]:
    """Resolve a top-level ``$ref`` to its target schema."""
    if "$ref" in schema:
        name = schema["$ref"].split("/")[-1]
        return _schemas(spec)[name]
    return schema


def get_schema(name: str, spec: str = FINCON) -> dict[str, Any]:
    return _schemas(spec)[name]


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return len(value) == 10


def validate(value: Any, schema: dict[str, Any], path: str = "$", spec: str = FINCON) -> list[str]:
    """Validate ``value`` against ``schema``. Returns a list of error strings.

    Supports the subset of JSON Schema used by the Alasco spec: ``$ref``,
    ``anyOf``, ``type`` (object/string/integer/boolean/null), ``required``,
    ``properties``, ``maxLength``, ``minimum``/``maximum``, ``enum``, ``const``,
    ``format: date``.
    Object validation rejects properties not declared in ``properties`` (the
    spec's create/update objects are closed in practice).
    """
    errors: list[str] = []
    schema = resolve(schema, spec)

    if "anyOf" in schema:
        # Value is valid if it matches any branch.
        branch_errors = []
        for branch in schema["anyOf"]:
            sub = validate(value, branch, path, spec)
            if not sub:
                return []
            branch_errors.append(sub)
        errors.append(f"{path}: did not match any anyOf branch: {branch_errors}")
        return errors

    if "const" in schema:
        if value != schema["const"]:
            errors.append(f"{path}: {value!r} != const {schema['const']!r}")
        return errors

    if "enum" in schema:
        if value not in schema["enum"]:
            errors.append(f"{path}: {value!r} not in enum {schema['enum']}")
        return errors

    stype = schema.get("type")

    if stype == "null":
        if value is not None:
            errors.append(f"{path}: expected null, got {value!r}")
        return errors

    if stype == "object" or "properties" in schema:
        if not isinstance(value, dict):
            errors.append(f"{path}: expected object, got {type(value).__name__}")
            return errors
        props = schema.get("properties", {})
        required = schema.get("required", [])
        for req in required:
            if req not in value:
                errors.append(f"{path}: missing required property '{req}'")
        for key, val in value.items():
            if key not in props:
                errors.append(f"{path}: unexpected property '{key}'")
                continue
            errors.extend(validate(val, props[key], f"{path}.{key}", spec))
        return errors

    if stype == "string":
        if not isinstance(value, str):
            errors.append(f"{path}: expected string, got {type(value).__name__}")
        elif "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{path}: exceeds maxLength {schema['maxLength']}")
        elif schema.get("format") == "date" and not _is_iso_date(value):
            errors.append(f"{path}: {value!r} is not an ISO date (YYYY-MM-DD)")
        return errors

    if stype == "integer":
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(f"{path}: expected integer, got {type(value).__name__}")
            return errors
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path}: below minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path}: above maximum {schema['maximum']}")
        return errors

    if stype == "boolean":
        if not isinstance(value, bool):
            errors.append(f"{path}: expected boolean, got {type(value).__name__}")
        return errors

    # No type constraint (e.g. free object) -> accept.
    return errors
