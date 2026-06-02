"""Minimal OpenAPI schema loader + validator for offline body-shape tests.

Loads ``alasco/openapi/openapi.json`` and validates a request body dict against
a named component schema, following ``$ref``, ``anyOf`` and ``required`` /
property constraints. This is deliberately small (no jsonschema dependency): it
checks that generated request bodies match the verified write contract.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

OPENAPI_PATH = (
    Path(__file__).resolve().parent.parent / "alasco" / "openapi" / "openapi.json"
)


@lru_cache(maxsize=1)
def load_spec() -> dict[str, Any]:
    return json.loads(OPENAPI_PATH.read_text())


def _schemas() -> dict[str, Any]:
    return load_spec()["components"]["schemas"]


def resolve(schema: dict[str, Any]) -> dict[str, Any]:
    """Resolve a top-level ``$ref`` to its target schema."""
    if "$ref" in schema:
        name = schema["$ref"].split("/")[-1]
        return _schemas()[name]
    return schema


def get_schema(name: str) -> dict[str, Any]:
    return _schemas()[name]


def validate(value: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    """Validate ``value`` against ``schema``. Returns a list of error strings.

    Supports the subset of JSON Schema used by the Alasco spec: ``$ref``,
    ``anyOf``, ``type`` (object/string/integer/boolean/null), ``required``,
    ``properties``, ``maxLength``, ``minimum``/``maximum``, ``enum``.
    Object validation rejects properties not declared in ``properties`` (the
    spec's create/update objects are closed in practice).
    """
    errors: list[str] = []
    schema = resolve(schema)

    if "anyOf" in schema:
        # Value is valid if it matches any branch.
        branch_errors = []
        for branch in schema["anyOf"]:
            sub = validate(value, branch, path)
            if not sub:
                return []
            branch_errors.append(sub)
        errors.append(f"{path}: did not match any anyOf branch: {branch_errors}")
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
            errors.extend(validate(val, props[key], f"{path}.{key}"))
        return errors

    if stype == "string":
        if not isinstance(value, str):
            errors.append(f"{path}: expected string, got {type(value).__name__}")
        elif "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{path}: exceeds maxLength {schema['maxLength']}")
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
