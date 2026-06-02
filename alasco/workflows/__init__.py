"""Opinionated, property-agnostic multi-step workflows over the core SDK.

This module depends on the core (``alasco.client`` + ``alasco.models``) and
never the reverse. It contains NO pandas/openpyxl and NO property-specific
logic: it is reusable orchestration that applications may use or replace with
their own.
"""

from alasco.workflows.contracts import create_contract_as_ordered
from alasco.workflows.runner import BatchResult, ItemResult, batch

__all__ = [
    "BatchResult",
    "ItemResult",
    "batch",
    "create_contract_as_ordered",
]
