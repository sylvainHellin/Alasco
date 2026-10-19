"""Shared building blocks for the CapEx models: money types and filter params."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, PlainSerializer


def _decimal_to_api_string(value: Decimal) -> str:
    """Render a Decimal as a plain fixed-point string (never ``1E+3``)."""
    return format(value, "f")


# Money goes over the wire as strings; the SDK holds it as Decimal.
DecimalString = Annotated[Decimal, PlainSerializer(_decimal_to_api_string, return_type=str)]


class Amount(BaseModel):
    """Read-side money object: ``{currency, net, tax}`` with string values parsed to Decimal."""

    model_config = ConfigDict(extra="ignore")

    currency: str | None = None
    net: Decimal | None = None
    tax: Decimal | None = None


class NetTaxAmount(BaseModel):
    """Write-side money object without currency (paid-invoice amounts): ``{net, tax}``."""

    model_config = ConfigDict(extra="forbid")

    net: DecimalString
    tax: DecimalString


class ContractAmount(BaseModel):
    """Write-side contract amount: ``{currency, net, tax}`` (net + tax, never gross)."""

    model_config = ConfigDict(extra="forbid")

    currency: str = "EUR"
    net: DecimalString
    tax: DecimalString


def filter_params(
    filters: dict[str, str | Sequence[str] | None],
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build JSON:API filter query params.

    ``filters`` maps ``attribute.operation`` (e.g. ``identifier.exact``,
    ``asset.in``) to a value; ``None`` skips the filter, a sequence is joined
    with commas for ``in`` / ``not_in``. ``extra`` is passed through verbatim
    (full keys such as ``filter[date_created.greater_than_equal]``).

    An empty sequence raises ``ValueError``: silently dropping it would turn
    "match none of these" into "match everything".
    """
    params: dict[str, str] = {}
    for key, value in filters.items():
        if value is None:
            continue
        if isinstance(value, str):
            params[f"filter[{key}]"] = value
            continue
        values = list(value)
        if not values:
            msg = f"filter {key!r} got an empty list; pass None to skip the filter"
            raise ValueError(msg)
        params[f"filter[{key}]"] = ",".join(values)
    if extra:
        params.update(extra)
    return params


def response_data(response: dict[str, Any]) -> dict[str, Any]:
    """Return the ``data`` member of a single-resource JSON:API response."""
    return response.get("data") or {}
