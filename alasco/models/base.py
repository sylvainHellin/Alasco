"""Base models for Alasco API responses."""

from typing import Any

from pydantic import BaseModel, ConfigDict


class MonetaryAmount(BaseModel):
    """Monetary amount with net and gross values."""

    model_config = ConfigDict(extra="ignore")

    net: str | None = None
    gross: str | None = None

    def net_float(self) -> float:
        """Get net amount as float."""
        if self.net is None:
            return 0.0
        return float(self.net)

    def gross_float(self) -> float:
        """Get gross amount as float."""
        if self.gross is None:
            return 0.0
        return float(self.gross)


class ResourceData[T: BaseModel](BaseModel):
    """JSON:API resource object wrapper."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str
    attributes: T
    relationships: dict[str, Any] | None = None


class PaginationLinks(BaseModel):
    """Pagination links from JSON:API response."""

    model_config = ConfigDict(extra="ignore")

    next: str | None = None
    prev: str | None = None
    first: str | None = None
    last: str | None = None


class ListResponse[T: BaseModel](BaseModel):
    """JSON:API list response wrapper."""

    model_config = ConfigDict(extra="ignore")

    data: list[ResourceData[T]]
    included: list[dict[str, Any]] | None = None
    links: PaginationLinks | None = None
    errors: list[dict[str, Any]] | None = None
