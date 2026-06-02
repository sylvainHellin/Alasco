"""Contractor model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from alasco.client import AlascoClient


class ContractorAttributes(BaseModel):
    """Attributes for a Contractor resource."""

    model_config = ConfigDict(extra="ignore")

    name: str
    # Additional fields can be added as needed
    # Contact info, tax info, etc.


class Contractor(BaseModel):
    """Contractor resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "contractor"
    attributes: ContractorAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @classmethod
    def get(cls, client: AlascoClient, contractor_id: str) -> Contractor:
        """Fetch a single contractor by ID."""
        data = client.get_single(f"/contractors/{contractor_id}/")
        return cls.model_validate(data)

    @classmethod
    def name_contains(cls, client: AlascoClient, query: str) -> list[Contractor]:
        """List all contractors whose name contains ``query`` (paginated).

        Generic lookup using the Alasco ``filter[name.contains]`` operator.
        Returns ALL matches across pages. Callers decide how to disambiguate
        when more than one contractor matches (e.g. present them to a user).

        Note: the filter key MUST be exactly ``filter[name.contains]`` or the
        API returns HTTP 400.
        """
        params = {"filter[name.contains]": query}
        data = client.get_all("/contractors/", params)
        return [cls.model_validate(item) for item in data]

    @classmethod
    def list(
        cls, client: AlascoClient, ids: list[str] | None = None
    ) -> list[Contractor]:
        """List contractors, optionally filtered by IDs.

        Note: When filtering by many IDs, requests are batched
        to avoid HTTP 414 URI Too Long errors.
        """
        if not ids:
            data = client.get_all("/contractors/", {})
            return [cls.model_validate(item) for item in data]

        # Batch IDs to avoid URI length limits
        batch_size = 50
        all_contractors: list[Contractor] = []

        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i : i + batch_size]
            params = {"filter[id.in]": ",".join(batch_ids)}
            data = client.get_all("/contractors/", params)
            all_contractors.extend(cls.model_validate(item) for item in data)

        return all_contractors
