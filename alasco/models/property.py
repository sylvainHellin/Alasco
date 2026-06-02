"""Property model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from alasco.client import AlascoClient
    from alasco.models.project import Project


class PropertyAttributes(BaseModel):
    """Attributes for a Property resource."""

    model_config = ConfigDict(extra="ignore")

    name: str
    address: str | None = None
    city: str | None = None
    zip_code: str | None = None
    country: str | None = None
    description: str | None = None
    date_created: str | None = None


class Property(BaseModel):
    """Property resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "property"
    attributes: PropertyAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @classmethod
    def get(cls, client: AlascoClient, property_id: str) -> Property:
        """Fetch a single property by ID."""
        data = client.get_single(f"/properties/{property_id}/")
        return cls.model_validate(data)

    @classmethod
    def list(cls, client: AlascoClient, ids: list[str] | None = None) -> list[Property]:
        """List properties, optionally filtered by IDs.

        Note: When filtering by many IDs, requests are batched
        to avoid HTTP 414 URI Too Long errors.
        """
        if not ids:
            data = client.get_all("/properties/", {})
            return [cls.model_validate(item) for item in data]

        # Batch IDs to avoid URI length limits
        batch_size = 50
        all_properties: list[Property] = []

        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i : i + batch_size]
            params = {"filter[id.in]": ",".join(batch_ids)}
            data = client.get_all("/properties/", params)
            all_properties.extend(cls.model_validate(item) for item in data)

        return all_properties

    def get_projects(self, client: AlascoClient) -> list[Project]:
        """Get all projects for this property."""
        from alasco.models.project import Project

        return Project.list(client, property_ids=[self.id])
