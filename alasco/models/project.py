"""Project model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from alasco.client import AlascoClient
    from alasco.models.contract_unit import ContractUnit


class ProjectAttributes(BaseModel):
    """Attributes for a Project resource."""

    model_config = ConfigDict(extra="ignore")

    name: str
    identifier: str | None = None
    description: str | None = None
    property: str  # UUID reference to Property
    is_active: bool = True
    cost_element_tree_template: str | None = None
    default_contracting_entity: str | None = None
    default_tax_rate: str | None = None
    # Area fields
    gross_floor_area: str | None = None
    rental_area: str | None = None
    residential_area: str | None = None
    commercial_area: str | None = None
    effective_floor_area: str | None = None
    gross_volume: str | None = None
    commercial_usage_ratio: str | None = None
    # Date fields
    estimated_start_date: str | None = None
    estimated_end_date: str | None = None


class Project(BaseModel):
    """Project resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "project"
    attributes: ProjectAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def property_id(self) -> str:
        return self.attributes.property

    @classmethod
    def get(cls, client: AlascoClient, project_id: str) -> Project:
        """Fetch a single project by ID."""
        data = client.get_single(f"/projects/{project_id}/")
        return cls.model_validate(data)

    @classmethod
    def list(
        cls, client: AlascoClient, property_ids: list[str] | None = None
    ) -> list[Project]:
        """List projects, optionally filtered by property IDs."""
        params = {}
        if property_ids:
            params["filter[property.in]"] = ",".join(property_ids)
        data = client.get_all("/projects/", params)
        return [cls.model_validate(item) for item in data]

    def get_contract_units(self, client: AlascoClient) -> list[ContractUnit]:
        """Get all contract units for this project."""
        from alasco.models.contract_unit import ContractUnit

        return ContractUnit.list(client, project_ids=[self.id])
