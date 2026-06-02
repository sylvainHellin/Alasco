"""Contract Unit model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from alasco.client import AlascoClient
    from alasco.models.contract import Contract


class ContractUnitAttributes(BaseModel):
    """Attributes for a Contract Unit resource."""

    model_config = ConfigDict(extra="ignore")

    name: str
    description: str | None = None
    project: str  # UUID reference to Project
    state: str | None = None  # ContractUnitState enum


class ContractUnit(BaseModel):
    """Contract Unit resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "contract_unit"
    attributes: ContractUnitAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def project_id(self) -> str:
        return self.attributes.project

    @classmethod
    def get(cls, client: AlascoClient, contract_unit_id: str) -> ContractUnit:
        """Fetch a single contract unit by ID."""
        data = client.get_single(f"/contract_units/{contract_unit_id}/")
        return cls.model_validate(data)

    @classmethod
    def list(
        cls, client: AlascoClient, project_ids: list[str] | None = None
    ) -> list[ContractUnit]:
        """List contract units, optionally filtered by project IDs."""
        params = {}
        if project_ids:
            params["filter[project.in]"] = ",".join(project_ids)
        data = client.get_all("/contract_units/", params)
        return [cls.model_validate(item) for item in data]

    def get_contracts(self, client: AlascoClient) -> list[Contract]:
        """Get all contracts for this contract unit."""
        from alasco.models.contract import Contract

        return Contract.list(client, contract_unit_ids=[self.id])
