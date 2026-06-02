"""Cost Element model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from alasco.client import AlascoClient


class CostElementAttributes(BaseModel):
    """Attributes for a Cost Element resource."""

    model_config = ConfigDict(extra="ignore")

    name: str
    description: str | None = None
    parent: str | None = None  # UUID reference to parent CostElement
    cost_category: str | None = None
    cost_element_tree_template: str | None = None  # UUID reference


class CostElement(BaseModel):
    """Cost Element resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "cost_element"
    attributes: CostElementAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def parent_id(self) -> str | None:
        return self.attributes.parent

    @classmethod
    def get(cls, client: AlascoClient, cost_element_id: str) -> CostElement:
        """Fetch a single cost element by ID."""
        data = client.get_single(f"/cost_elements/{cost_element_id}/")
        return cls.model_validate(data)

    @classmethod
    def list(
        cls,
        client: AlascoClient,
        cost_element_tree_template_ids: list[str] | None = None,
    ) -> list[CostElement]:
        """List cost elements, optionally filtered by tree template IDs."""
        params = {}
        if cost_element_tree_template_ids:
            params["filter[cost_element_tree_template.in]"] = ",".join(
                cost_element_tree_template_ids
            )
        data = client.get_all("/cost_elements/", params)
        return [cls.model_validate(item) for item in data]
