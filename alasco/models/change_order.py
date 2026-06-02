"""ChangeOrder model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from alasco.models.base import MonetaryAmount

if TYPE_CHECKING:
    from alasco.client import AlascoClient
    from alasco.models.documents import ChangeOrderDocument


class ChangeOrderAttributes(BaseModel):
    """Attributes for a ChangeOrder resource (FinCon API)."""

    model_config = ConfigDict(extra="ignore")

    # Required fields
    name: str
    identifier: str  # Change order number
    contract: str  # UUID reference to Contract
    change_order_date: str  # ISO date
    state: str  # OPEN | DETAILS_ENTERED | ACKNOWLEDGED | REJECTED
    basis_of_claim: str  # BasisOfClaim enum
    trigger: str  # Trigger enum

    # Optional fields
    other_basis_of_claim: str = ""
    other_trigger: str = ""
    acknowledged_at: str | None = None  # ISO datetime
    declined_at: str | None = None  # ISO datetime
    weighting_factor: float | None = None

    # Monetary fields
    approved_amount: MonetaryAmount | dict | None = None
    audited_amount: MonetaryAmount | dict | None = None
    claimed_amount: MonetaryAmount | dict | None = None


class ChangeOrder(BaseModel):
    """ChangeOrder resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "change_order"
    attributes: ChangeOrderAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def identifier(self) -> str:
        return self.attributes.identifier

    @property
    def contract_id(self) -> str:
        return self.attributes.contract

    @property
    def state(self) -> str:
        return self.attributes.state

    def get_documents(self, client: AlascoClient) -> list[ChangeOrderDocument]:
        """Fetch documents for this change order."""
        from alasco.models.documents import ChangeOrderDocument

        return ChangeOrderDocument.list(client, change_order_id=self.id)

    @classmethod
    def get(cls, client: AlascoClient, change_order_id: str) -> ChangeOrder:
        """Fetch a single change order by ID."""
        data = client.get_single(f"/change_orders/{change_order_id}/")
        return cls.model_validate(data)

    @classmethod
    def list(
        cls,
        client: AlascoClient,
        contract_ids: list[str] | None = None,
        state: list[str] | None = None,
    ) -> list[ChangeOrder]:
        """List change orders with optional filters.

        Args:
            client: AlascoClient instance
            contract_ids: Filter by contract UUIDs (filter[contract.in])
            state: Filter by state (filter[state.in])
        """
        params: dict[str, str] = {}
        if contract_ids:
            params["filter[contract.in]"] = ",".join(contract_ids)
        if state:
            params["filter[state.in]"] = ",".join(state)

        data = client.get_all("/change_orders/", params or None)
        return [cls.model_validate(item) for item in data]
