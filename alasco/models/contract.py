"""Contract model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from alasco.models.base import MonetaryAmount

if TYPE_CHECKING:
    from alasco.client import AlascoClient
    from alasco.models.change_order import ChangeOrder
    from alasco.models.documents import ContractDocument
    from alasco.models.invoice import Invoice


class ContractAttributes(BaseModel):
    """Attributes for a Contract resource (FinCon API)."""

    model_config = ConfigDict(extra="ignore")

    name: str
    contract_number: str | None = None
    description: str | None = None
    contract_unit: str  # UUID reference to ContractUnit
    contractor: str | None = None  # UUID reference to Contractor
    contracting_entity: str | None = None  # UUID reference
    contract_type: str | None = None  # ContractType enum
    status: str | None = None  # ContractStatus enum
    amount: MonetaryAmount | dict[str, Any] | None = None
    reserve_amount: MonetaryAmount | dict[str, Any] | None = None
    date_contract: str | None = None
    date_created: str | None = None
    is_reverse_charge: bool = False
    booking_account_number: int | None = None
    cost_center: str | None = None


class Contract(BaseModel):
    """Contract resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "contract"
    attributes: ContractAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def contract_number(self) -> str | None:
        return self.attributes.contract_number

    @property
    def contract_unit_id(self) -> str:
        return self.attributes.contract_unit

    @property
    def contractor_id(self) -> str | None:
        return self.attributes.contractor

    @classmethod
    def get(cls, client: AlascoClient, contract_id: str) -> Contract:
        """Fetch a single contract by ID."""
        data = client.get_single(f"/contracts/{contract_id}/")
        return cls.model_validate(data)

    @classmethod
    def create(
        cls,
        client: AlascoClient,
        *,
        name: str,
        is_reverse_charge: bool,
        contract_unit_id: str,
        contracting_entity_id: str,
        contractor_id: str | None = None,
        amount_net: str | None = None,
        amount_tax: str | None = None,
        currency: str = "EUR",
        booking_account_number: int | None = None,
        contract_number: str | None = None,
        cost_center: str | None = None,
        date_contract: str | None = None,
        description: str | None = None,
    ) -> Contract:
        """Create a new contract via ``POST /contracts/``.

        The created contract is always a DRAFT: the create endpoint has no
        ``status`` and no ``contract_type``. ``contract_unit`` and
        ``contracting_entity`` are both required; ``contractor`` is optional.

        Money is sent as 2-decimal STRINGS using net + tax (not gross). Pass
        both ``amount_net`` and ``amount_tax`` together, or neither.

        Returns:
            The created Contract (parsed from the response ``data``).
        """
        from alasco.models.write import (
            RELATIONSHIP_TYPE_CONTRACT_UNIT,
            RELATIONSHIP_TYPE_CONTRACTING_ENTITY,
            RELATIONSHIP_TYPE_CONTRACTOR,
            CreateContractAttributes,
            CreateContractData,
            CreateContractRelationships,
            CreateContractRequest,
            RelationshipData,
            RelationshipRef,
            WriteAmount,
        )

        if (amount_net is None) != (amount_tax is None):
            msg = "amount_net and amount_tax must be provided together (or neither)"
            raise ValueError(msg)

        amount = (
            WriteAmount(currency=currency, net=amount_net, tax=amount_tax)
            if amount_net is not None and amount_tax is not None
            else None
        )

        contractor_rel = (
            RelationshipData(
                data=RelationshipRef(id=contractor_id, type=RELATIONSHIP_TYPE_CONTRACTOR)
            )
            if contractor_id is not None
            else None
        )

        request = CreateContractRequest(
            data=CreateContractData(
                attributes=CreateContractAttributes(
                    name=name,
                    is_reverse_charge=is_reverse_charge,
                    amount=amount,
                    booking_account_number=booking_account_number,
                    contract_number=contract_number,
                    cost_center=cost_center,
                    date_contract=date_contract,
                    description=description,
                ),
                relationships=CreateContractRelationships(
                    contracting_entity=RelationshipData(
                        data=RelationshipRef(
                            id=contracting_entity_id,
                            type=RELATIONSHIP_TYPE_CONTRACTING_ENTITY,
                        )
                    ),
                    contract_unit=RelationshipData(
                        data=RelationshipRef(
                            id=contract_unit_id, type=RELATIONSHIP_TYPE_CONTRACT_UNIT
                        )
                    ),
                    contractor=contractor_rel,
                ),
            )
        )

        response = client.post("/contracts/", json=request.to_api_dict())
        return cls.model_validate(response.get("data", {}))

    @classmethod
    def update(
        cls,
        client: AlascoClient,
        contract_id: str,
        *,
        status: str | None = None,
        contracting_entity_id: str | None = None,
        contractor_id: str | None = None,
        name: str | None = None,
        amount_net: str | None = None,
        amount_tax: str | None = None,
        currency: str = "EUR",
        booking_account_number: int | None = None,
        contract_number: str | None = None,
        cost_center: str | None = None,
        date_contract: str | None = None,
        description: str | None = None,
    ) -> Contract:
        """Update a contract via ``PATCH /contracts/{id}/``.

        ``status`` is patchable. Relationships on update are only
        ``contracting_entity`` and ``contractor`` (NOT ``contract_unit``).

        Money is sent as 2-decimal STRINGS using net + tax. Pass both
        ``amount_net`` and ``amount_tax`` together, or neither.

        Returns:
            The updated Contract (parsed from the response ``data``).
        """
        from alasco.models.write import (
            RELATIONSHIP_TYPE_CONTRACTING_ENTITY,
            RELATIONSHIP_TYPE_CONTRACTOR,
            RelationshipData,
            RelationshipRef,
            UpdateContractAttributes,
            UpdateContractData,
            UpdateContractRelationships,
            UpdateContractRequest,
            WriteAmount,
        )

        if (amount_net is None) != (amount_tax is None):
            msg = "amount_net and amount_tax must be provided together (or neither)"
            raise ValueError(msg)

        amount = (
            WriteAmount(currency=currency, net=amount_net, tax=amount_tax)
            if amount_net is not None and amount_tax is not None
            else None
        )

        relationships: UpdateContractRelationships | None = None
        if contracting_entity_id is not None or contractor_id is not None:
            relationships = UpdateContractRelationships(
                contracting_entity=(
                    RelationshipData(
                        data=RelationshipRef(
                            id=contracting_entity_id,
                            type=RELATIONSHIP_TYPE_CONTRACTING_ENTITY,
                        )
                    )
                    if contracting_entity_id is not None
                    else None
                ),
                contractor=(
                    RelationshipData(
                        data=RelationshipRef(
                            id=contractor_id, type=RELATIONSHIP_TYPE_CONTRACTOR
                        )
                    )
                    if contractor_id is not None
                    else None
                ),
            )

        request = UpdateContractRequest(
            data=UpdateContractData(
                id=contract_id,
                attributes=UpdateContractAttributes(
                    name=name,
                    status=status,
                    amount=amount,
                    booking_account_number=booking_account_number,
                    contract_number=contract_number,
                    cost_center=cost_center,
                    date_contract=date_contract,
                    description=description,
                ),
                relationships=relationships,
            )
        )

        response = client.patch(
            f"/contracts/{contract_id}/", json=request.to_api_dict()
        )
        return cls.model_validate(response.get("data", {}))

    def get_invoices(self, client: AlascoClient) -> list[Invoice]:
        """Fetch invoices for this contract."""
        from alasco.models.invoice import Invoice

        return Invoice.list(client, contract_ids=[self.id])

    def get_change_orders(self, client: AlascoClient) -> list[ChangeOrder]:
        """Fetch change orders for this contract."""
        from alasco.models.change_order import ChangeOrder

        return ChangeOrder.list(client, contract_ids=[self.id])

    def get_documents(self, client: AlascoClient) -> list[ContractDocument]:
        """Fetch documents for this contract."""
        from alasco.models.documents import ContractDocument

        return ContractDocument.list(client, contract_id=self.id)

    @classmethod
    def list(
        cls, client: AlascoClient, contract_unit_ids: list[str] | None = None
    ) -> list[Contract]:
        """List contracts, optionally filtered by contract unit IDs.

        Note: When filtering by many contract_unit_ids, requests are batched
        to avoid HTTP 414 URI Too Long errors.
        """
        if not contract_unit_ids:
            data = client.get_all("/contracts/", {})
            return [cls.model_validate(item) for item in data]

        # Batch contract_unit_ids to avoid URI length limits
        # Each UUID is ~36 chars + comma, ~50 IDs per batch keeps URLs reasonable
        batch_size = 50
        all_contracts: list[Contract] = []

        for i in range(0, len(contract_unit_ids), batch_size):
            batch_ids = contract_unit_ids[i : i + batch_size]
            params = {"filter[contract_unit.in]": ",".join(batch_ids)}
            data = client.get_all("/contracts/", params)
            all_contracts.extend(cls.model_validate(item) for item in data)

        return all_contracts
