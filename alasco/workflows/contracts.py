"""Contract-related workflows.

Property-agnostic, depends only on the core SDK.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alasco.models.contract import Contract

if TYPE_CHECKING:
    from alasco.client import AlascoClient


def create_contract_as_ordered(
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
    """Create a contract and immediately promote it to ORDERED.

    The Alasco create endpoint cannot set ``status`` (a created contract is
    always DRAFT), so "create as ordered" is necessarily two steps: create the
    DRAFT, then PATCH ``status=ORDERED``.

    Returns:
        The contract after the status PATCH (status=ORDERED).
    """
    created = Contract.create(
        client,
        name=name,
        is_reverse_charge=is_reverse_charge,
        contract_unit_id=contract_unit_id,
        contracting_entity_id=contracting_entity_id,
        contractor_id=contractor_id,
        amount_net=amount_net,
        amount_tax=amount_tax,
        currency=currency,
        booking_account_number=booking_account_number,
        contract_number=contract_number,
        cost_center=cost_center,
        date_contract=date_contract,
        description=description,
    )
    return Contract.update(client, created.id, status="ORDERED")
