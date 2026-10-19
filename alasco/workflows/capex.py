"""CapEx workflows. Property-agnostic, depends only on the core SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING

from alasco.models.capex import Contract

if TYPE_CHECKING:
    from datetime import date
    from decimal import Decimal

    from alasco.client import CapexClient


def create_capex_contract_as_ordered(
    client: CapexClient,
    *,
    name: str,
    is_reverse_charge: bool,
    measure_id: str,
    contracting_entity_id: str,
    contractor_id: str | None = None,
    amount_net: Decimal | None = None,
    amount_tax: Decimal | None = None,
    currency: str = "EUR",
    booking_account_number: int | None = None,
    contract_number: str | None = None,
    cost_center: str | None = None,
    date_contract: date | None = None,
    description: str | None = None,
) -> Contract:
    """Create a CapEx contract and promote it to ORDERED.

    ``POST /contracts/`` cannot set ``status``, so this is two requests: create
    the DRAFT, then ``PATCH status=ORDERED``. Not atomic: if the PATCH fails,
    the DRAFT contract stays in Alasco and the error propagates.
    """
    created = Contract.create(
        client,
        name=name,
        is_reverse_charge=is_reverse_charge,
        measure_id=measure_id,
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
