"""Live integration tests against the Alasco SANDBOX property only.

Run with: ``uv run pytest -m integration`` (with sandbox creds in the env).

These prove Contract.create, Contract.update, ContractDocument.upload, and
Contractor.name_contains work end to end. Writes target the sandbox property
exclusively; the conftest guards against the TUC prod id.

Note: the Alasco API has NO contract-delete endpoint, so created sandbox
contracts cannot be removed via the SDK. Created records are tagged with a
"[SDK-TEST]" name prefix so they are identifiable in the sandbox.
"""

from __future__ import annotations

import datetime as dt
from decimal import ROUND_HALF_UP, Decimal

import pytest

from alasco.models import ContractUnit, Project
from alasco.models.contract import Contract
from alasco.models.contractor import Contractor
from alasco.models.documents import ContractDocument
from alasco.workflows import batch, create_contract_as_ordered

pytestmark = pytest.mark.integration

NAME_PREFIX = "[SDK-TEST]"


def _compute_tax(net: str, rate: str = "0.19") -> str:
    tax = (Decimal(net) * Decimal(rate)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return str(tax)


@pytest.fixture(scope="session")
def sandbox_targets(sandbox_property_id):
    """Resolve a usable (contract_unit_id, contracting_entity_id) in the sandbox."""
    import os

    from alasco import AlascoClient

    token = os.environ.get("ALASCO_TEST_TOKEN") or os.environ.get("token_alasco_test_env")
    key = os.environ.get("ALASCO_TEST_KEY") or os.environ.get("key_alasco_test_env")
    if not token or not key:
        pytest.skip("Sandbox credentials not set.")
    with AlascoClient(token=token, key=key) as client:
        projects = Project.list(client, property_ids=[sandbox_property_id])
        assert projects, "No projects in sandbox property"
        project = projects[0]
        entity_id = project.attributes.default_contracting_entity
        assert entity_id, "Sandbox project has no default contracting entity"
        units = ContractUnit.list(client, project_ids=[project.id])
        assert units, "No contract units in sandbox project"
        # CONTINGENCY units (e.g. "Reserve") reject contract assignment (HTTP 422).
        assignable = [u for u in units if u.attributes.state != "CONTINGENCY"]
        assert assignable, "No assignable (non-contingency) contract units in sandbox"
        contractors = Contractor.list(client)
        assert contractors, "No contractors in sandbox"
        return {
            "project_id": project.id,
            "contract_unit_id": assignable[0].id,
            "contracting_entity_id": entity_id,
            "contractor_id": contractors[0].id,
        }


def _ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")


def test_create_contract(sandbox_client, sandbox_targets):
    net = "1000.00"
    contract = Contract.create(
        sandbox_client,
        name=f"{NAME_PREFIX} create {_ts()}",
        is_reverse_charge=False,
        contract_unit_id=sandbox_targets["contract_unit_id"],
        contracting_entity_id=sandbox_targets["contracting_entity_id"],
        amount_net=net,
        amount_tax=_compute_tax(net),
        contract_number=f"SDKTEST-{_ts()}",
        date_contract="2024-05-19",
        description="Created by alasco SDK integration test",
    )
    assert contract.id
    assert contract.name.startswith(NAME_PREFIX)


def test_update_contract_status(sandbox_client, sandbox_targets):
    net = "500.00"
    contract = Contract.create(
        sandbox_client,
        name=f"{NAME_PREFIX} update {_ts()}",
        is_reverse_charge=False,
        contract_unit_id=sandbox_targets["contract_unit_id"],
        contracting_entity_id=sandbox_targets["contracting_entity_id"],
        contractor_id=sandbox_targets["contractor_id"],
        amount_net=net,
        amount_tax=_compute_tax(net),
    )
    # Promotion to ORDERED requires a contractor to be set (Alasco rule).
    updated = Contract.update(sandbox_client, contract.id, status="ORDERED")
    assert updated.id == contract.id
    # Re-fetch to confirm status persisted.
    fetched = Contract.get(sandbox_client, contract.id)
    assert fetched.attributes.status == "ORDERED"


def test_upload_document(sandbox_client, sandbox_targets, minimal_pdf_bytes):
    net = "250.00"
    contract = Contract.create(
        sandbox_client,
        name=f"{NAME_PREFIX} upload {_ts()}",
        is_reverse_charge=False,
        contract_unit_id=sandbox_targets["contract_unit_id"],
        contracting_entity_id=sandbox_targets["contracting_entity_id"],
        amount_net=net,
        amount_tax=_compute_tax(net),
    )
    doc = ContractDocument.upload(
        sandbox_client,
        contract.id,
        document_type="CONTRACT",
        content=minimal_pdf_bytes,
        filename=f"sdk-test-{_ts()}.pdf",
    )
    assert doc.id
    assert doc.document_type == "CONTRACT"
    docs = ContractDocument.list(sandbox_client, contract.id)
    assert any(d.id == doc.id for d in docs)


def test_name_contains_returns_list(sandbox_client):
    # Pick a substring known to exist in the sandbox contractor set.
    all_contractors = Contractor.list(sandbox_client)
    assert all_contractors, "No contractors in sandbox"
    sample_name = all_contractors[0].name
    query = sample_name[:4]
    results = Contractor.name_contains(sandbox_client, query)
    assert isinstance(results, list)
    assert all(query.lower() in r.name.lower() for r in results)


def test_workflow_create_as_ordered(sandbox_client, sandbox_targets):
    net = "750.00"
    contract = create_contract_as_ordered(
        sandbox_client,
        name=f"{NAME_PREFIX} ordered {_ts()}",
        is_reverse_charge=False,
        contract_unit_id=sandbox_targets["contract_unit_id"],
        contracting_entity_id=sandbox_targets["contracting_entity_id"],
        contractor_id=sandbox_targets["contractor_id"],
        amount_net=net,
        amount_tax=_compute_tax(net),
    )
    fetched = Contract.get(sandbox_client, contract.id)
    assert fetched.attributes.status == "ORDERED"


def test_workflow_batch_per_item_results(sandbox_client, sandbox_targets):
    nets = ["100.00", "200.00"]

    def make(net: str) -> Contract:
        return Contract.create(
            sandbox_client,
            name=f"{NAME_PREFIX} batch {_ts()} {net}",
            is_reverse_charge=False,
            contract_unit_id=sandbox_targets["contract_unit_id"],
            contracting_entity_id=sandbox_targets["contracting_entity_id"],
            amount_net=net,
            amount_tax=_compute_tax(net),
        )

    result = batch(nets, make)
    assert result.all_ok
    assert len(result.succeeded) == 2
