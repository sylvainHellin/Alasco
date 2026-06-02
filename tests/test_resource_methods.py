"""Offline unit tests for resource write methods (no network).

Use a fake client that captures requests and returns canned JSON:API responses,
proving Contract.create / Contract.update / ContractDocument.upload /
Contractor.name_contains build correct endpoints + bodies.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from alasco.models.contract import Contract
from alasco.models.contractor import Contractor
from alasco.models.documents import ContractDocument
from tests.openapi_schema import get_schema, validate

UNIT_ID = "11111111-1111-1111-1111-111111111111"
ENTITY_ID = "22222222-2222-2222-2222-222222222222"
CONTRACTOR_ID = "33333333-3333-3333-3333-333333333333"
CONTRACT_ID = "44444444-4444-4444-4444-444444444444"


class FakeClient:
    """Captures the last request and returns canned responses."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def post(self, endpoint: str, json: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"verb": "post", "endpoint": endpoint, "json": json})
        return {
            "data": {
                "id": CONTRACT_ID,
                "type": "contract",
                "attributes": {"name": json["data"]["attributes"]["name"],
                               "contract_unit": UNIT_ID},
            }
        }

    def patch(self, endpoint: str, json: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"verb": "patch", "endpoint": endpoint, "json": json})
        return {
            "data": {
                "id": CONTRACT_ID,
                "type": "contract",
                "attributes": {"name": "n", "contract_unit": UNIT_ID,
                               "status": json["data"]["attributes"].get("status")},
            }
        }

    def post_multipart(
        self, endpoint: str, data: dict[str, Any] | None = None, files: Any = None
    ) -> dict[str, Any]:
        self.calls.append(
            {"verb": "multipart", "endpoint": endpoint, "data": data, "files": files}
        )
        return {
            "data": {
                "id": "doc-1",
                "type": "contract_document",
                "attributes": {
                    "document_type": data["document_type"],
                    "filename": files["upload"][0],
                    "uploaded_at": "2024-01-01T00:00:00Z",
                },
            }
        }

    def get_all(self, endpoint: str, params: dict[str, Any] | None = None):
        self.calls.append({"verb": "get_all", "endpoint": endpoint, "params": params})
        return [
            {"id": CONTRACTOR_ID, "type": "contractor",
             "attributes": {"name": "Bauer GmbH (394909)"}},
        ]


def test_contract_create_builds_valid_body():
    client = FakeClient()
    contract = Contract.create(
        client,
        name="My contract",
        is_reverse_charge=False,
        contract_unit_id=UNIT_ID,
        contracting_entity_id=ENTITY_ID,
        contractor_id=CONTRACTOR_ID,
        amount_net="1000.00",
        amount_tax="190.00",
        contract_number="23-51112-V3-001",
        date_contract="2024-05-19",
    )
    assert contract.id == CONTRACT_ID
    call = client.calls[0]
    assert call["verb"] == "post"
    assert call["endpoint"] == "/contracts/"
    errors = validate(call["json"], get_schema("CreateContractRequestFincon"))
    assert errors == [], errors
    # relationship type strings are uppercase
    rels = call["json"]["data"]["relationships"]
    assert rels["contract_unit"]["data"]["type"] == "CONTRACT_UNIT"
    assert rels["contracting_entity"]["data"]["type"] == "CONTRACTING_ENTITY"
    assert rels["contractor"]["data"]["type"] == "CONTRACTOR"


def test_contract_create_rejects_partial_amount():
    client = FakeClient()
    with pytest.raises(ValueError):
        Contract.create(
            client,
            name="x",
            is_reverse_charge=False,
            contract_unit_id=UNIT_ID,
            contracting_entity_id=ENTITY_ID,
            amount_net="1000.00",  # tax missing
        )


def test_contract_update_builds_valid_body():
    client = FakeClient()
    Contract.update(
        client,
        CONTRACT_ID,
        status="ORDERED",
        contractor_id=CONTRACTOR_ID,
    )
    call = client.calls[0]
    assert call["verb"] == "patch"
    assert call["endpoint"] == f"/contracts/{CONTRACT_ID}/"
    errors = validate(call["json"], get_schema("UpdateContractRequest"))
    assert errors == [], errors
    assert call["json"]["data"]["attributes"]["status"] == "ORDERED"


def test_document_upload_builds_multipart():
    client = FakeClient()
    doc = ContractDocument.upload(
        client,
        CONTRACT_ID,
        document_type="CONTRACT",
        content=b"%PDF-1.4 fake",
        filename="contract.pdf",
    )
    assert doc.document_type == "CONTRACT"
    call = client.calls[0]
    assert call["verb"] == "multipart"
    assert call["endpoint"] == f"/contracts/{CONTRACT_ID}/documents/"
    assert call["data"] == {"document_type": "CONTRACT"}
    assert call["files"]["upload"][0] == "contract.pdf"
    assert call["files"]["upload"][2] == "application/pdf"


def test_document_upload_rejects_bad_type():
    client = FakeClient()
    with pytest.raises(ValidationError):
        ContractDocument.upload(
            client, CONTRACT_ID, document_type="INVOICE", content=b"x", filename="a.pdf"
        )


def test_contractor_name_contains_uses_correct_filter():
    client = FakeClient()
    results = Contractor.name_contains(client, "394909")
    call = client.calls[0]
    assert call["endpoint"] == "/contractors/"
    assert call["params"] == {"filter[name.contains]": "394909"}
    assert isinstance(results, list)
    assert results[0].name == "Bauer GmbH (394909)"
