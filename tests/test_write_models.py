"""Offline unit tests: validate WRITE model request bodies against openapi.json.

No network. These prove the generated JSON:API bodies match the verified write
contract (CreateContractRequestFincon / UpdateContractRequest / document upload).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from alasco.models.write import (
    ContractDocumentUpload,
    CreateContractAttributes,
    CreateContractData,
    CreateContractRelationships,
    CreateContractRequest,
    RelationshipData,
    RelationshipRef,
    UpdateContractAttributes,
    UpdateContractData,
    UpdateContractRelationships,
    UpdateContractRequest,
    WriteAmount,
)
from tests.openapi_schema import get_schema, validate

UNIT_ID = "11111111-1111-1111-1111-111111111111"
ENTITY_ID = "22222222-2222-2222-2222-222222222222"
CONTRACTOR_ID = "33333333-3333-3333-3333-333333333333"
CONTRACT_ID = "44444444-4444-4444-4444-444444444444"


def _create_request(**overrides) -> CreateContractRequest:
    attrs = dict(name="Test contract", is_reverse_charge=False)
    attrs.update(overrides.pop("attributes", {}))
    contractor = overrides.pop("contractor", None)
    return CreateContractRequest(
        data=CreateContractData(
            attributes=CreateContractAttributes(**attrs),
            relationships=CreateContractRelationships(
                contracting_entity=RelationshipData(
                    data=RelationshipRef(id=ENTITY_ID, type="CONTRACTING_ENTITY")
                ),
                contract_unit=RelationshipData(
                    data=RelationshipRef(id=UNIT_ID, type="CONTRACT_UNIT")
                ),
                contractor=contractor,
            ),
        )
    )


def test_create_minimal_matches_spec():
    body = _create_request().to_api_dict()
    errors = validate(body, get_schema("CreateContractRequestFincon"))
    assert errors == [], errors


def test_create_full_matches_spec():
    req = _create_request(
        attributes=dict(
            amount=WriteAmount(currency="EUR", net="1000.00", tax="190.00"),
            booking_account_number=12345,
            contract_number="23-51112-V3-001",
            cost_center="KG. 800",
            date_contract="2024-05-19",
            description="full contract",
        ),
        contractor=RelationshipData(
            data=RelationshipRef(id=CONTRACTOR_ID, type="CONTRACTOR")
        ),
    )
    body = req.to_api_dict()
    errors = validate(body, get_schema("CreateContractRequestFincon"))
    assert errors == [], errors


def test_create_amount_uses_net_and_tax_strings_not_gross():
    body = _create_request(
        attributes=dict(
            amount=WriteAmount(currency="EUR", net="1000.00", tax="190.00")
        )
    ).to_api_dict()
    amount = body["data"]["attributes"]["amount"]
    assert set(amount.keys()) == {"currency", "net", "tax"}
    assert "gross" not in amount
    assert isinstance(amount["net"], str)
    assert isinstance(amount["tax"], str)


def test_create_has_no_status_and_no_contract_type():
    body = _create_request(
        attributes=dict(
            amount=WriteAmount(currency="EUR", net="1.00", tax="0.19"),
            description="x",
        )
    ).to_api_dict()
    attrs = body["data"]["attributes"]
    assert "status" not in attrs
    assert "contract_type" not in attrs


def test_create_relationships_required_entity_and_unit():
    body = _create_request().to_api_dict()
    rels = body["data"]["relationships"]
    assert "contracting_entity" in rels
    assert "contract_unit" in rels
    # contractor omitted when None
    assert "contractor" not in rels


def test_create_name_too_long_rejected_by_model():
    with pytest.raises(ValidationError):
        CreateContractAttributes(name="x" * 251, is_reverse_charge=False)


def _update_request(**kwargs) -> UpdateContractRequest:
    rels = kwargs.pop("relationships", None)
    return UpdateContractRequest(
        data=UpdateContractData(
            id=CONTRACT_ID,
            attributes=UpdateContractAttributes(**kwargs),
            relationships=rels,
        )
    )


def test_update_status_matches_spec():
    body = _update_request(status="ORDERED").to_api_dict()
    errors = validate(body, get_schema("UpdateContractRequest"))
    assert errors == [], errors
    assert body["data"]["attributes"]["status"] == "ORDERED"


def test_update_relationships_only_entity_and_contractor():
    body = _update_request(
        relationships=UpdateContractRelationships(
            contracting_entity=RelationshipData(
                data=RelationshipRef(id=ENTITY_ID, type="CONTRACTING_ENTITY")
            ),
            contractor=RelationshipData(
                data=RelationshipRef(id=CONTRACTOR_ID, type="CONTRACTOR")
            ),
        )
    ).to_api_dict()
    errors = validate(body, get_schema("UpdateContractRequest"))
    assert errors == [], errors
    rels = body["data"]["relationships"]
    assert set(rels.keys()) == {"contracting_entity", "contractor"}
    assert "contract_unit" not in rels


def test_update_relationships_cannot_set_contract_unit():
    # The update relationships model forbids contract_unit entirely.
    with pytest.raises(ValidationError):
        UpdateContractRelationships(
            contract_unit=RelationshipData(
                data=RelationshipRef(id=UNIT_ID, type="CONTRACT_UNIT")
            )
        )


def test_update_full_attributes_match_spec():
    body = _update_request(
        name="renamed",
        status="DRAFT",
        amount=WriteAmount(currency="EUR", net="500.00", tax="95.00"),
        booking_account_number=42,
        contract_number="abc",
        cost_center="KG. 700",
        date_contract="2025-01-01",
        description="updated",
    ).to_api_dict()
    errors = validate(body, get_schema("UpdateContractRequest"))
    assert errors == [], errors


def test_document_upload_type_enum():
    assert ContractDocumentUpload(document_type="CONTRACT", filename="a.pdf")
    assert ContractDocumentUpload(document_type="ATTACHMENT", filename="b.pdf")
    body = {"document_type": "CONTRACT"}
    errors = validate(
        body,
        get_schema(
            "Body_create_contract_document_contracts__contract_id__documents__post"
        ),
    )
    # 'upload' (binary) is supplied as the file part, not the metadata body; the
    # schema marks it required, so validating metadata-only is expected to flag it.
    assert any("upload" in e for e in errors)


def test_document_upload_rejects_invalid_type():
    with pytest.raises(ValidationError):
        ContractDocumentUpload(document_type="INVOICE", filename="a.pdf")
