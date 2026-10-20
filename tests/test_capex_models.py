"""Offline tests for the CapEx models through a real CapexClient on a fake transport.

Every request goes through ``httpx.MockTransport``: no network. Write tests
assert the exact path and JSON:API body and validate the body against the
CapEx OpenAPI spec (``alasco/openapi/capex-openapi.json``).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from decimal import Decimal

import httpx
import pytest
from pydantic import ValidationError

from alasco import CapexClient
from alasco.models.capex import (
    Asset,
    Contract,
    ContractingEntity,
    Contractor,
    Invoice,
    InvoiceDocument,
    Measure,
    NetTaxAmount,
    PaidInvoice,
    filter_params,
)
from alasco.workflows import create_capex_contract_as_ordered
from tests.fake_transport import FAKE_KEY, FAKE_TOKEN, FakeAlasco
from tests.openapi_schema import CAPEX, get_schema, validate

BASE = "/capex/v1"
ASSET_ID = "aaaaaaaa-0000-0000-0000-000000000251"
MEASURE_ID = "bbbbbbbb-0000-0000-0000-000000000001"
ENTITY_ID = "cccccccc-0000-0000-0000-000000000001"
CONTRACTOR_ID = "dddddddd-0000-0000-0000-000000000001"
CONTRACT_ID = "eeeeeeee-0000-0000-0000-000000000001"
INVOICE_ID = "ffffffff-0000-0000-0000-000000000001"
DOCUMENT_ID = "99999999-0000-0000-0000-000000000001"


@pytest.fixture
def fake() -> FakeAlasco:
    return FakeAlasco()


@pytest.fixture
def client(fake: FakeAlasco) -> Iterator[CapexClient]:
    with CapexClient(token=FAKE_TOKEN, key=FAKE_KEY, transport=fake.transport) as c:
        yield c


def _spec_errors(body: dict, schema_name: str) -> list[str]:
    return validate(body, get_schema(schema_name, CAPEX), spec=CAPEX)


# --- JSON:API fixtures (shapes from the spec's response schemas) ---

ASSET = {
    "id": ASSET_ID,
    "type": "ASSET",
    "attributes": {
        "name": "KQ Development",
        "identifier": "251",
        "date_created": "2025-01-02T10:00:00Z",
    },
}
MEASURE = {
    "id": MEASURE_ID,
    "type": "MEASURE",
    "attributes": {
        "name": "Lobby refurbishment",
        "identifier": "251-0001",
        "description": None,
        "date_created": "2025-01-02T10:00:00Z",
        "status": "IN_PROGRESS",
        "asset": ASSET_ID,
    },
    "relationships": {"asset": {"data": {"id": ASSET_ID, "type": "ASSET"}}},
}
ENTITY = {
    "id": ENTITY_ID,
    "type": "CONTRACTING_ENTITY",
    "attributes": {"name": "Hines Fund GmbH", "city": "Muenchen", "zip_code": "80331"},
}
CONTRACTOR = {
    "id": CONTRACTOR_ID,
    "type": "CONTRACTOR",
    "attributes": {"name": "Bau AG", "identifier": "des03348", "accounts_payable_number": None},
}
CONTRACT = {
    "id": CONTRACT_ID,
    "type": "CONTRACT",
    "attributes": {
        "name": "Bau AG lobby",
        "contract_number": "",
        "cost_center": "",
        "description": "",
        "status": "DRAFT",
        "is_reverse_charge": False,
        "date_contract": None,
        "date_created": "2026-10-19T09:00:00Z",
        "measure": MEASURE_ID,
        "contracting_entity": ENTITY_ID,
        "contractor": CONTRACTOR_ID,
        "contract_type": "SINGLE_CONTRACT",
        "amount": {"currency": "EUR", "net": "1000.00", "tax": "190.00"},
    },
}
INVOICE = {
    "id": INVOICE_ID,
    "type": "INVOICE",
    "attributes": {
        "external_identifier": "RE-2025-17",
        "internal_identifier": "P-2048871",
        "invoice_type": "SINGLE",
        "invoice_process_state": "PAID",
        "contract": CONTRACT_ID,
        "is_reverse_charge": False,
        "date_created": "2026-10-19T09:00:00Z",
        "date_posted": "2025-09-30",
        "date_approved": "2025-09-30T00:00:00Z",
        "payment_date": "2025-09-30",
        "unaudited_amount": {"currency": "EUR", "net": "1234.56", "tax": "234.57"},
    },
}
DOCUMENT = {
    "id": DOCUMENT_ID,
    "type": "INVOICE_DOCUMENT",
    "attributes": {
        "filename": "P-2048871.pdf",
        "document_type": "INVOICE",
        "uploaded_at": "2026-10-19T09:00:00Z",
    },
    "links": {
        "self": f"https://api.alasco.de/capex/v1/invoices/{INVOICE_ID}/documents/{DOCUMENT_ID}/",
        "download": f"https://api.alasco.de/capex/v1/invoices/{INVOICE_ID}/documents/{DOCUMENT_ID}/download/",
        "download_annotated": f"https://api.alasco.de/capex/v1/invoices/{INVOICE_ID}/documents/{DOCUMENT_ID}/download-annotated/",
    },
}


def _paid_invoice(**overrides) -> PaidInvoice:
    amount = NetTaxAmount(net=Decimal("1234.56"), tax=Decimal("234.57"))
    values = {
        "external_identifier": "RE-2025-17",
        "internal_identifier": "P-2048871",
        "invoice_type": "SINGLE",
        "date_posted": date(2025, 9, 30),
        "date_received": date(2025, 9, 30),
        "date_due": date(2025, 9, 30),
        "date_approved": date(2025, 9, 30),
        "date_discount": None,
        "payment_date": date(2025, 9, 30),
        "unaudited_amount": amount,
        "audited_amount": amount,
        "discounted_approved_amount": amount,
        "paid_amount": amount,
        "cash_discount": None,
    }
    values.update(overrides)
    return PaidInvoice(**values)


# --- Read: list (with filters) + get, one test per resource ---


def test_asset_list_and_get(fake: FakeAlasco, client: CapexClient):
    fake.add("GET", f"{BASE}/assets/", {"data": [ASSET], "links": {"next": None}})
    fake.add("GET", f"{BASE}/assets/{ASSET_ID}/", {"data": ASSET})

    assets = Asset.list(client, identifier="251")
    assert fake.requests[0].url.params["filter[identifier.exact]"] == "251"
    assert [a.identifier for a in assets] == ["251"]

    asset = Asset.get(client, ASSET_ID)
    assert asset.name == "KQ Development"
    assert asset.attributes.date_created is not None


def test_measure_list_and_get(fake: FakeAlasco, client: CapexClient):
    fake.add("GET", f"{BASE}/measures/", {"data": [MEASURE]})
    fake.add("GET", f"{BASE}/measures/{MEASURE_ID}/", {"data": MEASURE})

    measures = Measure.list(client, identifier="251-0001", asset_ids=[ASSET_ID, "other"])
    params = fake.requests[0].url.params
    assert params["filter[identifier.exact]"] == "251-0001"
    assert params["filter[asset.in]"] == f"{ASSET_ID},other"
    assert measures[0].asset_id == ASSET_ID

    measure = Measure.get(client, MEASURE_ID)
    assert measure.identifier == "251-0001"
    assert measure.attributes.status == "IN_PROGRESS"


def test_contracting_entity_list_and_get(fake: FakeAlasco, client: CapexClient):
    fake.add("GET", f"{BASE}/contracting_entities/", {"data": [ENTITY]})
    fake.add("GET", f"{BASE}/contracting_entities/{ENTITY_ID}/", {"data": ENTITY})

    entities = ContractingEntity.list(client, name="Hines Fund GmbH")
    assert fake.requests[0].url.params["filter[name.exact]"] == "Hines Fund GmbH"
    assert entities[0].id == ENTITY_ID
    assert ContractingEntity.get(client, ENTITY_ID).name == "Hines Fund GmbH"


def test_contractor_list_and_get(fake: FakeAlasco, client: CapexClient):
    fake.add("GET", f"{BASE}/contractors/", {"data": [CONTRACTOR]})
    fake.add("GET", f"{BASE}/contractors/{CONTRACTOR_ID}/", {"data": CONTRACTOR})

    contractors = Contractor.list(client, identifier="des03348")
    assert fake.requests[0].url.params["filter[identifier.exact]"] == "des03348"
    assert contractors[0].identifier == "des03348"
    assert Contractor.get(client, CONTRACTOR_ID).name == "Bau AG"


def test_contract_list_and_get(fake: FakeAlasco, client: CapexClient):
    fake.add("GET", f"{BASE}/contracts/", {"data": [CONTRACT]})
    fake.add("GET", f"{BASE}/contracts/{CONTRACT_ID}/", {"data": CONTRACT})

    contracts = Contract.list(client, measure_ids=[MEASURE_ID], contractor_ids=[CONTRACTOR_ID])
    params = fake.requests[0].url.params
    assert params["filter[measure.in]"] == MEASURE_ID
    assert params["filter[contractor.in]"] == CONTRACTOR_ID
    assert contracts[0].measure_id == MEASURE_ID

    contract = Contract.get(client, CONTRACT_ID)
    assert contract.contracting_entity_id == ENTITY_ID
    assert contract.attributes.amount is not None
    assert contract.attributes.amount.net == Decimal("1000.00")


def test_invoice_list_and_get(fake: FakeAlasco, client: CapexClient):
    fake.add("GET", f"{BASE}/invoices/", {"data": [INVOICE]})
    fake.add("GET", f"{BASE}/invoices/{INVOICE_ID}/", {"data": INVOICE})

    invoices = Invoice.list(client, internal_identifier="P-2048871")
    assert fake.requests[0].url.params["filter[internal_identifier.exact]"] == "P-2048871"
    assert invoices[0].contract_id == CONTRACT_ID

    invoice = Invoice.get(client, INVOICE_ID)
    assert invoice.attributes.date_posted == date(2025, 9, 30)
    assert invoice.attributes.unaudited_amount is not None
    assert invoice.attributes.unaudited_amount.tax == Decimal("234.57")


def test_invoice_document_list_and_get(fake: FakeAlasco, client: CapexClient):
    fake.add("GET", f"{BASE}/invoices/{INVOICE_ID}/documents/", {"data": [DOCUMENT]})
    fake.add("GET", f"{BASE}/invoices/{INVOICE_ID}/documents/{DOCUMENT_ID}/", {"data": DOCUMENT})

    documents = InvoiceDocument.list(client, INVOICE_ID, document_types=["INVOICE"])
    assert fake.requests[0].url.params["filter[document_type.in]"] == "INVOICE"
    assert documents[0].filename == "P-2048871.pdf"

    document = InvoiceDocument.get(client, INVOICE_ID, DOCUMENT_ID)
    assert document.links is not None
    assert document.links.download is not None
    assert document.links.download.endswith("/download/")


def test_list_follows_pagination(fake: FakeAlasco, client: CapexClient):
    second = {**MEASURE, "id": "bbbbbbbb-0000-0000-0000-000000000002"}
    next_url = "https://api.alasco.de/capex/v1/measures/?cursor%5Bposition%5D=p2"
    fake.add("GET", f"{BASE}/measures/", {"data": [MEASURE], "links": {"next": next_url}})
    fake.add("GET", f"{BASE}/measures/", {"data": [second], "links": {"next": None}})

    measures = Measure.list(client, asset_ids=[ASSET_ID])
    assert [m.id for m in measures] == [MEASURE_ID, second["id"]]
    assert fake.requests[1].url.params["cursor[position]"] == "p2"


def test_filter_params_rejects_empty_list_and_passes_extra():
    with pytest.raises(ValueError):
        filter_params({"asset.in": []})
    params = filter_params(
        {"identifier.exact": None},
        {"filter[date_created.greater_than_equal]": "2026-01-01T00:00:00"},
    )
    assert params == {"filter[date_created.greater_than_equal]": "2026-01-01T00:00:00"}


# --- Write: exact path + exact JSON:API body + spec validation ---


def test_contractor_create_body(fake: FakeAlasco, client: CapexClient):
    fake.add("POST", f"{BASE}/contractors/", {"data": CONTRACTOR}, status=201)

    contractor = Contractor.create(client, name="Bau AG", identifier="des03348")

    request = fake.last
    assert request.method == "POST"
    assert request.url.path == f"{BASE}/contractors/"
    body = fake.last_json()
    assert body == {
        "data": {"type": "CONTRACTOR", "attributes": {"name": "Bau AG", "identifier": "des03348"}}
    }
    assert _spec_errors(body, "Request_ContractorCreateAttributes_Literal_CONTRACTOR__") == []
    assert contractor.id == CONTRACTOR_ID


def test_contract_create_body(fake: FakeAlasco, client: CapexClient):
    fake.add("POST", f"{BASE}/contracts/", {"data": CONTRACT})

    contract = Contract.create(
        client,
        name="Bau AG lobby",
        is_reverse_charge=False,
        measure_id=MEASURE_ID,
        contracting_entity_id=ENTITY_ID,
        contractor_id=CONTRACTOR_ID,
        amount_net=Decimal("1000.00"),
        amount_tax=Decimal("190.00"),
        date_contract=date(2025, 9, 30),
    )

    assert fake.last.url.path == f"{BASE}/contracts/"
    body = fake.last_json()
    assert body == {
        "data": {
            "type": "CONTRACT",
            "attributes": {
                "name": "Bau AG lobby",
                "is_reverse_charge": False,
                "amount": {"currency": "EUR", "net": "1000.00", "tax": "190.00"},
                "date_contract": "2025-09-30",
            },
            "relationships": {
                "measure": {"data": {"id": MEASURE_ID, "type": "MEASURE"}},
                "contracting_entity": {"data": {"id": ENTITY_ID, "type": "CONTRACTING_ENTITY"}},
                "contractor": {"data": {"id": CONTRACTOR_ID, "type": "CONTRACTOR"}},
            },
        }
    }
    assert _spec_errors(body, "CreateContractRequestCapex") == []
    assert contract.status == "DRAFT"


def test_contract_create_without_contractor_and_partial_amount_rejected(client: CapexClient):
    with pytest.raises(ValueError):
        Contract.create(
            client,
            name="x",
            is_reverse_charge=False,
            measure_id=MEASURE_ID,
            contracting_entity_id=ENTITY_ID,
            amount_net=Decimal("1"),
        )


def test_contract_update_status_body(fake: FakeAlasco, client: CapexClient):
    ordered = {**CONTRACT, "attributes": {**CONTRACT["attributes"], "status": "ORDERED"}}
    fake.add("PATCH", f"{BASE}/contracts/{CONTRACT_ID}/", {"data": ordered})

    contract = Contract.update(client, CONTRACT_ID, status="ORDERED")

    assert fake.last.method == "PATCH"
    assert fake.last.url.path == f"{BASE}/contracts/{CONTRACT_ID}/"
    body = fake.last_json()
    assert body == {
        "data": {"id": CONTRACT_ID, "type": "CONTRACT", "attributes": {"status": "ORDERED"}}
    }
    assert _spec_errors(body, "UpdateContractRequest") == []
    assert contract.status == "ORDERED"


def test_contract_update_rejects_unknown_status(client: CapexClient):
    with pytest.raises(ValidationError):
        Contract.update(client, CONTRACT_ID, status="SIGNED")  # type: ignore[arg-type]


def test_create_capex_contract_as_ordered_posts_then_patches(fake: FakeAlasco, client: CapexClient):
    ordered = {**CONTRACT, "attributes": {**CONTRACT["attributes"], "status": "ORDERED"}}
    fake.add("POST", f"{BASE}/contracts/", {"data": CONTRACT})
    fake.add("PATCH", f"{BASE}/contracts/{CONTRACT_ID}/", {"data": ordered})

    contract = create_capex_contract_as_ordered(
        client,
        name="Bau AG lobby",
        is_reverse_charge=False,
        measure_id=MEASURE_ID,
        contracting_entity_id=ENTITY_ID,
        contractor_id=CONTRACTOR_ID,
    )

    assert [(r.method, r.url.path) for r in fake.requests] == [
        ("POST", f"{BASE}/contracts/"),
        ("PATCH", f"{BASE}/contracts/{CONTRACT_ID}/"),
    ]
    assert contract.status == "ORDERED"


def test_add_paid_invoice_body(fake: FakeAlasco, client: CapexClient):
    created = {
        "id": INVOICE_ID,
        "type": "INVOICE",
        "attributes": {
            "date_created": "2026-10-19T09:00:00Z",
            "date_received": "2025-09-30",
            "invoice_process_state": "PAID",
        },
    }
    fake.add("POST", f"{BASE}/invoices/add-paid-invoice/", {"data": created}, status=201)

    invoice = Invoice.add_paid(client, contract_id=CONTRACT_ID, invoice=_paid_invoice())

    assert fake.last.url.path == f"{BASE}/invoices/add-paid-invoice/"
    amount = {"net": "1234.56", "tax": "234.57"}
    body = fake.last_json()
    assert body == {
        "data": {
            "type": "INVOICE",
            "attributes": {
                "external_identifier": "RE-2025-17",
                "invoice_type": "SINGLE",
                "date_posted": "2025-09-30",
                "date_received": "2025-09-30",
                "date_due": "2025-09-30",
                "date_approved": "2025-09-30",
                "date_discount": None,
                "payment_date": "2025-09-30",
                "unaudited_amount": amount,
                "audited_amount": amount,
                "discounted_approved_amount": amount,
                "paid_amount": amount,
                "cash_discount": None,
                "internal_identifier": "P-2048871",
            },
            "relationships": {"contract": {"data": {"id": CONTRACT_ID, "type": "CONTRACT"}}},
        }
    }
    assert _spec_errors(body, "AddPaidInvoiceRequest") == []
    assert invoice.id == INVOICE_ID
    assert invoice.attributes.invoice_process_state == "PAID"


def test_paid_invoice_decimal_serialisation_and_required_fields():
    paid = _paid_invoice(
        paid_amount=NetTaxAmount(net=Decimal("1E+3"), tax=Decimal("-0.50")),
        payment_comment="Batch 369458",
    )
    attributes = paid.model_dump(mode="json")
    assert attributes["paid_amount"] == {"net": "1000", "tax": "-0.50"}
    assert attributes["payment_comment"] == "Batch 369458"

    with pytest.raises(ValidationError):
        PaidInvoice(external_identifier="x", invoice_type="SINGLE")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        _paid_invoice(invoice_type="CREDIT_NOTE")


def test_invoice_document_upload_multipart(fake: FakeAlasco, client: CapexClient):
    fake.add("POST", f"{BASE}/invoices/{INVOICE_ID}/documents/", {"data": DOCUMENT}, status=201)
    pdf = b"%PDF-1.4 fake invoice"

    document = InvoiceDocument.upload(
        client, INVOICE_ID, document_type="INVOICE", content=pdf, filename="P-2048871.pdf"
    )

    request = fake.last
    assert request.method == "POST"
    assert request.url.path == f"{BASE}/invoices/{INVOICE_ID}/documents/"
    content_type = request.headers["Content-Type"]
    assert content_type.startswith("multipart/form-data; boundary=")
    raw = request.content
    assert b'Content-Disposition: form-data; name="document_type"\r\n\r\nINVOICE\r\n' in raw
    assert b'name="upload"; filename="P-2048871.pdf"' in raw
    assert b"Content-Type: application/pdf" in raw
    assert pdf in raw
    assert document.document_type == "INVOICE"


def test_invoice_document_upload_rejects_bad_type(client: CapexClient):
    with pytest.raises(ValidationError):
        InvoiceDocument.upload(
            client,
            INVOICE_ID,
            document_type="CONTRACT",  # type: ignore[arg-type]
            content=b"%PDF",
            filename="a.pdf",
        )


def test_write_error_raises_http_status_error(fake: FakeAlasco, client: CapexClient):
    fake.add("POST", f"{BASE}/contractors/", {"detail": [{"msg": "invalid"}]}, status=422)
    with pytest.raises(httpx.HTTPStatusError):
        Contractor.create(client, name="Bau AG")
