# alasco

Standalone Python SDK for the [Alasco](https://www.alasco.de) FinCon and CapEx APIs.

`httpx` + `pydantic` v2, read and write. JSON:API throughout. No module-level state:
construct an `AlascoClient` (FinCon) or `CapexClient` (CapEx) per use and pass it to the
typed resource models.

## Install

```bash
uv sync
```

## Usage

```python
from alasco import AlascoClient
from alasco.models import Project, Contract, Contractor, ContractDocument

with AlascoClient(token="...", key="...") as client:
    # Read
    projects = Project.list(client, property_ids=["<property-uuid>"])

    # Contractor lookup (generic, returns all matches, paginated)
    matches = Contractor.name_contains(client, "394909")

    # Create a DRAFT contract (contracting_entity + contract_unit required)
    contract = Contract.create(
        client,
        name="My contract",
        is_reverse_charge=False,
        contract_unit_id="<unit-uuid>",
        contracting_entity_id="<entity-uuid>",
        contractor_id="<contractor-uuid>",   # optional
        amount_net="1000.00",
        amount_tax="190.00",
        contract_number="23-51112-V3-001",
        date_contract="2024-05-19",
    )

    # Update (status + contracting_entity + contractor only on relationships)
    Contract.update(client, contract.id, status="ORDERED")

    # Upload a PDF (CONTRACT | ATTACHMENT)
    ContractDocument.upload(
        client,
        contract_id=contract.id,
        document_type="CONTRACT",
        file_path="contract.pdf",
    )
```

## CapEx

The CapEx API lives at `https://api.alasco.de/capex/v1` and uses the same token and key headers as FinCon.
`CapexClient` is `AlascoClient` with that base URL; the CapEx models in `alasco.models.capex` take a `CapexClient`.
The spec is `alasco/openapi/capex-openapi.json`.

```python
from datetime import date
from decimal import Decimal

from alasco import CapexClient
from alasco.models.capex import (
    Asset, Contract, Contractor, Invoice, InvoiceDocument, Measure, NetTaxAmount, PaidInvoice,
)
from alasco.workflows import create_capex_contract_as_ordered

with CapexClient(token="...", key="...") as client:
    # Read (all pages, filters per the spec)
    asset = Asset.list(client, identifier="251")[0]
    measure = Measure.list(client, identifier="251-0001", asset_ids=[asset.id])[0]
    contractors = Contractor.list(client, identifier="des03348")
    contracts = Contract.list(client, measure_ids=[measure.id])
    already = Invoice.list(client, internal_identifier="P-2048871")

    # Write
    contractor = Contractor.create(client, name="Bau AG", identifier="des03348")
    contract = create_capex_contract_as_ordered(
        client,
        name="Bau AG / 251-0001",
        is_reverse_charge=False,
        measure_id=measure.id,
        contracting_entity_id="<entity-uuid>",
        contractor_id=contractor.id,
    )
    amount = NetTaxAmount(net=Decimal("1234.56"), tax=Decimal("234.57"))
    invoice = Invoice.add_paid(
        client,
        contract_id=contract.id,
        invoice=PaidInvoice(
            external_identifier="RE-2025-17",
            internal_identifier="P-2048871",
            invoice_type="SINGLE",
            date_posted=date(2025, 9, 30),
            date_received=date(2025, 9, 30),
            date_due=date(2025, 9, 30),
            date_approved=date(2025, 9, 30),
            date_discount=None,
            payment_date=date(2025, 9, 30),
            unaudited_amount=amount,
            audited_amount=amount,
            discounted_approved_amount=amount,
            paid_amount=amount,
            cash_discount=None,
        ),
    )
    InvoiceDocument.upload(
        client, invoice.id, document_type="INVOICE", file_path="P-2048871.pdf"
    )
```

CapEx notes, verified against `alasco/openapi/capex-openapi.json`:

- Assets and measures are read-only: the API has no create for either.
- `POST /contracts/` needs the `measure` and `contracting_entity` relationships, `contractor` is optional, and there is no `status`; a new contract is a DRAFT until `Contract.update(..., status="ORDERED")`.
- `create_capex_contract_as_ordered` does that create and PATCH in two requests, which is not atomic.
- `PaidInvoice` requires all 13 spec attributes; `cash_discount`, `date_approved` and `date_discount` accept `None` and are then sent as `null`.
- Money is `Decimal` in the SDK and a fixed-point string on the wire (`Decimal("1E+3")` goes out as `"1000"`); paid-invoice amounts are `{net, tax}` without currency.
- Dates are `datetime.date` and go out as `YYYY-MM-DD`.
- Invoice documents are multipart uploads, PDF only, with a `document_type` such as `INVOICE` or `ATTACHMENT`.
- Any filter without a keyword argument goes through `filters={"filter[...]": "..."}`.

## Workflows

Opinionated, property-agnostic multi-step helpers live in `alasco.workflows`:

```python
from alasco.workflows import create_contract_as_ordered, batch

# Create as ORDERED (create DRAFT, then PATCH status=ORDERED)
contract = create_contract_as_ordered(client, name=..., ...)

# Run a callable per item, collecting per-item success/failure
results = batch(items, lambda item: do_something(item))
```

## Money

API money is sent and received as **strings**. Reads expose `net_float()` /
`gross_float()` on `MonetaryAmount`. Contract writes use `net` + `tax` (NOT gross),
2-decimal strings. The caller computes `tax = (net * rate).quantize(0.01, ROUND_HALF_UP)`.

## Write contract notes (verified against `alasco/openapi/openapi.json`)

- `POST /contracts/` has **no** `status` and **no** `contract_type`. Create yields a DRAFT.
- `PATCH /contracts/{id}/` can set `status`; relationships only `contracting_entity` +
  `contractor` (not `contract_unit`).
- Document upload is multipart, PDF only, type `CONTRACT` or `ATTACHMENT`.

## Tests

```bash
uv run pytest -m "not integration"   # offline unit tests (fake transport, bodies vs both specs)
uv run pytest -m integration         # live, sandbox property only (needs env creds)
```
