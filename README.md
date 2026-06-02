# alasco

Standalone Python SDK for the [Alasco](https://www.alasco.de) FinCon API.

`httpx` + `pydantic` v2, read and write. JSON:API throughout. No module-level state:
construct an `AlascoClient` per use and pass it to the typed resource models.

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
uv run pytest -m "not integration"   # offline unit tests (body shapes vs openapi.json)
uv run pytest -m integration         # live, sandbox property only (needs env creds)
```
