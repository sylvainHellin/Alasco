"""READ models (and their write methods) for the Alasco CapEx API.

Every resource is a JSON:API object ``{id, type, attributes, relationships}``.
Attributes are lenient (``extra="ignore"``, optional where the API may omit
them) so a new API field never breaks parsing. List methods follow
``links.next`` across all pages (page size 100).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from pydantic import BaseModel, ConfigDict

from alasco.models.capex._common import Amount, ContractAmount, filter_params, response_data
from alasco.models.capex.write import (
    RELATIONSHIP_TYPE_CONTRACT,
    RELATIONSHIP_TYPE_CONTRACTING_ENTITY,
    RELATIONSHIP_TYPE_CONTRACTOR,
    RELATIONSHIP_TYPE_MEASURE,
    AddPaidInvoiceData,
    AddPaidInvoiceRelationships,
    AddPaidInvoiceRequest,
    ContractorCreateAttributes,
    ContractStatus,
    CreateContractAttributes,
    CreateContractData,
    CreateContractorData,
    CreateContractorRequest,
    CreateContractRelationships,
    CreateContractRequest,
    InvoiceDocumentType,
    InvoiceDocumentUpload,
    PaidInvoice,
    UpdateContractAttributes,
    UpdateContractData,
    UpdateContractRelationships,
    UpdateContractRequest,
    relationship,
)

if TYPE_CHECKING:
    from decimal import Decimal

    from alasco.client import CapexClient

Filters = dict[str, str] | None


class _Resource(BaseModel):
    """Common JSON:API envelope plus list/get helpers."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str
    relationships: dict[str, Any] | None = None

    @classmethod
    def _list(cls, client: CapexClient, endpoint: str, params: dict[str, str]) -> list[Self]:
        return [cls.model_validate(item) for item in client.get_all(endpoint, params)]

    @classmethod
    def _get(cls, client: CapexClient, endpoint: str) -> Self:
        return cls.model_validate(client.get_single(endpoint))


# --- Asset ---


class AssetAttributes(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    identifier: str
    date_created: datetime | None = None


class Asset(_Resource):
    """A CapEx asset (property). Read-only: the API has no asset create."""

    type: str = "ASSET"
    attributes: AssetAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def identifier(self) -> str:
        return self.attributes.identifier

    @classmethod
    def list(
        cls,
        client: CapexClient,
        *,
        identifier: str | None = None,
        name: str | None = None,
        ids: Sequence[str] | None = None,
        filters: Filters = None,
    ) -> list[Asset]:
        """``GET /assets/`` with ``identifier.exact``, ``name.exact``, ``id.in``."""
        params = filter_params(
            {"identifier.exact": identifier, "name.exact": name, "id.in": ids}, filters
        )
        return cls._list(client, "/assets/", params)

    @classmethod
    def get(cls, client: CapexClient, asset_id: str) -> Asset:
        return cls._get(client, f"/assets/{asset_id}/")


# --- Measure ---


class MeasureAttributes(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    identifier: str
    description: str | None = None
    date_created: datetime | None = None
    status: str | None = None  # MeasureStatus: BUDGETED | IN_PROGRESS | COMPLETED
    asset: str | None = None  # asset UUID


class Measure(_Resource):
    """A CapEx measure (Works Code). Read-only: the API has no measure create."""

    type: str = "MEASURE"
    attributes: MeasureAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def identifier(self) -> str:
        return self.attributes.identifier

    @property
    def asset_id(self) -> str | None:
        return self.attributes.asset

    @classmethod
    def list(
        cls,
        client: CapexClient,
        *,
        identifier: str | None = None,
        asset_ids: Sequence[str] | None = None,
        statuses: Sequence[str] | None = None,
        ids: Sequence[str] | None = None,
        filters: Filters = None,
    ) -> list[Measure]:
        """``GET /measures/`` with ``identifier.exact``, ``asset.in``, ``status.in``, ``id.in``."""
        params = filter_params(
            {
                "identifier.exact": identifier,
                "asset.in": asset_ids,
                "status.in": statuses,
                "id.in": ids,
            },
            filters,
        )
        return cls._list(client, "/measures/", params)

    @classmethod
    def get(cls, client: CapexClient, measure_id: str) -> Measure:
        return cls._get(client, f"/measures/{measure_id}/")


# --- Contracting entity ---


class ContractingEntityAttributes(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    address: str | None = None
    zip_code: str | None = None
    city: str | None = None
    tax_id: str | None = None
    vat_id: str | None = None


class ContractingEntity(_Resource):
    """A contracting entity (Auftraggeber). Read-only here."""

    type: str = "CONTRACTING_ENTITY"
    attributes: ContractingEntityAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @classmethod
    def list(
        cls,
        client: CapexClient,
        *,
        name: str | None = None,
        ids: Sequence[str] | None = None,
        filters: Filters = None,
    ) -> list[ContractingEntity]:
        """``GET /contracting_entities/`` with ``name.exact``, ``id.in``."""
        params = filter_params({"name.exact": name, "id.in": ids}, filters)
        return cls._list(client, "/contracting_entities/", params)

    @classmethod
    def get(cls, client: CapexClient, entity_id: str) -> ContractingEntity:
        return cls._get(client, f"/contracting_entities/{entity_id}/")


# --- Contractor ---


class ContractorAttributes(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    identifier: str | None = None
    accounts_payable_number: int | None = None
    address: str | None = None
    zip_code: str | None = None
    city: str | None = None
    country: str | None = None
    vat_id: str | None = None
    tax_id: str | None = None
    iban: str | None = None
    bic: str | None = None


class Contractor(_Resource):
    """A contractor (Auftragnehmer)."""

    type: str = "CONTRACTOR"
    attributes: ContractorAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def identifier(self) -> str | None:
        return self.attributes.identifier

    @classmethod
    def list(
        cls,
        client: CapexClient,
        *,
        identifier: str | None = None,
        name: str | None = None,
        asset_ids: Sequence[str] | None = None,
        ids: Sequence[str] | None = None,
        filters: Filters = None,
    ) -> list[Contractor]:
        """``GET /contractors/`` with ``identifier.exact``, ``name.exact``,
        ``asset.in``, ``id.in``."""
        params = filter_params(
            {
                "identifier.exact": identifier,
                "name.exact": name,
                "asset.in": asset_ids,
                "id.in": ids,
            },
            filters,
        )
        return cls._list(client, "/contractors/", params)

    @classmethod
    def get(cls, client: CapexClient, contractor_id: str) -> Contractor:
        return cls._get(client, f"/contractors/{contractor_id}/")

    @classmethod
    def create(
        cls,
        client: CapexClient,
        *,
        name: str,
        identifier: str | None = None,
        accounts_payable_number: int | None = None,
        address: str | None = None,
        zip_code: str | None = None,
        city: str | None = None,
        country: str | None = None,
        vat_id: str | None = None,
        tax_id: str | None = None,
        tax_authority: str | None = None,
        iban: str | None = None,
        bic: str | None = None,
        contact_person_name: str | None = None,
        contact_person_email: str | None = None,
        contact_person_phone_number: str | None = None,
    ) -> Contractor:
        """``POST /contractors/``. Only ``name`` is required; unset fields are omitted."""
        request = CreateContractorRequest(
            data=CreateContractorData(
                attributes=ContractorCreateAttributes(
                    name=name,
                    identifier=identifier,
                    accounts_payable_number=accounts_payable_number,
                    address=address,
                    zip_code=zip_code,
                    city=city,
                    country=country,
                    vat_id=vat_id,
                    tax_id=tax_id,
                    tax_authority=tax_authority,
                    iban=iban,
                    bic=bic,
                    contact_person_name=contact_person_name,
                    contact_person_email=contact_person_email,
                    contact_person_phone_number=contact_person_phone_number,
                )
            )
        )
        response = client.post("/contractors/", json=request.to_api_dict())
        return cls.model_validate(response_data(response))


# --- Contract ---


class ContractAttributes(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    contract_number: str | None = None
    description: str | None = None
    status: str | None = None  # ContractStatus
    contract_type: str | None = None
    measure: str | None = None  # measure UUID
    contracting_entity: str | None = None  # contracting entity UUID
    contractor: str | None = None  # contractor UUID
    amount: Amount | None = None
    reserve_amount: Amount | None = None
    date_contract: date | None = None
    date_created: datetime | None = None
    is_reverse_charge: bool = False
    booking_account_number: int | None = None
    cost_center: str | None = None


def _contract_amount(
    net: Decimal | None, tax: Decimal | None, currency: str
) -> ContractAmount | None:
    if (net is None) != (tax is None):
        msg = "amount_net and amount_tax must be provided together (or neither)"
        raise ValueError(msg)
    if net is None or tax is None:
        return None
    return ContractAmount(currency=currency, net=net, tax=tax)


class Contract(_Resource):
    """A CapEx contract, attached to a measure."""

    type: str = "CONTRACT"
    attributes: ContractAttributes

    @property
    def name(self) -> str:
        return self.attributes.name

    @property
    def status(self) -> str | None:
        return self.attributes.status

    @property
    def measure_id(self) -> str | None:
        return self.attributes.measure

    @property
    def contractor_id(self) -> str | None:
        return self.attributes.contractor

    @property
    def contracting_entity_id(self) -> str | None:
        return self.attributes.contracting_entity

    @classmethod
    def list(
        cls,
        client: CapexClient,
        *,
        measure_ids: Sequence[str] | None = None,
        contractor_ids: Sequence[str] | None = None,
        contracting_entity_ids: Sequence[str] | None = None,
        statuses: Sequence[str] | None = None,
        ids: Sequence[str] | None = None,
        filters: Filters = None,
    ) -> list[Contract]:
        """``GET /contracts/`` with ``measure.in``, ``contractor.in``,
        ``contracting_entity.in``, ``status.in``, ``id.in``."""
        params = filter_params(
            {
                "measure.in": measure_ids,
                "contractor.in": contractor_ids,
                "contracting_entity.in": contracting_entity_ids,
                "status.in": statuses,
                "id.in": ids,
            },
            filters,
        )
        return cls._list(client, "/contracts/", params)

    @classmethod
    def get(cls, client: CapexClient, contract_id: str) -> Contract:
        return cls._get(client, f"/contracts/{contract_id}/")

    @classmethod
    def create(
        cls,
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
        """``POST /contracts/``. The result is always a DRAFT (no status on create).

        Use ``Contract.update(..., status="ORDERED")`` or the
        ``alasco.workflows.create_capex_contract_as_ordered`` helper to order it.
        """
        request = CreateContractRequest(
            data=CreateContractData(
                attributes=CreateContractAttributes(
                    name=name,
                    is_reverse_charge=is_reverse_charge,
                    amount=_contract_amount(amount_net, amount_tax, currency),
                    booking_account_number=booking_account_number,
                    contract_number=contract_number,
                    cost_center=cost_center,
                    date_contract=date_contract,
                    description=description,
                ),
                relationships=CreateContractRelationships(
                    measure=relationship(measure_id, RELATIONSHIP_TYPE_MEASURE),
                    contracting_entity=relationship(
                        contracting_entity_id, RELATIONSHIP_TYPE_CONTRACTING_ENTITY
                    ),
                    contractor=(
                        relationship(contractor_id, RELATIONSHIP_TYPE_CONTRACTOR)
                        if contractor_id is not None
                        else None
                    ),
                ),
            )
        )
        response = client.post("/contracts/", json=request.to_api_dict())
        return cls.model_validate(response_data(response))

    @classmethod
    def update(
        cls,
        client: CapexClient,
        contract_id: str,
        *,
        status: ContractStatus | None = None,
        contracting_entity_id: str | None = None,
        contractor_id: str | None = None,
        name: str | None = None,
        amount_net: Decimal | None = None,
        amount_tax: Decimal | None = None,
        currency: str = "EUR",
        booking_account_number: int | None = None,
        contract_number: str | None = None,
        cost_center: str | None = None,
        date_contract: date | None = None,
        description: str | None = None,
    ) -> Contract:
        """``PATCH /contracts/{id}/``. Unset fields are omitted from the body."""
        relationships: UpdateContractRelationships | None = None
        if contracting_entity_id is not None or contractor_id is not None:
            relationships = UpdateContractRelationships(
                contracting_entity=(
                    relationship(contracting_entity_id, RELATIONSHIP_TYPE_CONTRACTING_ENTITY)
                    if contracting_entity_id is not None
                    else None
                ),
                contractor=(
                    relationship(contractor_id, RELATIONSHIP_TYPE_CONTRACTOR)
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
                    amount=_contract_amount(amount_net, amount_tax, currency),
                    booking_account_number=booking_account_number,
                    contract_number=contract_number,
                    cost_center=cost_center,
                    date_contract=date_contract,
                    description=description,
                ),
                relationships=relationships,
            )
        )
        response = client.patch(f"/contracts/{contract_id}/", json=request.to_api_dict())
        return cls.model_validate(response_data(response))


# --- Invoice ---


class InvoiceAttributes(BaseModel):
    """Invoice read attributes. All optional: the add-paid-invoice response
    returns only a basic subset (date_created, date_received, invoice_process_state)."""

    model_config = ConfigDict(extra="ignore")

    external_identifier: str | None = None
    internal_identifier: str | None = None
    invoice_type: str | None = None
    invoice_process_state: str | None = None
    contract: str | None = None  # contract UUID
    is_reverse_charge: bool | None = None
    date_created: datetime | None = None
    date_posted: date | None = None
    date_received: date | None = None
    date_due: date | None = None
    date_discount: date | None = None
    date_approved: datetime | None = None
    payment_date: date | None = None
    payment_created: datetime | None = None
    payment_comment: str | None = None
    unaudited_amount: Amount | None = None
    audited_amount: Amount | None = None
    discounted_approved_amount: Amount | None = None
    undiscounted_approved_amount: Amount | None = None
    cash_discount: Amount | None = None


class Invoice(_Resource):
    """A CapEx invoice."""

    type: str = "INVOICE"
    attributes: InvoiceAttributes

    @property
    def external_identifier(self) -> str | None:
        return self.attributes.external_identifier

    @property
    def internal_identifier(self) -> str | None:
        return self.attributes.internal_identifier

    @property
    def contract_id(self) -> str | None:
        """Contract UUID from the attributes, else from ``relationships.contract``."""
        if self.attributes.contract:
            return self.attributes.contract
        rel = (self.relationships or {}).get("contract") or {}
        return (rel.get("data") or {}).get("id")

    @classmethod
    def list(
        cls,
        client: CapexClient,
        *,
        internal_identifier: str | None = None,
        external_identifier: str | None = None,
        contract_ids: Sequence[str] | None = None,
        ids: Sequence[str] | None = None,
        filters: Filters = None,
    ) -> list[Invoice]:
        """``GET /invoices/`` with ``internal_identifier.exact``,
        ``external_identifier.exact``, ``contract.in``, ``id.in``."""
        params = filter_params(
            {
                "internal_identifier.exact": internal_identifier,
                "external_identifier.exact": external_identifier,
                "contract.in": contract_ids,
                "id.in": ids,
            },
            filters,
        )
        return cls._list(client, "/invoices/", params)

    @classmethod
    def get(cls, client: CapexClient, invoice_id: str) -> Invoice:
        return cls._get(client, f"/invoices/{invoice_id}/")

    @classmethod
    def add_paid(cls, client: CapexClient, *, contract_id: str, invoice: PaidInvoice) -> Invoice:
        """``POST /invoices/add-paid-invoice/``: book an already paid invoice on a contract."""
        request = AddPaidInvoiceRequest(
            data=AddPaidInvoiceData(
                attributes=invoice,
                relationships=AddPaidInvoiceRelationships(
                    contract=relationship(contract_id, RELATIONSHIP_TYPE_CONTRACT)
                ),
            )
        )
        response = client.post("/invoices/add-paid-invoice/", json=request.to_api_dict())
        return cls.model_validate(response_data(response))


# --- Invoice document ---


class InvoiceDocumentAttributes(BaseModel):
    model_config = ConfigDict(extra="ignore")

    filename: str
    document_type: str
    uploaded_at: datetime | None = None


class InvoiceDocumentLinks(BaseModel):
    model_config = ConfigDict(extra="ignore")

    self: str | None = None
    download: str | None = None
    download_annotated: str | None = None


class InvoiceDocument(_Resource):
    """A document attached to a CapEx invoice."""

    type: str = "INVOICE_DOCUMENT"
    attributes: InvoiceDocumentAttributes
    links: InvoiceDocumentLinks | None = None

    @property
    def filename(self) -> str:
        return self.attributes.filename

    @property
    def document_type(self) -> str:
        return self.attributes.document_type

    @classmethod
    def list(
        cls,
        client: CapexClient,
        invoice_id: str,
        *,
        document_types: Sequence[str] | None = None,
        filters: Filters = None,
    ) -> list[InvoiceDocument]:
        """``GET /invoices/{invoice_id}/documents/`` with ``document_type.in``."""
        params = filter_params({"document_type.in": document_types}, filters)
        return cls._list(client, f"/invoices/{invoice_id}/documents/", params)

    @classmethod
    def get(cls, client: CapexClient, invoice_id: str, document_id: str) -> InvoiceDocument:
        return cls._get(client, f"/invoices/{invoice_id}/documents/{document_id}/")

    @classmethod
    def upload(
        cls,
        client: CapexClient,
        invoice_id: str,
        *,
        document_type: InvoiceDocumentType,
        file_path: str | Path | None = None,
        content: bytes | None = None,
        filename: str | None = None,
    ) -> InvoiceDocument:
        """``POST /invoices/{invoice_id}/documents/`` as multipart (PDF only).

        Provide either ``file_path`` or ``content`` + ``filename``.
        """
        if file_path is not None:
            path = Path(file_path)
            file_bytes = path.read_bytes()
            resolved_name = filename or path.name
        elif content is not None:
            if filename is None:
                msg = "filename is required when uploading raw content"
                raise ValueError(msg)
            file_bytes = content
            resolved_name = filename
        else:
            msg = "Provide either file_path or content + filename"
            raise ValueError(msg)

        form = InvoiceDocumentUpload(document_type=document_type, filename=resolved_name)
        response = client.post_multipart(
            f"/invoices/{invoice_id}/documents/",
            data={"document_type": form.document_type},
            files={"upload": (form.filename, file_bytes, "application/pdf")},
        )
        return cls.model_validate(response_data(response))

    def download(self, client: CapexClient) -> bytes:
        """Download the file via ``links.download`` (follows the pre-signed redirect)."""
        if self.links is None or self.links.download is None:
            msg = f"invoice document {self.id} has no download link"
            raise ValueError(msg)
        return client.download(self.links.download)
