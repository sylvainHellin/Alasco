"""WRITE models for the Alasco CapEx API.

These mirror the CapEx OpenAPI request schemas (``alasco/openapi/capex-openapi.json``).
They are separate from the READ models, which describe response payloads.

Key facts (verified against the spec):
- ``POST /contractors/`` (Request_ContractorCreateAttributes): only ``name`` is
  required; ``identifier`` is a string (max 50), ``accounts_payable_number`` an integer.
- ``POST /contracts/`` (CreateContractRequestCapex): attributes require ``name`` +
  ``is_reverse_charge``; relationships require ``measure`` + ``contracting_entity``,
  ``contractor`` optional. There is NO ``status`` on create: a created contract is
  a DRAFT and ``PATCH /contracts/{id}/`` sets the status.
- ``POST /invoices/add-paid-invoice/`` (AddPaidInvoiceRequest): 13 required
  attributes, three of them nullable (``cash_discount``, ``date_approved``,
  ``date_discount``) and therefore always sent, as ``null`` when unset.
  Amounts are ``{net, tax}`` strings without currency.
- ``POST /invoices/{id}/documents/`` is multipart, PDF only, with a
  ``document_type`` from InvoiceDocumentCreateType.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from alasco.models.capex._common import ContractAmount, NetTaxAmount
from alasco.models.write import RelationshipData, RelationshipRef

# --- Enums (verified against capex-openapi.json) ---

ContractStatus = Literal["COMPLETED", "DRAFT", "ORDERED", "PARTIAL_COMPLETED"]
InvoiceType = Literal[
    "ADVANCE", "FINAL", "INSTALLMENT", "PARTIAL_FINAL", "RECURRING", "RETAINAGE_PAYOUT", "SINGLE"
]
InvoiceDocumentType = Literal[
    "ATTACHMENT",
    "AUDITED_INVOICE",
    "COVERSHEET_EXTERNAL",
    "EXTERNAL_CORRESPONDENCE",
    "INTERNAL_CORRESPONDENCE",
    "INVOICE",
    "OTHER",
    "PAYMENT_CERTIFICATE",
    "PLANS",
    "PROTOCOL",
    "REVISED_INVOICE",
    "VALUATIONS",
]

# JSON:API relationship type strings (uppercase, as the resources report them).
RELATIONSHIP_TYPE_CONTRACT = "CONTRACT"
RELATIONSHIP_TYPE_CONTRACTING_ENTITY = "CONTRACTING_ENTITY"
RELATIONSHIP_TYPE_CONTRACTOR = "CONTRACTOR"
RELATIONSHIP_TYPE_MEASURE = "MEASURE"


def relationship(resource_id: str, resource_type: str) -> RelationshipData:
    """Build ``{"data": {"id": ..., "type": ...}}``."""
    return RelationshipData(data=RelationshipRef(id=resource_id, type=resource_type))


# --- POST /contractors/ ---


class ContractorCreateAttributes(BaseModel):
    """Attributes for creating a contractor (``tax_waiver`` not supported here)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=250)
    identifier: str | None = Field(default=None, max_length=50)
    accounts_payable_number: int | None = None
    address: str | None = Field(default=None, max_length=255)
    zip_code: str | None = Field(default=None, max_length=12)
    city: str | None = Field(default=None, max_length=255)
    country: str | None = None
    vat_id: str | None = Field(default=None, max_length=250)
    tax_id: str | None = Field(default=None, max_length=250)
    tax_authority: str | None = None
    iban: str | None = None
    bic: str | None = None
    contact_person_name: str | None = Field(default=None, max_length=250)
    contact_person_email: str | None = None
    contact_person_phone_number: str | None = None


class CreateContractorData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["CONTRACTOR"] = "CONTRACTOR"
    attributes: ContractorCreateAttributes


class CreateContractorRequest(BaseModel):
    """Top-level body for ``POST /contractors/``."""

    model_config = ConfigDict(extra="forbid")

    data: CreateContractorData

    def to_api_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


# --- POST /contracts/ (CreateContractRequestCapex) ---


class CreateContractAttributes(BaseModel):
    """Attributes for creating a CapEx contract. No status, no contract_type."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(max_length=250)
    is_reverse_charge: bool
    amount: ContractAmount | None = None
    booking_account_number: int | None = Field(default=None, ge=1, le=999999999)
    contract_number: str | None = Field(default=None, max_length=250)
    cost_center: str | None = Field(default=None, max_length=100)
    date_contract: date | None = None
    description: str | None = None


class CreateContractRelationships(BaseModel):
    """``measure`` and ``contracting_entity`` required, ``contractor`` optional."""

    model_config = ConfigDict(extra="forbid")

    measure: RelationshipData
    contracting_entity: RelationshipData
    contractor: RelationshipData | None = None


class CreateContractData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["CONTRACT"] = "CONTRACT"
    attributes: CreateContractAttributes
    relationships: CreateContractRelationships


class CreateContractRequest(BaseModel):
    """Top-level body for ``POST /contracts/`` (CreateContractRequestCapex)."""

    model_config = ConfigDict(extra="forbid")

    data: CreateContractData

    def to_api_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


# --- PATCH /contracts/{id}/ (UpdateContractRequest) ---


class UpdateContractAttributes(BaseModel):
    """All optional; ``status`` transitions are restricted server-side."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, max_length=250)
    status: ContractStatus | None = None
    amount: ContractAmount | None = None
    booking_account_number: int | None = Field(default=None, ge=1, le=999999999)
    contract_number: str | None = Field(default=None, max_length=250)
    cost_center: str | None = Field(default=None, max_length=100)
    date_contract: date | None = None
    description: str | None = None


class UpdateContractRelationships(BaseModel):
    """Only ``contracting_entity`` and ``contractor`` (the measure cannot change)."""

    model_config = ConfigDict(extra="forbid")

    contracting_entity: RelationshipData | None = None
    contractor: RelationshipData | None = None


class UpdateContractData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: Literal["CONTRACT"] = "CONTRACT"
    attributes: UpdateContractAttributes
    relationships: UpdateContractRelationships | None = None


class UpdateContractRequest(BaseModel):
    """Top-level body for ``PATCH /contracts/{id}/``."""

    model_config = ConfigDict(extra="forbid")

    data: UpdateContractData

    def to_api_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


# --- POST /invoices/add-paid-invoice/ (AddPaidInvoiceRequest) ---


class PaidInvoice(BaseModel):
    """The attributes of a paid invoice (PaidInvoiceAttributes).

    All 13 spec-required attributes are required here too, with no defaults;
    the three nullable ones (``cash_discount``, ``date_approved``,
    ``date_discount``) take an explicit ``None``.
    """

    model_config = ConfigDict(extra="forbid")

    external_identifier: str
    invoice_type: InvoiceType
    date_posted: date
    date_received: date
    date_due: date
    date_approved: date | None
    date_discount: date | None
    payment_date: date
    unaudited_amount: NetTaxAmount
    audited_amount: NetTaxAmount
    discounted_approved_amount: NetTaxAmount
    paid_amount: NetTaxAmount
    cash_discount: NetTaxAmount | None
    internal_identifier: str = ""
    payment_comment: str | None = None


class AddPaidInvoiceRelationships(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract: RelationshipData


class AddPaidInvoiceData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["INVOICE"] = "INVOICE"
    attributes: PaidInvoice
    relationships: AddPaidInvoiceRelationships


class AddPaidInvoiceRequest(BaseModel):
    """Top-level body for ``POST /invoices/add-paid-invoice/``."""

    model_config = ConfigDict(extra="forbid")

    data: AddPaidInvoiceData

    def to_api_dict(self) -> dict[str, Any]:
        """Serialize, keeping required-nullable attributes as explicit ``null``."""
        body = self.model_dump(mode="json")
        attributes = body["data"]["attributes"]
        if attributes.get("payment_comment") is None:
            attributes.pop("payment_comment", None)
        return body


# --- POST /invoices/{invoice_id}/documents/ (multipart) ---


class InvoiceDocumentUpload(BaseModel):
    """Multipart form metadata for an invoice document upload (PDF only)."""

    model_config = ConfigDict(extra="forbid")

    document_type: InvoiceDocumentType
    filename: str
