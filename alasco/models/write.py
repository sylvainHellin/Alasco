"""Deliberate WRITE models for the Alasco FinCon API.

These mirror the OpenAPI create/update schemas EXACTLY (verified against
``alasco/openapi/openapi.json``). They are intentionally separate from the READ
models (``contract.py`` etc.), which describe response payloads. Applications
should reuse these write models rather than re-declaring request shapes.

Key facts (verified):
- ``POST /contracts/`` (CreateContractRequestFincon): attributes require only
  ``name`` + ``is_reverse_charge``; money is ``amount{currency, net, tax}`` as
  STRINGS (net/tax, NOT net/gross); relationships require ``contracting_entity``
  + ``contract_unit``, optional ``contractor``. There is NO ``status`` and NO
  ``contract_type`` on create -> a created contract is always DRAFT.
- ``PATCH /contracts/{id}/`` (UpdateContractRequest): all attributes optional
  incl. ``status``; relationships only ``contracting_entity`` + ``contractor``
  (NOT ``contract_unit``).
- Document upload is multipart, PDF only, type ``CONTRACT`` | ``ATTACHMENT``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# --- Enums (verified against openapi.json) ---

ContractStatus = Literal["COMPLETED", "DRAFT", "ORDERED", "PARTIAL_COMPLETED"]
ContractDocumentType = Literal["ATTACHMENT", "CONTRACT"]

# JSON:API relationship resource-type strings (uppercase, per the spec ResourceType).
RELATIONSHIP_TYPE_CONTRACT_UNIT = "CONTRACT_UNIT"
RELATIONSHIP_TYPE_CONTRACTING_ENTITY = "CONTRACTING_ENTITY"
RELATIONSHIP_TYPE_CONTRACTOR = "CONTRACTOR"


# --- Shared building blocks ---


class WriteAmount(BaseModel):
    """Contract amount for writes. Money is sent as STRINGS (net/tax, not gross)."""

    model_config = ConfigDict(extra="forbid")

    currency: str = "EUR"
    net: str
    tax: str


class RelationshipRef(BaseModel):
    """A JSON:API relationship reference attributes object: ``{id, type}``."""

    model_config = ConfigDict(extra="forbid")

    id: str
    type: str


class RelationshipData(BaseModel):
    """A JSON:API relationship wrapper: ``{data: {id, type}}``."""

    model_config = ConfigDict(extra="forbid")

    data: RelationshipRef


# --- CREATE: POST /contracts/  (CreateContractRequestFincon) ---


class CreateContractAttributes(BaseModel):
    """Attributes for creating a contract. No status, no contract_type."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(max_length=250)
    is_reverse_charge: bool
    amount: WriteAmount | None = None
    booking_account_number: int | None = Field(default=None, ge=1, le=999999999)
    contract_number: str | None = Field(default=None, max_length=250)
    cost_center: str | None = Field(default=None, max_length=100)
    date_contract: str | None = None
    description: str | None = None


class CreateContractRelationships(BaseModel):
    """Relationships for creating a contract.

    ``contracting_entity`` and ``contract_unit`` are both REQUIRED;
    ``contractor`` is optional.
    """

    model_config = ConfigDict(extra="forbid")

    contracting_entity: RelationshipData
    contract_unit: RelationshipData
    contractor: RelationshipData | None = None


class CreateContractData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = "CONTRACT"
    attributes: CreateContractAttributes
    relationships: CreateContractRelationships


class CreateContractRequest(BaseModel):
    """Top-level body for ``POST /contracts/`` (CreateContractRequestFincon)."""

    model_config = ConfigDict(extra="forbid")

    data: CreateContractData

    def to_api_dict(self) -> dict:
        """Serialize to the JSON:API request body, dropping unset optionals."""
        return self.model_dump(exclude_none=True)


# --- UPDATE: PATCH /contracts/{id}/  (UpdateContractRequest) ---


class UpdateContractAttributes(BaseModel):
    """Attributes for updating a contract. All optional, includes status."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, max_length=250)
    status: ContractStatus | None = None
    amount: WriteAmount | None = None
    booking_account_number: int | None = Field(default=None, ge=1, le=999999999)
    contract_number: str | None = Field(default=None, max_length=250)
    cost_center: str | None = Field(default=None, max_length=100)
    date_contract: str | None = None
    description: str | None = None


class UpdateContractRelationships(BaseModel):
    """Relationships for updating a contract.

    Only ``contracting_entity`` and ``contractor`` (NOT ``contract_unit``).
    """

    model_config = ConfigDict(extra="forbid")

    contracting_entity: RelationshipData | None = None
    contractor: RelationshipData | None = None


class UpdateContractData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str = "CONTRACT"
    attributes: UpdateContractAttributes
    relationships: UpdateContractRelationships | None = None


class UpdateContractRequest(BaseModel):
    """Top-level body for ``PATCH /contracts/{id}/`` (UpdateContractRequest)."""

    model_config = ConfigDict(extra="forbid")

    data: UpdateContractData

    def to_api_dict(self) -> dict:
        """Serialize to the JSON:API request body, dropping unset optionals."""
        return self.model_dump(exclude_none=True)


# --- DOCUMENT UPLOAD: POST /contracts/{contract_id}/documents/ (multipart) ---


class ContractDocumentUpload(BaseModel):
    """Multipart form fields for a contract document upload (PDF only).

    The actual binary is supplied separately as the ``upload`` file part; this
    model captures and validates the form metadata.
    """

    model_config = ConfigDict(extra="forbid")

    document_type: ContractDocumentType
    filename: str
