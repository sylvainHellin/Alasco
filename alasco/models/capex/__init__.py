"""Typed models for the Alasco CapEx API (``https://api.alasco.de/capex/v1``).

Use with ``alasco.CapexClient``. Resources: Asset, Measure, ContractingEntity,
Contractor, Contract, Invoice, InvoiceDocument. ``PaidInvoice`` carries the
attributes for ``Invoice.add_paid``.
"""

from alasco.models.capex._common import Amount, ContractAmount, NetTaxAmount, filter_params
from alasco.models.capex.resources import (
    Asset,
    AssetAttributes,
    Contract,
    ContractAttributes,
    ContractingEntity,
    ContractingEntityAttributes,
    Contractor,
    ContractorAttributes,
    Invoice,
    InvoiceAttributes,
    InvoiceDocument,
    InvoiceDocumentAttributes,
    InvoiceDocumentLinks,
    Measure,
    MeasureAttributes,
)
from alasco.models.capex.write import (
    AddPaidInvoiceRequest,
    ContractorCreateAttributes,
    ContractStatus,
    CreateContractorRequest,
    CreateContractRequest,
    InvoiceDocumentType,
    InvoiceType,
    PaidInvoice,
    UpdateContractRequest,
)

__all__ = [
    "AddPaidInvoiceRequest",
    "Amount",
    "Asset",
    "AssetAttributes",
    "Contract",
    "ContractAmount",
    "ContractAttributes",
    "ContractStatus",
    "ContractingEntity",
    "ContractingEntityAttributes",
    "Contractor",
    "ContractorAttributes",
    "ContractorCreateAttributes",
    "CreateContractRequest",
    "CreateContractorRequest",
    "Invoice",
    "InvoiceAttributes",
    "InvoiceDocument",
    "InvoiceDocumentAttributes",
    "InvoiceDocumentLinks",
    "InvoiceDocumentType",
    "InvoiceType",
    "Measure",
    "MeasureAttributes",
    "NetTaxAmount",
    "PaidInvoice",
    "UpdateContractRequest",
    "filter_params",
]
