"""Invoice model for Alasco API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from alasco.models.base import MonetaryAmount

if TYPE_CHECKING:
    from alasco.client import AlascoClient
    from alasco.models.documents import InvoiceDocument


class InvoiceAttributes(BaseModel):
    """Attributes for an Invoice resource (FinCon API)."""

    model_config = ConfigDict(extra="ignore")

    # Required fields
    date_created: str  # ISO datetime
    invoice_process_state: str  # NEW | DETAILS_ENTERED | CHECKED | APPROVED | PAID
    is_reverse_charge: bool = False

    # Optional fields
    contract: str | None = None  # UUID reference to Contract
    # SINGLE | INSTALLMENT | FINAL | ADVANCE | PARTIAL_FINAL | RECURRING | RETAINAGE_PAYOUT
    invoice_type: str | None = None
    external_identifier: str | None = None  # External invoice number
    internal_identifier: str | None = None  # Internal invoice number
    installment_number: int | None = None

    # Date fields
    date_posted: str | None = None  # Invoice date
    date_due: str | None = None
    date_received: str | None = None
    date_approved: str | None = None
    date_discount: str | None = None

    # Monetary fields (API returns as strings in {net, tax, currency} objects)
    unaudited_amount: MonetaryAmount | dict | None = None
    audited_amount: MonetaryAmount | dict | None = None
    undiscounted_approved_amount: MonetaryAmount | dict | None = None
    discounted_approved_amount: MonetaryAmount | dict | None = None
    cash_discount: MonetaryAmount | dict | None = None

    # Payment fields
    payment_date: str | None = None
    payment_created: str | None = None
    payment_comment: str | None = None
    payment_amount_gross: dict | None = None  # Different shape: {amount, currency}


class Invoice(BaseModel):
    """Invoice resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "invoice"
    attributes: InvoiceAttributes

    @property
    def contract_id(self) -> str | None:
        return self.attributes.contract

    @property
    def invoice_process_state(self) -> str:
        return self.attributes.invoice_process_state

    @property
    def invoice_type(self) -> str | None:
        return self.attributes.invoice_type

    @property
    def external_identifier(self) -> str | None:
        return self.attributes.external_identifier

    def get_documents(self, client: AlascoClient) -> list[InvoiceDocument]:
        """Fetch documents for this invoice."""
        from alasco.models.documents import InvoiceDocument

        return InvoiceDocument.list(client, invoice_id=self.id)

    @classmethod
    def get(cls, client: AlascoClient, invoice_id: str) -> Invoice:
        """Fetch a single invoice by ID."""
        data = client.get_single(f"/invoices/{invoice_id}/")
        return cls.model_validate(data)

    @classmethod
    def list(
        cls,
        client: AlascoClient,
        contract_ids: list[str] | None = None,
        invoice_type: list[str] | None = None,
        invoice_process_state: list[str] | None = None,
    ) -> list[Invoice]:
        """List invoices with optional filters.

        Args:
            client: AlascoClient instance
            contract_ids: Filter by contract UUIDs (filter[contract.in])
            invoice_type: Filter by type (filter[invoice_type.in])
            invoice_process_state: Filter by state (filter[invoice_process_state.in])
        """
        params: dict[str, str] = {}
        if contract_ids:
            params["filter[contract.in]"] = ",".join(contract_ids)
        if invoice_type:
            params["filter[invoice_type.in]"] = ",".join(invoice_type)
        if invoice_process_state:
            params["filter[invoice_process_state.in]"] = ",".join(invoice_process_state)

        data = client.get_all("/invoices/", params or None)
        return [cls.model_validate(item) for item in data]
