"""Document models for Alasco API (Contract, ChangeOrder, Invoice documents)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from alasco.client import AlascoClient


class DocumentLinks(BaseModel):
    """Links included in document resource responses."""

    model_config = ConfigDict(extra="ignore")

    self: str
    download: str
    download_annotated: str | None = None  # Not available for ContractDocuments


# --- Contract Documents ---


class ContractDocumentAttributes(BaseModel):
    """Attributes for a ContractDocument resource."""

    model_config = ConfigDict(extra="ignore")

    document_type: str  # CONTRACT | ATTACHMENT
    filename: str
    uploaded_at: str  # ISO datetime


class ContractDocument(BaseModel):
    """Contract document resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "contract_document"
    attributes: ContractDocumentAttributes
    links: DocumentLinks | None = None

    @property
    def filename(self) -> str:
        return self.attributes.filename

    @property
    def document_type(self) -> str:
        return self.attributes.document_type

    @property
    def download_url(self) -> str | None:
        return self.links.download if self.links else None

    def download(self, client: AlascoClient) -> bytes:
        """Download the document file.

        Returns:
            File content as bytes
        """
        if not self.links or not self.links.download:
            msg = f"No download link for document {self.id}"
            raise ValueError(msg)
        return client.download(self.links.download)

    @classmethod
    def list(
        cls, client: AlascoClient, contract_id: str
    ) -> list[ContractDocument]:
        """List documents for a contract."""
        data = client.get_all(f"/contracts/{contract_id}/documents/")
        return [cls.model_validate(item) for item in data]

    @classmethod
    def upload(
        cls,
        client: AlascoClient,
        contract_id: str,
        *,
        document_type: str,
        file_path: str | None = None,
        content: bytes | None = None,
        filename: str | None = None,
    ) -> ContractDocument:
        """Upload a document to a contract via multipart POST.

        The file MUST be a valid PDF and ``document_type`` must be ``CONTRACT``
        or ``ATTACHMENT`` (validated via the write model).

        Provide either ``file_path`` (read from disk) or ``content`` + a
        ``filename``.

        Returns:
            The created ContractDocument (parsed from the response ``data``).
        """
        from pathlib import Path

        from alasco.models.write import ContractDocumentUpload

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

        # Validate form metadata (document_type enum + filename) via the write model.
        form = ContractDocumentUpload(document_type=document_type, filename=resolved_name)

        response = client.post_multipart(
            f"/contracts/{contract_id}/documents/",
            data={"document_type": form.document_type},
            files={"upload": (form.filename, file_bytes, "application/pdf")},
        )
        return cls.model_validate(response.get("data", {}))


# --- ChangeOrder Documents ---


class ChangeOrderDocumentAttributes(BaseModel):
    """Attributes for a ChangeOrderDocument resource."""

    model_config = ConfigDict(extra="ignore")

    # CHANGE_ORDER | CHANGE_ORDER_OFFER | ATTACHMENT | AUDITED_CHANGE_ORDER | ...
    document_type: str
    filename: str
    uploaded_at: str  # ISO datetime


class ChangeOrderDocument(BaseModel):
    """Change order document resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "change_order_document"
    attributes: ChangeOrderDocumentAttributes
    links: DocumentLinks | None = None

    @property
    def filename(self) -> str:
        return self.attributes.filename

    @property
    def document_type(self) -> str:
        return self.attributes.document_type

    @property
    def download_url(self) -> str | None:
        return self.links.download if self.links else None

    @property
    def download_annotated_url(self) -> str | None:
        return self.links.download_annotated if self.links else None

    def download(self, client: AlascoClient) -> bytes:
        """Download the document file."""
        if not self.links or not self.links.download:
            msg = f"No download link for document {self.id}"
            raise ValueError(msg)
        return client.download(self.links.download)

    def download_annotated(self, client: AlascoClient) -> bytes:
        """Download the annotated version of the document file."""
        if not self.links or not self.links.download_annotated:
            msg = f"No annotated download link for document {self.id}"
            raise ValueError(msg)
        return client.download(self.links.download_annotated)

    @classmethod
    def list(
        cls, client: AlascoClient, change_order_id: str
    ) -> list[ChangeOrderDocument]:
        """List documents for a change order."""
        data = client.get_all(
            f"/change_orders/{change_order_id}/documents/"
        )
        return [cls.model_validate(item) for item in data]


# --- Invoice Documents ---


class InvoiceDocumentAttributes(BaseModel):
    """Attributes for an InvoiceDocument resource."""

    model_config = ConfigDict(extra="ignore")

    document_type: str  # INVOICE | ATTACHMENT | AUDITED_INVOICE | REVISED_INVOICE | ...
    filename: str
    uploaded_at: str  # ISO datetime


class InvoiceDocument(BaseModel):
    """Invoice document resource (JSON:API format)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    type: str = "invoice_document"
    attributes: InvoiceDocumentAttributes
    links: DocumentLinks | None = None

    @property
    def filename(self) -> str:
        return self.attributes.filename

    @property
    def document_type(self) -> str:
        return self.attributes.document_type

    @property
    def download_url(self) -> str | None:
        return self.links.download if self.links else None

    @property
    def download_annotated_url(self) -> str | None:
        return self.links.download_annotated if self.links else None

    def download(self, client: AlascoClient) -> bytes:
        """Download the document file."""
        if not self.links or not self.links.download:
            msg = f"No download link for document {self.id}"
            raise ValueError(msg)
        return client.download(self.links.download)

    def download_annotated(self, client: AlascoClient) -> bytes:
        """Download the annotated version of the document file."""
        if not self.links or not self.links.download_annotated:
            msg = f"No annotated download link for document {self.id}"
            raise ValueError(msg)
        return client.download(self.links.download_annotated)

    @classmethod
    def list(
        cls, client: AlascoClient, invoice_id: str
    ) -> list[InvoiceDocument]:
        """List documents for an invoice."""
        data = client.get_all(f"/invoices/{invoice_id}/documents/")
        return [cls.model_validate(item) for item in data]
