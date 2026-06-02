"""Shared pytest fixtures.

Integration tests run ONLY against the Alasco sandbox property. Credentials and
the property id are env-selected and NEVER hardcoded to TUC prod. The TUC prod
property id (be091b14-...) must never be used by any write test.

Env vars (read from process env; the repo's own ``.env`` or the caller's shell
should populate them):
- ALASCO_TEST_TOKEN / ALASCO_TEST_KEY     sandbox credentials
  (falls back to token_alasco_test_env / key_alasco_test_env)
- ALASCO_TEST_PROPERTY_ID                 sandbox property (default Wohnpark Unteraching)
"""

from __future__ import annotations

import os

import pytest

from alasco import AlascoClient

# Default sandbox property: Wohnpark Unteraching. NEVER the TUC prod id.
DEFAULT_SANDBOX_PROPERTY_ID = "97bad92e-0fd8-4987-9f8e-aafcd4eafcd7"
TUC_PROD_PROPERTY_ID = "be091b14-021d-4041-9ff0-13d271df159c"


def _sandbox_token() -> str | None:
    return os.environ.get("ALASCO_TEST_TOKEN") or os.environ.get("token_alasco_test_env")


def _sandbox_key() -> str | None:
    return os.environ.get("ALASCO_TEST_KEY") or os.environ.get("key_alasco_test_env")


@pytest.fixture(scope="session")
def sandbox_property_id() -> str:
    pid = os.environ.get("ALASCO_TEST_PROPERTY_ID", DEFAULT_SANDBOX_PROPERTY_ID)
    if pid == TUC_PROD_PROPERTY_ID:
        pytest.fail("Refusing to run write tests against the TUC prod property id.")
    return pid


@pytest.fixture
def sandbox_client():
    token = _sandbox_token()
    key = _sandbox_key()
    if not token or not key:
        pytest.skip(
            "Sandbox credentials not set "
            "(ALASCO_TEST_TOKEN/KEY or token_alasco_test_env/key_alasco_test_env)."
        )
    with AlascoClient(token=token, key=key) as client:
        yield client


def _minimal_pdf() -> bytes:
    """A tiny but structurally valid single-page PDF."""
    return (
        b"%PDF-1.4\n"
        b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
        b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
        b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]>>endobj\n"
        b"xref\n0 4\n"
        b"0000000000 65535 f \n"
        b"0000000009 00000 n \n"
        b"0000000052 00000 n \n"
        b"0000000101 00000 n \n"
        b"trailer<</Size 4/Root 1 0 R>>\n"
        b"startxref\n164\n"
        b"%%EOF\n"
    )


@pytest.fixture
def minimal_pdf_bytes() -> bytes:
    return _minimal_pdf()
