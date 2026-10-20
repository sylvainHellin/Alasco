"""Offline tests for the FinCon/CapEx client split (fake httpx transport)."""

from __future__ import annotations

from alasco import AlascoClient, CapexClient
from tests.fake_transport import FAKE_KEY, FAKE_TOKEN, FakeAlasco


def test_capex_client_targets_capex_base_path_with_shared_auth():
    fake = FakeAlasco()
    fake.add("GET", "/capex/v1/assets/", {"data": []})
    with CapexClient(token=FAKE_TOKEN, key=FAKE_KEY, transport=fake.transport) as client:
        assert client.base_url == "https://api.alasco.de/capex/v1"
        client.get("/assets/")
    request = fake.last
    assert str(request.url) == "https://api.alasco.de/capex/v1/assets/"
    assert request.headers["X-API-TOKEN"] == FAKE_TOKEN
    assert request.headers["X-API-KEY"] == FAKE_KEY


def test_fincon_client_default_base_url_unchanged():
    fake = FakeAlasco()
    fake.add("GET", "/v1/properties/", {"data": []})
    with AlascoClient(token=FAKE_TOKEN, key=FAKE_KEY, transport=fake.transport) as client:
        assert client.base_url == "https://api.alasco.de/v1"
        client.get("/properties/")
    assert str(fake.last.url) == "https://api.alasco.de/v1/properties/"


def test_explicit_base_url_still_overrides_default():
    client = CapexClient(token=FAKE_TOKEN, key=FAKE_KEY, base_url="https://example.test/capex/v1/")
    assert client.base_url == "https://example.test/capex/v1"
    client.close()


def test_get_all_follows_absolute_next_link_on_capex():
    fake = FakeAlasco()
    next_url = "https://api.alasco.de/capex/v1/assets/?cursor%5Bposition%5D=abc"
    fake.add("GET", "/capex/v1/assets/", {"data": [{"id": "a1"}], "links": {"next": next_url}})
    fake.add("GET", "/capex/v1/assets/", {"data": [{"id": "a2"}], "links": {"next": None}})
    with CapexClient(token=FAKE_TOKEN, key=FAKE_KEY, transport=fake.transport) as client:
        data = client.get_all("/assets/", {"filter[identifier.exact]": "251"})
    assert [d["id"] for d in data] == ["a1", "a2"]
    assert fake.requests[0].url.params["filter[identifier.exact]"] == "251"
    assert fake.requests[1].url.params["cursor[position]"] == "abc"
