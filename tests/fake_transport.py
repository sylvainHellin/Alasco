"""Fake httpx transport for offline client tests (no network).

``FakeAlasco`` records every request and answers from a route table keyed by
``(METHOD, path)``. Paths include the base path (e.g. ``/capex/v1/assets/``),
so a test also proves which API a client targets.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx

Responder = dict[str, Any] | Callable[[httpx.Request], httpx.Response]

FAKE_TOKEN = "fake-token"
FAKE_KEY = "fake-key"


class FakeAlasco:
    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], list[Responder]] = {}
        self.requests: list[httpx.Request] = []

    def add(self, method: str, path: str, body: Responder, status: int = 200) -> None:
        """Queue a response for ``(method, path)``; repeated adds answer in order."""
        if isinstance(body, dict):
            payload = body

            def responder(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(status, json=payload)

            self.routes.setdefault((method, path), []).append(responder)
        else:
            self.routes.setdefault((method, path), []).append(body)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        queue = self.routes.get((request.method, request.url.path))
        if not queue:
            return httpx.Response(404, json={"errors": [{"detail": "no fake route"}]})
        responder = queue.pop(0) if len(queue) > 1 else queue[0]
        assert callable(responder)
        return responder(request)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    @property
    def last(self) -> httpx.Request:
        return self.requests[-1]

    def last_json(self) -> dict[str, Any]:
        return json.loads(self.last.content)
