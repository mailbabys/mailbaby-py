from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from mailbaby import (
    AsyncMailBabyClient,
    AuthenticationError,
    DeliveryError,
    EnqueueError,
    MailBabyClient,
    RequestFailedError,
    UnavailableError,
    ValidationError,
)
from mailbaby.rest.client import _json
from tests.factories import make_attachment, make_email


def _response(status: int, body: dict[str, Any] | None) -> httpx.Response:
    return httpx.Response(status, json=body)


class _FakeTransport(httpx.MockTransport):
    """Records the last request for assertions, delegating to the handler."""

    last_request: httpx.Request | None = None

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.last_request = request
        return super().handle_request(request)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.last_request = request
        return await super().handle_async_request(request)


_SENT = {"id": "abc", "status": "sent", "message": "ok", "sent_at": 1000}
_QUEUED = {"id": "abc", "status": "queued", "message": "ok", "sent_at": 1000}


def _dispatch(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path == "/v1/email/send":
        return _response(200, _SENT)
    if path == "/v1/email/batch":
        return _response(
            200,
            {
                "total": 2,
                "succeeded": 1,
                "failed": 1,
                "results": [
                    {"id": "a", "status": "sent", "message": "ok", "sent_at": 1},
                    {"id": "b", "status": "failed", "message": "no", "sent_at": 2},
                ],
            },
        )
    if path == "/livez":
        return _response(200, {"status": "UP", "timestamp": "t"})
    if path == "/readyz":
        return _response(
            503,
            {"status": "DOWN", "components": {"smtp": "DOWN: bad"}},
        )
    if path == "/healthz":
        return httpx.Response(200, text="OK")
    return _response(404, {"error": "not_found"})


@pytest.fixture
def transport() -> _FakeTransport:
    return _FakeTransport(_dispatch)


def _client(transport: _FakeTransport, **kwargs: Any) -> MailBabyClient:
    return MailBabyClient(transport=transport, **kwargs)


def _async_client(transport: _FakeTransport, **kwargs: Any) -> AsyncMailBabyClient:
    return AsyncMailBabyClient(transport=transport, **kwargs)


def _payload(transport: _FakeTransport) -> dict[str, Any]:
    assert transport.last_request is not None
    return json.loads(transport.last_request.content)


class TestSend:
    def test_send_success(self, transport: _FakeTransport) -> None:
        with _client(transport) as c:
            r = c.send(make_email())
        assert r.status == "sent"
        assert r.id == "abc"

    def test_send_payload_shape(self, transport: _FakeTransport) -> None:
        email = make_email(
            id="fix-me",
            account="mkt",
            from_="x@y.com",
            reply_to="r@y.com",
            html_body="<p>hi</p>",
            headers={"X-A": "1"},
            attachments=[make_attachment()],
            tags=["t"],
            metadata={"k": "v"},
        )
        with _client(transport) as c:
            c.send(email)
        p = _payload(transport)
        assert p["id"] == "fix-me"
        assert p["from"] == "x@y.com"
        assert p["attachments"][0]["filename"] == "report.pdf"
        assert "async" not in p

    def test_send_async_sets_flag(self, transport: _FakeTransport) -> None:
        with _client(transport, base_url="http://h") as c:
            r = c.send(make_email(), async_=True)
        assert r.status == "sent"
        assert _payload(transport)["async"] is True

    def test_send_generates_id_when_missing(self, transport: _FakeTransport) -> None:
        with _client(transport) as c:
            c.send(make_email())
        assert len(_payload(transport)["id"]) == 32


class TestAuth:
    def test_bearer_header(self, transport: _FakeTransport) -> None:
        with _client(transport, api_key="secret") as c:
            c.send(make_email())
        assert (
            transport.last_request.headers.get("Authorization") == "Bearer secret"
        )

    def test_custom_header(self, transport: _FakeTransport) -> None:
        with _client(transport, api_key="k", header_name="X-API-Key") as c:
            c.send(make_email())
        assert transport.last_request.headers.get("X-API-Key") == "k"
        assert "Authorization" not in transport.last_request.headers


class TestBatch:
    def test_batch_result(self, transport: _FakeTransport) -> None:
        with _client(transport) as c:
            b = c.send_batch([make_email(), make_email()])
        assert b.total == 2 and b.succeeded == 1 and b.failed == 1
        assert b.results[0].id == "a"

    def test_batch_async(self, transport: _FakeTransport) -> None:
        with _client(transport) as c:
            c.send_batch([make_email()], async_=True)
        assert _payload(transport)["async"] is True
        assert _payload(transport)["emails"][0]["subject"] == "Test Subject"


class TestErrors:
    def test_validation_error(self) -> None:
        t = _FakeTransport(
            lambda r: _response(
                400, {"code": 400, "error": "validation_error", "details": "to required"}
            )
        )
        with _client(t) as c:
            with pytest.raises(ValidationError) as ei:
                c.send(make_email())
        assert ei.value.status_code == 400
        assert ei.value.details == "to required"

    def test_auth_error(self) -> None:
        t = _FakeTransport(
            lambda r: _response(
                401, {"code": 401, "error": "unauthorized", "message": "bad key"}
            )
        )
        with _client(t) as c:
            with pytest.raises(AuthenticationError) as ei:
                c.send(make_email())
        assert "bad key" in str(ei.value)

    def test_delivery_error(self) -> None:
        t = _FakeTransport(
            lambda r: _response(
                500, {"code": 500, "error": "delivery_failed", "details": "smtp down"}
            )
        )
        with _client(t) as c:
            with pytest.raises(DeliveryError):
                c.send(make_email())

    def test_enqueue_error(self) -> None:
        t = _FakeTransport(
            lambda r: _response(
                500, {"code": 500, "error": "enqueue_failed", "details": "broken"}
            )
        )
        with _client(t) as c:
            with pytest.raises(EnqueueError):
                c.send(make_email(), async_=True)

    def test_transport_error(self) -> None:
        def boom(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        with _client(_FakeTransport(boom)) as c:
            with pytest.raises(RequestFailedError):
                c.send(make_email())


class TestHealth:
    def test_livez(self, transport: _FakeTransport) -> None:
        with _client(transport) as c:
            h = c.livez()
        assert h.status == "UP"

    def test_readyz_down(self, transport: _FakeTransport) -> None:
        with _client(transport) as c:
            with pytest.raises(UnavailableError):
                c.readyz()

    def test_healthz_plain(self, transport: _FakeTransport) -> None:
        with _client(transport) as c:
            h = c.healthz()
        assert h.status == "OK"


class TestAsyncClient:
    async def test_send(self, transport: _FakeTransport) -> None:
        async with _async_client(transport) as c:
            r = await c.send(make_email())
        assert r.status == "sent"
        assert transport.last_request is not None

    async def test_batch(self, transport: _FakeTransport) -> None:
        async with _async_client(transport) as c:
            b = await c.send_batch([make_email()], async_=True)
        assert b.total == 2
        assert _payload(transport)["async"] is True

    async def test_auth_header(self, transport: _FakeTransport) -> None:
        async with _async_client(transport, api_key="sk") as c:
            await c.send(make_email())
        assert transport.last_request.headers.get("Authorization") == "Bearer sk"


def test_json_helper_non_json() -> None:
    r = httpx.Response(200, text="not json")
    assert _json(r) is None
