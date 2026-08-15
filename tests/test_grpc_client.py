from __future__ import annotations

from typing import Any

import pytest

import mailbaby.grpc.client as grpc_client_mod
from mailbaby import (
    AsyncMailBabyGrpcClient,
    MailBabyGrpcClient,
)
from mailbaby.grpc.gen import pb2
from tests.factories import make_attachment, make_email


class FakeStub:
    """Records calls for the sync client; methods return responses directly."""

    def __init__(self, channel: Any) -> None:
        self.channel = channel
        self.calls: list[tuple[str, pb2.SendMailRequest]] = []
        self.metadata_calls: list[tuple] = []

    def Send(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        self.calls.append(("Send", req))
        self.metadata_calls.append((metadata, timeout))
        is_async = bool(getattr(req, "async", False))
        return pb2.SendMailResponse(
            id=req.id or "server-id",
            status="queued" if is_async else "sent",
            message="email sent successfully",
            sent_at=1234,
        )

    def SendBatch(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        self.calls.append(("SendBatch", req))
        results = []
        for e in req.emails:
            r = pb2.SendMailResponse(
                id=e.id or "server-id",
                status="sent",
                message="email sent successfully",
                sent_at=1234,
            )
            results.append(r)
        return pb2.BatchSendMailResponse(
            total=len(req.emails),
            succeeded=len(req.emails),
            failed=0,
            results=results,
        )

    def Ping(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        return pb2.PingResponse(status="OK", version="test", timestamp=99)

    def HealthCheck(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        return pb2.HealthCheckResponse(
            status=pb2.HealthCheckResponse.SERVING, details={"queue": "ok"}
        )


class FakeAsyncStub(FakeStub):
    """Same recorder but methods return coroutines."""

    async def Send(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        return super().Send(req, metadata=metadata, timeout=timeout)

    async def SendBatch(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        self.calls.append(("SendBatch", req))
        total = len(req.emails)
        results = [
            pb2.SendMailResponse(
                id=e.id or "server-id",
                status="sent",
                message="email sent successfully",
                sent_at=1234,
            )
            for e in req.emails
        ]
        return pb2.BatchSendMailResponse(
            total=total, succeeded=total, failed=0, results=results
        )

    async def Ping(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        return super().Ping(req)

    async def HealthCheck(self, req, metadata=None, timeout=None, **kwargs):  # noqa: N802
        return super().HealthCheck(req)


def _monkeypatch_stub(monkeypatch: pytest.MonkeyPatch, cls: Any) -> FakeStub:
    fake = cls(None)
    monkeypatch.setattr(grpc_client_mod.pb2_grpc, "MailServiceStub", lambda ch: fake)
    return fake


class TestGrpcSync:
    def test_send(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _monkeypatch_stub(monkeypatch, FakeStub)
        with MailBabyGrpcClient("h:8081") as c:
            r = c.send(make_email(id="e1"))
        assert r.status == "sent"
        req = fake.calls[0][1]
        assert req.id == "e1"
        assert req.subject == "Test Subject"

    def test_send_full_mapping(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _monkeypatch_stub(monkeypatch, FakeStub)
        email = make_email(
            id="x",
            account="mkt",
            from_="a@b.c",
            from_name="AB",
            reply_to="r@b.c",
            cc=["c@b.c"],
            bcc=["d@b.c"],
            text_body="t",
            html_body="<b>h</b>",
            headers={"H": "1"},
            tags=["z"],
            metadata={"m": "n"},
            attachments=[make_attachment()],
        )
        with MailBabyGrpcClient() as c:
            c.send(email)
        req = fake.calls[0][1]
        assert getattr(req, "from") == "a@b.c"
        assert req.attachments[0].data == b"%PDF-1.4 fake pdf"
        assert req.headers["H"] == "1"
        assert req.metadata["m"] == "n"
        assert req.tags == ["z"]

    def test_send_async_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _monkeypatch_stub(monkeypatch, FakeStub)
        with MailBabyGrpcClient() as c:
            r = c.send(make_email(), async_=True)
        assert r.status == "queued"
        assert getattr(fake.calls[0][1], "async") is True

    def test_batch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _monkeypatch_stub(monkeypatch, FakeStub)
        with MailBabyGrpcClient() as c:
            b = c.send_batch([make_email(), make_email()], async_=True)
        assert b.total == 2 and b.succeeded == 2
        assert fake.calls[0][0] == "SendBatch"
        assert getattr(fake.calls[0][1], "async") is True

    def test_metadata_auth(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _monkeypatch_stub(monkeypatch, FakeStub)
        with MailBabyGrpcClient("h:8081", api_key="secret") as c:
            c.send(make_email())
        md, _ = fake.metadata_calls[0]
        assert md == (("authorization", "Bearer secret"),)

    def test_custom_header_metadata(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _monkeypatch_stub(monkeypatch, FakeStub)
        with MailBabyGrpcClient("h:8081", api_key="k", header_name="X-API-Key") as c:
            c.send(make_email())
        md, _ = fake.metadata_calls[0]
        assert md == (("X-API-Key", "k"),)

    def test_ping(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _monkeypatch_stub(monkeypatch, FakeStub)
        with MailBabyGrpcClient() as c:
            p = c.ping()
        assert p["status"] == "OK"

    def test_health(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _monkeypatch_stub(monkeypatch, FakeStub)
        with MailBabyGrpcClient() as c:
            h = c.health_check()
        assert h.status == "SERVING"
        assert h.components == {"queue": "ok"}


class TestGrpcAsync:
    async def test_send(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _monkeypatch_stub(monkeypatch, FakeAsyncStub)
        async with AsyncMailBabyGrpcClient("h:8081") as c:
            r = await c.send(make_email())
        assert r.id == "server-id"

    async def test_metadata(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _monkeypatch_stub(monkeypatch, FakeAsyncStub)
        async with AsyncMailBabyGrpcClient("h:8081", api_key="sk") as c:
            await c.send(make_email())
        md, _ = fake.metadata_calls[0]
        assert md == (("authorization", "Bearer sk"),)

    async def test_batch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _monkeypatch_stub(monkeypatch, FakeAsyncStub)
        async with AsyncMailBabyGrpcClient("h:8081") as c:
            b = await c.send_batch([make_email()])
        assert b.total == 1

    async def test_ping_health(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _monkeypatch_stub(monkeypatch, FakeAsyncStub)
        async with AsyncMailBabyGrpcClient("h:8081") as c:
            p = await c.ping()
            h = await c.health_check()
        assert p["version"] == "test"
        assert h.components == {"queue": "ok"}
