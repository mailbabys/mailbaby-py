from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import grpc

from mailbaby.grpc.convert import email_to_proto, proto_health, proto_ping, proto_send_result
from mailbaby.grpc.gen import pb2, pb2_grpc
from mailbaby.models import BatchResult, Email, HealthStatus, SendResult

__all__ = ["MailBabyGrpcClient", "AsyncMailBabyGrpcClient"]

_UNAUTHENTICATED = grpc.StatusCode.UNAUTHENTICATED
_INVALID_ARGUMENT = grpc.StatusCode.INVALID_ARGUMENT
_NOT_FOUND = grpc.StatusCode.NOT_FOUND
_UNAVAILABLE = grpc.StatusCode.UNAVAILABLE
_INTERNAL = grpc.StatusCode.INTERNAL


def _metadata(api_key: str | None, header_name: str | None) -> tuple[tuple[str, str], ...]:
    """Auth metadata as accepted by the server (rpc/auth.go)."""
    if not api_key:
        return ()
    if header_name:
        return ((header_name, api_key),)
    return (("authorization", f"Bearer {api_key}"),)


class MailBabyGrpcClient:
    """Synchronous gRPC client for ``mailbaby.v1.MailService``.

    Usage::

        client = MailBabyGrpcClient("localhost:8081", api_key="secret")
        result = client.send(Email(to=["a@example.com"], subject="Hi"))
    """

    def __init__(
        self,
        target: str = "localhost:8081",
        *,
        api_key: str | None = None,
        header_name: str | None = None,
        secure: bool = False,
        options: Sequence[tuple[str, Any]] | None = None,
    ) -> None:
        if secure:
            self._channel = grpc.secure_channel(
                target, grpc.ssl_channel_credentials(), options=options
            )
        else:
            self._channel = grpc.insecure_channel(target, options=options)
        self._stub = pb2_grpc.MailServiceStub(self._channel)
        self._metadata = _metadata(api_key, header_name)

    def send(
        self, email: Email, *, async_: bool = False, timeout: float | None = None
    ) -> SendResult:
        resp: pb2.SendMailResponse = self._stub.Send(
            email_to_proto(email, async_=async_),
            metadata=self._metadata,
            timeout=timeout,
        )
        return proto_send_result(resp)

    def send_batch(
        self,
        emails: Sequence[Email],
        *,
        async_: bool = False,
        timeout: float | None = None,
    ) -> BatchResult:
        req = pb2.BatchSendMailRequest(
            emails=[email_to_proto(e, async_=async_) for e in emails],
            **{"async": async_},  # field named "async" is a Python keyword
        )
        resp: pb2.BatchSendMailResponse = self._stub.SendBatch(
            req, metadata=self._metadata, timeout=timeout
        )
        return BatchResult(
            total=resp.total,
            succeeded=resp.succeeded,
            failed=resp.failed,
            results=[proto_send_result(r) for r in resp.results],
        )

    def ping(self, message: str = "", *, timeout: float | None = None) -> dict[str, Any]:
        resp: pb2.PingResponse = self._stub.Ping(
            pb2.PingRequest(message=message), metadata=self._metadata, timeout=timeout
        )
        return proto_ping(resp)

    def health_check(self, service: str = "", *, timeout: float | None = None) -> HealthStatus:
        resp: pb2.HealthCheckResponse = self._stub.HealthCheck(
            pb2.HealthCheckRequest(service=service),
            metadata=self._metadata,
            timeout=timeout,
        )
        return proto_health(resp)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> MailBabyGrpcClient:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


class AsyncMailBabyGrpcClient:
    """Asynchronous gRPC client backed by :mod:`grpc.aio`."""

    def __init__(
        self,
        target: str = "localhost:8081",
        *,
        api_key: str | None = None,
        header_name: str | None = None,
        secure: bool = False,
        options: Sequence[tuple[str, Any]] | None = None,
    ) -> None:
        if secure:
            self._channel = grpc.aio.secure_channel(
                target, grpc.ssl_channel_credentials(), options=options
            )
        else:
            self._channel = grpc.aio.insecure_channel(target, options=options)
        self._stub = pb2_grpc.MailServiceStub(self._channel)
        self._metadata = _metadata(api_key, header_name)

    async def send(
        self, email: Email, *, async_: bool = False, timeout: float | None = None
    ) -> SendResult:
        resp = await self._stub.Send(
            email_to_proto(email, async_=async_),
            metadata=self._metadata,
            timeout=timeout,
        )
        return proto_send_result(resp)

    async def send_batch(
        self,
        emails: Sequence[Email],
        *,
        async_: bool = False,
        timeout: float | None = None,
    ) -> BatchResult:
        req = pb2.BatchSendMailRequest(
            emails=[email_to_proto(e, async_=async_) for e in emails],
            **{"async": async_},  # field named "async" is a Python keyword
        )
        resp = await self._stub.SendBatch(req, metadata=self._metadata, timeout=timeout)
        return BatchResult(
            total=resp.total,
            succeeded=resp.succeeded,
            failed=resp.failed,
            results=[proto_send_result(r) for r in resp.results],
        )

    async def ping(self, message: str = "", *, timeout: float | None = None) -> dict[str, Any]:
        resp = await self._stub.Ping(
            pb2.PingRequest(message=message), metadata=self._metadata, timeout=timeout
        )
        return proto_ping(resp)

    async def health_check(
        self, service: str = "", *, timeout: float | None = None
    ) -> HealthStatus:
        resp = await self._stub.HealthCheck(
            pb2.HealthCheckRequest(service=service),
            metadata=self._metadata,
            timeout=timeout,
        )
        return proto_health(resp)

    async def close(self) -> None:
        await self._channel.close()

    async def __aenter__(self) -> AsyncMailBabyGrpcClient:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()
