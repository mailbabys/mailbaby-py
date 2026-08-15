from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import httpx

from mailbaby.models import BatchResult, Email, HealthStatus, SendResult
from mailbaby.rest.base import (
    BATCH_PATH,
    SEND_PATH,
    build_auth_headers,
    payload,
    raise_for_status,
    wrap_transport_error,
)
from mailbaby.rest.client import _json

__all__ = ["AsyncMailBabyClient"]


class AsyncMailBabyClient:
    """Asynchronous REST client backed by :class:`httpx.AsyncClient`.

    Mirror of :class:`~mailbaby.rest.client.MailBabyClient` with the same
    method signatures, provided as coroutines.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        api_key: str | None = None,
        header_name: str | None = None,
        timeout: float = 30.0,
        **httpx_kwargs: Any,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._auth_headers = build_auth_headers(api_key, header_name)
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._auth_headers,
            timeout=timeout,
            **httpx_kwargs,
        )

    async def send(self, email: Email, *, async_: bool = False) -> SendResult:
        body = payload(email)
        if async_:
            body["async"] = True
        try:
            resp = await self._client.post(SEND_PATH, json=body)
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context="send") from exc
        raise_for_status(resp.status_code, _json(resp), context="send")
        return SendResult.from_dict(_json(resp) or {})

    async def send_batch(
        self, emails: Sequence[Email], *, async_: bool = False
    ) -> BatchResult:
        items = [payload(e) for e in emails]
        body: dict[str, Any] = {"emails": items}
        if async_:
            body["async"] = True
        try:
            resp = await self._client.post(BATCH_PATH, json=body)
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context="send_batch") from exc
        raise_for_status(resp.status_code, _json(resp), context="send_batch")
        return BatchResult.from_dict(_json(resp) or {})

    async def livez(self) -> HealthStatus:
        return await self._health("livez")

    async def readyz(self) -> HealthStatus:
        return await self._health("readyz")

    async def healthz(self) -> HealthStatus:
        try:
            resp = await self._client.get("/healthz")
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context="healthz") from exc
        raise_for_status(resp.status_code, None, context="healthz")
        return HealthStatus(status=resp.text.strip() or "UP")

    async def _health(self, path: str) -> HealthStatus:
        try:
            resp = await self._client.get(f"/{path}")
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context=path) from exc
        raise_for_status(resp.status_code, _json(resp), context=path)
        return HealthStatus.from_dict(_json(resp) or {})

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AsyncMailBabyClient:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()
