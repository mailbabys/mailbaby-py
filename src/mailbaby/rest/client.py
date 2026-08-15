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

__all__ = ["MailBabyClient"]


class MailBabyClient:
    """Synchronous REST client for the MailBaby HTTP API.

    Usage::

        client = MailBabyClient("http://localhost:8080", api_key="secret")
        result = client.send(Email(to=["a@example.com"], subject="Hi", text_body="Hello"))
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
        self._client = httpx.Client(
            base_url=self.base_url,
            headers=self._auth_headers,
            timeout=timeout,
            **httpx_kwargs,
        )

    # ------------------------------------------------------------------ send
    def send(self, email: Email, *, async_: bool = False) -> SendResult:
        """Deliver a single email.

        Sync delivery blocks until SMTP acknowledges (``200``, status
        ``sent``); with ``async_=True`` the job is enqueued and the server
        replies ``202 Accepted`` with status ``queued``.
        """
        body = payload(email)
        if async_:
            body["async"] = True
        try:
            resp = self._client.post(SEND_PATH, json=body)
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context="send") from exc
        raise_for_status(resp.status_code, _json(resp), context="send")
        return SendResult.from_dict(_json(resp) or {})

    def send_batch(
        self, emails: Sequence[Email], *, async_: bool = False
    ) -> BatchResult:
        """Deliver multiple emails in one request (parallel on the server)."""
        items = [payload(e) for e in emails]
        body: dict[str, Any] = {"emails": items}
        if async_:
            body["async"] = True
        try:
            resp = self._client.post(BATCH_PATH, json=body)
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context="send_batch") from exc
        raise_for_status(resp.status_code, _json(resp), context="send_batch")
        return BatchResult.from_dict(_json(resp) or {})

    # ---------------------------------------------------------------- health
    def livez(self) -> HealthStatus:
        """Liveness probe (``GET /livez``)."""
        return self._health("livez")

    def readyz(self) -> HealthStatus:
        """Readiness probe (``GET /readyz``) with per-component details."""
        return self._health("readyz")

    def healthz(self) -> HealthStatus:
        """Plain-text health probe (``GET /healthz``)."""
        try:
            resp = self._client.get("/healthz")
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context="healthz") from exc
        raise_for_status(resp.status_code, None, context="healthz")
        return HealthStatus(status=resp.text.strip() or "UP")

    def _health(self, path: str) -> HealthStatus:
        try:
            resp = self._client.get(f"/{path}")
        except httpx.HTTPError as exc:
            raise wrap_transport_error(exc, context=path) from exc
        raise_for_status(resp.status_code, _json(resp), context=path)
        return HealthStatus.from_dict(_json(resp) or {})

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> MailBabyClient:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


def _json(resp: httpx.Response) -> dict[str, Any] | None:
    if resp.status_code in (204, 205):
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    return data if isinstance(data, dict) else None
