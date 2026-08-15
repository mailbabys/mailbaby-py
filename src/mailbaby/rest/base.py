from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any

from mailbaby.exceptions import (
    AuthenticationError,
    DeliveryError,
    EnqueueError,
    MailBabyError,
    MethodNotAllowedError,
    NotFoundError,
    RequestFailedError,
    UnavailableError,
    ValidationError,
)
from mailbaby.models import Email

__all__ = [
    "SEND_PATH",
    "BATCH_PATH",
    "build_auth_headers",
    "payload",
    "raise_for_status",
    "generate_id",
]

SEND_PATH = "/v1/email/send"
BATCH_PATH = "/v1/email/batch"


def generate_id() -> str:
    """Generate a 32-hex-char message id, matching the server (16 random bytes)."""
    return uuid.uuid4().hex


def payload(email: Email) -> dict[str, Any]:
    """Full REST body for a single email, resolving a missing id."""
    body = email.to_dict()
    if not body.get("id"):
        body["id"] = generate_id()
    return body


def build_auth_headers(api_key: str | None, header_name: str | None) -> dict[str, str]:
    """Auth headers as accepted by the server (handler/auth.go).

    Priority: ``Authorization: Bearer <key>`` when ``header_name`` is unset,
    otherwise the custom header name (falls back to ``X-API-Key``).
    """
    if not api_key:
        return {}
    if header_name:
        return {header_name: api_key}
    return {"Authorization": f"Bearer {api_key}"}


def raise_for_status(
    status_code: int,
    body: Mapping[str, Any] | None,
    *,
    context: str = "request",
) -> None:
    """Map an HTTP response to the mailbaby-client exception hierarchy.

    Parses the server error shape ``{code, error, details}`` (handler/email.go).
    """
    if 200 <= status_code < 300:
        return

    data = dict(body) if isinstance(body, Mapping) else {}
    err_code = str(data.get("error", "") or "")
    details = str(data.get("details", "") or "")
    message = str(data.get("message", "") or "")
    reason = details or message or err_code or f"HTTP {status_code}"

    if status_code == 400:
        raise ValidationError(
            reason, status_code=status_code, code=err_code or "invalid_request", details=details
        )
    if status_code == 401:
        raise AuthenticationError(
            reason, status_code=status_code, code=err_code or "unauthorized", details=details
        )
    if status_code == 404:
        raise NotFoundError(
            reason, status_code=status_code, code=err_code or "not_found", details=details
        )
    if status_code == 405:
        raise MethodNotAllowedError(
            reason,
            status_code=status_code,
            code=err_code or "method_not_allowed",
            details=details,
        )
    if status_code == 503:
        raise UnavailableError(
            reason, status_code=status_code, code=err_code or "unavailable", details=details
        )
    if status_code >= 500:
        if err_code == "enqueue_failed":
            raise EnqueueError(reason, status_code=status_code, code=err_code, details=details)
        raise DeliveryError(
            reason,
            status_code=status_code,
            code=err_code or "delivery_failed",
            details=details,
        )
    raise MailBabyError(
        reason, status_code=status_code, code=err_code or f"http_{status_code}", details=details
    )


def wrap_transport_error(exc: Exception, *, context: str = "request") -> RequestFailedError:
    return RequestFailedError(f"{context} failed: {exc}")
