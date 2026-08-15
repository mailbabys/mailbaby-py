from __future__ import annotations

__all__ = [
    "MailBabyError",
    "ValidationError",
    "AuthenticationError",
    "NotFoundError",
    "MethodNotAllowedError",
    "DeliveryError",
    "EnqueueError",
    "UnavailableError",
    "RequestFailedError",
]


class MailBabyError(Exception):
    """Base class for all mailbaby-client errors."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        details: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code
        self.details = details

    def __str__(self) -> str:
        parts = [self.message]
        if self.code:
            parts.append(f"(code={self.code})")
        if self.status_code is not None:
            parts.append(f"(http={self.status_code})")
        return " ".join(parts)


class ValidationError(MailBabyError):
    """HTTP 400 — request failed server-side validation."""


class AuthenticationError(MailBabyError):
    """HTTP 401 — missing or invalid API key/token."""


class NotFoundError(MailBabyError):
    """HTTP 404 — endpoint not found."""


class MethodNotAllowedError(MailBabyError):
    """HTTP 405 — method not allowed on the endpoint."""


class DeliveryError(MailBabyError):
    """HTTP 500 — synchronous SMTP delivery failed."""


class EnqueueError(MailBabyError):
    """HTTP 500 — enqueueing to the message queue failed."""


class UnavailableError(MailBabyError):
    """HTTP 503 — service not ready."""


class RequestFailedError(MailBabyError):
    """Transport-level failure: network error, timeout, or bad response."""
