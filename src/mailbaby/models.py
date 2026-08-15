from __future__ import annotations

import base64
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

__all__ = [
    "Attachment",
    "Email",
    "SendResult",
    "BatchResult",
    "HealthStatus",
]


def _b64encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


@dataclass(slots=True)
class Attachment:
    """A file attached to an email.

    ``data`` is raw bytes; the client base64-encodes it when serializing,
    matching the server's ``Attachment`` JSON schema.
    """

    filename: str
    content_type: str = "application/octet-stream"
    data: bytes = b""
    inline: bool = False
    content_id: str | None = None

    @classmethod
    def from_bytes(
        cls,
        data: bytes | BinaryIO,
        filename: str,
        content_type: str = "application/octet-stream",
        inline: bool = False,
        content_id: str | None = None,
    ) -> Attachment:
        if hasattr(data, "read"):
            data = data.read()
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes or a binary file-like object")
        return cls(
            filename=filename,
            content_type=content_type,
            data=data,
            inline=inline,
            content_id=content_id,
        )

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        content_type: str = "application/octet-stream",
        inline: bool = False,
        content_id: str | None = None,
    ) -> Attachment:
        p = Path(path)
        return cls(
            filename=p.name,
            content_type=content_type,
            data=p.read_bytes(),
            inline=inline,
            content_id=content_id,
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "filename": self.filename,
            "content_type": self.content_type,
            "data": _b64encode(self.data),
            "inline": self.inline,
        }
        if self.content_id:
            d["content_id"] = self.content_id
        return d


@dataclass(slots=True)
class Email:
    """An email delivery request.

    Field names and JSON tags mirror the server contract in
    ``internal/handler/email.go`` / ``internal/sender/email.go``.
    """

    to: list[str]
    subject: str
    id: str | None = None
    account: str | None = None
    from_: str | None = field(default=None, metadata={"json": "from"})
    from_name: str | None = None
    reply_to: str | None = None
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    text_body: str | None = None
    html_body: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    attachments: list[Attachment] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)

    def _body(self, include_async: bool, async_: bool) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.id:
            d["id"] = self.id
        if self.account:
            d["account"] = self.account
        if self.from_:
            d["from"] = self.from_
        if self.from_name:
            d["from_name"] = self.from_name
        if self.reply_to:
            d["reply_to"] = self.reply_to
        d["to"] = list(self.to)
        if self.cc:
            d["cc"] = list(self.cc)
        if self.bcc:
            d["bcc"] = list(self.bcc)
        d["subject"] = self.subject
        if self.text_body:
            d["text_body"] = self.text_body
        if self.html_body:
            d["html_body"] = self.html_body
        if self.headers:
            d["headers"] = dict(self.headers)
        if self.attachments:
            d["attachments"] = [a.to_dict() for a in self.attachments]
        if self.tags:
            d["tags"] = list(self.tags)
        if self.metadata:
            d["metadata"] = dict(self.metadata)
        if include_async and async_:
            d["async"] = True
        return d

    def to_dict(self) -> dict[str, Any]:
        """JSON body for the REST /v1/email/send endpoint (sync)."""
        return self._body(include_async=True, async_=False)

    def to_async_dict(self) -> dict[str, Any]:
        """JSON body for REST with async delivery."""
        return self._body(include_async=True, async_=True)

    def to_mq_json(self) -> bytes:
        """Wire payload for message-queue ingestion (no ``async`` field).

        Matches ``sender.Email.ToJSON()`` consumed by the MQ drivers.
        """
        return json.dumps(self._body(include_async=False, async_=False)).encode(
            "utf-8"
        )


@dataclass(slots=True)
class SendResult:
    """Result of a single email delivery."""

    id: str
    status: str
    message: str
    sent_at: int

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SendResult:
        return cls(
            id=str(d.get("id", "")),
            status=str(d.get("status", "")),
            message=str(d.get("message", "")),
            sent_at=int(d.get("sent_at", 0)),
        )


@dataclass(slots=True)
class BatchResult:
    """Result of a batch email delivery."""

    total: int
    succeeded: int
    failed: int
    results: list[SendResult]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> BatchResult:
        raw_results: Iterable[Any] = d.get("results") or []
        return cls(
            total=int(d.get("total", 0)),
            succeeded=int(d.get("succeeded", 0)),
            failed=int(d.get("failed", 0)),
            results=[
                SendResult.from_dict(r)
                for r in raw_results
                if isinstance(r, dict)
            ],
        )


@dataclass(slots=True)
class HealthStatus:
    """Parsed response of a health probe (/livez, /readyz, /healthz)."""

    status: str
    components: dict[str, str] = field(default_factory=dict)
    timestamp: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> HealthStatus:
        return cls(
            status=str(d.get("status", "")),
            components={
                str(k): str(v)
                for k, v in (d.get("components") or {}).items()
            },
            timestamp=d.get("timestamp"),
            raw=d,
        )
