from __future__ import annotations

import base64
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from mailbaby.models import Email

__all__ = [
    "PublishHeaders",
    "Producer",
    "AsyncProducer",
    "envelope",
    "envelope_json",
    "message_id",
    "resolve_id",
    "payload_bytes",
]

PublishHeaders = dict[str, str]


def message_id(explicit: str | None) -> str:
    """Resolve the message id: explicit, or a server-style random id."""
    return explicit or uuid.uuid4().hex


def resolve_id(email: Email, explicit: str | None) -> str:
    """Message id priority: explicit arg, then ``email.id``, then generated."""
    return message_id(explicit or email.id)


def payload_bytes(email: Email, msg_id: str) -> bytes:
    """Email JSON for the MQ wire, with the resolved id embedded.

    Matches the server, which embeds the generated id in the payload before
    publishing (``handler/email.go requestToEmail`` + ``sendAsync``).
    """
    if email.id == msg_id:
        return email.to_mq_json()
    data = json.loads(email.to_mq_json())
    data["id"] = msg_id
    return json.dumps(data).encode("utf-8")


def envelope(
    email: Email,
    msg_id: str,
    topic: str,
    *,
    headers: PublishHeaders | None = None,
    timestamp: str | None = None,
    attempts: int = 1,
) -> dict[str, Any]:
    """Build the Redis queue envelope (matches ``redisMessageEnvelope`` in
    ``internal/queue/driver/redis/redis.go``).

    The ``payload`` field carries the raw email JSON bytes; when serialized to
    JSON (list/pubsub modes) Go's ``[]byte`` marshals as base64, which we
    reproduce for byte-for-byte wire parity.
    """
    payload = payload_bytes(email, msg_id)
    return {
        "id": msg_id,
        "topic": topic,
        "payload": payload,
        "headers": headers or {},
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "attempts": attempts,
    }


def envelope_json(env: dict[str, Any]) -> str:
    """Serialize the envelope the way Go does (``[]byte`` -> base64 string)."""
    out = dict(env)
    out["payload"] = base64.b64encode(env["payload"]).decode("ascii")
    return json.dumps(out)


@runtime_checkable
class Producer(Protocol):
    """Synchronous message-queue producer."""

    def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        """Publish an email job; returns the message id."""
        ...

    def close(self) -> None:
        """Release broker resources."""
        ...


@runtime_checkable
class AsyncProducer(Protocol):
    """Asynchronous message-queue producer."""

    async def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        """Publish an email job; returns the message id."""
        ...

    async def close(self) -> None:
        """Release broker resources."""
        ...
