from __future__ import annotations

from typing import Any

from mailbaby.grpc.gen import pb2
from mailbaby.models import Email, HealthStatus, SendResult

__all__ = ["email_to_proto", "proto_send_result", "proto_ping", "proto_health"]


def email_to_proto(email: Email, *, async_: bool = False) -> pb2.SendMailRequest:
    """Convert an :class:`Email` dataclass to a protobuf ``SendMailRequest``."""
    req = pb2.SendMailRequest(
        to=list(email.to),
        subject=email.subject,
        **{"from": email.from_ or "", "async": async_},  # Python keywords
    )
    if email.id:
        req.id = email.id
    if email.account:
        req.account = email.account
    if email.from_name:
        req.from_name = email.from_name
    if email.reply_to:
        req.reply_to = email.reply_to
    if email.cc:
        req.cc.extend(email.cc)
    if email.bcc:
        req.bcc.extend(email.bcc)
    if email.text_body:
        req.text_body = email.text_body
    if email.html_body:
        req.html_body = email.html_body
    if email.headers:
        req.headers.update(email.headers)
    if email.tags:
        req.tags.extend(email.tags)
    if email.metadata:
        req.metadata.update(email.metadata)
    for att in email.attachments:
        req.attachments.append(
            pb2.Attachment(
                filename=att.filename,
                content_type=att.content_type,
                data=att.data,
                inline=att.inline,
                content_id=att.content_id or "",
            )
        )
    return req


def proto_send_result(resp: pb2.SendMailResponse) -> SendResult:
    return SendResult(
        id=resp.id,
        status=resp.status,
        message=resp.message,
        sent_at=resp.sent_at,
    )


def proto_ping(resp: pb2.PingResponse) -> dict[str, Any]:
    return {
        "status": resp.status,
        "version": resp.version,
        "timestamp": resp.timestamp,
    }


def proto_health(resp: pb2.HealthCheckResponse) -> HealthStatus:
    return HealthStatus(
        status=pb2.HealthCheckResponse.ServingStatus.Name(resp.status),
        components=dict(resp.details),
    )
