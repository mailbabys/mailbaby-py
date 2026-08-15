from __future__ import annotations

import base64
import json

import pytest

from mailbaby.models import Attachment, BatchResult, HealthStatus, SendResult
from tests.factories import make_attachment, make_email


class TestAttachment:
    def test_from_bytes(self) -> None:
        att = Attachment.from_bytes(b"hello", "a.txt", content_type="text/plain")
        assert att.data == b"hello"
        assert att.filename == "a.txt"

    def test_from_path(self, tmp_path) -> None:
        p = tmp_path / "x.bin"
        p.write_bytes(b"\x00\x01")
        att = Attachment.from_path(p, content_type="application/octet-stream")
        assert att.data == b"\x00\x01"
        assert att.filename == "x.bin"

    def test_from_bytes_rejects_str(self) -> None:
        with pytest.raises(TypeError):
            Attachment.from_bytes("not-bytes", "a.txt")  # type: ignore[arg-type]

    def test_to_dict_base64(self) -> None:
        d = Attachment(data=b"abc", filename="f", content_type="text/plain").to_dict()
        assert d["data"] == base64.b64encode(b"abc").decode()
        assert d["inline"] is False
        assert "content_id" not in d


class TestEmailSerialization:
    def test_to_dict_minimal(self) -> None:
        email = make_email()
        d = email.to_dict()
        assert d["to"] == ["alice@example.com"]
        assert d["subject"] == "Test Subject"
        assert "id" not in d

    def test_to_dict_full(self) -> None:
        email = make_email(
            id="abc123",
            account="marketing",
            from_="news@example.com",
            from_name="News",
            reply_to="reply@example.com",
            cc=["cc@example.com"],
            bcc=["bcc@example.com"],
            text_body="text",
            html_body="<b>html</b>",
            headers={"X-Priority": "1"},
            attachments=[make_attachment()],
            tags=["news"],
            metadata={"env": "prod"},
        )
        d = email.to_dict()
        assert d["id"] == "abc123"
        assert d["from"] == "news@example.com"
        assert d["from_name"] == "News"
        assert d["reply_to"] == "reply@example.com"
        assert d["cc"] == ["cc@example.com"]
        assert d["bcc"] == ["bcc@example.com"]
        assert d["attachments"][0]["filename"] == "report.pdf"
        assert "async" not in d

    def test_to_async_dict_sets_async(self) -> None:
        assert make_email().to_async_dict()["async"] is True

    def test_to_mq_json_no_async_field(self) -> None:
        raw = json.loads(make_email().to_mq_json())
        assert "async" not in raw
        assert raw["to"] == ["alice@example.com"]

    def test_to_mq_json_uses_no_async_body(self) -> None:
        raw = json.loads(make_email().to_mq_json())
        assert "async" not in raw


class TestFromDict:
    def test_send_result(self) -> None:
        r = SendResult.from_dict(
            {"id": "1", "status": "sent", "message": "ok", "sent_at": 123}
        )
        assert r.status == "sent"
        assert r.sent_at == 123

    def test_batch_result(self) -> None:
        b = BatchResult.from_dict(
            {
                "total": 2,
                "succeeded": 1,
                "failed": 1,
                "results": [
                    {"id": "a", "status": "sent", "message": "ok", "sent_at": 1},
                    {"id": "b", "status": "failed", "message": "nope", "sent_at": 2},
                ],
            }
        )
        assert b.total == 2
        assert b.succeeded == 1
        assert b.failed == 1
        assert b.results[1].message == "nope"

    def test_health_status(self) -> None:
        h = HealthStatus.from_dict(
            {"status": "UP", "components": {"queue": "UP", "smtp": "DOWN: x"}}
        )
        assert h.status == "UP"
        assert h.components["smtp"].startswith("DOWN")
