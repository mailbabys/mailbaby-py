from __future__ import annotations

from mailbaby.models import Attachment, Email


def make_email(**overrides) -> Email:
    defaults: dict = {
        "to": ["alice@example.com"],
        "subject": "Test Subject",
    }
    defaults.update(overrides)
    return Email(**defaults)


def make_attachment(**overrides) -> Attachment:
    defaults: dict = {
        "filename": "report.pdf",
        "content_type": "application/pdf",
        "data": b"%PDF-1.4 fake pdf",
    }
    defaults.update(overrides)
    return Attachment(**defaults)
