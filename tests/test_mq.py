from __future__ import annotations

import base64
import json

import pytest

from mailbaby.mq.base import (
    envelope,
    envelope_json,
    message_id,
    payload_bytes,
)
from mailbaby.mq.kafka import KafkaProducer
from mailbaby.mq.rabbitmq import RabbitMQProducer
from mailbaby.mq.redis import RedisProducer
from tests.factories import make_email


class TestBase:
    def test_message_id_custom(self) -> None:
        assert message_id("custom-1") == "custom-1"

    def test_message_id_generated(self) -> None:
        mid = message_id(None)
        assert len(mid) == 32

    def test_payload_bytes_embeds_generated_id(self) -> None:
        email = make_email()  # no id
        msg_id = message_id("gen-1")
        data = json.loads(payload_bytes(email, msg_id))
        assert data["id"] == "gen-1"
        assert data["subject"] == "Test Subject"

    def test_payload_bytes_keeps_existing_id(self) -> None:
        email = make_email(id="mail-7")
        data = json.loads(payload_bytes(email, "mail-7"))
        assert data["id"] == "mail-7"
        assert data == json.loads(email.to_mq_json())

    def test_envelope_shape(self) -> None:
        email = make_email(id="mail-1")
        env = envelope(email, "mail-1", "mb:q")
        assert env["id"] == "mail-1"
        assert env["topic"] == "mb:q"
        assert env["attempts"] == 1
        assert env["payload"] == email.to_mq_json()

    def test_envelope_json_base64_payload(self) -> None:
        email = make_email()
        msg_id = message_id("x")
        env = envelope(email, msg_id, "t")
        raw = json.loads(envelope_json(env))
        # Go marshals []byte as base64 — we must match for the server consumer.
        assert raw["payload"] == base64.b64encode(payload_bytes(email, msg_id)).decode()

    def test_envelope_json_has_required_keys(self) -> None:
        env = envelope(make_email(), "x", "t")
        raw = json.loads(envelope_json(env))
        for key in ("id", "topic", "payload", "headers", "timestamp", "attempts"):
            assert key in raw, key


class _FakeChannel:
    def __init__(self) -> None:
        self.published: list[dict] = []
        self.closed = False

    def exchange_declare(self, **kwargs: object) -> None:
        self.exchange_declare_kwargs = kwargs

    def confirm_delivery(self) -> None:
        pass

    def basic_publish(self, exchange, routing_key, body, properties) -> None:
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    is_open = True

    def __init__(self, channel: _FakeChannel) -> None:
        self._channel = channel
        self.closed = False

    def channel(self) -> _FakeChannel:
        return self._channel

    def close(self) -> None:
        self.closed = True


class TestRabbitMQProducer:
    def _producer(self, **kwargs) -> tuple[RabbitMQProducer, _FakeChannel]:
        channel = _FakeChannel()
        conn = _FakeConnection(channel)
        p = RabbitMQProducer(**kwargs)
        p._params = None  # type: ignore[assignment]
        p._connection = conn
        p._channel = channel
        return p, channel

    def test_publish_body_is_email_json(self) -> None:
        p, channel = self._producer()
        email = make_email(id="mail-9")
        p.publish(email)
        msg = channel.published[0]
        assert msg["body"] == email.to_mq_json()

    def test_publish_routing_default(self) -> None:
        p, channel = self._producer(
            exchange="ex", routing_key="mail.send.#"
        )
        p.publish(make_email())
        msg = channel.published[0]
        assert msg["exchange"] == "ex"
        assert msg["routing_key"] == "mail.send.#"

    def test_publish_topic_override(self) -> None:
        p, channel = self._producer()
        p.publish(make_email(), topic="custom.routing")
        assert channel.published[0]["routing_key"] == "custom.routing"

    def test_publish_message_id_property(self) -> None:
        p, channel = self._producer()
        mid = p.publish(make_email(), id="mid-1")
        props = channel.published[0]["properties"]
        assert props.message_id == "mid-1"
        assert mid == "mid-1"

    def test_publish_content_type_from_header(self) -> None:
        p, channel = self._producer()
        p.publish(make_email(), headers={"Content-Type": "application/json"})
        assert (
            channel.published[0]["properties"].content_type == "application/json"
        )

    def test_publish_delivery_mode_persistent(self) -> None:
        p, channel = self._producer()
        p.publish(make_email())
        assert channel.published[0]["properties"].delivery_mode == 2

    def test_close(self) -> None:
        p, channel = self._producer()
        p.close()
        assert channel.closed


class _FakeRedis:
    def __init__(self) -> None:
        self.xadds: list[tuple] = []
        self.rpushes: list[tuple] = []
        self.publishes: list[tuple] = []
        self.closed = False

    def xadd(self, name, values, **kw) -> str:
        self.xadds.append((name, values))
        return "0-1"

    def rpush(self, name, *values) -> int:
        self.rpushes.append((name, values))
        return len(values)

    def publish(self, name, value) -> int:
        self.publishes.append((name, value))
        return 1

    def close(self) -> None:
        self.closed = True


class TestRedisProducer:
    def _producer(self, mode="stream", **kwargs) -> tuple[RedisProducer, _FakeRedis]:
        client = _FakeRedis()
        p = RedisProducer(client, mode=mode, **kwargs)
        return p, client

    def test_stream_wire_format(self) -> None:
        p, client = self._producer()
        email = make_email(id="rm-1")
        mid = p.publish(email)
        name, values = client.xadds[0]
        assert name == "mailbaby:queue:email"
        assert values["id"] == "rm-1"
        assert values["payload"] == email.to_mq_json()
        assert mid == "rm-1"
        # "data" field carries the full envelope (base64 payload, like Go).
        data = json.loads(values["data"])
        assert data["id"] == "rm-1"
        assert data["payload"] == base64.b64encode(email.to_mq_json()).decode()

    def test_stream_topic_override_and_maxlen(self) -> None:
        p, client = self._producer(max_len=100)
        p.publish(make_email(), topic="other:key")
        name, values = client.xadds[0]
        assert name == "other:key"
        assert client.xadds[0][1]["id"]

    def test_uses_email_id_when_no_explicit(self) -> None:
        p, client = self._producer()
        mid = p.publish(make_email(id="rm-2"))
        name, values = client.xadds[0]
        assert mid == "rm-2"
        assert values["id"] == "rm-2"

    def test_list_mode(self) -> None:
        p, client = self._producer(mode="list")
        email = make_email(id="rl-1")
        p.publish(email)
        name, values = client.rpushes[0]
        raw = json.loads(values[0])
        assert name == "mailbaby:queue:email"
        assert raw["id"] == "rl-1"

    def test_pubsub_mode(self) -> None:
        p, client = self._producer(mode="pubsub")
        p.publish(make_email())
        name, value = client.publishes[0]
        data = json.loads(value)
        assert name == "mailbaby:queue:email"
        assert "id" in data and "payload" in data

    def test_invalid_mode(self) -> None:
        with pytest.raises(ValueError):
            RedisProducer(_FakeRedis(), mode="bogus")


class _FakeAsyncRedis(_FakeRedis):
    async def xadd(self, name, values, **kw) -> str:
        return super().xadd(name, values, **kw)

    async def rpush(self, name, *values) -> int:
        return super().rpush(name, *values)

    async def publish(self, name, value) -> int:
        return super().publish(name, value)

    async def aclose(self) -> None:
        self.closed = True


class TestAsyncRedisProducer:
    async def test_stream(self) -> None:
        from mailbaby.mq import AsyncRedisProducer

        client = _FakeAsyncRedis()
        p = AsyncRedisProducer(client)
        email = make_email(id="arm-1")
        mid = await p.publish(email)
        name, values = client.xadds[0]
        assert mid == "arm-1"
        assert values["id"] == "arm-1"
        assert values["payload"] == email.to_mq_json()
        await p.close()
        assert client.closed


class _FakeAioMessage:
    pass


class _FakeAioExchange:
    def __init__(self) -> None:
        self.published: list[tuple] = []

    async def publish(self, message, routing_key: str) -> None:
        self.published.append((message, routing_key))


class TestAsyncRabbitMQProducer:
    async def test_publish(self, monkeypatch) -> None:
        from mailbaby.mq import AsyncRabbitMQProducer

        exchange = _FakeAioExchange()
        p = AsyncRabbitMQProducer()
        p._exchange = exchange  # type: ignore[attr-defined]
        email = make_email(id="am-1")
        mid = await p.publish(email)
        assert mid == "am-1"
        msg, rkey = exchange.published[0]
        assert rkey == "mail.send.#"
        assert msg.message_id == "am-1"
        assert msg.body == email.to_mq_json()
        await p.close()


class _FakeConfluent:
    def __init__(self) -> None:
        self.produced: list[tuple] = []
        self.flushed = 0

    def produce(self, *, topic=None, value=None, headers=None) -> None:
        self.produced.append((topic, value, headers))
        self.flushed += 1

    def flush(self, timeout=None) -> None:
        self.flushed += 1


class TestKafkaProducer:
    def test_wire_format(self, monkeypatch) -> None:
        fake = _FakeConfluent()
        import confluent_kafka

        monkeypatch.setattr(confluent_kafka, "Producer", lambda conf: fake)
        p = KafkaProducer("127.0.0.1:9092")
        email = make_email(id="k-1")
        mid = p.publish(email)
        topic, value, headers = fake.produced[0]
        assert topic == "mailbaby_tasks"
        assert value == email.to_mq_json()
        assert ("X-Message-ID", b"k-1") in headers
        assert mid == "k-1"

    def test_topic_override_and_user_headers(self, monkeypatch) -> None:
        fake = _FakeConfluent()
        import confluent_kafka

        monkeypatch.setattr(confluent_kafka, "Producer", lambda conf: fake)
        p = KafkaProducer()
        p.publish(make_email(), topic="other", headers={"X-A": "b"})
        topic, _, headers = fake.produced[0]
        assert topic == "other"
        assert ("X-A", b"b") in headers
        assert any(h == "X-Message-ID" for h, _ in headers)
