from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mailbaby.models import Email
from mailbaby.mq.base import PublishHeaders, payload_bytes, resolve_id

__all__ = ["KafkaProducer", "AsyncKafkaProducer"]

_MESSAGE_ID_HEADER = "X-Message-ID"


class KafkaProducer:
    """Publish email jobs to Apache Kafka via :mod:`confluent-kafka`.

    Wire format matches ``internal/queue/driver/kafka/kafka.go``: the message
    value is the raw email JSON with an ``X-Message-ID`` header. Messages are
    flushed synchronously per publish so delivery is confirmed before return.
    """

    def __init__(
        self,
        bootstrap_servers: str | Sequence[str] = "127.0.0.1:9092",
        *,
        topic: str = "mailbaby_tasks",
        client_id: str = "mailbaby_py_producer",
        flush_timeout: float = 30.0,
        **producer_kwargs: Any,
    ) -> None:
        from confluent_kafka import Producer as ConfluentProducer

        brokers = (
            bootstrap_servers
            if isinstance(bootstrap_servers, str)
            else ",".join(bootstrap_servers)
        )
        self.topic = topic
        self.flush_timeout = flush_timeout
        self._producer = ConfluentProducer(
            {
                "bootstrap.servers": brokers,
                "client.id": client_id,
                **producer_kwargs,
            }
        )

    def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        msg_id = resolve_id(email, id)
        kafka_headers: list[tuple[str, bytes]] = [
            (k, v.encode("utf-8")) for k, v in (headers or {}).items()
        ]
        kafka_headers.append((_MESSAGE_ID_HEADER, msg_id.encode("ascii")))

        self._producer.produce(
            topic=topic or self.topic,
            value=payload_bytes(email, msg_id),
            headers=kafka_headers,
        )
        self._producer.flush(self.flush_timeout)
        return msg_id

    def close(self) -> None:
        self._producer.flush(self.flush_timeout)


class AsyncKafkaProducer:
    """Publish email jobs to Apache Kafka via :mod:`aiokafka`.

    Wire format identical to :class:`KafkaProducer` (``X-Message-ID`` header,
    email JSON value).
    """

    def __init__(
        self,
        bootstrap_servers: str | Sequence[str] = "127.0.0.1:9092",
        *,
        topic: str = "mailbaby_tasks",
        client_id: str = "mailbaby_py_producer",
        **producer_kwargs: Any,
    ) -> None:
        self.bootstrap_servers = (
            bootstrap_servers
            if isinstance(bootstrap_servers, (list, tuple))
            else [bootstrap_servers]
        )
        self.topic = topic
        self._producer: Any = None
        self._producer_kwargs = {"client_id": client_id, **producer_kwargs}

    async def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        from aiokafka import AIOKafkaProducer

        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                **self._producer_kwargs,
            )
            await self._producer.start()

        msg_id = resolve_id(email, id)
        kafka_headers: list[tuple[str, str]] = [
            (k, v) for k, v in (headers or {}).items()
        ]
        kafka_headers.append((_MESSAGE_ID_HEADER, msg_id))

        await self._producer.send_and_wait(
            topic=topic or self.topic,
            value=payload_bytes(email, msg_id),
            headers=kafka_headers,
        )
        return msg_id

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
