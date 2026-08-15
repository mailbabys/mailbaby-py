from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties

from mailbaby.models import Email
from mailbaby.mq.base import PublishHeaders, payload_bytes, resolve_id

__all__ = ["RabbitMQProducer", "AsyncRabbitMQProducer"]


class RabbitMQProducer:
    """Publish email jobs to RabbitMQ (AMQP 0-9-1) via :mod:`pika`.

    Wire format matches ``internal/queue/driver/rabbitmq/rabbitmq.go``:
    the message body is the raw email JSON, routed through the configured
    exchange/routing key with ``message_id`` set.
    """

    def __init__(
        self,
        url: str = "amqp://guest:guest@127.0.0.1:5672/",
        *,
        exchange: str = "mailbaby_exchange",
        routing_key: str = "mail.send.#",
        durable: bool = True,
        heartbeat: int = 60,
    ) -> None:
        self.url = url
        self.exchange = exchange
        self.routing_key = routing_key
        self.durable = durable
        self._params = pika.URLParameters(url)
        self._params.heartbeat = heartbeat
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    # ---------------------------------------------------------------- core
    def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        msg_id = resolve_id(email, id)
        channel = self._channel or self._ensure_channel()

        # Server routes on the routing key; per-publish topic overrides it.
        routing_key = topic or self.routing_key

        table: dict[str, str] = headers or {}
        content_type = "application/octet-stream"
        if "Content-Type" in table:
            content_type = table["Content-Type"]

        channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=payload_bytes(email, msg_id),
            properties=BasicProperties(
                headers=table,
                content_type=content_type,
                delivery_mode=2,  # persistent
                message_id=msg_id,
                timestamp=int(datetime.now(timezone.utc).timestamp()),
            ),
        )
        return msg_id

    def close(self) -> None:
        if self._channel:
            try:
                self._channel.close()
            except pika.exceptions.ChannelWrongStateError:
                pass
            self._channel = None
        if self._connection and self._connection.is_open:
            self._connection.close()
        self._connection = None

    # -------------------------------------------------------------- private
    def _ensure_channel(self) -> BlockingChannel:
        if self._connection is None or not self._connection.is_open:
            self._connection = pika.BlockingConnection(self._params)
        if self._channel is None or not self._channel.is_open:
            self._channel = self._connection.channel()
            self._channel.confirm_delivery()
            if self.exchange:
                self._channel.exchange_declare(
                    exchange=self.exchange,
                    exchange_type="topic",
                    durable=self.durable,
                )
        return self._channel

    def __enter__(self) -> RabbitMQProducer:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


class AsyncRabbitMQProducer:
    """Publish email jobs to RabbitMQ via :mod:`aio-pika` (robust connection).

    Wire format identical to :class:`RabbitMQProducer`.
    """

    def __init__(
        self,
        url: str = "amqp://guest:guest@127.0.0.1:5672/",
        *,
        exchange: str = "mailbaby_exchange",
        exchange_type: str = "topic",
        routing_key: str = "mail.send.#",
        durable: bool = True,
        declare_exchange: bool = False,
    ) -> None:
        self.url = url
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.durable = durable
        self.declare_exchange = declare_exchange
        self._connection: Any = None
        self._channel: Any = None
        self._exchange: Any = None

    async def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        from aio_pika import Message

        msg_id = resolve_id(email, id)
        exchange = await self._ensure_exchange()
        routing_key = topic or self.routing_key

        await exchange.publish(
            Message(
                body=payload_bytes(email, msg_id),
                content_type="application/octet-stream",
                delivery_mode=2,
                message_id=msg_id,
                headers=headers or {},
                timestamp=datetime.now(timezone.utc),
            ),
            routing_key=routing_key,
        )
        return msg_id

    async def close(self) -> None:
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None
        self._exchange = None

    # -------------------------------------------------------------- private
    async def _ensure_exchange(self) -> Any:
        if self._exchange is not None:
            return self._exchange
        from aio_pika import connect_robust

        self._connection = await connect_robust(self.url)
        self._channel = await self._connection.channel()
        if not self.exchange:
            self._exchange = self._channel.default_exchange
            return self._exchange
        if self.declare_exchange:
            self._exchange = await self._channel.declare_exchange(
                self.exchange,
                self.exchange_type,
                durable=self.durable,
                auto_delete=False,
            )
        else:
            self._exchange = await self._channel.get_exchange(self.exchange)
        return self._exchange

    async def __aenter__(self) -> AsyncRabbitMQProducer:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()
