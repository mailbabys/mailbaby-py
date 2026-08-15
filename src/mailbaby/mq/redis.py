from __future__ import annotations

from typing import Any

from mailbaby.models import Email
from mailbaby.mq.base import (
    PublishHeaders,
    envelope,
    envelope_json,
    resolve_id,
)

__all__ = ["RedisProducer", "AsyncRedisProducer"]

_STREAM = "stream"
_LIST = "list"
_PUBSUB = "pubsub"
_MODES = (_STREAM, _LIST, _PUBSUB)


class RedisProducer:
    """Publish email jobs to Redis via :mod:`redis` (sync).

    Wire format matches ``internal/queue/driver/redis/redis.go``:

    - ``stream``: ``XADD key {"id": ..., "payload": <email JSON>, "data": <envelope JSON>}``
    - ``list``:   ``RPUSH key <envelope JSON>``
    - ``pubsub``: ``PUBLISH key <envelope JSON>``

    Accepts either a pre-connected :class:`redis.Redis` client or connection
    parameters (``host``/``port``/``db``/...).
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        key: str = "mailbaby:queue:email",
        mode: str = _STREAM,
        max_len: int = 0,
        **connection_kwargs: Any,
    ) -> None:
        if mode not in _MODES:
            raise ValueError(f"redis mode must be one of {_MODES}, got {mode!r}")
        if client is None:
            import redis

            client = redis.Redis(**connection_kwargs)
        self._client: Any = client
        self.key = key
        self.mode = mode
        self.max_len = max_len

    def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        msg_id = resolve_id(email, id)
        key = topic or self.key
        env = envelope(email, msg_id, key, headers=headers)
        raw = envelope_json(env)

        if self.mode == _STREAM:
            values: dict[str, Any] = {
                "id": msg_id,
                "payload": env["payload"],
                "data": raw,
            }
            if self.max_len > 0:
                self._client.xadd(key, values, maxlen=self.max_len, approximate=True)
            else:
                self._client.xadd(key, values)
        elif self.mode == _LIST:
            self._client.rpush(key, raw)
        else:
            self._client.publish(key, raw)
        return msg_id

    def close(self) -> None:
        """Close the underlying Redis client if this producer created it."""
        try:
            self._client.close()
        except AttributeError:
            pass


class AsyncRedisProducer:
    """Publish email jobs to Redis via :mod:`redis.asyncio` (async).

    Accepts either a pre-connected :class:`redis.asyncio.Redis` client or
    connection parameters; wire format identical to :class:`RedisProducer`.
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        key: str = "mailbaby:queue:email",
        mode: str = _STREAM,
        max_len: int = 0,
        **connection_kwargs: Any,
    ) -> None:
        if mode not in _MODES:
            raise ValueError(f"redis mode must be one of {_MODES}, got {mode!r}")
        if client is None:
            from redis import asyncio as redis_async

            client = redis_async.Redis(**connection_kwargs)
        self._client: Any = client
        self.key = key
        self.mode = mode
        self.max_len = max_len

    async def publish(
        self,
        email: Email,
        *,
        topic: str | None = None,
        id: str | None = None,
        headers: PublishHeaders | None = None,
    ) -> str:
        msg_id = resolve_id(email, id)
        key = topic or self.key
        env = envelope(email, msg_id, key, headers=headers)
        raw = envelope_json(env)

        if self.mode == _STREAM:
            values: dict[str, Any] = {
                "id": msg_id,
                "payload": env["payload"],
                "data": raw,
            }
            if self.max_len > 0:
                await self._client.xadd(key, values, maxlen=self.max_len, approximate=True)
            else:
                await self._client.xadd(key, values)
        elif self.mode == _LIST:
            await self._client.rpush(key, raw)
        else:
            await self._client.publish(key, raw)
        return msg_id

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except AttributeError:
            pass
