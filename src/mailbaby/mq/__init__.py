from __future__ import annotations

from typing import Any

from mailbaby.mq.base import AsyncProducer, Producer
from mailbaby.mq.kafka import AsyncKafkaProducer, KafkaProducer
from mailbaby.mq.rabbitmq import AsyncRabbitMQProducer, RabbitMQProducer
from mailbaby.mq.redis import AsyncRedisProducer, RedisProducer

__all__ = [
    "Producer",
    "AsyncProducer",
    "RabbitMQProducer",
    "AsyncRabbitMQProducer",
    "RedisProducer",
    "AsyncRedisProducer",
    "KafkaProducer",
    "AsyncKafkaProducer",
    "rabbitmq_producer",
    "async_rabbitmq_producer",
    "redis_producer",
    "async_redis_producer",
    "kafka_producer",
    "async_kafka_producer",
]


def rabbitmq_producer(**kwargs: Any) -> RabbitMQProducer:
    return RabbitMQProducer(**kwargs)


def async_rabbitmq_producer(**kwargs: Any) -> AsyncRabbitMQProducer:
    return AsyncRabbitMQProducer(**kwargs)


def redis_producer(**kwargs: Any) -> RedisProducer:
    return RedisProducer(**kwargs)


def async_redis_producer(**kwargs: Any) -> AsyncRedisProducer:
    return AsyncRedisProducer(**kwargs)


def kafka_producer(**kwargs: Any) -> KafkaProducer:
    return KafkaProducer(**kwargs)


def async_kafka_producer(**kwargs: Any) -> AsyncKafkaProducer:
    return AsyncKafkaProducer(**kwargs)
