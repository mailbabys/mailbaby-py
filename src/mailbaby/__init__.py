"""mailbaby-client — Python client for the MailBaby email delivery service.

Supports three ingestion transports:

- REST: :class:`MailBabyClient` / :class:`AsyncMailBabyClient`
- gRPC: :class:`MailBabyGrpcClient` / :class:`AsyncMailBabyGrpcClient`
- MQ:   :class:`RabbitMQProducer`, :class:`RedisProducer`, :class:`KafkaProducer`
  (plus async variants), publishing the server's message-queue ingestion payload.
"""

from mailbaby.exceptions import (
    AuthenticationError,
    DeliveryError,
    EnqueueError,
    MailBabyError,
    MethodNotAllowedError,
    NotFoundError,
    RequestFailedError,
    UnavailableError,
    ValidationError,
)
from mailbaby.grpc.client import AsyncMailBabyGrpcClient, MailBabyGrpcClient
from mailbaby.models import (
    Attachment,
    BatchResult,
    Email,
    HealthStatus,
    SendResult,
)
from mailbaby.mq import (
    AsyncKafkaProducer,
    AsyncRabbitMQProducer,
    AsyncRedisProducer,
    KafkaProducer,
    RabbitMQProducer,
    RedisProducer,
    async_kafka_producer,
    async_rabbitmq_producer,
    async_redis_producer,
    kafka_producer,
    rabbitmq_producer,
    redis_producer,
)
from mailbaby.rest.async_client import AsyncMailBabyClient
from mailbaby.rest.client import MailBabyClient

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # errors
    "MailBabyError",
    "ValidationError",
    "AuthenticationError",
    "NotFoundError",
    "MethodNotAllowedError",
    "DeliveryError",
    "EnqueueError",
    "UnavailableError",
    "RequestFailedError",
    # models
    "Email",
    "Attachment",
    "SendResult",
    "BatchResult",
    "HealthStatus",
    # REST clients
    "MailBabyClient",
    "AsyncMailBabyClient",
    # gRPC clients
    "MailBabyGrpcClient",
    "AsyncMailBabyGrpcClient",
    # MQ producers
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
