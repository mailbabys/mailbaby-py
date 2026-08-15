# mailbaby-client

Python client for [MailBaby](https://github.com/mailbabys/mailbaby) — the
message-queue-driven email delivery service. Supports all three ingestion
transports of the server:

- **REST** — `POST /v1/email/send`, `POST /v1/email/batch`, health probes
- **gRPC** — `mailbaby.v1.MailService` (sync + async)
- **MQ** — publish email jobs directly to RabbitMQ, Redis, or Kafka

## Installation

```bash
uv add mailbaby-client                 # REST + gRPC core
uv add mailbaby-client[rabbitmq]       # + pika / aio-pika
uv add mailbaby-client[redis]          # + redis-py
uv add mailbaby-client[kafka]          # + confluent-kafka / aiokafka
uv add mailbaby-client[mq]             # all broker clients
```

Requires Python >= 3.10.

## Quick start

### REST (sync)

```python
from mailbaby import Email, Attachment, MailBabyClient

client = MailBabyClient("http://localhost:8080", api_key="your_secret_key")

email = Email(
    to=["alice@example.com"],
    subject="Order Confirmation #10024",
    html_body="<h2>Order confirmed</h2>",
    text_body="Thank you for your order!",
    account="default",
    attachments=[Attachment.from_path("invoice.pdf", content_type="application/pdf")],
)

result = client.send(email)            # blocks until SMTP acks -> status "sent"
queued = client.send(email, async_=True)  # enqueued  -> status "queued" (202)

batch = client.send_batch([email1, email2], async_=False)
print(batch.succeeded, batch.failed)
```

### REST (async)

```python
from mailbaby import AsyncMailBabyClient

async with AsyncMailBabyClient("http://localhost:8080", api_key="...") as client:
    result = await client.send(email)
```

Health probes:

```python
client.livez()          # liveness — raises UnavailableError when DOWN
status = client.readyz()  # readiness, per-component details in status.components
client.healthz()
```

### gRPC

```python
from mailbaby import MailBabyGrpcClient, AsyncMailBabyGrpcClient, Email

with MailBabyGrpcClient("localhost:8081", api_key="your_secret_key") as client:
    result = client.send(Email(to=["a@example.com"], subject="Hi"))
    pong = client.ping()

async with AsyncMailBabyGrpcClient("localhost:8081") as client:
    result = await client.send(email, async_=True)
    health = await client.health_check()
```

### Message queue ingestion

Publish the same email payload directly to a broker; the server consumes and
delivers it (see the MailBaby "Message Queue Ingestion" section).

```python
from mailbaby import Email, RabbitMQProducer, RedisProducer, KafkaProducer

# RabbitMQ — body is the raw email JSON, routed via exchange/routing key
with RabbitMQProducer("amqp://guest:guest@localhost:5672/") as producer:
    msg_id = producer.publish(email)  # returns message id

# async variant
from mailbaby import AsyncRabbitMQProducer
async with AsyncRabbitMQProducer(exchange="mailbaby_exchange", routing_key="mail.send.#") as p:
    await p.publish(email)

# Redis — stream mode (default), mirrors the server redis driver
producer = RedisProducer(key="mailbaby:queue:email", mode="stream")
producer.publish(email, id="custom-id")
producer.close()

# Kafka — value = email JSON + X-Message-ID header
producer = KafkaProducer("127.0.0.1:9092", topic="mailbaby_tasks")
producer.publish(email)
producer.close()
```

Producers accept a pre-connected broker client (e.g. a `redis.Redis` instance)
or connection parameters, and support per-call `topic`/`id`/`headers`
overrides. Async variants (`AsyncRabbitMQProducer`, `AsyncRedisProducer`,
`AsyncKafkaProducer`) mirror the sync API.

## Emails

`mailbaby.Email` mirrors the server request schema exactly
(`account`, `from_`, `from_name`, `reply_to`, `to`, `cc`, `bcc`, `subject`,
`text_body`, `html_body`, `headers`, `attachments`, `tags`, `metadata`).

```python
email = Email(
    to=["a@example.com"],
    subject="Hello",
    from_="noreply@example.com",   # overrides the account default sender
    html_body="<p>Hi</p>",
    headers={"X-Priority": "1"},
    metadata={"tenant_id": "42"},
)
```

Attachments accept raw bytes, file-like objects, or paths; `data` is
base64-encoded automatically for REST and MQ, and sent raw over gRPC:

```python
Attachment.from_path("report.pdf")                       # filename from path
Attachment.from_bytes(b"...", "data.bin")                # raw bytes
Attachment(..., inline=True, content_id="img1")          # CID for <img src="cid:img1">
```

## Errors

All failures map to `mailbaby.MailBabyError` subclasses carrying
`status_code`, `code`, and `details`:

| Exception | HTTP | Meaning |
|---|---|---|
| `ValidationError` | 400 | server-side validation failure |
| `AuthenticationError` | 401 | missing/invalid API key |
| `NotFoundError` | 404 | endpoint not found |
| `MethodNotAllowedError` | 405 | wrong HTTP method |
| `DeliveryError` | 500 | sync SMTP delivery failed |
| `EnqueueError` | 500 | queue enqueue failed |
| `UnavailableError` | 503 | service not ready |
| `RequestFailedError` | — | network / timeout / bad response |

```python
from mailbaby import MailBabyError

try:
    client.send(email)
except MailBabyError as exc:
    print(exc.status_code, exc.code, exc.details)
```

## Development

```bash
uv sync --all-extras        # install deps incl. broker clients + dev tools
uv run ruff check src tests
uv run pytest
uv build

# regenerate gRPC stubs when the server proto changes
uv run python -m grpc_tools.protoc -I ../mailbaby/proto \
    --python_out=src/mailbaby/grpc/gen \
    --grpc_python_out=src/mailbaby/grpc/gen \
    mailbaby.proto
```

## License

Apache 2.0 — see the MailBaby project.