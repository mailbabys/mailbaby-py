"""Smoke test against a running MailBaby server.

Usage::

    uv run python scripts/smoke_test.py [base_url] [api_key]

Requires the server running with the ``memory`` queue driver
(e.g. ``mailbaby server -c config.yaml`` with a reachable SMTP account).
"""

from __future__ import annotations

import asyncio
import sys

from mailbaby import (
    AsyncMailBabyClient,
    AsyncMailBabyGrpcClient,
    Email,
    MailBabyClient,
    MailBabyGrpcClient,
)


def main(base_url: str, api_key: str | None) -> int:
    print(f"[rest] connecting to {base_url}")
    with MailBabyClient(base_url, api_key=api_key) as client:
        print("[rest] livez:", client.livez().status)
        try:
            ready = client.readyz()
            print("[rest] readyz:", ready.status, ready.components)
        except Exception as exc:  # noqa: BLE001
            print("[rest] readyz DOWN:", exc)

        email = Email(
            to=["smoke@example.com"],
            subject="mailbaby-py smoke test",
            text_body="Hello from mailbaby-py!",
        )
        result = client.send(email, async_=True)
        print("[rest] send async:", result.status, result.id)
        batch = client.send_batch([email, email], async_=True)
        print(f"[rest] batch: {batch.succeeded}/{batch.total} queued")
    return 0


async def amain(base_url: str, api_key: str | None) -> None:
    host = base_url.removeprefix("http://").removeprefix("https://").split(":")[0]
    port = 8081
    target = f"{host}:{port}"
    async with AsyncMailBabyClient(base_url, api_key=api_key) as client:
        result = await client.send(
            Email(to=["smoke@example.com"], subject="async smoke"), async_=True
        )
        print("[rest-async] send:", result.status)
    async with AsyncMailBabyGrpcClient(target, api_key=api_key) as client:
        r = await client.send(Email(to=["smoke@example.com"], subject="grpc smoke"))
        print("[grpc-async] send:", r.status, "| ping:", (await client.ping())["status"])


if __name__ == "__main__":
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    api_key = sys.argv[2] if len(sys.argv) > 2 else None
    # sync grpc sanity (best-effort; server gRPC port must be up)
    grpc_host = base_url.removeprefix("http://").split(":")[0]
    with MailBabyGrpcClient(f"{grpc_host}:8081", api_key=api_key) as g:
        try:
            print("[grpc] ping:", g.ping()["status"])
        except Exception as exc:  # noqa: BLE001
            print("[grpc] ping failed:", exc)
    main(base_url, api_key)
    asyncio.run(amain(base_url, api_key))
    print("smoke test passed")
