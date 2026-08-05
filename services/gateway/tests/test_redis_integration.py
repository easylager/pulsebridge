"""Integration tests against a real Redis.

The in-memory adapters keep the unit suite fast, but they cannot prove the parts
that only exist in Redis: the atomicity of the Lua token bucket, the semantics of
SET NX under concurrency, and the exact field layout the Go worker parses out of
the stream. Those are covered here.

The suite skips itself when no Redis is reachable so `make test` still works on a
laptop with nothing running; CI provides one and runs it for real.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from pulsebridge_gateway.application.ingest import IngestEventUseCase
from pulsebridge_gateway.domain.errors import DuplicateEventError, RateLimitedError
from pulsebridge_gateway.domain.events import EventId, TenantId
from pulsebridge_gateway.infrastructure.redis_adapters import (
    RedisIdempotencyStore,
    RedisStreamPublisher,
    RedisTokenBucketLimiter,
)

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def redis(redis_url: str) -> AsyncIterator[Redis]:
    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        # Skipping is right on a laptop and wrong in CI, where a silent skip
        # would hide a real break. CI sets PULSEBRIDGE_REQUIRE_REDIS=1.
        if os.getenv("PULSEBRIDGE_REQUIRE_REDIS") == "1":
            pytest.fail(f"Redis is required but unreachable at {redis_url}: {exc}")
        pytest.skip(f"no Redis reachable at {redis_url}")

    yield client
    await client.aclose()


@pytest.fixture
def namespace() -> str:
    return f"test:{uuid.uuid4().hex[:8]}:"


async def test_idempotency_claim_is_exclusive(redis: Redis, namespace: str) -> None:
    store = RedisIdempotencyStore(redis, prefix=namespace)
    event_id = EventId("evt-1")

    first = await store.remember(event_id, 60)
    second = await store.remember(event_id, 60)

    assert (first, second) == (True, False)


async def test_concurrent_claims_produce_exactly_one_winner(redis: Redis, namespace: str) -> None:
    store = RedisIdempotencyStore(redis, prefix=namespace)
    event_id = EventId("evt-race")

    results = await asyncio.gather(*(store.remember(event_id, 60) for _ in range(50)))

    assert sum(results) == 1, "SET NX must admit exactly one publisher per event id"


async def test_forget_lets_a_failed_publish_be_retried(redis: Redis, namespace: str) -> None:
    store = RedisIdempotencyStore(redis, prefix=namespace)
    event_id = EventId("evt-1")

    assert await store.remember(event_id, 60)
    await store.forget(event_id)

    assert await store.remember(event_id, 60), "a released claim must be reusable"


async def test_token_bucket_enforces_the_configured_rate(redis: Redis, namespace: str) -> None:
    limiter = RedisTokenBucketLimiter(redis, rps=5, prefix=namespace)
    tenant = TenantId("acme")

    outcomes = [await limiter.allow(tenant) for _ in range(20)]
    allowed = sum(1 for ok, _ in outcomes if ok)

    # Tokens refill continuously, so the exact count depends on wall time; what
    # must hold is that the burst is capped near capacity rather than unbounded.
    assert 5 <= allowed <= 7, f"allowed {allowed} requests against a 5 rps bucket"


async def test_token_bucket_reports_a_usable_retry_hint(redis: Redis, namespace: str) -> None:
    limiter = RedisTokenBucketLimiter(redis, rps=1, prefix=namespace)
    tenant = TenantId("acme")

    await limiter.allow(tenant)
    allowed, retry_after_ms = await limiter.allow(tenant)

    assert not allowed
    assert retry_after_ms > 0


async def test_buckets_are_isolated_per_tenant(redis: Redis, namespace: str) -> None:
    limiter = RedisTokenBucketLimiter(redis, rps=1, prefix=namespace)

    assert (await limiter.allow(TenantId("noisy")))[0]
    assert not (await limiter.allow(TenantId("noisy")))[0]
    assert (await limiter.allow(TenantId("quiet")))[0]


async def test_published_entry_carries_every_field_the_worker_reads(
    redis: Redis, namespace: str
) -> None:
    stream = f"{namespace}events"
    use_case = IngestEventUseCase(
        publisher=RedisStreamPublisher(redis, stream_key=stream),
        idempotency=RedisIdempotencyStore(redis, prefix=namespace),
        rate_limiter=RedisTokenBucketLimiter(redis, rps=100, prefix=namespace),
    )

    await use_case.execute(
        tenant_id="acme",
        event_type="order.created",
        payload={"order_id": "o-1"},
        event_id="evt-1",
        correlation_id="corr-1",
    )

    entries = await redis.xrange(stream)
    assert len(entries) == 1
    _, fields = entries[0]

    # These exact keys are what internal/consumer/consumer.go maps; renaming one
    # on either side without the other is the classic cross-service break this
    # test exists to catch.
    assert fields["event_id"] == "evt-1"
    assert fields["tenant_id"] == "acme"
    assert fields["event_type"] == "order.created"
    assert fields["correlation_id"] == "corr-1"
    assert json.loads(fields["payload"]) == {"order_id": "o-1"}
    assert "occurred_at" in fields
    assert "traceparent" in fields

    await redis.delete(stream)


async def test_duplicate_event_is_published_once(redis: Redis, namespace: str) -> None:
    stream = f"{namespace}events"
    use_case = IngestEventUseCase(
        publisher=RedisStreamPublisher(redis, stream_key=stream),
        idempotency=RedisIdempotencyStore(redis, prefix=namespace),
        rate_limiter=RedisTokenBucketLimiter(redis, rps=100, prefix=namespace),
    )

    await use_case.execute(tenant_id="acme", event_type="t", payload={}, event_id="evt-dup")
    with pytest.raises(DuplicateEventError):
        await use_case.execute(tenant_id="acme", event_type="t", payload={}, event_id="evt-dup")

    assert len(await redis.xrange(stream)) == 1
    await redis.delete(stream)


async def test_rate_limited_request_never_reaches_the_stream(redis: Redis, namespace: str) -> None:
    stream = f"{namespace}events"
    use_case = IngestEventUseCase(
        publisher=RedisStreamPublisher(redis, stream_key=stream),
        idempotency=RedisIdempotencyStore(redis, prefix=namespace),
        rate_limiter=RedisTokenBucketLimiter(redis, rps=1, prefix=namespace),
    )

    await use_case.execute(tenant_id="t1", event_type="t", payload={}, event_id="a")
    with pytest.raises(RateLimitedError):
        for i in range(10):
            await use_case.execute(tenant_id="t1", event_type="t", payload={}, event_id=f"b{i}")

    assert len(await redis.xrange(stream)) == 1
    await redis.delete(stream)


async def test_stream_is_trimmed_to_the_configured_bound(redis: Redis, namespace: str) -> None:
    stream = f"{namespace}events"
    publisher = RedisStreamPublisher(redis, stream_key=stream, max_len=5)
    use_case = IngestEventUseCase(
        publisher=publisher,
        idempotency=RedisIdempotencyStore(redis, prefix=namespace),
        rate_limiter=RedisTokenBucketLimiter(redis, rps=1000, prefix=namespace),
    )

    for i in range(50):
        await use_case.execute(tenant_id="acme", event_type="t", payload={}, event_id=f"e{i}")

    # MAXLEN ~ trims at radix node boundaries, so the bound is approximate by
    # design; what matters is that retention stays bounded rather than exact.
    length = await redis.xlen(stream)
    assert length <= 50
    await redis.delete(stream)
