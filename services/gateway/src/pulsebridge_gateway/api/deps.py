from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass

from redis.asyncio import BlockingConnectionPool, Redis

from pulsebridge_gateway.application.ingest import IngestEventUseCase
from pulsebridge_gateway.infrastructure.memory import (
    InMemoryEventPublisher,
    InMemoryIdempotencyStore,
    InMemoryRateLimiter,
)
from pulsebridge_gateway.infrastructure.redis_adapters import (
    RedisIdempotencyStore,
    RedisStreamPublisher,
    RedisTokenBucketLimiter,
)
from pulsebridge_gateway.infrastructure.settings import Settings, get_settings


@dataclass
class AppContainer:
    settings: Settings
    redis: Redis | None
    ingest: IngestEventUseCase


_container: AppContainer | None = None


def get_container() -> AppContainer:
    if _container is None:
        raise RuntimeError("container is not initialized")
    return _container


async def build_container(settings: Settings | None = None) -> AppContainer:
    settings = settings or get_settings()
    if settings.use_memory:
        ingest = IngestEventUseCase(
            publisher=InMemoryEventPublisher(),
            idempotency=InMemoryIdempotencyStore(),
            rate_limiter=InMemoryRateLimiter(settings.rate_limit_rps),
            idempotency_ttl_seconds=settings.idempotency_ttl_seconds,
        )
        return AppContainer(settings=settings, redis=None, ingest=ingest)

    # BlockingConnectionPool rather than the default pool. The default raises
    # "Too many connections" the instant the pool is exhausted, which turns a
    # short burst into 503s even though a connection would have freed up
    # milliseconds later. Blocking with a bounded timeout absorbs the burst and
    # still refuses to queue without limit: past the timeout the request fails,
    # loudly and quickly, instead of piling up.
    pool = BlockingConnectionPool.from_url(
        settings.redis_url,
        decode_responses=True,
        max_connections=settings.redis_max_connections,
        timeout=settings.redis_pool_timeout_seconds,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_connect_timeout_seconds,
    )
    redis = Redis(connection_pool=pool)

    ingest = IngestEventUseCase(
        publisher=RedisStreamPublisher(
            redis,
            stream_key=settings.stream_key,
            max_len=settings.stream_max_len,
        ),
        idempotency=RedisIdempotencyStore(redis),
        rate_limiter=RedisTokenBucketLimiter(redis, rps=settings.rate_limit_rps),
        idempotency_ttl_seconds=settings.idempotency_ttl_seconds,
    )
    return AppContainer(settings=settings, redis=redis, ingest=ingest)


async def lifespan_container(settings: Settings | None = None) -> AsyncIterator[AppContainer]:
    global _container
    container = await build_container(settings)
    _container = container
    try:
        yield container
    finally:
        if container.redis is not None:
            await container.redis.aclose()
        _container = None
