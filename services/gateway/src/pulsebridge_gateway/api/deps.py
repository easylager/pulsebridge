from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator

from redis.asyncio import Redis

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

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    ingest = IngestEventUseCase(
        publisher=RedisStreamPublisher(redis, stream_key=settings.stream_key),
        idempotency=RedisIdempotencyStore(redis),
        rate_limiter=RedisTokenBucketLimiter(redis, rps=settings.rate_limit_rps),
        idempotency_ttl_seconds=settings.idempotency_ttl_seconds,
    )
    return AppContainer(settings=settings, redis=redis, ingest=ingest)


async def lifespan_container() -> AsyncIterator[AppContainer]:
    global _container
    container = await build_container()
    _container = container
    try:
        yield container
    finally:
        if container.redis is not None:
            await container.redis.aclose()
        _container = None
