from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pulsebridge_gateway.domain.errors import DuplicateEventError, RateLimitedError
from pulsebridge_gateway.domain.events import EventEnvelope
from pulsebridge_gateway.domain.ports import EventPublisher, IdempotencyStore, RateLimiter


@dataclass(frozen=True, slots=True)
class IngestResult:
    event_id: str
    stream_id: str
    duplicate: bool = False


class IngestEventUseCase:
    """Accept → rate-limit → idempotency → publish.

    Designed for high fan-in: all I/O is async; no blocking calls on the event loop.
    """

    def __init__(
        self,
        *,
        publisher: EventPublisher,
        idempotency: IdempotencyStore,
        rate_limiter: RateLimiter,
        idempotency_ttl_seconds: int = 86_400,
    ) -> None:
        self._publisher = publisher
        self._idempotency = idempotency
        self._rate_limiter = rate_limiter
        self._idempotency_ttl_seconds = idempotency_ttl_seconds

    async def execute(
        self,
        *,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any],
        event_id: str | None = None,
        correlation_id: str | None = None,
    ) -> IngestResult:
        event = EventEnvelope.create(
            tenant_id=tenant_id,
            event_type=event_type,
            payload=payload,
            event_id=event_id,
            correlation_id=correlation_id,
        )

        allowed, retry_after_ms = await self._rate_limiter.allow(event.tenant_id)
        if not allowed:
            raise RateLimitedError(str(event.tenant_id), retry_after_ms)

        stored = await self._idempotency.remember(event.event_id, self._idempotency_ttl_seconds)
        if not stored:
            raise DuplicateEventError(str(event.event_id))

        stream_id = await self._publisher.publish(event)
        return IngestResult(event_id=str(event.event_id), stream_id=stream_id)
