from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from pulsebridge_gateway.domain.errors import DuplicateEventError, RateLimitedError
from pulsebridge_gateway.domain.events import EventEnvelope, EventId
from pulsebridge_gateway.domain.ports import EventPublisher, IdempotencyStore, RateLimiter


@dataclass(frozen=True, slots=True)
class IngestResult:
    event_id: str
    stream_id: str
    duplicate: bool = False


class IngestEventUseCase:
    """Accept → rate-limit → claim → publish.

    Designed for high fan-in: all I/O is async; no blocking calls on the event
    loop.

    The step order is deliberate. Rate limiting runs first so a tenant flooding
    the endpoint is rejected before it can consume Redis writes. The idempotency
    claim is taken before publishing, not after, because a claim taken afterwards
    leaves a window in which two concurrent requests carrying the same event id
    both publish. The cost of claiming first is that a failed publish must give
    the claim back, which is what the rollback below does.
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

        try:
            stream_id = await self._publisher.publish(event)
        except Exception:
            # Without this the event id stays claimed for the full TTL while
            # nothing was ever published: the client's retries would all come
            # back as duplicates and the event would be lost silently.
            await self._release(event.event_id)
            raise

        return IngestResult(event_id=str(event.event_id), stream_id=stream_id)

    async def _release(self, event_id: EventId) -> None:
        # The publish failure is the error worth surfacing; a failed rollback only
        # costs the caller a retry once the claim's TTL expires.
        with suppress(Exception):
            await self._idempotency.forget(event_id)
