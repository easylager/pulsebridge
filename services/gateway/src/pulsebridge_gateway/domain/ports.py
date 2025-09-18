from __future__ import annotations

from typing import Protocol

from pulsebridge_gateway.domain.events import EventEnvelope, EventId, TenantId


class EventPublisher(Protocol):
    async def publish(self, event: EventEnvelope) -> str:
        """Publish event to the durable stream. Returns stream message id."""


class IdempotencyStore(Protocol):
    async def seen(self, event_id: EventId) -> bool: ...

    async def remember(self, event_id: EventId, ttl_seconds: int) -> bool:
        """Return True if key was newly stored, False if already existed."""


class RateLimiter(Protocol):
    async def allow(self, tenant_id: TenantId) -> tuple[bool, int]:
        """Return (allowed, retry_after_ms)."""
