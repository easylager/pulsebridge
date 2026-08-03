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

    async def forget(self, event_id: EventId) -> None:
        """Release a reservation whose event never reached the stream.

        The claim is taken before publishing so two concurrent requests cannot
        both publish. That ordering means a failed publish leaves a claim for an
        event nobody can observe, and every retry is then rejected as a
        duplicate. Releasing it restores the caller's ability to retry.
        """


class RateLimiter(Protocol):
    async def allow(self, tenant_id: TenantId) -> tuple[bool, int]:
        """Return (allowed, retry_after_ms)."""
