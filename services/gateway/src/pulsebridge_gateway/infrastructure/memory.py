from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque

from pulsebridge_gateway.domain.events import EventEnvelope, EventId, TenantId


class InMemoryIdempotencyStore:
    def __init__(self) -> None:
        self._keys: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def seen(self, event_id: EventId) -> bool:
        async with self._lock:
            self._purge()
            return str(event_id) in self._keys

    async def remember(self, event_id: EventId, ttl_seconds: int) -> bool:
        async with self._lock:
            self._purge()
            key = str(event_id)
            if key in self._keys:
                return False
            self._keys[key] = time.monotonic() + ttl_seconds
            return True

    async def forget(self, event_id: EventId) -> None:
        async with self._lock:
            self._keys.pop(str(event_id), None)

    def _purge(self) -> None:
        now = time.monotonic()
        expired = [k for k, exp in self._keys.items() if exp <= now]
        for k in expired:
            del self._keys[k]


class InMemoryRateLimiter:
    def __init__(self, rps: int) -> None:
        self._rps = max(1, rps)
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def allow(self, tenant_id: TenantId) -> tuple[bool, int]:
        async with self._lock:
            now = time.monotonic()
            window = self._hits[str(tenant_id)]
            while window and now - window[0] >= 1.0:
                window.popleft()
            if len(window) >= self._rps:
                retry_after_ms = int((1.0 - (now - window[0])) * 1000)
                return False, max(retry_after_ms, 1)
            window.append(now)
            return True, 0


class InMemoryEventPublisher:
    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []
        self._seq = 0
        self._lock = asyncio.Lock()

    async def publish(self, event: EventEnvelope) -> str:
        async with self._lock:
            self._seq += 1
            self.events.append(event)
            return f"mem-{self._seq}"
