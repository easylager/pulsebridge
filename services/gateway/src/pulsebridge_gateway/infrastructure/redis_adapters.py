from __future__ import annotations

import json
import time

from redis.asyncio import Redis
from redis.typing import EncodableT, FieldT

from pulsebridge_gateway.domain.events import EventEnvelope, EventId, TenantId
from pulsebridge_gateway.infrastructure.telemetry import TRACEPARENT_FIELD, current_traceparent


class RedisIdempotencyStore:
    def __init__(self, redis: Redis, *, prefix: str = "pb:idemp:") -> None:
        self._redis = redis
        self._prefix = prefix

    def _key(self, event_id: EventId) -> str:
        return f"{self._prefix}{event_id}"

    async def seen(self, event_id: EventId) -> bool:
        return bool(await self._redis.exists(self._key(event_id)))

    async def remember(self, event_id: EventId, ttl_seconds: int) -> bool:
        # SET NX is atomic across gateway replicas, so exactly one concurrent
        # request for a given event id gets True and proceeds to publish.
        ok = await self._redis.set(self._key(event_id), "1", ex=ttl_seconds, nx=True)
        return bool(ok)

    async def forget(self, event_id: EventId) -> None:
        await self._redis.delete(self._key(event_id))


class RedisTokenBucketLimiter:
    """Per-tenant token bucket using a small Lua script.

    The read-modify-write of the bucket has to be atomic, otherwise two gateway
    replicas refill from the same stale timestamp and a tenant gets roughly
    double its quota. Lua runs server-side in one step, which is cheaper and
    simpler than WATCH/MULTI retry loops on a hot path.
    """

    _SCRIPT = """
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refill_per_ms = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])
    local requested = tonumber(ARGV[4])

    local data = redis.call('HMGET', key, 'tokens', 'ts')
    local tokens = tonumber(data[1])
    local ts = tonumber(data[2])
    if tokens == nil then
      tokens = capacity
      ts = now
    end

    local delta = math.max(0, now - ts)
    tokens = math.min(capacity, tokens + delta * refill_per_ms)
    local allowed = 0
    local retry_after = 0
    if tokens >= requested then
      tokens = tokens - requested
      allowed = 1
    else
      retry_after = math.ceil((requested - tokens) / refill_per_ms)
    end
    redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
    redis.call('PEXPIRE', key, 2000)
    return {allowed, retry_after}
    """

    def __init__(self, redis: Redis, *, rps: int, prefix: str = "pb:rl:") -> None:
        self._redis = redis
        self._rps = max(1, rps)
        self._prefix = prefix
        self._script = self._redis.register_script(self._SCRIPT)

    async def allow(self, tenant_id: TenantId) -> tuple[bool, int]:
        now_ms = int(time.time() * 1000)
        result = await self._script(
            keys=[f"{self._prefix}{tenant_id}"],
            args=[self._rps, self._rps / 1000.0, now_ms, 1],
        )
        allowed = int(result[0]) == 1
        retry_after = int(result[1])
        return allowed, retry_after


class RedisStreamPublisher:
    def __init__(self, redis: Redis, *, stream_key: str, max_len: int = 1_000_000) -> None:
        self._redis = redis
        self._stream_key = stream_key
        self._max_len = max_len

    async def publish(self, event: EventEnvelope) -> str:
        wire = event.to_wire()
        fields: dict[FieldT, EncodableT] = {
            "event_id": wire["event_id"],
            "tenant_id": wire["tenant_id"],
            "event_type": wire["event_type"],
            "payload": json.dumps(wire["payload"], separators=(",", ":")),
            "occurred_at": wire["occurred_at"],
            "correlation_id": wire["correlation_id"],
            # Carrying the trace context on the entry is what lets the Go worker
            # attach its processing span to the HTTP request that produced the
            # event, instead of starting an orphan trace.
            TRACEPARENT_FIELD: current_traceparent(),
        }
        message_id = await self._redis.xadd(
            self._stream_key,
            fields,
            maxlen=self._max_len,
            approximate=True,
        )
        return str(message_id)
