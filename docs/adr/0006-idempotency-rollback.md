# ADR 0006: Releasing the idempotency claim when publishing fails

## Status

Accepted

## Context

The ingest use case runs three steps: rate limit, idempotency claim, publish.

Claiming before publishing is not incidental. If the claim came afterwards, two
concurrent requests carrying the same `event_id` would both pass the check and
both publish, which is precisely what the claim exists to prevent. So the claim
must come first.

That ordering has a consequence the original implementation did not handle. The
claim was written with `SET NX` and a 24-hour TTL, and `XADD` ran next with no
error handling. When the publish failed — Redis unreachable, pool exhausted, a
timeout — the claim stayed. Every subsequent retry of that `event_id` matched the
claim and was answered `200 duplicate`.

The event was never published, and the caller was told it was a replay of
something already accepted. The event was gone, silently, for 24 hours, and the
only way to notice was to reconcile counts with the producer.

This is worse than an outage, because an outage is visible.

## Decision

Wrap the publish and release the claim on failure.

```python
stored = await self._idempotency.remember(event.event_id, ttl)
if not stored:
    raise DuplicateEventError(str(event.event_id))

try:
    stream_id = await self._publisher.publish(event)
except Exception:
    await self._release(event.event_id)
    raise
```

`IdempotencyStore` gains a `forget` method, implemented in both the Redis and
in-memory adapters, so the rollback is part of the port rather than a Redis
detail leaking into the use case.

The release is best-effort and deliberately swallows its own errors. The publish
failure is the one worth surfacing; a failed rollback costs the caller a retry
once the TTL expires, and reporting it instead would replace a clear error with a
confusing one.

The API layer completes the change: an unhandled exception on the ingest path now
returns **503** rather than propagating as a 500 or, worse, being mistaken for
success. A `202` means the event is durably in the stream, and it must never mean
anything else.

## Alternatives rejected

**A short claim TTL, extended after a successful publish.** Bounds the damage to
the short TTL instead of eliminating it. Rejected because it turns a correctness
property into a race: two requests arriving inside the extension window still
both publish.

**Claiming after publishing.** Removes the rollback entirely and reintroduces the
double-publish window the claim exists to close.

**A transactional outbox.** The general solution: write the event and the claim
atomically to one store, then relay to the stream. Genuinely better, and the
right answer when there is already a database in the ingest path. Rejected here
because the gateway is intentionally stateless — adding Postgres just to hold an
outbox would be a much larger change than the failure justifies, and the rollback
covers the realistic failure modes.

## Consequences

* A failed publish leaves no trace, so a retrying client is served normally
  rather than being told it is a duplicate.
* Callers can distinguish "already accepted" (200) from "not accepted, try again"
  (503). Previously both looked like 200.
* Covered by tests in both directions: `test_failed_publish_releases_the_idempotency_claim`
  proves the claim is gone, and `test_client_can_retry_after_a_failed_publish`
  proves the retry succeeds and publishes exactly once.
* There remains a window where the publish succeeds but the response is lost in
  transit. The client retries, gets `200 duplicate`, and is correct to treat it
  as success — the event is in the stream. This is the intended behaviour, not a
  gap.
