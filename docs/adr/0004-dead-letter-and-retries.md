# ADR 0004: Bounded retries with a dead-letter stream

## Status

Accepted

## Context

The original consumer had two failure paths, and both lost information.

A message whose handler returned an error was simply not acknowledged. That is
correct as far as it goes, but nothing ever redelivered it: `XREADGROUP ">"`
only returns entries never handed to the group, so a failed message sat in the
pending entries list permanently. The delivery guarantee was at-least-once on
paper and at-most-once in practice for any message that failed.

A message that could not be parsed was logged and acknowledged. That is worse:
the event was silently destroyed, and the only record was a log line that would
age out.

Both paths also had no notion of a retry budget, so even after adding
redelivery, a permanently broken message would cycle forever, occupying pending
slots and polluting the metrics that operators use to judge health.

## Decision

Three mechanisms, each covering a different failure.

**1. Reclaim.** A background loop lists pending entries with `XPENDING`, filters
those idle longer than `ReclaimMinIdle`, and takes them over with `XCLAIM`. This
is what makes redelivery real, and it covers the case a retry counter cannot: a
consumer that was killed between `XREADGROUP` and `XACK` never got the chance to
fail, so only another consumer noticing the stale entry can recover it.

The idle window is enforced to be at least `2 × ProcessTimeout` in
`Options.Normalise`. A window shorter than a handler's own deadline means every
slow-but-successful event gets reclaimed while the first attempt is still
running, and duplicates are manufactured by the recovery mechanism itself.

**2. A retry budget.** Redis tracks a delivery count per pending entry.
`MaxAttempts` (default 5) caps it. The count is read from `XPENDING` rather than
kept in the worker's memory, so the budget survives a restart — an in-memory
counter would reset on every deploy and let a poison message retry indefinitely
across rollouts.

**3. A dead-letter stream.** Terminal failures are written to
`pulsebridge.events.dlq` and then acknowledged on the source stream. Two reasons
are distinguished, because they call for different responses:

* `undecodable` — the envelope is malformed. Quarantined on the first attempt;
  the bytes will not change, so spending the retry budget on them is pure delay.
* `retries_exhausted` — the handler kept failing. Usually a downstream outage,
  which means these events are replayable once it recovers.

The DLQ entry preserves the original fields verbatim and adds `dlq_`-prefixed
metadata: reason, last error, attempt count, timestamp, source stream and
original id. Replay is therefore a filtered copy back to the source stream, not
a reconstruction.

If the dead-letter write itself fails, the message is **not** acknowledged and
`pulsebridge_worker_dead_letter_failures_total` increments. Retrying a poison
message forever is recoverable; acknowledging one that was never recorded is not.

## Alternatives rejected

**`XAUTOCLAIM` instead of `XPENDING` + `XCLAIM`.** One round trip instead of two,
and it is the more modern command. Rejected because `XAUTOCLAIM` does not return
delivery counts, which the retry budget needs, and its support in test doubles is
patchier — the current pair works against any Redis 5+ and against miniredis, so
the reclaim logic is covered by tests that run in CI without a container.

**Redis's `IDLE` filter on `XPENDING`.** Available, and it would move the filter
server-side. Rejected for the same portability reason; the pending page is capped
at `Concurrency` entries, so filtering in Go costs nothing measurable.

**Exponential backoff between retries.** The reclaim interval is fixed. A growing
delay is better behaviour against a downstream that is rate-limiting, but Redis
Streams has no delayed-delivery primitive, so implementing it means a second
stream or a sorted set keyed by ready-at. That is real complexity, and the
constant interval is honest about what it is. Worth revisiting when there is a
real downstream to be gentle to.

**A DLQ table in Postgres instead of a stream.** Better query ergonomics for
investigation. Rejected because it introduces a second datastore on the failure
path — the path that must work when things are already going wrong.

## Consequences

* At-least-once delivery holds under consumer death, not only under handler
  failure.
* A poison message can no longer block the pending list or be silently dropped.
* Operators get a queryable record of every terminal failure with enough context
  to replay it, and a documented replay procedure in [`../runbook.md`](../runbook.md).
* Two extra Redis commands per reclaim cycle, once per `ReclaimInterval`. Not on
  the hot path.
* One more stream to keep an eye on: the DLQ is trimmed to `DLQMaxLen`, and its
  eviction policy must not allow it to be dropped under memory pressure.
