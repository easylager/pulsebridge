# ADR 0003: Bounded goroutine pool in the worker

## Status

Accepted

## Context

Ingest is bursty. A consumer that spawns a goroutine per message has no upper
bound on in-flight work, so a spike becomes memory growth and then an OOM kill —
which loses every message that was in flight, since none of them had been
acknowledged yet. Processing serially instead wastes the cores that were the
reason for writing this half in Go.

The question is not whether to bound concurrency but **where the queue lives**
when the bound is reached.

## Decision

A buffered channel sized to `Concurrency` acts as a semaphore. `dispatch` blocks
when it is full, which stops the read loop from pulling more messages, which
leaves the backlog in Redis.

That is the whole mechanism, and the property that matters is the last step. The
stream is durable and already bounded by `MAXLEN`; the worker's heap is neither.
Pushing back-pressure into Redis means a spike costs latency, while buffering it
in memory means a spike costs data.

Each message additionally gets its own `context` timeout, so one wedged handler
occupies one slot rather than the whole pool.

At shutdown the pool is drained rather than cancelled: in-flight handlers keep
their full timeout, because acknowledgement happens after the handler returns and
cutting them off would leave completed work unacknowledged for the next process
to redo.

## Alternatives rejected

**A worker-goroutine pool fed by a job channel.** The more common Go idiom: N
long-lived goroutines reading from a channel. Functionally equivalent here, and
rejected only because the semaphore expresses the intent more directly — the
limit is on *concurrent messages*, and with a semaphore that limit is one number
in one place rather than an emergent property of how many goroutines were
started.

**An unbounded pool with a memory-based circuit breaker.** Higher peak
throughput, and it fails in the worst way: by the time the breaker trips, the
in-flight messages are already at risk.

**A bounded in-memory buffer in front of the pool.** Smooths bursts. Rejected
because it duplicates what the stream already does, with worse durability — the
stream survives a restart and the buffer does not.

**`errgroup.SetLimit`.** Would replace the manual semaphore and `WaitGroup` with
one construct. Reasonable, and mostly a matter of taste; the explicit version
makes the shutdown ordering easier to read, and the shutdown ordering is the
subtle part.

## Consequences

* Maximum in-flight work is predictable and tunable with
  `PULSEBRIDGE_CONCURRENCY`. Two replicas at 64 means at most 128 concurrent
  handlers against downstream systems — a number a downstream owner can be told.
* Backlog accumulates in Redis, visible as `pulsebridge_worker_pending_entries`
  and bounded by `MAXLEN`. Past that bound the oldest entries are trimmed, which
  is the real deadline during an outage.
* The read loop stalls while the pool is full. Intentional, and the reason
  throughput degrades smoothly rather than collapsing.
* A handler failure does not acknowledge, which makes redelivery possible but
  does not by itself make it happen — that needs the reclaim loop in
  [ADR 0004](0004-dead-letter-and-retries.md).
