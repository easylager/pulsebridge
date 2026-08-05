# Service level objectives

These objectives exist to make two decisions mechanical: when to page someone,
and when to stop shipping features and go fix reliability instead. They are
written from the caller's point of view, because that is the only perspective
that makes an alert worth waking up for.

Every objective below is measured from a metric the services already export, and
every one maps to a rule in [`../deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml).

---

## What counts as a failure

This distinction does more work than the numbers do.

| Response | Counts against the SLO? | Why |
|---|---|---|
| `202 accepted` | No | The event is durably in the stream. |
| `200 duplicate` | No | A replay of an event already accepted. The caller's intent is satisfied. |
| `429 rate_limited` | No | A deliberate answer with a `Retry-After`. The system is protecting itself and told the caller exactly what to do. |
| `422 invalid` | No | The caller sent something the contract forbids. |
| `503 unavailable` | **Yes** | The event was not written and the caller cannot know when it will be. |

A rate limit is not an outage. Counting throttling as failure creates pressure to
raise limits during incidents, which is precisely backwards.

---

## Objectives

### SLO 1 — Ingest availability

> Over a rolling 28 days, at least **99.9%** of ingest requests are answered with
> something other than a 503.

```promql
1 - (
  sum(rate(pulsebridge_ingest_total{result="unavailable"}[28d]))
  /
  clamp_min(sum(rate(pulsebridge_ingest_total[28d])), 0.001)
)
```

**Error budget:** 0.1%, roughly 43 minutes of total unavailability per 28 days,
or 1 failed request in 1 000.

**Rationale:** producers are expected to retry, so a brief failure is
recoverable. It is not free — a retry storm is the usual second act of an ingest
outage — but it does not warrant the cost of chasing another nine here rather
than in the consumption path, where a lost event is not recoverable by retrying.

---

### SLO 2 — Ingest latency

> Over a rolling 28 days, the **p99** of accepted ingest requests stays under
> **50 ms**, and the p95 under **25 ms**.

```promql
histogram_quantile(
  0.99,
  sum(rate(pulsebridge_ingest_latency_seconds_bucket[28d])) by (le)
)
```

**Rationale:** the endpoint is a write-and-return edge. Its whole purpose is to
get the event durable and hand control back, so that producers are never coupled
to consumer speed. If the p99 approaches a producer's own timeout, that decoupling
stops being real.

**Measured headroom:** [`benchmarks.md`](benchmarks.md) puts a single gateway
process inside this objective up to about 400 rps, and past it at 600 rps. That
is the number to scale on, not CPU utilisation.

---

### SLO 3 — Processing freshness

> Over a rolling 28 days, **99%** of the time the oldest unacknowledged event is
> younger than **60 seconds**.

```promql
avg_over_time(pulsebridge_worker_oldest_pending_seconds[28d]) < 60
```

**Rationale:** this is the objective that is easy to get wrong. Backlog *depth*
is not a health signal — a healthy consumer under load always has entries in
flight. Backlog *age* is, because it only grows when messages stop being
acknowledged, which means a consumer died or a handler is wedged.

The reclaim loop is what defends this objective: a message stranded by a dead
consumer is picked up again after `PULSEBRIDGE_RECLAIM_MIN_IDLE`, so age recovers
without human involvement.

---

### SLO 4 — Event durability

> **Zero** events are lost. An event that is accepted is either processed or
> visible in the dead-letter stream.

This is a hard invariant, not a percentage. The properties that hold it up:

* the idempotency claim is released if the publish fails, so a `202` is never
  returned for an event that was not written ([`adr/0006`](adr/0006-idempotency-rollback.md));
* a handler failure does not acknowledge, so the message stays in the pending
  list and is redelivered;
* exhausting the retry budget moves the event to the dead-letter stream rather
  than dropping it ([`adr/0004`](adr/0004-dead-letter-and-retries.md));
* if the dead-letter write itself fails, the message is left pending and
  `pulsebridge_worker_dead_letter_failures_total` pages immediately — that
  counter is the only path by which an event could vanish.

**Deliberately not claimed:** exactly-once delivery. The system is at-least-once,
and closes the gap with an idempotent handler. See
[`adr/0005`](adr/0005-at-least-once-semantics.md).

---

## Alert mapping

| Objective | Alert | Severity |
|---|---|---|
| Availability | `IngestErrorBudgetBurn` | page |
| Latency | `IngestLatencyHigh` | ticket |
| Freshness | `ConsumerBacklogStuck` | page |
| Freshness | `ConsumerBacklogGrowing` | ticket |
| Durability | `DeadLetterWritesFailing` | page |
| Durability | `DeadLetterRateElevated` | page |
| — | `RetryStormDetected` | ticket |
| — | `TenantSustainedThrottling` | ticket |

Only four conditions page, and each one means either that callers are being
turned away or that an event is at risk. Everything else is a ticket, because
waking someone for a condition they will not act on until morning is how a team
learns to ignore its pager.

Each alert carries a `runbook` annotation pointing at the matching section of
[`runbook.md`](runbook.md).

---

## Reviewing these numbers

The objectives are guesses until there is production traffic to calibrate them
against. They should be revisited when:

* the error budget is consistently untouched — the target is too loose to be
  informing any decision;
* the budget burns every month without anyone changing course — the target is
  aspirational rather than agreed;
* the worker's handler gains a real downstream, at which point SLO 3 needs to
  account for that dependency's own availability.
