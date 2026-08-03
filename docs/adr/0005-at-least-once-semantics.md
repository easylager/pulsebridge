# ADR 0005: At-least-once delivery with an idempotent handler

## Status

Accepted

## Context

"Exactly-once" is the guarantee everyone asks for and nobody delivers at the
transport layer. The reason is a single unavoidable window: after a handler
performs its side effect and before the acknowledgement is durable, the process
can die. On restart the message is still pending, so it is delivered again — and
the side effect has already happened.

Making that window smaller does not close it. Acknowledging before processing
converts the problem into lost messages, which is strictly worse.

Meanwhile the worker's original processor had no side effect at all: it computed
a hash and incremented a counter. That made the whole question theoretical, and
a reviewer had no way to judge whether the design would survive a real write.

## Decision

Be explicit that the transport is **at-least-once**, and close the gap in the
handler.

The worker's processor now performs a real, ordered side effect:

1. `Claim` the event id with `SET NX` plus a TTL. Atomic, so exactly one of any
   number of concurrent deliveries wins.
2. If the claim was refused, the event was already applied. Acknowledge and
   return — the work is done, and failing here would loop the message until it
   hit the dead-letter stream for no reason.
3. If the claim succeeded, `Apply` the effect: a transactional pipeline updating
   a per-tenant, per-type counter and a last-seen timestamp.
4. If `Apply` fails, `Release` the claim before returning the error. Without
   this the retry is mistaken for a duplicate, acknowledged, and the event is
   silently dropped — the exact failure the dead-letter path exists to prevent.

The projection is deliberately trivial. The point of this repository is the
delivery semantics around the side effect, not the business logic inside it; what
matters is that claim and write are separate, ordered steps, which is the same
shape a handler has when it writes to Postgres or calls an API.

The dedupe TTL (default 24 h) must comfortably outlive the retry budget. A claim
that expires between attempts lets the same event be applied twice, which quietly
defeats the whole mechanism.

## Alternatives rejected

**Claiming inside the same transaction as the effect.** The genuinely correct
answer when the store supports it: a Postgres handler should insert into a
processed-events table and do its write in one transaction, and then no separate
release is needed. Not available here because the claim lives in Redis and the
effect is a Redis pipeline that cannot span a conditional. Documented as the
preferred pattern for a real downstream.

**Acknowledging before processing.** Turns at-least-once into at-most-once.
Acceptable for metrics or logs; unacceptable for business events.

**Kafka transactions / exactly-once semantics.** Genuinely exactly-once, within
Kafka. Rejected because it only holds for Kafka-to-Kafka flows — the moment a
handler calls an external API, the same window reappears. Redis Streams has no
equivalent primitive regardless.

**No deduplication, relying on naturally idempotent effects.** Works when every
effect happens to be a `PUT` or an upsert. Rejected as a design principle: it is
a property nobody can enforce, and it fails the first time someone adds a
counter increment.

## Consequences

* The guarantee is stated plainly rather than implied, so nobody builds on a
  promise the system does not make.
* Duplicate deliveries are absorbed. `pulsebridge_worker_processed_total{result="duplicate"}`
  makes them visible, and a non-zero value there is healthy — it is the mechanism
  working.
* Every handler added later must be idempotent. This is a real constraint on
  future contributors, and it is why it is written down here rather than only in
  code.
* One extra Redis round trip per event for the claim.
* `Release` is best-effort. If it fails, the event waits out the dedupe TTL and
  the case is counted as `claim_leaked` rather than hidden.
