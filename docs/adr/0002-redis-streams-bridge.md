# ADR 0002: Redis Streams as the transport

## Status

Accepted

## Context

The gateway needs a durable handoff to the workers. Producers must not wait for
processing, processing must survive a gateway restart, and multiple worker
replicas must split the work without coordinating with each other.

There is also a constraint specific to what this repository is for: a reviewer
should be able to run the whole system with one command. A transport that needs
a broker cluster, a schema registry and a control plane before anything happens
fails that test regardless of its technical merits.

## Decision

Redis Streams with consumer groups.

* Gateway: `XADD` with an approximate `MAXLEN` trim.
* Worker: `XREADGROUP` for new entries, `XPENDING` + `XCLAIM` for stale ones,
  `XACK` on success.

Consumer groups give the three properties that matter: entries are distributed
across replicas without coordination, unacknowledged entries are tracked
server-side in the pending list, and delivery counts are available per entry —
which is what makes a durable retry budget possible without the worker keeping
state of its own.

## Alternatives rejected

**Kafka or Redpanda.** The right answer for multi-AZ durability, long replay
windows and partition-ordered consumption at scale. Rejected for this repository
on operational weight: a reviewer would need a broker, a controller and topic
provisioning before seeing a single event move. The gateway's publisher is a
port, so the swap is confined to `infrastructure/`.

**RabbitMQ.** Excellent routing and a mature dead-letter mechanism built in.
Rejected because messages are gone once acknowledged — no replay, no offset to
rewind, and no way to inspect what was consumed. The pending-entries model in
Streams is closer to Kafka's semantics, which is what the design is meant to
demonstrate.

**A Redis list with `BRPOPLPUSH`.** Simpler, and it does give a reliable-queue
pattern. Rejected because it has no consumer-group concept: distributing work
across replicas and tracking per-message delivery counts would both have to be
built by hand, and building them badly is the usual outcome.

**Postgres as a queue (`SELECT ... FOR UPDATE SKIP LOCKED`).** Genuinely good
when a database is already in the stack, and it makes the transactional outbox
trivial. Rejected because there is no database here otherwise, and adding one for
the queue alone inverts the dependency.

## Consequences

* `docker compose up` is the entire setup.
* Consumer-group semantics are close enough to Kafka that the design discussion
  transfers, which is the point.
* Retention is bounded by `MAXLEN` rather than by time or disk. Past that bound
  the oldest entries are trimmed — the deadline documented in
  [`../runbook.md`](../runbook.md#service-down).
* Single-node Redis is a single point of failure. Production needs Sentinel or
  Cluster, and AOF is not optional.
* `MAXLEN ~` trims at radix-node boundaries, so retention is approximate by
  design. Exact trimming would scan the stream on every write.
* This is **not** a claim that Redis Streams replaces Kafka at multi-terabyte
  scale. The production path is in [`../high-load.md`](../high-load.md).
