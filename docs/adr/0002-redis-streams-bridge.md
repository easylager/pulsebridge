# ADR 0002: Redis Streams as the demo transport

## Status

Accepted

## Context

We need a durable handoff between Python ingest and Go workers without standing up Kafka for local demos.

## Decision

Use Redis Streams + consumer groups:

- Gateway: `XADD`
- Worker: `XREADGROUP` + `XACK`

## Consequences

- Excellent local DX (`docker compose up`).
- Consumer group semantics mirror Kafka enough for teaching.
- Not a claim that Redis Streams replaces Kafka at multi-TB scale — see `docs/high-load.md` for the production path.
