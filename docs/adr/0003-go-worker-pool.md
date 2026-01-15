# ADR 0003: Go worker with bounded goroutine pool

## Status

Accepted

## Context

Ingest can spike. Unbounded goroutines risk memory blowups; single-threaded processing wastes cores.

## Decision

Worker uses a semaphore-bounded pool (`concurrency` env) and per-event timeouts.

## Consequences

- Predictable max in-flight work.
- Easy to tune via `PULSEBRIDGE_CONCURRENCY`.
- Failed process does not ACK → message can be retried (at-least-once).
