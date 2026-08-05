# PulseBridge Worker

Go consumer for the `pulsebridge.events` stream. At-least-once delivery with
reclaim of stale entries, a bounded retry budget, and a dead-letter path.

See the [root README](../../README.md) for the whole system.

## Layout

```
cmd/worker/          entry point, signal handling, metrics server
internal/
├── consumer/        read, reclaim and sampling loops over one bounded pool
├── processor/       idempotent handler + Redis-backed projection
├── dlq/             quarantine for events that must not be retried again
├── domain/          the worker's view of the wire envelope
├── telemetry/       OpenTelemetry setup and traceparent extraction
├── metrics/         Prometheus collectors
└── config/          environment-driven configuration
```

## The three loops

| Loop | Command | Purpose |
|---|---|---|
| read | `XREADGROUP ">"` | Entries never delivered to this group |
| reclaim | `XPENDING` + `XCLAIM` | Entries a dead or wedged consumer never acknowledged |
| sample | `XLEN` + `XPENDING` | Backlog depth and age, exported as gauges |

The reclaim loop is what makes the delivery guarantee hold under consumer death
rather than only under handler failure — see
[ADR 0004](../../docs/adr/0004-dead-letter-and-retries.md).

## Run

```bash
docker compose up -d redis   # from the repository root
go run ./cmd/worker
```

## Test

```bash
go test -race ./...
go vet ./...
```

Tests run against [miniredis](https://github.com/alicebob/miniredis), so the
reclaim, retry-budget and dead-letter paths are covered in CI without a
container. The race detector is not optional here: all three loops share one
worker pool.

## Endpoints

| | |
|---|---|
| `:9100/metrics` | Prometheus exposition |
| `:9100/healthz` | Liveness |
| `:9100/readyz` | Readiness; returns 503 while draining on SIGTERM |

## Configuration

Environment-driven with the `PULSEBRIDGE_` prefix. See
[`.env.example`](../../.env.example).

One constraint is enforced in code rather than trusted from configuration:
`PULSEBRIDGE_RECLAIM_MIN_IDLE` is raised to at least twice
`PULSEBRIDGE_PROCESS_TIMEOUT`. A shorter window means consumers reclaim work that
is still legitimately in flight, and every slow event is processed twice.
