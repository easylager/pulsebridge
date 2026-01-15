# High-load readiness

This document describes what is already implemented and what you would harden next in production.

## Implemented now

### Gateway (Python / asyncio)

- Non-blocking Redis client (`redis.asyncio`)
- Per-tenant token bucket (atomic Lua) — protects noisy neighbors
- Idempotency keys with TTL — safe client retries
- Approximate stream maxlen trim — bounds memory growth
- Prometheus histograms for ingest latency
- Liveness (`/healthz`) and readiness (`/readyz` with Redis ping)

### Worker (Go)

- Consumer group read (`XREADGROUP`)
- Bounded concurrency semaphore
- Per-event `context` timeout
- Prometheus counters/gauges/histograms
- Graceful shutdown on SIGINT/SIGTERM

### Local bench

```bash
# terminal 1
PULSEBRIDGE_USE_MEMORY=true make gateway

# terminal 2
make bench
```

`scripts/bench_ingest.py` fires concurrent async HTTP POSTs and reports p50/p95/p99.

## Production checklist (next steps)

| Area | Recommendation |
|------|----------------|
| Broker | Prefer Kafka / Redpanda for multi-AZ durability & replay |
| Idempotency | Redis Cluster or DynamoDB with TTL; partition by tenant |
| AuthN/Z | mTLS or JWT at edge; tenant scope claims |
| DLQ | Separate stream + poison threshold |
| Autoscaling | HPA on ingest RPS + consumer lag |
| Tracing | OpenTelemetry across gateway → worker |
| Chaos | Kill Redis primary; verify reconnect + no silent drop |
| Load test | k6/Gatling against staging with production-like payload mix |

## Target shape (illustrative)

```
         ┌────────────┐
  N×API  │  gateway   │──► Redis/Kafka
         └────────────┘
                │
         ┌──────┴──────┐
         ▼             ▼
      worker-a      worker-b   (HPA on lag)
```

PulseBridge intentionally stays small so reviewers can read the whole design in one sitting — while still demonstrating the patterns that survive real peak traffic.
