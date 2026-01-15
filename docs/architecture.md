# Architecture

PulseBridge is a **reference implementation** of a high-fan-in async event bridge:

```
 clients ──HTTP──► Python Gateway (asyncio / FastAPI)
                         │
                         │ Redis Streams (XADD)
                         ▼
                   Go Worker pool
                   (XREADGROUP + goroutines)
                         │
                         ▼
                   process + ACK
```

## Design goals

1. **Real async on the edge** — Python gateway never blocks the event loop on Redis I/O.
2. **High concurrency in the worker** — Go consumes with a bounded worker pool and per-event timeouts.
3. **Safety under retries** — idempotency keys at ingest; consumer group ACKs after successful process.
4. **Backpressure** — token-bucket rate limit per tenant; stream maxlen approx trim; worker concurrency cap.
5. **Observability first** — structured JSON logs + Prometheus metrics on both services.
6. **Testable core** — domain/use-cases depend on ports; in-memory adapters for unit tests.

## Layers (Gateway)

| Layer | Responsibility |
|-------|----------------|
| `domain/` | Entities, validation, ports (protocols) |
| `application/` | Use-cases (`IngestEventUseCase`) |
| `infrastructure/` | Redis / memory adapters, settings, logging |
| `api/` | FastAPI routes, schemas, DI, metrics |

Dependency rule: **inward only**. Domain has zero framework imports.

## Why Python + Go

| Concern | Choice | Why |
|---------|--------|-----|
| Ingest API, validation, tenancy | Python + FastAPI + asyncio | Fast iteration, excellent async HTTP ergonomics, clear DI |
| Fan-out processing | Go | Cheap goroutines, predictable concurrency under load |
| Transport | Redis Streams | Simple durable log + consumer groups without full Kafka ops cost for demos |

## Failure modes considered

- Duplicate client retries → idempotency store (`SET NX`)
- Hot tenant abuse → token bucket (Lua, atomic)
- Slow handlers → context timeout in worker
- Poison messages → log + ACK (demo policy; production would dead-letter)
- Redis blips → worker retries read loop with backoff

See also: [high-load.md](./high-load.md), ADRs in `docs/adr/`.
