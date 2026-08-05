# PulseBridge Gateway

Async ingest edge: accepts events over HTTP, enforces per-tenant rate limits and
idempotency, publishes to a Redis Stream.

See the [root README](../../README.md) for the whole system, and
[`docs/architecture.md`](../../docs/architecture.md) for why the steps are
ordered the way they are.

## Layout

```
src/pulsebridge_gateway/
├── domain/          EventEnvelope, ports, errors    — no framework imports
├── application/     IngestEventUseCase              — orchestration only
├── infrastructure/  Redis + in-memory adapters      — all I/O
└── api/             routes, middleware, DI          — thin HTTP shell
```

Dependencies point inwards ([ADR 0001](../../docs/adr/0001-hexagonal-gateway.md)).

## Run

```bash
uv sync --all-groups

# no Redis needed
PULSEBRIDGE_USE_MEMORY=true uv run uvicorn pulsebridge_gateway.api.main:app --port 8080

# against Redis
uv run uvicorn pulsebridge_gateway.api.main:app --port 8080
```

## Test

```bash
uv run pytest -q -m "not integration"   # in-memory adapters, no infrastructure
uv run pytest -q -m integration         # requires a reachable Redis
uv run ruff check . && uv run mypy src
```

The integration suite covers what fakes cannot prove: Lua atomicity in the rate
limiter, `SET NX` under concurrency, and the exact stream field layout the Go
worker parses.

## Configuration

Every setting is environment-driven with the `PULSEBRIDGE_` prefix. See
[`.env.example`](../../.env.example) for the full list with defaults and the
reasoning behind each one.
