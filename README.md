# PulseBridge

**High-throughput async event bridge** — a public reference implementation of production backend patterns.

Python **asyncio + FastAPI** gateway ingests events; a **Go** worker pool consumes them from **Redis Streams** with backpressure, idempotency, and Prometheus metrics.

> Built as an open engineering showcase: readable architecture, real async (not fake `time.sleep` demos), high-load readiness notes, and a commit history you can audit.

![python](https://img.shields.io/badge/python-3.11%2B%20asyncio-blue)
![go](https://img.shields.io/badge/go-1.22-00ADD8)
![fastapi](https://img.shields.io/badge/FastAPI-async-009688)
![license](https://img.shields.io/badge/license-MIT-green)

---

## Why this repo exists

Hiring teams often ask for a public repo that shows:

- clean architecture / hexagonal boundaries
- real async I/O
- multi-language systems thinking (Python + Go)
- idempotency, rate limiting, consumer groups
- observability and load-test hooks
- documentation that a staff engineer would actually read

PulseBridge is that artifact — compact enough to review in one sitting, serious enough to discuss in a system-design interview.

---

## Architecture (30 seconds)

```
HTTP clients
    │
    ▼
┌─────────────────────────┐
│  Gateway (Python)       │  FastAPI · asyncio · token bucket · idempotency
│  POST /v1/events        │
└───────────┬─────────────┘
            │ XADD
            ▼
      Redis Streams
            │ XREADGROUP
            ▼
┌─────────────────────────┐
│  Worker (Go)            │  bounded goroutine pool · timeouts · ACK
└─────────────────────────┘
```

Details: [`docs/architecture.md`](docs/architecture.md) · High-load notes: [`docs/high-load.md`](docs/high-load.md) · ADRs: [`docs/adr/`](docs/adr/)

---

## Quick start

### Option A — memory mode (gateway only, no Docker)

```bash
cd services/gateway
uv sync
PULSEBRIDGE_USE_MEMORY=true uv run uvicorn pulsebridge_gateway.api.main:app --port 8080
```

```bash
curl -s localhost:8080/v1/events \
  -H 'content-type: application/json' \
  -d '{"tenant_id":"acme","event_type":"order.created","payload":{"id":"1"},"event_id":"evt-1"}'
```

### Option B — full stack

```bash
docker compose up --build
# gateway :8080  · worker metrics :9100  · prometheus :9090
```

### Tests & bench

```bash
make test
make bench   # against a running gateway
```

---

## Patterns demonstrated

| Pattern | Where |
|---------|--------|
| Hexagonal / Clean Architecture | `services/gateway/src/pulsebridge_gateway/` |
| Dependency inversion via Protocols | `domain/ports.py` |
| Idempotent ingest (`SET NX`) | Redis / memory adapters |
| Per-tenant rate limiting | Lua token bucket |
| Consumer groups + ACK | Go worker |
| Bounded concurrency | Go semaphore pool |
| Structured logging | `structlog` JSON |
| Metrics | Prometheus on gateway & worker |
| Contract-first envelope | `contracts/events.schema.json` |

---

## Project layout

```
pulsebridge/
├── services/
│   ├── gateway/          # Python asyncio FastAPI
│   └── worker/           # Go consumer
├── contracts/            # JSON Schema
├── docs/                 # architecture + ADRs
├── scripts/              # async bench
├── deploy/               # prometheus scrape config
└── docker-compose.yml
```

---

## API

`POST /v1/events`

```json
{
  "tenant_id": "acme",
  "event_type": "order.created",
  "payload": { "order_id": "o-1" },
  "event_id": "optional-client-id",
  "correlation_id": "optional-trace"
}
```

Responses: `202 accepted` · `200 duplicate` · `429 rate_limited`

Also: `GET /healthz` · `GET /readyz` · `GET /metrics`

---

## License

MIT — see [LICENSE](LICENSE).

---

### Author

Maksim Vasilenko — backend engineer (Python · Go · high-stakes integrations).  
Public showcase repository for architecture & async systems practice.
