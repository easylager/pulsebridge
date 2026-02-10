# Contributing

Thanks for looking at PulseBridge.

## Local setup

```bash
# Gateway
cd services/gateway && uv sync
PULSEBRIDGE_USE_MEMORY=true uv run uvicorn pulsebridge_gateway.api.main:app --port 8080

# Worker (requires Go + Redis)
docker compose up -d redis
cd services/worker && go run ./cmd/worker
```

## Guidelines

1. Keep domain/application free of FastAPI/Redis imports.
2. Prefer small PRs with tests.
3. Update ADRs when changing architectural decisions.
4. Run `make test` and `make lint` before opening a PR.

## Commit style

```
feat(gateway): add readiness probe redis ping
fix(worker): ack only after successful process
docs: clarify backpressure strategy
```
