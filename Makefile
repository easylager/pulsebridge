.PHONY: help install test lint gateway worker up down bench fmt

help:
	@echo "PulseBridge targets:"
	@echo "  make install   - install Python deps (uv)"
	@echo "  make test      - run gateway unit tests"
	@echo "  make lint      - ruff + mypy (gateway)"
	@echo "  make gateway   - run FastAPI gateway locally"
	@echo "  make worker    - run Go worker locally"
	@echo "  make up        - docker compose up"
	@echo "  make down      - docker compose down"
	@echo "  make bench     - async ingest micro-benchmark"

install:
	cd services/gateway && uv sync

test:
	cd services/gateway && uv run pytest -q

lint:
	cd services/gateway && uv run ruff check src tests && uv run mypy src

gateway:
	cd services/gateway && uv run uvicorn pulsebridge_gateway.api.main:app --host 0.0.0.0 --port 8080 --reload

worker:
	cd services/worker && go run ./cmd/worker

up:
	docker compose up --build -d

down:
	docker compose down -v

bench:
	cd services/gateway && uv run python ../../scripts/bench_ingest.py

fmt:
	cd services/gateway && uv run ruff format src tests
	@command -v gofmt >/dev/null && cd services/worker && gofmt -w . || true
