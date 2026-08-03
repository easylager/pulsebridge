.DEFAULT_GOAL := help
.PHONY: help install test test-go test-py test-integration lint lint-go lint-py fmt \
        gateway worker up down logs bench load openapi verify

GATEWAY := services/gateway
WORKER  := services/worker
COMPOSE_NET := pulsebridge_default

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies
	cd $(GATEWAY) && uv sync --all-groups

# --- tests -------------------------------------------------------------------

test: test-go test-py ## Run all unit tests

test-py: ## Python unit tests (no infrastructure needed)
	cd $(GATEWAY) && uv run pytest -q -m "not integration"

test-go: ## Go tests with the race detector
	cd $(WORKER) && go test -race -count=1 ./...

test-integration: ## Python tests against a real Redis (starts one if needed)
	@docker compose up -d redis
	cd $(GATEWAY) && PULSEBRIDGE_TEST_REDIS_URL=redis://localhost:6379/15 \
		uv run pytest -q -m integration

cover: ## Coverage for both services
	cd $(GATEWAY) && uv run pytest -q -m "not integration" --cov --cov-report=term-missing
	cd $(WORKER) && go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

# --- quality -----------------------------------------------------------------

lint: lint-py lint-go ## Lint and type-check everything

lint-py: ## ruff + mypy --strict
	cd $(GATEWAY) && uv run ruff check . && uv run ruff format --check . && uv run mypy src

lint-go: ## gofmt + go vet
	@cd $(WORKER) && test -z "$$(gofmt -l .)" || { echo "unformatted:"; gofmt -l $(WORKER); exit 1; }
	cd $(WORKER) && go vet ./...

fmt: ## Format both codebases
	cd $(GATEWAY) && uv run ruff format . && uv run ruff check --fix .
	cd $(WORKER) && gofmt -w .

verify: lint test openapi-check ## Everything CI runs, except the compose smoke test
	@echo "OK"

# --- contracts ---------------------------------------------------------------

openapi: ## Regenerate contracts/openapi.json
	uv run --project $(GATEWAY) python scripts/export_openapi.py

openapi-check: ## Fail if the committed OpenAPI spec is stale
	uv run --project $(GATEWAY) python scripts/export_openapi.py --check

# --- running -----------------------------------------------------------------

gateway: ## Run the gateway locally in memory mode
	cd $(GATEWAY) && PULSEBRIDGE_USE_MEMORY=true uv run uvicorn \
		pulsebridge_gateway.api.main:app --host 0.0.0.0 --port 8080 --reload

worker: ## Run the worker locally (needs Redis)
	cd $(WORKER) && go run ./cmd/worker

up: ## Start the full stack and wait for it to be healthy
	docker compose up -d --build --wait

down: ## Stop the stack and remove volumes
	docker compose down -v

logs: ## Tail service logs
	docker compose logs -f gateway worker-1 worker-2

# --- load --------------------------------------------------------------------

bench: ## Quick dependency-free smoke benchmark (see docs/benchmarks.md for caveats)
	uv run --project $(GATEWAY) python scripts/bench_ingest.py

load: ## k6 load profile against the running stack
	docker run --rm -i --network $(COMPOSE_NET) \
		-v "$(PWD)/scripts/load:/scripts:ro" \
		-e BASE_URL=http://gateway:8080 \
		-e RATE=$${RATE:-400} -e DURATION=$${DURATION:-30s} -e TENANTS=$${TENANTS:-20} \
		grafana/k6 run /scripts/ingest.js
