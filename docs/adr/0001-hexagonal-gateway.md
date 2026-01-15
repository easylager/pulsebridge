# ADR 0001: Hexagonal / Clean Architecture for the gateway

## Status

Accepted

## Context

The ingest edge must stay testable without Redis and must not leak FastAPI types into business rules.

## Decision

Split the gateway into `domain` → `application` → `infrastructure` → `api`.

- Domain defines `EventEnvelope` and ports (`EventPublisher`, `IdempotencyStore`, `RateLimiter`).
- Application orchestrates ingest.
- Infrastructure provides Redis and in-memory adapters.
- API is a thin HTTP adapter.

## Consequences

- Unit tests run with in-memory fakes (no Docker required).
- Swapping Redis Streams for Kafka later only touches infrastructure.
- Slightly more files than a "fat router" style — acceptable for clarity.
