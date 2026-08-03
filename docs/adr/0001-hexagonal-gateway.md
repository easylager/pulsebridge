# ADR 0001: Hexagonal layering in the gateway

## Status

Accepted

## Context

The ingest rules — validate, rate limit, claim, publish, and what to do when each
step fails — are the part of this service worth reasoning about. In a
conventional FastAPI layout they end up inside route handlers, entangled with
`Request` objects, dependency injection and Redis clients, and the only way to
test them becomes an HTTP call against a running Redis.

That is not a testing inconvenience so much as a design one: rules that can only
be exercised through two layers of infrastructure are rules nobody refactors.

## Decision

Four layers, with dependencies pointing inwards:

```
domain/          EventEnvelope, ports, errors    — no framework imports at all
application/     IngestEventUseCase              — orchestration only
infrastructure/  Redis + in-memory adapters      — all I/O
api/             routes, middleware, DI          — thin HTTP shell
```

Ports are `typing.Protocol` rather than abstract base classes, so adapters are
structurally typed and never import the domain to declare conformance. Each port
has a Redis implementation and an in-memory one.

The test for whether the boundary is real is simple and mechanical: the entire
ingest use case runs against in-memory adapters with no Docker and no network. It
does.

## Alternatives rejected

**Fat route handlers.** Fewer files, and for a CRUD endpoint the right answer.
Rejected because the interesting logic here is failure ordering — release the
claim when the publish fails, check the rate limit before claiming — and that
logic deserves tests that do not go through HTTP.

**A service layer without ports.** Extract the logic into a class that takes a
Redis client. Most of the benefit for less ceremony. Rejected because the tests
then need a Redis client, real or mocked, and mocking a Redis client means
asserting on commands rather than on behaviour.

**Abstract base classes instead of Protocols.** Explicit conformance, and the
error arrives at import time rather than at type-check time. Rejected because it
forces adapters to import the domain, which is the dependency direction this
layout exists to avoid.

**A full DI framework.** Rejected as disproportionate: the container is a
dataclass built once at startup, and the only thing that needs overriding is the
container itself in tests.

## Consequences

* The unit suite runs in under a second with no infrastructure.
* Redis-specific behaviour that fakes cannot prove — Lua atomicity, `SET NX`
  under concurrency, the exact stream field layout the Go worker parses — gets a
  separate integration suite that is skipped when no Redis is reachable.
* Replacing Redis Streams with Kafka touches `infrastructure/` and nothing else.
* More files than a fat-router version. The cost is real and it is accepted for
  a service whose value is in its failure handling.
* The in-memory adapters must stay behaviourally faithful. Where they cannot be —
  the memory rate limiter is a sliding window, not a token bucket — the
  difference is covered by the integration suite rather than assumed away.
