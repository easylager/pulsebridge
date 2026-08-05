# ADR 0007: W3C trace context carried on the stream entry

## Status

Accepted

## Context

The gateway is the only process that sees the HTTP request. The worker is the
only process that sees the side effect. A stream sits between them, and a stream
breaks causality: given a slow event, there is no way to get from the worker's
log line back to the request that produced it.

A `correlation_id` was already carried end to end, and it helps — if you know
which id to grep for. It does not answer the questions that actually come up:
which tenant's events are slow, how long an event waited between publish and
consumption, whether the latency is in the handler or in the queue.

Metrics do not answer these either. They aggregate, and the interesting cases are
the ones aggregation hides.

## Decision

Propagate W3C trace context through the stream entry.

The gateway serialises the active span into a `traceparent` field on the `XADD`,
alongside the business fields. The worker extracts it, builds a remote parent
context, and starts a consumer span that continues the same trace. One trace then
spans the HTTP request, the Redis write and the Go handler — across two
languages.

Design constraints that shaped the implementation:

**Tracing is optional.** With no `OTEL_EXPORTER_OTLP_ENDPOINT` set, both services
install the propagator and nothing else, leaving the no-op tracer in place. The
repository has to stay runnable with a bare `uvicorn` and `go run`, and CI has to
pass without anyone standing up a collector.

**`traceparent` is transport metadata, not part of the contract.** It is
deliberately absent from `contracts/events.schema.json`. Consumers must not
depend on it, and a producer without tracing must remain valid — the worker
handles an empty value by starting an unlinked span rather than failing.

**Health and metrics endpoints are excluded.** They are scraped every few
seconds; tracing them buries real traffic and inflates the collector bill for no
information.

**Resource attributes are schemaless.** Merging a pinned `semconv` version with
the SDK's default resource fails outright on a schema-URL mismatch — which is
exactly what happened first: the worker logged
`conflicting Schema URL: 1.41.0 and 1.26.0` and ran with tracing silently
disabled. `resource.NewSchemaless` avoids the merge conflict and survives an SDK
upgrade. That the worker kept consuming through the failure was the intended
behaviour; that it was easy to miss is why it is recorded here.

## Alternatives rejected

**Correlation id only.** Already present, zero dependencies, and enough to
reconstruct a single request by grepping two log streams. Rejected because it
carries no timing and no parent-child structure, so it cannot show where the time
went — the main thing anyone asks a trace.

**A separate Redis key holding trace context, keyed by event id.** Keeps the
stream entry clean. Rejected: an extra round trip on both sides, plus a second
thing to expire, in exchange for nothing the field does not already do.

**OpenTelemetry logs and metrics as well as traces.** The natural next step.
Deferred because Prometheus already covers metrics well and the OTel logging SDK
is less settled; adding it now would trade a working setup for a fashionable one.

**Auto-instrumenting the Redis client.** Would produce spans for every Redis
command. Rejected as noise: three spans per ingest that all say "Redis was fast"
obscure the one span that matters.

## Consequences

* A single Jaeger trace shows the full path from HTTP request to Go handler,
  verified on the running stack and recorded in [`../benchmarks.md`](../benchmarks.md).
* Queue time becomes visible as the gap between the producer span ending and the
  consumer span starting — the number that decides whether to scale the gateway
  or the workers.
* Four OTel dependencies on each side. The Python SDK imports are deferred behind
  the endpoint check so a deployment without tracing does not pay to load the
  exporter stack.
* Every future publisher must inject the header, or its events lose trace
  continuity. It happens in `RedisStreamPublisher`, so there is one place to get
  it right.
* No sampling strategy yet. The compose stack traces everything, which is fine
  at demo volume and would not be at production volume.
