# Contributing

Thanks for looking at PulseBridge.

## Local setup

```bash
make install        # Python dependencies
make up             # full stack: Redis, gateway, 2 workers, Prometheus, Grafana, Jaeger
make verify         # everything CI runs, except the compose smoke test
```

Without Docker:

```bash
make gateway        # memory mode, no Redis needed
```

Optional, and recommended:

```bash
pip install pre-commit && pre-commit install
```

## Guidelines

1. **Keep the dependency direction.** `domain/` and `application/` import
   nothing from FastAPI or Redis. The check is mechanical: the ingest use case
   must stay testable with in-memory adapters and no network.

2. **Test the failure, not the success.** The happy path here is a handful of
   lines; the design is what happens when a step fails. A change to delivery
   semantics needs a test that exercises the failure it is about.

3. **Handlers must be idempotent.** Delivery is at-least-once
   ([ADR 0005](docs/adr/0005-at-least-once-semantics.md)). Any new side effect
   claims its event id before applying, and releases the claim if the effect
   fails.

4. **Write an ADR for architectural changes**, including the alternatives you
   rejected and why. That section is the one people read.

5. **Regenerate the OpenAPI spec** when the API changes: `make openapi`. CI
   fails on drift, and the pre-commit hook catches it earlier.

6. **Update the numbers, do not estimate them.** If a change affects
   performance, re-run the load profile and update
   [`docs/benchmarks.md`](docs/benchmarks.md) with the method alongside the
   result.

## Commit style

```
feat(worker): reclaim stale pending entries
fix(gateway): release idempotency claim when publish fails
docs(adr): record dead-letter and retry decision
perf(gateway): use a blocking Redis pool under overload
test(worker): cover drain-on-shutdown
```
