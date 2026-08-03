# Benchmarks

Every number here was measured on the commit that introduced this file, with the
commands shown. Nothing is extrapolated, and the limits are reported as
prominently as the throughput.

---

## What was measured, and what it is not

This is a **capacity curve on one laptop**, not a production benchmark. It is
useful for two things: knowing where the knee is, and catching a regression.
It is not a claim about what the system does on real hardware behind a real load
balancer.

Three caveats that matter for reading the tables:

1. **The load generator shares the machine.** k6, Redis, both workers and the
   gateway all compete for the same 10 cores. Real deployments do not have their
   client on the same CPU.
2. **Redis is a single unreplicated node** with AOF enabled, in a container.
3. **The worker's side effect is small** — a `SET NX` claim plus a hash update.
   A handler that calls Postgres or an external API is the dominant cost in any
   real system, and none of these numbers survive that change.

### Why the Python benchmark script is not the source of these numbers

`scripts/bench_ingest.py` was the original benchmark. It reports roughly 350
requests per second, and that figure is meaningless: a control run against a
**bare FastAPI app with a single `return {"status": "ok"}` handler** produced the
same 366–530 rps from the same client. The asyncio client saturates its own
event loop before the gateway is anywhere near its limit, so the script was
measuring httpx.

The script is kept as a dependency-free smoke test. The measurements below come
from `scripts/load/ingest.js` under k6, which drives load from a Go runtime with
independent VUs.

---

## Environment

| | |
|---|---|
| Host | Apple M1 Max, 10 cores, 32 GB, macOS 26.6 |
| Docker | Docker Desktop, Linux VM with 10 CPUs / 8 GB |
| Stack | `docker compose up` — Redis 7 (AOF on), gateway, 2 worker replicas, Prometheus, Grafana, Jaeger |
| Load generator | k6 v2.1.0, `constant-arrival-rate`, in the same Docker network |
| Gateway | uvicorn, rate limit raised to 100 000 rps so the limiter is not what is measured |
| Profile | 30 s steady state, 20 tenants, unique event id per request |

---

## Ingest capacity — single gateway process

```bash
docker compose up -d
docker run --rm -i --network pulsebridge_default -v "$PWD/scripts/load:/scripts:ro" \
  -e BASE_URL=http://gateway:8080 -e RATE=500 -e DURATION=30s -e TENANTS=20 \
  grafana/k6 run --quiet /scripts/ingest.js
```

Latencies are for accepted (`202`) responses, measured client-side, in
milliseconds.

| Offered rps | Achieved rps | 5xx | Dropped | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100.0 | 0 | 0 | 2.5 | 4.2 | 10.6 |
| 250 | 250.0 | 0 | 0 | 2.0 | 8.9 | 32.4 |
| 400 | 400.0 | 0 | 0 | 2.0 | 14.3 | 36.7 |
| 500 | 500.0 | 0 | 0 | 2.6 | 26.9 | 61.8 |
| 600 | 599.9 | 0 | 0 | 8.4 | 74.4 | 209.5 |
| 800 | 532.6 | 0 | 6 645 | 1 565.9 | 14 154.0 | 17 442.2 |

**The knee is at roughly 500 rps per gateway process.** Up to 400 rps the p99
stays inside the 50 ms objective in [`slo.md`](slo.md). At 600 rps the service
still keeps up but queues, and the tail is already four times the objective. At
800 rps it is past capacity: throughput falls back to ~530 rps, k6 drops 6 645
iterations it could not start, and latency is measured in seconds.

That collapse shape is the expected one. The gateway does not fall over — it
degrades into queueing, which is why the arrival-rate executor reports dropped
iterations rather than errors.

### Where the limit comes from

One gateway container runs one uvicorn process, so ingest is bounded by a single
Python event loop. Per request the path is three Redis round trips: the Lua
token bucket, the `SET NX` idempotency claim, and the `XADD`. At 500 rps that is
1 500 Redis operations per second from one process, on top of JSON parsing and
Pydantic validation.

---

## Ingest capacity — four gateway processes

Same container, `--workers 4`, everything else unchanged:

| Offered rps | Achieved rps | 5xx | Dropped | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 800 | 799.9 | 0 | 0 | 2.6 | 13.1 | 30.7 |
| 1 200 | 1 199.9 | 0 | 0 | 4.0 | 24.7 | 70.9 |
| 1 600 | 1 592.6 | 0 | 206 | 8.5 | 41.9 | 126.6 |
| 2 000 | 1 984.4 | 0 | 403 | 25.2 | 88.5 | 179.7 |

Four processes sustain **800 rps at a p99 of 30.7 ms**, against ~500 rps for one.
That is about 2.4x from 4x the processes — sub-linear, and the reason is visible
in the setup: Redis, both workers and k6 itself are on the same 10 cores, so the
gateway is not the only thing competing for CPU.

Scaling by process is the right first move because the bottleneck is a single
event loop rather than a shared lock. Beyond this machine the same argument
applies to replicas: the ingest path holds no local state, and both the rate
limiter and the idempotency store are atomic in Redis, so replicas do not need
to coordinate.

---

## A measured fix: connection-pool behaviour under overload

The first 800 rps run returned **72 responses with HTTP 503**, logged as
`Too many connections` from redis-py.

The cause was the default `ConnectionPool`, which raises the moment the pool is
exhausted. Under a burst that meant rejecting requests a few milliseconds before
a connection would have freed up. The fix was `BlockingConnectionPool` with a
bounded `timeout`: a request waits briefly for a connection, and only fails if
the wait itself exceeds the budget.

After the change, **every run in both tables above completed with zero 5xx**,
including the 800 rps overload run. The service still degrades past capacity —
it just degrades as latency and dropped client-side iterations rather than as
errors returned to callers who could have been served.

The bound is still a bound. `PULSEBRIDGE_REDIS_MAX_CONNECTIONS` and
`PULSEBRIDGE_REDIS_POOL_TIMEOUT_SECONDS` cap both the connection count and the
wait, so overload surfaces as a fast failure instead of an unbounded queue.

---

## Consumption

Measured over the same runs, from the worker `/metrics` endpoints.

| | |
|---|---|
| Events processed | 258 510 |
| Split across replicas | 129 089 / 129 421 (50.1% / 49.9%) |
| Mean handler latency | 0.379 ms |
| Under 0.5 ms | 86.9% |
| Under 1 ms | 96.4% |
| Under 5 ms | 99.5% |
| Backlog at the end of each run | 0 pending entries |

The near-perfect split between the two replicas is the point of the consumer
group: neither was configured with a share, and neither coordinated with the
other. Redis handed each of them work as they asked for it.

Consumption was never the constraint in any run. The workers kept the pending
list empty even during the 800 rps overload, because the gateway could not
produce faster than the two Go replicas could consume.

---

## Reliability behaviour, verified on the running stack

Not throughput, but measured the same way — against the real compose stack
rather than in a unit test.

Two malformed entries were written straight to the stream, bypassing the
gateway's validation:

```bash
docker compose exec redis redis-cli XADD pulsebridge.events '*' \
  tenant_id acme event_type order.created payload '{"broken"'
```

Result, within one poll cycle:

* both entries appeared in `pulsebridge.events.dlq` with `dlq_reason=undecodable`,
  `dlq_error=missing required field: event_id`, the original stream id, and the
  original field values preserved for replay;
* `XPENDING` on the source stream returned **0** — the poison messages were
  quarantined rather than left to be retried forever;
* `pulsebridge_worker_dead_lettered_total{reason="undecodable"}` incremented, and
  `pulsebridge_worker_dead_letter_failures_total` stayed at 0.

Distributed tracing was verified the same way: a single trace in Jaeger contains
the gateway's `POST /v1/events` span and the worker's `pulsebridge.events process`
span, linked through the `traceparent` field carried on the stream entry.

### Killing a worker mid-load

The claim that matters most is the one that unit tests cannot make: that a worker
dying with work in its hands loses nothing. It was tested by `SIGKILL` — not
`SIGTERM`, because a graceful drain proves nothing about crash recovery.

```bash
# 600 rps in the background, then kill one replica outright six seconds in
docker compose kill -s SIGKILL worker-1
```

Timeline, read from `XPENDING` and the surviving replica's metrics:

| | |
|---|---|
| Entries stranded in the pending list, owned by the dead consumer | 1, delivery count 1 |
| Reclaimed by `worker-2` | after ~25 s (`RECLAIM_MIN_IDLE=30s`, polled every 10 s) |
| `pulsebridge_worker_reclaimed_total` on the survivor | 0 → 1 |
| Pending entries once the load stopped | 0 |

Accounting across the whole session, after restarting the killed replica:

| | |
|---:|---|
| 27 004 | entries written to the stream |
| 27 003 | applied by the handler |
| 1 | dead-lettered (the deliberately malformed entry) |
| 0 | still pending |

`27 003 + 1 = 27 004`. Nothing was lost, and — because the applied figure is a
**sum of per-aggregate counters** rather than a count of deliveries — nothing was
double-counted either. An event applied twice would have pushed that sum above
the stream length. It did not, which is the idempotent handler from
[ADR 0005](adr/0005-at-least-once-semantics.md) doing its job on a redelivery
that actually happened.

The cost of the guarantee is visible in the same table: recovery took ~25 s, not
milliseconds. That is `RECLAIM_MIN_IDLE` deliberately set above the process
timeout, because reclaiming sooner would mean stealing work from replicas that
are merely slow. The trade-off is discussed in
[ADR 0004](adr/0004-dead-letter-and-retries.md).

---

## Reproducing

```bash
docker compose up -d --build

# capacity ladder
for RATE in 100 250 400 500 600 800; do
  docker run --rm -i --network pulsebridge_default \
    -v "$PWD/scripts/load:/scripts:ro" \
    -e BASE_URL=http://gateway:8080 -e RATE=$RATE -e DURATION=30s -e TENANTS=20 \
    grafana/k6 run --quiet /scripts/ingest.js
done
```

The k6 script emits one JSON summary line per run, so a ladder can be piped
straight into a table instead of copied by hand.

Note that the compose default is `PULSEBRIDGE_RATE_LIMIT_RPS=200` per tenant.
Leave it there and you are measuring the rate limiter; the runs above raised it
so the ingest path itself was the subject.
