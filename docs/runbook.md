# Runbook

One section per alert in [`../deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml).
Each section answers the same three questions in the same order: what is
actually broken for someone, how to confirm it in under a minute, and what to do.

Commands assume the compose stack. On Kubernetes, substitute `kubectl exec` for
`docker compose exec` and the pattern holds.

---

## First 60 seconds, regardless of the alert

```bash
# Are callers being served?
curl -s localhost:8080/readyz

# Is the backlog draining or stuck?
docker compose exec redis redis-cli XPENDING pulsebridge.events pulsebridge-workers

# Is anything being quarantined?
docker compose exec redis redis-cli XLEN pulsebridge.events.dlq
```

Three numbers separate almost every incident here: readiness tells you whether
ingest is up, the pending summary tells you whether consumption is moving, and
the DLQ length tells you whether events are being discarded from the happy path.

The Grafana board at `localhost:3000` ("PulseBridge — ingest & consumption") is
laid out in the same order.

---

<a id="ingest-returning-503"></a>

## `IngestErrorBudgetBurn` — ingest returning 503
**Impact:** producers are being told their events were not accepted. They will
retry, so the load does not go away — it compounds.

**Confirm:**

```bash
docker compose logs gateway --since 5m | grep ingest_failed | tail -20
```

The `error` field names the cause directly.

**Common causes, in the order they actually occur:**

| `error` contains | Meaning | Action |
|---|---|---|
| `Connection refused` / `Timeout` | Redis is down or unreachable | Check Redis first; the gateway recovers on its own once it is back |
| `Too many connections` | Connection pool exhausted | See below |
| `Read timed out` | Redis is alive but slow | Check Redis CPU and `slowlog`; look for a large `MAXLEN` trim or an expensive neighbour on the same instance |

**Pool exhaustion** means concurrent requests exceeded
`PULSEBRIDGE_REDIS_MAX_CONNECTIONS` for longer than
`PULSEBRIDGE_REDIS_POOL_TIMEOUT_SECONDS`. Raising the pool is the wrong first
move: each connection is another concurrent command against the same Redis, so a
larger pool converts a fast failure into a slow one. Add gateway replicas
instead. Raise the pool only after confirming Redis has headroom.

```bash
docker compose exec redis redis-cli INFO clients | grep connected_clients
docker compose exec redis redis-cli --latency
```

**Do not** silence this by returning 202 on publish failure. The 503 is the only
honest answer when an event was not written.

---

<a id="ingest-latency-regression"></a>

## `IngestLatencyHigh` — latency regression
**Impact:** callers are slower but still served. Nothing is lost yet; this is the
early warning before the knee.

**Confirm which of the three Redis round trips is slow:**

```bash
docker compose exec redis redis-cli --latency-history
docker compose exec redis redis-cli SLOWLOG GET 10
```

**Then check whether it is load or a regression.** Compare current throughput
against the capacity curve in [`benchmarks.md`](benchmarks.md). A single gateway
process holds the 50 ms p99 objective to roughly 400 rps and breaks it at 600.

* Approaching the knee → scale out. More gateway replicas, or more uvicorn
  workers per container.
* Well below the knee and still slow → Redis is the suspect, not the gateway.
* Neither → look at what changed. A new middleware or an added Redis call on the
  ingest path shows up here before it shows up anywhere else.

---

<a id="backlog-not-draining"></a>

## `ConsumerBacklogStuck` / `ConsumerBacklogGrowing`
**Impact:** accepted events are not being applied. Nothing is lost — they are
durable in the stream — but downstream state is stale and getting staler.

**Distinguish the two cases first, because the responses are opposite:**

```bash
# depth
docker compose exec redis redis-cli XPENDING pulsebridge.events pulsebridge-workers

# age and ownership — who is holding what, and for how long
docker compose exec redis redis-cli XPENDING pulsebridge.events pulsebridge-workers - + 10
```

**Deep but young → the workers are behind.** Production outpaces consumption.

```bash
docker compose up -d --scale worker-1=3
```

Or raise `PULSEBRIDGE_CONCURRENCY`. Prefer more replicas: concurrency inside one
process is bounded by that process, while replicas also survive one of them dying.

**Old, regardless of depth → something is stuck.** Read the consumer column of
the extended `XPENDING` output. If the messages belong to a consumer name that no
longer exists, a replica died and the reclaim loop should be recovering them
after `PULSEBRIDGE_RECLAIM_MIN_IDLE` (default 30 s).

If age keeps climbing past that, reclaim itself is failing:

```bash
docker compose logs worker-1 --since 10m | grep -E "reclaim failed|xclaim|xpending"
```

**Stuck on a live consumer** means a wedged handler — a downstream call with no
timeout, most likely. `PULSEBRIDGE_PROCESS_TIMEOUT` bounds each attempt, so a
handler stuck for longer than that is holding a resource the timeout does not
cover. Restart the replica; its messages will be reclaimed by the survivors.

---

<a id="dead-letter-spike"></a>

## `DeadLetterRateElevated` — events being quarantined
**Impact:** events are being taken out of the happy path. They are not lost, but
nothing downstream will see them until someone acts.

**Read the reason first — it determines everything that follows:**

```bash
docker compose exec redis redis-cli XREVRANGE pulsebridge.events.dlq + - COUNT 5
```

**`dlq_reason=undecodable`** — the envelope was malformed. Retrying cannot help,
so these are quarantined on the first attempt. A sudden spike almost always means
a producer deployed a change. Check `dlq_error` for the specific field and
`tenant_id` for who is sending it.

**`dlq_reason=retries_exhausted`** — the handler failed
`PULSEBRIDGE_MAX_ATTEMPTS` times. `dlq_error` carries the last failure. This is
usually a downstream that was unavailable for longer than the retry budget, which
means these events are replayable once it recovers.

**Replaying** — the DLQ preserves the original fields verbatim, so re-publishing
is a copy:

```bash
docker compose exec redis redis-cli --eval /dev/stdin <<'LUA'
local entries = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[2])
for _, entry in ipairs(entries) do
  local fields, out = entry[2], {}
  for i = 1, #fields, 2 do
    local key = fields[i]
    -- Drop the dlq_ metadata; republish only the original envelope.
    if string.sub(key, 1, 4) ~= 'dlq_' then
      table.insert(out, key); table.insert(out, fields[i + 1])
    end
  end
  redis.call('XADD', KEYS[2], '*', unpack(out))
end
return #entries
LUA
0 pulsebridge.events.dlq pulsebridge.events - +
```

Verify the root cause is fixed before replaying. Replaying into a downstream that
is still broken just refills the DLQ, with the retry budget spent twice.

Replay is safe with respect to duplicates: the handler claims each event id
before applying it, so an event that did get through will be recognised as a
duplicate rather than applied again.

---

<a id="dead-letter-writes-failing"></a>

## `DeadLetterWritesFailing`
**Impact:** the most serious alert here. Quarantine is unavailable, so poison
messages stay in the pending list and are retried indefinitely. Backlog age will
climb next.

**Confirm:**

```bash
docker compose logs worker-1 --since 10m | grep "dead-letter publish failed"
docker compose exec redis redis-cli INFO memory | grep -E "used_memory_human|maxmemory"
```

The usual cause is Redis rejecting writes — out of memory, or an eviction policy
that is dropping the DLQ stream. Check `maxmemory-policy`: the dead-letter stream
must not be evictable.

The worker deliberately does not acknowledge a message it failed to quarantine.
That is the correct trade: retrying a poison message forever is recoverable,
acknowledging it destroys the event with no record anywhere.

---

<a id="retry-storm"></a>

## `RetryStormDetected`
**Impact:** work is being done several times. Throughput drops and downstream
load multiplies.

**Two causes, and they need opposite fixes.**

**1. A flapping downstream.** Handlers fail, messages are redelivered, the
downstream fails again.

```bash
docker compose logs worker-1 --since 10m | grep "process failed" | tail -20
```

**2. Reclaim stealing healthy work.** If `PULSEBRIDGE_RECLAIM_MIN_IDLE` is close
to how long a handler legitimately takes, a slow-but-successful event is reclaimed
by another replica while the first is still working on it, and gets processed
twice.

```bash
docker compose logs worker-1 | grep "worker started" | tail -1
```

Compare `reclaim_min_idle` with the observed p99 in
`pulsebridge_worker_process_seconds`. The consumer enforces a floor of
`2 × PULSEBRIDGE_PROCESS_TIMEOUT`, but that only helps if the process timeout
itself reflects reality. If handlers routinely take 4 s with a 5 s timeout, a
30 s reclaim window is fine; if they take 25 s, it is not.

The idempotent handler means a duplicate is absorbed rather than applied twice,
so this is a throughput problem, not a correctness one.

---

<a id="sustained-throttling"></a>

## `TenantSustainedThrottling`
**Impact:** a tenant is being turned away. Whether that is correct depends on
whether their traffic is legitimate.

**Find who:**

```bash
docker compose logs gateway --since 15m | grep rate_limited | tail -20
docker compose exec redis redis-cli --scan --pattern 'pb:rl:*' | head -20
```

Then decide, and the decision is deliberately manual:

* traffic is legitimate and growing → raise `PULSEBRIDGE_RATE_LIMIT_RPS` after
  checking against the capacity curve in [`benchmarks.md`](benchmarks.md);
* traffic is a retry loop or a bug → the limiter is working; contact the
  producer;
* one tenant is crowding out others → the per-tenant bucket is already containing
  the blast radius. Leave it.

Do not auto-scale the rate limit. Its job is to hold the boundary between one
tenant's problem and everyone else's.

---

<a id="service-down"></a>

## `ServiceDown`
```bash
docker compose ps
docker compose logs <service> --tail 50
```

**Gateway down:** ingest is refused at the edge, but nothing is lost — producers
retry. Events already in the stream continue to be consumed.

**One worker down:** the survivors take the load and reclaim the dead replica's
pending messages after `PULSEBRIDGE_RECLAIM_MIN_IDLE`. No action needed unless
the remaining capacity cannot keep up.

**All workers down:** ingest keeps accepting, and the stream absorbs the backlog
up to `PULSEBRIDGE_STREAM_MAX_LEN` (default 1 000 000). Past that, trimming
starts discarding the oldest entries. That is the real deadline: at the measured
consumption rate the buffer is large, but it is not infinite.

**Redis down:** everything stops. Ingest returns 503, workers cannot read.
Nothing already acknowledged is lost if AOF is enabled — which it is in compose,
and must be wherever this runs.
