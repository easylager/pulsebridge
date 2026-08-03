// k6 load profile for the ingest endpoint.
//
// The Python benchmark in scripts/bench_ingest.py is a smoke test: a single
// asyncio client saturates its own event loop long before the gateway does, so
// its numbers describe the client. k6 drives load from a Go runtime with real
// VUs, which is why the figures in docs/benchmarks.md come from here.
//
// Run:
//   docker compose up -d
//   k6 run scripts/load/ingest.js
//   # or without installing k6:
//   docker run --rm -i --network host grafana/k6 run - < scripts/load/ingest.js
//
// Tunables:
//   BASE_URL   target gateway            (default http://127.0.0.1:8080)
//   TENANTS    distinct tenant ids       (default 10)
//   RATE       target requests/second    (default 2000)
//   DURATION   steady-state duration     (default 60s)

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Event ids are built from the VU and iteration counters rather than pulled from
// an external jslib import, so the script runs unchanged in an offline CI runner.
// The pair is unique per run by construction, which is all idempotency needs.
function eventId() {
  return `k6-${__VU}-${__ITER}-${Date.now()}`;
}

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const TENANTS = parseInt(__ENV.TENANTS || '10', 10);
const RATE = parseInt(__ENV.RATE || '2000', 10);
const DURATION = __ENV.DURATION || '60s';

const accepted = new Counter('ingest_accepted');
const throttled = new Counter('ingest_throttled');
const unavailable = new Counter('ingest_unavailable');
const acceptedLatency = new Trend('ingest_accepted_latency', true);

export const options = {
  // p99 is the objective in docs/slo.md, and k6 does not compute it by default.
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    // Arrival-rate rather than fixed VUs: the question is how the gateway
    // behaves at a given offered load, not how fast a fixed pool can loop.
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 200,
      maxVUs: 2000,
      gracefulStop: '10s',
    },
  },
  thresholds: {
    // Mirrors the objectives in docs/slo.md so a regression fails the run
    // instead of quietly shifting a number in a table.
    'http_req_failed': ['rate<0.01'],
    'ingest_accepted_latency': ['p(99)<50', 'p(95)<25'],
  },
};

// A compact machine-readable summary, so a capacity ladder can be scripted and
// the numbers in docs/benchmarks.md can be regenerated rather than transcribed.
export function handleSummary(data) {
  const m = data.metrics;
  const value = (name, field) => (m[name] && m[name].values ? m[name].values[field] || 0 : 0);

  const summary = {
    offered_rps: RATE,
    duration: DURATION,
    tenants: TENANTS,
    achieved_rps: value('http_reqs', 'rate'),
    accepted: value('ingest_accepted', 'count'),
    throttled: value('ingest_throttled', 'count'),
    unavailable: value('ingest_unavailable', 'count'),
    dropped_iterations: value('dropped_iterations', 'count'),
    latency_ms: {
      p50: value('ingest_accepted_latency', 'med'),
      p95: value('ingest_accepted_latency', 'p(95)'),
      p99: value('ingest_accepted_latency', 'p(99)'),
      max: value('ingest_accepted_latency', 'max'),
    },
  };

  return { stdout: JSON.stringify(summary) + '\n' };
}

export default function () {
  const tenant = `tenant-${(__ITER % TENANTS) + 1}`;
  const payload = JSON.stringify({
    tenant_id: tenant,
    event_type: 'bench.ping',
    payload: { n: __ITER },
    event_id: eventId(),
  });

  const res = http.post(`${BASE_URL}/v1/events`, payload, {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'ingest' },
  });

  if (res.status === 202) {
    accepted.add(1);
    // Only successful ingests feed the latency objective; a 429 is answered
    // immediately and would flatter the percentiles.
    acceptedLatency.add(res.timings.duration);
  } else if (res.status === 429) {
    throttled.add(1);
  } else if (res.status >= 500) {
    unavailable.add(1);
  }

  check(res, {
    'not a server error': (r) => r.status < 500,
  });
}
