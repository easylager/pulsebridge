#!/usr/bin/env python3
"""Async ingest benchmark against a running gateway.

Measures the ingest path end to end over HTTP. Two properties matter more than
the headline number:

* A warm-up phase is discarded, because the first requests pay for connection
  setup and the Lua script upload and would otherwise dominate the tail.
* Outcomes are counted, not just timed. A benchmark that reports 20k rps while
  silently collecting 429s is measuring the rate limiter, not the ingest path.

Run against memory mode to measure the application, or against the Redis-backed
stack to measure the real path. Both numbers are useful and they are not the
same; docs/benchmarks.md records which is which.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import httpx


@dataclass
class Outcome:
    latency_s: float
    status: int


@dataclass
class Report:
    started_at: str
    base_url: str
    requests: int
    concurrency: int
    tenants: int
    warmup: int
    wall_seconds: float
    rps: float
    latency_ms: dict[str, float]
    statuses: dict[str, int]
    environment: dict[str, str] = field(default_factory=dict)

    def render(self) -> str:
        lines = [
            f"started_at   : {self.started_at}",
            f"target       : {self.base_url}",
            f"requests     : {self.requests} (after {self.warmup} warm-up)",
            f"concurrency  : {self.concurrency}",
            f"tenants      : {self.tenants}",
            f"wall_seconds : {self.wall_seconds:.3f}",
            f"rps          : {self.rps:.1f}",
        ]
        for name, value in self.latency_ms.items():
            lines.append(f"{name + '_ms':<13}: {value:.2f}")
        lines.append(f"statuses     : {self.statuses}")
        return "\n".join(lines)


async def send(client: httpx.AsyncClient, url: str, tenant: str, index: int) -> Outcome:
    started = time.perf_counter()
    response = await client.post(
        url,
        json={
            "tenant_id": tenant,
            "event_type": "bench.ping",
            "payload": {"n": index},
            "event_id": str(uuid.uuid4()),
        },
        timeout=10.0,
    )
    return Outcome(latency_s=time.perf_counter() - started, status=response.status_code)


async def drive(
    client: httpx.AsyncClient,
    url: str,
    count: int,
    concurrency: int,
    tenants: int,
    offset: int = 0,
) -> tuple[list[Outcome], float]:
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded(index: int) -> Outcome:
        async with semaphore:
            return await send(client, url, f"tenant-{(index % tenants) + 1}", index)

    started = time.perf_counter()
    outcomes = await asyncio.gather(*(bounded(offset + i) for i in range(count)))
    return list(outcomes), time.perf_counter() - started


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(int(len(sorted_values) * p), len(sorted_values) - 1)
    return sorted_values[index] * 1000


async def run(
    base_url: str, total: int, concurrency: int, tenants: int, warmup: int
) -> Report:
    url = f"{base_url.rstrip('/')}/v1/events"
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)

    async with httpx.AsyncClient(limits=limits) as client:
        if warmup:
            await drive(client, url, warmup, concurrency, tenants)
        outcomes, elapsed = await drive(client, url, total, concurrency, tenants, offset=warmup)

    latencies = sorted(o.latency_s for o in outcomes)
    statuses = Counter(str(o.status) for o in outcomes)

    return Report(
        started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        base_url=base_url,
        requests=total,
        concurrency=concurrency,
        tenants=tenants,
        warmup=warmup,
        wall_seconds=elapsed,
        rps=total / elapsed if elapsed else 0.0,
        latency_ms={
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": latencies[-1] * 1000 if latencies else 0.0,
            "mean": statistics.fmean(latencies) * 1000 if latencies else 0.0,
        },
        statuses=dict(sorted(statuses.items())),
        environment={
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--total", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--tenants", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=500)
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = parser.parse_args()

    report = asyncio.run(
        run(args.base_url, args.total, args.concurrency, args.tenants, args.warmup)
    )

    if args.json:
        print(json.dumps(asdict(report), indent=2))
    else:
        print(report.render())

    # A run where most requests were throttled or rejected says nothing about
    # ingest throughput, so it exits non-zero rather than being quoted as a
    # result.
    accepted = report.statuses.get("202", 0)
    if accepted < report.requests * 0.9:
        print(
            f"\nWARNING: only {accepted}/{report.requests} requests were accepted; "
            "raise PULSEBRIDGE_RATE_LIMIT_RPS or lower --concurrency before "
            "quoting these numbers.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
