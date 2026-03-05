#!/usr/bin/env python3
"""Async ingest micro-benchmark against a running gateway."""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid

import httpx


async def one(client: httpx.AsyncClient, url: str, tenant: str, i: int) -> float:
    started = time.perf_counter()
    resp = await client.post(
        url,
        json={
            "tenant_id": tenant,
            "event_type": "bench.ping",
            "payload": {"n": i},
            "event_id": str(uuid.uuid4()),
        },
        timeout=5.0,
    )
    resp.raise_for_status()
    return time.perf_counter() - started


async def run(base_url: str, total: int, concurrency: int, tenants: int) -> None:
    url = f"{base_url.rstrip('/')}/v1/events"
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async with httpx.AsyncClient() as client:

        async def bound(i: int) -> None:
            async with sem:
                tenant = f"tenant-{(i % tenants) + 1}"
                latencies.append(await one(client, url, tenant, i))

        t0 = time.perf_counter()
        await asyncio.gather(*(bound(i) for i in range(total)))
        elapsed = time.perf_counter() - t0

    latencies.sort()
    def pct(p: float) -> float:
        idx = min(int(len(latencies) * p), len(latencies) - 1)
        return latencies[idx] * 1000

    print(f"requests     : {total}")
    print(f"concurrency  : {concurrency}")
    print(f"wall_seconds : {elapsed:.3f}")
    print(f"rps          : {total / elapsed:.1f}")
    print(f"p50_ms       : {pct(0.50):.2f}")
    print(f"p95_ms       : {pct(0.95):.2f}")
    print(f"p99_ms       : {pct(0.99):.2f}")
    print(f"mean_ms      : {statistics.fmean(latencies) * 1000:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--total", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--tenants", type=int, default=10)
    args = parser.parse_args()
    asyncio.run(run(args.base_url, args.total, args.concurrency, args.tenants))


if __name__ == "__main__":
    main()
