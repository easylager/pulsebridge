from __future__ import annotations

from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from pulsebridge_gateway.api.deps import AppContainer, get_container
from pulsebridge_gateway.api.main import create_app, lifespan
from pulsebridge_gateway.api.middleware import CORRELATION_HEADER, REQUEST_ID_HEADER
from pulsebridge_gateway.application.ingest import IngestEventUseCase
from pulsebridge_gateway.infrastructure.memory import (
    InMemoryIdempotencyStore,
    InMemoryRateLimiter,
)

EVENT: dict[str, Any] = {
    "tenant_id": "acme",
    "event_type": "order.created",
    "payload": {"order_id": "1"},
    "event_id": "e-100",
}


async def test_liveness_does_not_depend_on_redis(client: AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_readiness_reports_ready(client: AsyncClient) -> None:
    response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"


async def test_readiness_fails_while_draining(container: AppContainer) -> None:
    app = create_app()
    app.state.ready = False
    app.dependency_overrides[get_container] = lambda: container

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/readyz")

    # Failing readiness while healthy is what lets an orchestrator drain a
    # replica instead of cutting connections mid-request.
    assert response.status_code == 503
    assert response.json()["status"] == "draining"


async def test_accepts_event(client: AsyncClient) -> None:
    response = await client.post("/v1/events", json=EVENT)

    assert response.status_code == 202
    body = response.json()
    assert body["event_id"] == "e-100"
    assert body["status"] == "accepted"
    assert body["stream_id"]


async def test_replay_returns_200_not_an_error(client: AsyncClient) -> None:
    await client.post("/v1/events", json=EVENT)
    replay = await client.post("/v1/events", json=EVENT)

    assert replay.status_code == 200
    assert replay.json()["status"] == "duplicate"


async def test_rate_limited_response_tells_the_client_when_to_retry(
    container: AppContainer,
) -> None:
    app = create_app()
    app.state.ready = True
    container.ingest = IngestEventUseCase(
        publisher=_NullPublisher(),
        idempotency=InMemoryIdempotencyStore(),
        rate_limiter=InMemoryRateLimiter(rps=1),
    )
    app.dependency_overrides[get_container] = lambda: container

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/v1/events", json={**EVENT, "event_id": "a"})
        throttled = await ac.post("/v1/events", json={**EVENT, "event_id": "b"})

    assert throttled.status_code == 429
    assert "Retry-After" in throttled.headers
    assert throttled.json()["retry_after_ms"] > 0


async def test_publish_failure_is_reported_as_unavailable(container: AppContainer) -> None:
    app = create_app()
    app.state.ready = True
    container.ingest = IngestEventUseCase(
        publisher=_ExplodingPublisher(),
        idempotency=InMemoryIdempotencyStore(),
        rate_limiter=InMemoryRateLimiter(rps=100),
    )
    app.dependency_overrides[get_container] = lambda: container

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/v1/events", json=EVENT)

    # A 202 here would tell the caller the event is durably accepted when it was
    # never written, which is the one answer the gateway must never give.
    assert response.status_code == 503
    assert response.json()["detail"] == "stream_unavailable"


async def test_schema_violations_are_rejected(client: AsyncClient) -> None:
    response = await client.post("/v1/events", json={"event_type": "order.created"})
    assert response.status_code == 422


async def test_correlation_id_is_echoed_back(client: AsyncClient) -> None:
    response = await client.post(
        "/v1/events",
        json={**EVENT, "event_id": "corr-1"},
        headers={CORRELATION_HEADER: "trace-me"},
    )

    assert response.headers[CORRELATION_HEADER] == "trace-me"
    assert response.headers[REQUEST_ID_HEADER]


async def test_correlation_id_is_generated_when_absent(client: AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.headers[CORRELATION_HEADER]


async def test_metrics_endpoint_exposes_ingest_counters(client: AsyncClient) -> None:
    await client.post("/v1/events", json={**EVENT, "event_id": "metric-1"})
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert "pulsebridge_ingest_total" in response.text


@pytest.mark.parametrize("path", ["/healthz", "/readyz", "/metrics"])
async def test_ops_endpoints_are_reachable(client: AsyncClient, path: str) -> None:
    assert (await client.get(path)).status_code == 200


async def test_lifespan_toggles_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSEBRIDGE_USE_MEMORY", "true")
    app = create_app()

    assert app.state.ready is False, "a replica must not advertise readiness before startup"
    async with lifespan(app):
        assert app.state.ready is True
    assert app.state.ready is False, "readiness must drop before the process exits"


class _NullPublisher:
    async def publish(self, event: object) -> str:
        return "stream-1"


class _ExplodingPublisher:
    async def publish(self, event: object) -> str:
        raise ConnectionError("redis unreachable")
