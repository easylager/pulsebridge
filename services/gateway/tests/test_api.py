import pytest
from httpx import ASGITransport, AsyncClient

from pulsebridge_gateway.api.deps import build_container, get_container
from pulsebridge_gateway.api.main import create_app
from pulsebridge_gateway.infrastructure.settings import Settings


@pytest.mark.asyncio
async def test_health_and_ingest() -> None:
    settings = Settings(use_memory=True, rate_limit_rps=1000)
    container = await build_container(settings)
    app = create_app()
    app.dependency_overrides[get_container] = lambda: container

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        health = await ac.get("/healthz")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        resp = await ac.post(
            "/v1/events",
            json={
                "tenant_id": "acme",
                "event_type": "order.created",
                "payload": {"order_id": "1"},
                "event_id": "e-100",
            },
        )
        assert resp.status_code == 202
        body = resp.json()
        assert body["event_id"] == "e-100"
        assert body["status"] == "accepted"

        dup = await ac.post(
            "/v1/events",
            json={
                "tenant_id": "acme",
                "event_type": "order.created",
                "payload": {"order_id": "1"},
                "event_id": "e-100",
            },
        )
        assert dup.status_code == 200
        assert dup.json()["status"] == "duplicate"
