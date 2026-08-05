from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from pulsebridge_gateway import __version__
from pulsebridge_gateway.api.deps import lifespan_container
from pulsebridge_gateway.api.middleware import CorrelationMiddleware
from pulsebridge_gateway.api.routes import router
from pulsebridge_gateway.infrastructure.logging import configure_logging
from pulsebridge_gateway.infrastructure.settings import get_settings
from pulsebridge_gateway.infrastructure.telemetry import configure_tracing, instrument_app

log = structlog.get_logger()

DESCRIPTION = """
Async event ingest edge for PulseBridge.

Accepts events over HTTP, enforces per-tenant rate limits and idempotency, and
publishes to a Redis Stream consumed by the Go worker pool.
""".strip()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_tracing(settings.service_name, __version__)

    async for _ in lifespan_container():
        # Readiness is separate from liveness so a rolling deploy can stop
        # sending traffic to this replica before the process goes away, and so a
        # replica that starts before Redis is reachable fails readiness instead
        # of accepting events it cannot publish.
        app.state.ready = True
        log.info("gateway_ready", version=__version__, service=settings.service_name)
        try:
            yield
        finally:
            # Flipping this first gives the load balancer a chance to drain
            # in-flight requests while uvicorn finishes them.
            app.state.ready = False
            log.info("gateway_draining")


def create_app() -> FastAPI:
    app = FastAPI(
        title="PulseBridge Gateway",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.ready = False
    app.add_middleware(CorrelationMiddleware)
    app.include_router(router)
    instrument_app(app)
    return app


app = create_app()
