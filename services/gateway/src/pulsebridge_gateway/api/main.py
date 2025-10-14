from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from pulsebridge_gateway import __version__
from pulsebridge_gateway.api.deps import lifespan_container
from pulsebridge_gateway.api.routes import router
from pulsebridge_gateway.infrastructure.logging import configure_logging
from pulsebridge_gateway.infrastructure.settings import get_settings


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)
    async for _ in lifespan_container():
        yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="PulseBridge Gateway",
        version=__version__,
        description="High-throughput async event ingest edge (Python asyncio + Redis Streams).",
        lifespan=lifespan,
    )
    app.include_router(router)
    app.mount("/metrics", make_asgi_app())
    return app


app = create_app()
