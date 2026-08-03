from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from pulsebridge_gateway.api.deps import AppContainer, build_container, get_container
from pulsebridge_gateway.api.main import create_app
from pulsebridge_gateway.infrastructure.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def contracts_dir() -> Path:
    return REPO_ROOT / "contracts"


@pytest.fixture(scope="session")
def redis_url() -> str:
    return os.getenv("PULSEBRIDGE_TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def settings() -> Settings:
    # Memory mode keeps the unit suite deterministic and Docker-free; the Redis
    # adapters get their own integration tests.
    return Settings(use_memory=True, rate_limit_rps=1000)


@pytest_asyncio.fixture
async def container(settings: Settings) -> AppContainer:
    return await build_container(settings)


@pytest_asyncio.fixture
async def client(container: AppContainer) -> AsyncIterator[AsyncClient]:
    app = create_app()
    # The lifespan is not run here, so readiness is set explicitly; a dedicated
    # test drives the real lifespan to prove the flag flips on shutdown.
    app.state.ready = True
    app.dependency_overrides[get_container] = lambda: container

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
