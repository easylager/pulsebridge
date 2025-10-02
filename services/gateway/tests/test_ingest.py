import pytest

from pulsebridge_gateway.application.ingest import IngestEventUseCase
from pulsebridge_gateway.domain.errors import DuplicateEventError, RateLimitedError, ValidationError
from pulsebridge_gateway.infrastructure.memory import (
    InMemoryEventPublisher,
    InMemoryIdempotencyStore,
    InMemoryRateLimiter,
)


@pytest.fixture
def use_case() -> IngestEventUseCase:
    return IngestEventUseCase(
        publisher=InMemoryEventPublisher(),
        idempotency=InMemoryIdempotencyStore(),
        rate_limiter=InMemoryRateLimiter(rps=1000),
    )


@pytest.mark.asyncio
async def test_ingest_accepts_event(use_case: IngestEventUseCase) -> None:
    result = await use_case.execute(
        tenant_id="acme",
        event_type="order.created",
        payload={"order_id": "o-1"},
        event_id="evt-1",
    )
    assert result.event_id == "evt-1"
    assert result.stream_id.startswith("mem-")


@pytest.mark.asyncio
async def test_ingest_rejects_duplicate(use_case: IngestEventUseCase) -> None:
    await use_case.execute(
        tenant_id="acme",
        event_type="order.created",
        payload={"order_id": "o-1"},
        event_id="evt-dup",
    )
    with pytest.raises(DuplicateEventError):
        await use_case.execute(
            tenant_id="acme",
            event_type="order.created",
            payload={"order_id": "o-1"},
            event_id="evt-dup",
        )


@pytest.mark.asyncio
async def test_ingest_rate_limits() -> None:
    use_case = IngestEventUseCase(
        publisher=InMemoryEventPublisher(),
        idempotency=InMemoryIdempotencyStore(),
        rate_limiter=InMemoryRateLimiter(rps=2),
    )
    await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="a")
    await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="b")
    with pytest.raises(RateLimitedError):
        await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="c")


@pytest.mark.asyncio
async def test_validation() -> None:
    use_case = IngestEventUseCase(
        publisher=InMemoryEventPublisher(),
        idempotency=InMemoryIdempotencyStore(),
        rate_limiter=InMemoryRateLimiter(rps=10),
    )
    with pytest.raises(ValidationError):
        await use_case.execute(tenant_id=" ", event_type="x", payload={})
