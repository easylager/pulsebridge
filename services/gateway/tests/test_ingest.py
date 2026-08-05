from __future__ import annotations

import asyncio

import pytest

from pulsebridge_gateway.application.ingest import IngestEventUseCase
from pulsebridge_gateway.domain.errors import DuplicateEventError, RateLimitedError, ValidationError
from pulsebridge_gateway.domain.events import EventEnvelope, EventId
from pulsebridge_gateway.infrastructure.memory import (
    InMemoryEventPublisher,
    InMemoryIdempotencyStore,
    InMemoryRateLimiter,
)


class ExplodingPublisher:
    """A publisher that always fails, standing in for an unreachable Redis."""

    def __init__(self) -> None:
        self.calls = 0

    async def publish(self, event: EventEnvelope) -> str:
        self.calls += 1
        raise ConnectionError("redis unreachable")


def build_use_case(
    *,
    publisher: object | None = None,
    idempotency: InMemoryIdempotencyStore | None = None,
    rps: int = 1000,
) -> IngestEventUseCase:
    return IngestEventUseCase(
        publisher=publisher or InMemoryEventPublisher(),  # type: ignore[arg-type]
        idempotency=idempotency or InMemoryIdempotencyStore(),
        rate_limiter=InMemoryRateLimiter(rps=rps),
    )


@pytest.fixture
def use_case() -> IngestEventUseCase:
    return build_use_case()


async def test_accepts_event(use_case: IngestEventUseCase) -> None:
    result = await use_case.execute(
        tenant_id="acme",
        event_type="order.created",
        payload={"order_id": "o-1"},
        event_id="evt-1",
    )
    assert result.event_id == "evt-1"
    assert result.stream_id.startswith("mem-")


async def test_rejects_duplicate(use_case: IngestEventUseCase) -> None:
    await use_case.execute(tenant_id="acme", event_type="order.created", payload={}, event_id="dup")

    with pytest.raises(DuplicateEventError):
        await use_case.execute(
            tenant_id="acme", event_type="order.created", payload={}, event_id="dup"
        )


async def test_concurrent_duplicates_publish_once() -> None:
    publisher = InMemoryEventPublisher()
    use_case = build_use_case(publisher=publisher)

    async def send() -> str | None:
        try:
            result = await use_case.execute(
                tenant_id="acme", event_type="order.created", payload={}, event_id="race"
            )
            return result.stream_id
        except DuplicateEventError:
            return None

    results = await asyncio.gather(*(send() for _ in range(20)))

    assert sum(1 for r in results if r is not None) == 1
    assert len(publisher.events) == 1


async def test_rate_limits_per_tenant() -> None:
    use_case = build_use_case(rps=2)

    await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="a")
    await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="b")

    with pytest.raises(RateLimitedError):
        await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="c")


async def test_rate_limit_is_scoped_to_one_tenant() -> None:
    use_case = build_use_case(rps=1)

    await use_case.execute(tenant_id="noisy", event_type="ping", payload={}, event_id="a")
    with pytest.raises(RateLimitedError):
        await use_case.execute(tenant_id="noisy", event_type="ping", payload={}, event_id="b")

    # A tenant exhausting its own budget must not affect anyone else's.
    result = await use_case.execute(tenant_id="quiet", event_type="ping", payload={}, event_id="c")
    assert result.event_id == "c"


async def test_rate_limit_is_checked_before_the_idempotency_claim() -> None:
    idempotency = InMemoryIdempotencyStore()
    use_case = build_use_case(idempotency=idempotency, rps=1)

    await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="first")
    with pytest.raises(RateLimitedError):
        await use_case.execute(tenant_id="t1", event_type="ping", payload={}, event_id="throttled")

    # A throttled request must leave no trace, otherwise a client that backs off
    # and retries the same event id is told it is a duplicate of an event that
    # was never accepted.
    assert not await idempotency.seen(EventId("throttled"))


async def test_failed_publish_releases_the_idempotency_claim() -> None:
    idempotency = InMemoryIdempotencyStore()
    use_case = build_use_case(publisher=ExplodingPublisher(), idempotency=idempotency)

    with pytest.raises(ConnectionError):
        await use_case.execute(
            tenant_id="acme", event_type="order.created", payload={}, event_id="evt-1"
        )

    assert not await idempotency.seen(EventId("evt-1"))


async def test_client_can_retry_after_a_failed_publish() -> None:
    idempotency = InMemoryIdempotencyStore()
    publisher = InMemoryEventPublisher()

    failing = build_use_case(publisher=ExplodingPublisher(), idempotency=idempotency)
    with pytest.raises(ConnectionError):
        await failing.execute(
            tenant_id="acme", event_type="order.created", payload={}, event_id="evt-1"
        )

    recovered = build_use_case(publisher=publisher, idempotency=idempotency)
    result = await recovered.execute(
        tenant_id="acme", event_type="order.created", payload={}, event_id="evt-1"
    )

    assert result.stream_id.startswith("mem-")
    assert len(publisher.events) == 1


async def test_generated_ids_when_client_omits_them(use_case: IngestEventUseCase) -> None:
    result = await use_case.execute(tenant_id="acme", event_type="order.created", payload={})
    assert result.event_id


@pytest.mark.parametrize(
    ("tenant_id", "event_type"),
    [(" ", "order.created"), ("acme", "  "), ("", "order.created")],
)
async def test_rejects_blank_identifiers(tenant_id: str, event_type: str) -> None:
    use_case = build_use_case()
    with pytest.raises(ValidationError):
        await use_case.execute(tenant_id=tenant_id, event_type=event_type, payload={})


async def test_rejects_oversized_payload(use_case: IngestEventUseCase) -> None:
    payload = {f"k{i}": i for i in range(65)}
    with pytest.raises(ValidationError):
        await use_case.execute(tenant_id="acme", event_type="order.created", payload=payload)
