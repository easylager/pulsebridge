from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, NewType
from uuid import uuid4

from pulsebridge_gateway.domain.errors import ValidationError

EventId = NewType("EventId", str)
TenantId = NewType("TenantId", str)


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Immutable domain event accepted at the ingest boundary."""

    event_id: EventId
    tenant_id: TenantId
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime
    correlation_id: str

    @staticmethod
    def create(
        *,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any],
        event_id: str | None = None,
        correlation_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> EventEnvelope:
        if not tenant_id.strip():
            raise ValidationError("tenant_id is required")
        if not event_type.strip():
            raise ValidationError("event_type is required")
        if not isinstance(payload, dict):
            raise ValidationError("payload must be an object")
        if len(payload) > 64:
            raise ValidationError("payload has too many keys")

        return EventEnvelope(
            event_id=EventId(event_id or str(uuid4())),
            tenant_id=TenantId(tenant_id.strip()),
            event_type=event_type.strip(),
            payload=payload,
            occurred_at=occurred_at or datetime.now(timezone.utc),
            correlation_id=correlation_id or str(uuid4()),
        )

    def to_wire(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "tenant_id": str(self.tenant_id),
            "event_type": self.event_type,
            "payload": self.payload,
            "occurred_at": self.occurred_at.isoformat(),
            "correlation_id": self.correlation_id,
        }
