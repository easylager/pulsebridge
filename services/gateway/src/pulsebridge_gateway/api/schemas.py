from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class IngestRequest(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    event_id: str | None = Field(default=None, max_length=64)
    correlation_id: str | None = Field(default=None, max_length=64)


class IngestResponse(BaseModel):
    event_id: str
    stream_id: str
    status: str = "accepted"


class HealthResponse(BaseModel):
    status: str
    service: str
