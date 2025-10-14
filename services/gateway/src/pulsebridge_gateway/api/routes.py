from __future__ import annotations

import time

import structlog
from fastapi import APIRouter, Depends, Header, Response, status
from fastapi.responses import JSONResponse

from pulsebridge_gateway.api.deps import AppContainer, get_container
from pulsebridge_gateway.api.metrics import INGEST_LATENCY, INGEST_TOTAL
from pulsebridge_gateway.api.schemas import HealthResponse, IngestRequest, IngestResponse
from pulsebridge_gateway.domain.errors import DuplicateEventError, RateLimitedError, ValidationError

router = APIRouter()
log = structlog.get_logger()


@router.get("/healthz", response_model=HealthResponse)
async def healthz(container: AppContainer = Depends(get_container)) -> HealthResponse:
    return HealthResponse(status="ok", service=container.settings.service_name)


@router.get("/readyz", response_model=HealthResponse)
async def readyz(container: AppContainer = Depends(get_container)) -> HealthResponse:
    if container.redis is not None:
        await container.redis.ping()
    return HealthResponse(status="ready", service=container.settings.service_name)


@router.post("/v1/events", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(
    body: IngestRequest,
    response: Response,
    container: AppContainer = Depends(get_container),
    x_correlation_id: str | None = Header(default=None),
) -> IngestResponse | JSONResponse:
    started = time.perf_counter()
    try:
        result = await container.ingest.execute(
            tenant_id=body.tenant_id,
            event_type=body.event_type,
            payload=body.payload,
            event_id=body.event_id,
            correlation_id=body.correlation_id or x_correlation_id,
        )
        INGEST_TOTAL.labels(result="accepted").inc()
        log.info(
            "event_accepted",
            event_id=result.event_id,
            stream_id=result.stream_id,
            tenant_id=body.tenant_id,
            event_type=body.event_type,
        )
        return IngestResponse(event_id=result.event_id, stream_id=result.stream_id)
    except DuplicateEventError as exc:
        INGEST_TOTAL.labels(result="duplicate").inc()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"event_id": exc.event_id, "stream_id": "", "status": "duplicate"},
        )
    except RateLimitedError as exc:
        INGEST_TOTAL.labels(result="rate_limited").inc()
        response.headers["Retry-After"] = str(max(exc.retry_after_ms // 1000, 1))
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "rate_limited", "retry_after_ms": exc.retry_after_ms},
        )
    except ValidationError as exc:
        INGEST_TOTAL.labels(result="invalid").inc()
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": str(exc)})
    finally:
        INGEST_LATENCY.observe(time.perf_counter() - started)
