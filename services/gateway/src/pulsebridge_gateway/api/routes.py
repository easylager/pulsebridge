from __future__ import annotations

import time

import structlog
from fastapi import APIRouter, Depends, Header, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from pulsebridge_gateway.api.deps import AppContainer, get_container
from pulsebridge_gateway.api.metrics import INGEST_LATENCY, INGEST_TOTAL
from pulsebridge_gateway.api.schemas import HealthResponse, IngestRequest, IngestResponse
from pulsebridge_gateway.domain.errors import DuplicateEventError, RateLimitedError, ValidationError

router = APIRouter()
log = structlog.get_logger()


@router.get("/healthz", response_model=HealthResponse, tags=["ops"])
async def healthz(container: AppContainer = Depends(get_container)) -> HealthResponse:
    """Liveness: the process is up. Never touches Redis.

    A liveness probe that depends on a downstream turns a brief Redis blip into a
    restart storm, which is strictly worse than serving 503s from readiness.
    """
    return HealthResponse(status="ok", service=container.settings.service_name)


@router.get("/readyz", response_model=HealthResponse, tags=["ops"])
async def readyz(
    request: Request,
    response: Response,
    container: AppContainer = Depends(get_container),
) -> HealthResponse:
    """Readiness: this replica can actually accept an event right now."""
    if not getattr(request.app.state, "ready", False):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(status="draining", service=container.settings.service_name)

    if container.redis is not None:
        try:
            await container.redis.ping()
        except Exception as exc:
            log.warning("readiness_check_failed", error=str(exc))
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return HealthResponse(status="degraded", service=container.settings.service_name)

    return HealthResponse(status="ready", service=container.settings.service_name)


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Prometheus exposition.

    Served as a normal route rather than a sub-application mount so that a
    scrape of `/metrics` returns the payload directly; a mount answers the exact
    path with a 307 to `/metrics/`, which costs every scrape a round trip and
    breaks the scrapers that do not follow redirects.
    """
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post(
    "/v1/events",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["ingest"],
    responses={
        200: {"description": "Duplicate event id; the original was already accepted."},
        429: {"description": "Per-tenant rate limit exceeded."},
        422: {"description": "Envelope failed validation."},
        503: {"description": "The stream is unavailable; the event was not accepted."},
    },
)
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
        # 200 rather than 409: the caller's intent was satisfied, and a retrying
        # client should treat a replay as success instead of as an error to
        # escalate.
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
            headers={"Retry-After": str(max(exc.retry_after_ms // 1000, 1))},
        )
    except ValidationError as exc:
        INGEST_TOTAL.labels(result="invalid").inc()
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": str(exc)},
        )
    except Exception as exc:
        # The event was not published, so the honest answer is "try again" rather
        # than a 202 the caller would take as durable acceptance.
        INGEST_TOTAL.labels(result="unavailable").inc()
        log.error("ingest_failed", tenant_id=body.tenant_id, error=str(exc), exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "stream_unavailable"},
        )
    finally:
        INGEST_LATENCY.observe(time.perf_counter() - started)
