"""OpenTelemetry wiring for the gateway.

The gateway is the only place that sees the HTTP request, and the worker is the
only place that sees the side effect. Between them sits a stream, which breaks
the causal chain that logs alone can reconstruct. Injecting a W3C ``traceparent``
into every published entry restores it: one trace spans the client request, the
Redis write, and the worker's handler, across two languages.

Tracing stays optional. With no OTLP endpoint configured only the propagator is
installed, so the repository still runs with a bare ``uvicorn`` and in CI with no
collector standing by.
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.propagate import inject

TRACEPARENT_FIELD = "traceparent"

_TRACER_NAME = "pulsebridge.gateway"


def tracing_enabled() -> bool:
    return bool(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))


def configure_tracing(service_name: str, version: str) -> None:
    """Install a tracer provider when an OTLP endpoint is configured.

    Imports of the SDK are deferred so that a deployment without tracing does not
    pay for loading the exporter stack at startup.
    """
    if not tracing_enabled():
        return

    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create({"service.name": service_name, "service.version": version})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


def instrument_app(app: FastAPI) -> None:
    """Attach automatic FastAPI/ASGI spans if the instrumentation is installed."""
    if not tracing_enabled():
        return
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    except ImportError:  # pragma: no cover - optional extra
        return
    # /metrics and the health probes are scraped continuously; tracing them buries
    # real traffic in noise and inflates the collector's bill for nothing.
    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,readyz,metrics")


def current_traceparent() -> str:
    """Serialise the active span context into a W3C traceparent header.

    Returns an empty string when there is no recording span, so callers can put
    the value on the wire unconditionally.
    """
    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier.get(TRACEPARENT_FIELD, "")


def get_tracer() -> trace.Tracer:
    return trace.get_tracer(_TRACER_NAME)
