from prometheus_client import Counter, Histogram

INGEST_TOTAL = Counter(
    "pulsebridge_ingest_total",
    "Ingest requests by result",
    labelnames=("result",),
)

INGEST_LATENCY = Histogram(
    "pulsebridge_ingest_latency_seconds",
    "Ingest end-to-end latency",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
