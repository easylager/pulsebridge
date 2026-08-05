# Build stage. The lockfile is copied with the manifest and installed with
# --frozen, so an image built today and one built next month resolve to exactly
# the same dependency versions.
FROM python:3.12-slim AS build
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock README.md ./
# Dependencies are installed before the source is copied so editing application
# code does not invalidate the dependency layer.
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

FROM python:3.12-slim
WORKDIR /app

RUN useradd --create-home --uid 10001 gateway
COPY --from=build --chown=10001:10001 /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER 10001
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/readyz').status==200 else 1)"

# Invoked directly rather than through `uv run` so the container has one process
# and SIGTERM reaches uvicorn, which is what makes graceful drain work.
CMD ["uvicorn", "pulsebridge_gateway.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
