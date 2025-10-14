FROM python:3.12-slim AS base
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml README.md ./
COPY src ./src
RUN uv sync --no-dev
EXPOSE 8080
CMD ["uv", "run", "uvicorn", "pulsebridge_gateway.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
