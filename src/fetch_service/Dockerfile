FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
COPY src/fetch_service/pyproject.toml ./src/fetch_service/

RUN uv sync --frozen --package fetch-service

COPY src/fetch_service/ ./src/fetch_service/

WORKDIR /app/src/fetch_service

EXPOSE 8000

CMD ["uv", "run", "python", "-u", "app.py"]