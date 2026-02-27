FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    libtesseract-dev \
    poppler-utils \
    gcc \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
COPY src/agents/matching_agent/pyproject.toml ./src/agents/matching_agent/

RUN uv sync --frozen --package matching-agent

COPY src/agents/matching_agent/ ./src/agents/matching_agent/

WORKDIR /app/src/agents/matching_agent

CMD ["uv", "run", "python", "-u", "match.py"]