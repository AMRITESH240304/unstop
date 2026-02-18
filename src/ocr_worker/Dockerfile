FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    libtesseract-dev \
    poppler-utils \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
COPY src/ocr_worker/pyproject.toml ./src/ocr_worker/

RUN uv sync --frozen --package ocr-worker

COPY src/ocr_worker/ ./src/ocr_worker/

WORKDIR /app/src/ocr_worker

CMD ["uv", "run", "python", "-u", "worker.py"]