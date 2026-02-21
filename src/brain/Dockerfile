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
COPY src/brain/pyproject.toml ./src/brain/

RUN uv sync --frozen --package brain

COPY src/brain/ ./src/brain/

WORKDIR /app/src/brain

CMD ["uv", "run", "python", "-u", "brain.py"]