# ─────────────────────────────────────────────────────────────
#  Trading System — Python Application Image
#  Single image, different CMD per service in docker-compose
# ─────────────────────────────────────────────────────────────

FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache)
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[dev]" 2>/dev/null || \
       pip install --no-cache-dir \
        confluent-kafka \
        "sqlalchemy[asyncio]" \
        alembic \
        asyncpg \
        psycopg2-binary \
        "redis[asyncio]" \
        aiohttp \
        httpx \
        "pydantic>=2.5.0" \
        "pydantic-settings>=2.1.0" \
        python-dotenv \
        structlog \
        prometheus-client \
        anthropic \
        polygon-api-client \
        websockets \
        minio \
        python-dateutil \
        pytz \
        tenacity \
        click \
        rich \
        asyncpg \
        websocket-client

# Copy application code
COPY app/ ./app/
COPY alembic/ ./alembic/
COPY alembic.ini .
COPY config/ ./config/
COPY scripts/ ./scripts/

# Health check default (overridden per service)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${METRICS_PORT:-8000}/metrics || exit 1

# Default entrypoint — overridden by docker-compose command:
ENTRYPOINT ["python", "-m"]
CMD ["app.connectors.benzinga"]
