FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    aiohttp==3.9.5 \
    redis[asyncio]==5.0.4 \
    asyncpg==0.29.0 \
    apscheduler==3.10.4 \
    pydantic==2.7.1 \
    python-dotenv==1.0.1

# Copy the service package
COPY stock_context_service/ ./stock_context_service/

# Copy watchlist config (fallback if DB table absent)
COPY config/ ./config/

EXPOSE 8082

CMD ["python", "-m", "stock_context_service.main"]
