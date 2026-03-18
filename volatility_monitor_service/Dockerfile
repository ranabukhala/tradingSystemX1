FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    aiohttp==3.9.5 \
    redis[asyncio]==5.0.4 \
    aiokafka==0.11.0 \
    apscheduler==3.10.4

# Copy the service package
COPY volatility_monitor_service/ ./volatility_monitor_service/

EXPOSE 8083

CMD ["python", "-u", "-m", "volatility_monitor_service.volatility_monitor"]
