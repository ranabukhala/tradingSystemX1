"""
Abstract base class for all data connectors.
Handles: metrics, logging, retry logic, graceful shutdown.
"""
from __future__ import annotations

import asyncio
import os
import sys
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from prometheus_client import Counter, Histogram, Gauge, start_http_server
from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


def _log(level: str, event: str, **kw) -> None:
    """Simple inline logger — no structlog dependency."""
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


class BaseConnector(ABC):

    def __init__(self) -> None:
        self._running = False
        self._setup_metrics()
        metrics_port = int(os.environ.get("METRICS_PORT", 8000))
        try:
            start_http_server(metrics_port)
            _log("info", "connector.metrics_server.started", port=metrics_port)
        except OSError:
            pass
        self.validate_config()
        _log("info", "connector.initialized", source=self.source_name)

    @property
    @abstractmethod
    def source_name(self) -> str: ...

    @property
    @abstractmethod
    def poll_interval_seconds(self) -> int: ...

    @abstractmethod
    async def fetch(self) -> int: ...

    @abstractmethod
    def validate_config(self) -> None: ...

    def _setup_metrics(self) -> None:
        self.metric_items_received = Counter(
            "connector_items_received_total", "Total items received", ["source"])
        self.metric_errors = Counter(
            "connector_errors_total", "Total errors", ["source", "error_type"])
        self.metric_fetch_latency = Histogram(
            "connector_fetch_latency_seconds", "Fetch duration", ["source"])
        self.metric_last_fetch = Gauge(
            "connector_last_fetch_timestamp", "Last successful fetch timestamp", ["source"])
        self.metric_up = Gauge(
            "connector_up", "1 if connector running", ["source"])

    async def run(self) -> None:
        self._running = True
        source = self.source_name
        _log("info", "connector.started", source=source, poll_interval=self.poll_interval_seconds)
        self.metric_up.labels(source=source).set(1)

        while self._running:
            start = datetime.now(timezone.utc)
            try:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=2, max=30),
                    retry=retry_if_exception_type((ConnectionError, TimeoutError)),
                    reraise=True,
                ):
                    with attempt:
                        with self.metric_fetch_latency.labels(source=source).time():
                            n = await self.fetch()

                self.metric_items_received.labels(source=source).inc(n)
                self.metric_last_fetch.labels(source=source).set(
                    datetime.now(timezone.utc).timestamp())
                elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                _log("debug", "connector.fetch.ok", source=source, items=n, elapsed_s=round(elapsed, 2))

            except RetryError as e:
                self.metric_errors.labels(source=source, error_type="retry_exhausted").inc()
                self.metric_up.labels(source=source).set(0)
                _log("error", "connector.fetch.retry_exhausted", source=source, error=str(e))
                await asyncio.sleep(self.poll_interval_seconds * 5)
                self.metric_up.labels(source=source).set(1)
                continue

            except Exception as e:
                self.metric_errors.labels(source=source, error_type=type(e).__name__).inc()
                _log("error", "connector.fetch.error", source=source, error=str(e))

            await asyncio.sleep(self.poll_interval_seconds)

        _log("info", "connector.stopped", source=source)

    def stop(self) -> None:
        self._running = False
        _log("info", "connector.stop_requested", source=self.source_name)
