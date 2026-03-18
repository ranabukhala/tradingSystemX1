"""
Abstract base class for all data connectors.
Handles: metrics, logging, retry logic, graceful shutdown.
"""
from __future__ import annotations

import asyncio
import json
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import redis.asyncio as aioredis
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from app.config import settings


def _log(level: str, event: str, **kw) -> None:
    """Simple inline logger — no structlog dependency."""
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


# ── Service heartbeat ─────────────────────────────────────────────────────────
_HEARTBEAT_INTERVAL_S = 30   # how often to write the heartbeat key
_HEARTBEAT_TTL_S      = 90   # Redis key TTL — watchdog alerts if missing after this


class BaseConnector(ABC):

    def __init__(self) -> None:
        self._running = False
        self._setup_metrics()
        metrics_port = settings.metrics_port
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

    async def _heartbeat_loop(self, redis_client) -> None:
        """
        Background task: writes service:heartbeat:<name> to Redis every
        _HEARTBEAT_INTERVAL_S seconds with TTL _HEARTBEAT_TTL_S.
        Errors are swallowed — never crashes the main fetch loop.
        """
        key = f"service:heartbeat:{self.source_name}"
        while self._running:
            try:
                await redis_client.setex(key, _HEARTBEAT_TTL_S, "1")
            except Exception as e:
                _log("debug", "connector.heartbeat_error",
                     source=self.source_name, error=str(e))
            await asyncio.sleep(_HEARTBEAT_INTERVAL_S)

    async def run(self) -> None:
        self._running = True
        source = self.source_name
        _log("info", "connector.started", source=source, poll_interval=self.poll_interval_seconds)
        self.metric_up.labels(source=source).set(1)

        # ── Heartbeat task ────────────────────────────────────────────────────
        # Wrapped in try/except — connectors that don't have REDIS_URL configured
        # will fail silently here; the heartbeat key simply won't be written.
        _hb_redis = None
        _heartbeat_task = None
        try:
            _hb_redis = aioredis.from_url(
                settings.redis_url,
                decode_responses=True,
                max_connections=2,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            _heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(_hb_redis),
                name=f"heartbeat-{self.source_name}",
            )
            _log("info", "connector.heartbeat_started",
                 source=self.source_name,
                 key=f"service:heartbeat:{self.source_name}",
                 interval_s=_HEARTBEAT_INTERVAL_S,
                 ttl_s=_HEARTBEAT_TTL_S)
        except Exception as e:
            _log("warning", "connector.heartbeat_unavailable",
                 source=self.source_name, error=str(e))

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

        # Clean up heartbeat — delete the key so the watchdog treats this as a
        # clean stop rather than a frozen service.
        if _heartbeat_task is not None:
            _heartbeat_task.cancel()
            try:
                await _heartbeat_task
            except asyncio.CancelledError:
                pass
        if _hb_redis is not None:
            try:
                await _hb_redis.delete(f"service:heartbeat:{self.source_name}")
            except Exception:
                pass
            try:
                await _hb_redis.aclose()
            except Exception:
                pass

        _log("info", "connector.stopped", source=source)

    def stop(self) -> None:
        self._running = False
        _log("info", "connector.stop_requested", source=self.source_name)
