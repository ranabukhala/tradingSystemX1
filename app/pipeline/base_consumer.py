"""
Base class for all pipeline consumer services.
Handles: Kafka consume loop, metrics, graceful shutdown, DLQ routing,
         idempotency (Redis with SQLite fallback), and correlation ID propagation.
"""
from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Histogram, Gauge, start_http_server

from app.config import settings
from app.logging import get_logger, bind_event_context, clear_event_context

# ── Kafka consumer lag monitoring ─────────────────────────────────────────────
# Lag = how many messages we are behind the head of each assigned partition.
# Computed periodically (not on every message) via a broker metadata call.

import os as _os
_LAG_CHECK_INTERVAL_S = float(_os.environ.get("LAG_CHECK_INTERVAL_SECONDS", "30"))
_LAG_WARN_THRESHOLD   = int(_os.environ.get("LAG_WARN_MESSAGES", "100"))

# ── Service heartbeat ─────────────────────────────────────────────────────────
# Written every _HEARTBEAT_INTERVAL_S to service:heartbeat:<name> with a TTL of
# _HEARTBEAT_TTL_S.  The watchdog alerts if the key goes missing (service frozen).
_HEARTBEAT_INTERVAL_S = 30   # how often to write the heartbeat key
_HEARTBEAT_TTL_S      = 90   # Redis key TTL — watchdog alerts if missing after this

log = get_logger("base_consumer")


def _log(level: str, event: str, **kw) -> None:
    """Legacy log helper — delegates to structlog."""
    getattr(log, level, log.info)(event, **kw)


class BaseConsumer(ABC):
    """
    Base for all pipeline stage consumers.

    Subclasses implement:
      - input_topic, output_topic, service_name
      - process(record: dict) -> dict | None
    """

    def __init__(self) -> None:
        self._running = False
        self._consumer: Consumer | None = None
        # SQLite store — lazy-created; used as Redis fallback and for order/loss tracking
        self._idempotency = None
        # Redis idempotency store — created in run() via _init_redis_idempotency()
        self._redis_idem = None
        # Lag tracking — updated periodically by _update_lag()
        self._current_lag: int = 0
        self._last_lag_check: float = 0.0
        self._setup_metrics()

        metrics_port = settings.metrics_port
        try:
            start_http_server(metrics_port)
            _log("info", "service.metrics_server.started", port=metrics_port)
        except OSError:
            pass

        _log("info", "service.initialized", service=self.service_name)

    @property
    @abstractmethod
    def service_name(self) -> str: ...

    @property
    @abstractmethod
    def input_topic(self) -> str: ...

    @property
    @abstractmethod
    def output_topic(self) -> str: ...

    @abstractmethod
    async def process(self, record: dict) -> dict | None:
        """Process one record. Return dict to emit or None to drop. Raise to DLQ."""
        ...

    async def on_start(self) -> None:
        pass

    async def on_stop(self) -> None:
        pass

    def _setup_metrics(self) -> None:
        self.metric_consumed = Counter(
            "pipeline_consumed_total", "Messages consumed", ["service"])
        self.metric_emitted = Counter(
            "pipeline_emitted_total", "Messages emitted", ["service"])
        self.metric_dropped = Counter(
            "pipeline_dropped_total", "Messages dropped (dedup/filter)", ["service"])
        self.metric_errors = Counter(
            "pipeline_errors_total", "Processing errors", ["service", "error_type"])
        self.metric_dlq = Counter(
            "pipeline_dlq_total", "Messages sent to DLQ", ["service"])
        self.metric_dedup_skipped = Counter(
            "pipeline_dedup_skipped_total", "Messages skipped by idempotency", ["service"])
        self.metric_process_latency = Histogram(
            "pipeline_process_latency_seconds", "Per-message processing time", ["service"])
        self.metric_lag = Gauge(
            "pipeline_consumer_lag", "Estimated consumer lag", ["service"])
        self.metric_up = Gauge(
            "pipeline_up", "1 if service healthy", ["service"])
        self.metric_idem_fallback = Counter(
            "pipeline_idem_redis_fallback_total",
            "Idempotency checks routed to SQLite fallback (Redis unreachable)",
            ["service"])

    def _make_consumer(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"{settings.kafka_group_id}-{self.service_name}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        })

    def _make_producer(self):
        from app.kafka import get_producer
        return get_producer()

    def _get_idempotency_store(self):
        """Return (and lazily create) the SQLite IdempotencyStore.

        Still used directly by the execution engine for order/loss tracking.
        Also acts as the Redis fallback store when enabled.
        """
        if self._idempotency is None:
            from app.idempotency import IdempotencyStore
            self._idempotency = IdempotencyStore()
        return self._idempotency

    async def _init_redis_idempotency(self):
        """
        Build and return the async idempotency store for use in the consume loop.

        Backend selection (IDEMPOTENCY_BACKEND env / config):
          "redis"  — RedisIdempotencyStore with optional SQLite fallback.
          "sqlite" — _SQLiteAsyncAdapter around the existing SQLite store.

        A dedicated low-connection Redis client is created (max_connections=5,
        connect/read timeout 2 s) so a slow/dead Redis never blocks message
        polling for longer than a single TCP handshake timeout.
        """
        if settings.idempotency_backend == "sqlite":
            from app.idempotency_redis import _SQLiteAsyncAdapter
            _log("info", "service.idempotency_backend",
                 service=self.service_name, backend="sqlite")
            return _SQLiteAsyncAdapter(self._get_idempotency_store())

        # ── Redis path ────────────────────────────────────────────────────
        import redis.asyncio as _aioredis
        from app.idempotency_redis import RedisIdempotencyStore

        redis_client = _aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=5,
            socket_connect_timeout=2,
            socket_timeout=2,
        )

        fallback = None
        if settings.idempotency_fallback_enabled:
            fallback = self._get_idempotency_store()

        store = RedisIdempotencyStore(
            redis_client=redis_client,
            ttl=settings.idempotency_ttl_seconds,
            fallback_store=fallback,
        )
        _log("info", "service.idempotency_backend",
             service=self.service_name,
             backend="redis",
             ttl=settings.idempotency_ttl_seconds,
             fallback=settings.idempotency_fallback_enabled)
        return store

    # ── Lag monitoring ────────────────────────────────────────────────────────

    def _compute_lag_sync(self) -> int:
        """
        Synchronously compute total Kafka consumer lag across all assigned partitions.

        Lag = sum over partitions of (high_watermark - current_position).
        Runs a broker metadata call — intended to be called from run_in_executor.
        Returns 0 on any error so a transient broker hiccup never crashes the loop.
        """
        if not self._consumer:
            return 0
        total = 0
        try:
            assigned = self._consumer.assignment()
            if not assigned:
                return 0
            positions = self._consumer.position(assigned)
            for tp in positions:
                if tp.offset < 0:
                    # OFFSET_INVALID / OFFSET_BEGINNING — not yet polled any messages
                    continue
                try:
                    low, high = self._consumer.get_watermark_offsets(tp, timeout=2.0)
                    total += max(0, high - tp.offset)
                except Exception:
                    pass  # partition-level failure; skip and count remaining
        except Exception as e:
            _log("warning", "service.lag_compute_error",
                 service=self.service_name, error=str(e))
        return total

    async def _update_lag(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Update self._current_lag on a time-bounded interval.
        Safe to call on every message — only hits the broker every
        LAG_CHECK_INTERVAL_SECONDS (default 30 s).
        """
        now = time.monotonic()
        if now - self._last_lag_check < _LAG_CHECK_INTERVAL_S:
            return
        self._last_lag_check = now
        try:
            lag = await loop.run_in_executor(None, self._compute_lag_sync)
            self._current_lag = lag
            self.metric_lag.labels(service=self.service_name).set(lag)
            if lag > _LAG_WARN_THRESHOLD:
                _log("warning", "service.consumer_lag_high",
                     service=self.service_name,
                     lag=lag,
                     warn_threshold=_LAG_WARN_THRESHOLD)
            else:
                _log("debug", "service.consumer_lag",
                     service=self.service_name, lag=lag)
        except Exception as e:
            _log("warning", "service.lag_update_error",
                 service=self.service_name, error=str(e))

    def _lag_too_high(self, threshold: int) -> bool:
        """
        Returns True when the cached consumer lag exceeds *threshold* messages.

        The lag value is refreshed every LAG_CHECK_INTERVAL_SECONDS; this method
        just reads the cached value — no I/O, safe to call inline in process().
        """
        return self._current_lag > threshold

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self, redis_client) -> None:
        """
        Background task: writes service:heartbeat:<name> to Redis every
        _HEARTBEAT_INTERVAL_S seconds with a TTL of _HEARTBEAT_TTL_S.

        Runs until self._running is False. Errors are swallowed — a Redis
        blip must never crash the main consume loop.
        """
        key = f"service:heartbeat:{self.service_name}"
        while self._running:
            try:
                await redis_client.setex(key, _HEARTBEAT_TTL_S, "1")
            except Exception as e:
                _log("debug", "service.heartbeat_error",
                     service=self.service_name, error=str(e))
            await asyncio.sleep(_HEARTBEAT_INTERVAL_S)

    # ── Main consume loop ─────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        svc = self.service_name

        await self.on_start()

        self._consumer = self._make_consumer()
        self._consumer.subscribe([self.input_topic])
        producer = self._make_producer()

        _log("info", "service.started",
             service=svc, input=self.input_topic, output=self.output_topic)
        self.metric_up.labels(service=svc).set(1)

        loop = asyncio.get_running_loop()
        self._redis_idem = await self._init_redis_idempotency()
        idem = self._redis_idem

        # ── Heartbeat task ────────────────────────────────────────────────────
        import redis.asyncio as _hb_redis
        _hb_redis_client = _hb_redis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=2,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        _heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(_hb_redis_client),
            name=f"heartbeat-{self.service_name}",
        )
        _log("info", "service.heartbeat_started",
             service=self.service_name,
             key=f"service:heartbeat:{self.service_name}",
             interval_s=_HEARTBEAT_INTERVAL_S,
             ttl_s=_HEARTBEAT_TTL_S)

        try:
            while self._running:
                msg = await loop.run_in_executor(
                    None, lambda: self._consumer.poll(timeout=1.0)
                )

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    _log("error", "service.kafka_error",
                         service=svc, error=str(msg.error()))
                    self.metric_errors.labels(service=svc, error_type="kafka").inc()
                    continue

                self.metric_consumed.labels(service=svc).inc()

                try:
                    raw = json.loads(msg.value().decode("utf-8"))
                except Exception as e:
                    _log("error", "service.decode_error", service=svc, error=str(e))
                    self.metric_errors.labels(service=svc, error_type="decode").inc()
                    self._consumer.commit(msg)
                    continue

                # ── Extract/generate event_id for correlation ──────────
                event_id = raw.get("event_id") or str(uuid4())
                bind_event_context(event_id, service=svc, topic=self.input_topic)

                # ── Idempotency check — read-only, does NOT mark yet ──
                already = await idem.is_processed(svc, event_id)
                # Track messages that fell back to SQLite (Redis unreachable)
                if idem._fallback_mode:
                    self.metric_idem_fallback.labels(service=svc).inc()
                if already:
                    _log("debug", "service.dedup_skipped",
                         service=svc, event_id=event_id)
                    self.metric_dedup_skipped.labels(service=svc).inc()
                    self._consumer.commit(msg)
                    clear_event_context()
                    continue

                try:
                    with self.metric_process_latency.labels(service=svc).time():
                        result = await self.process(raw)

                    if result is None:
                        self.metric_dropped.labels(service=svc).inc()
                    else:
                        # Propagate correlation IDs
                        if "event_id" not in result:
                            result["event_id"] = str(uuid4())
                        if "parent_id" not in result:
                            result["parent_id"] = event_id

                        key = result.get("vendor_id") or result.get("id")
                        producer.produce(
                            topic=self.output_topic,
                            value=result,
                            key=str(key) if key else None,
                        )
                        self.metric_emitted.labels(service=svc).inc()

                    # Mark as processed only after process() returns cleanly
                    # (whether drop or emit).  If process() raised, we do NOT
                    # mark — the message stays retryable on the next restart.
                    await idem.mark_processed(svc, event_id)

                except Exception as e:
                    _log("error", "service.process_error",
                         service=svc, error=str(e), event_id=event_id)
                    self.metric_errors.labels(service=svc, error_type=type(e).__name__).inc()
                    self.metric_dlq.labels(service=svc).inc()
                    try:
                        from app.models.news import DLQMessage
                        dlq_msg = DLQMessage(
                            original_topic=self.input_topic,
                            error=str(e),
                            error_type=type(e).__name__,
                            event_id=event_id,
                            payload=raw,
                        )
                        producer.produce(
                            topic=f"{self.input_topic}.dlq",
                            value=dlq_msg.to_kafka_dict(),
                            key=event_id,
                        )
                    except Exception:
                        pass

                self._consumer.commit(msg)
                clear_event_context()

                # Refresh lag estimate periodically (non-blocking — cached)
                await self._update_lag(loop)

        except Exception as e:
            _log("error", "service.fatal_error", service=svc, error=str(e))
            self.metric_up.labels(service=svc).set(0)
            raise
        finally:
            if self._consumer:
                self._consumer.close()
            if self._redis_idem:
                # Closes the Redis client owned by the store.
                # Does NOT close the SQLite fallback (handled next line).
                await self._redis_idem.close()
            if self._idempotency:
                # SQLite store — used as Redis fallback and by execution engine
                # for order / loss tracking.
                self._idempotency.close()
            # Cancel heartbeat task and delete the key so the watchdog knows the
            # service stopped cleanly (rather than treating absence as a freeze).
            _heartbeat_task.cancel()
            try:
                await _heartbeat_task
            except asyncio.CancelledError:
                pass
            try:
                await _hb_redis_client.delete(
                    f"service:heartbeat:{self.service_name}"
                )
            except Exception:
                pass
            try:
                await _hb_redis_client.aclose()
            except Exception:
                pass
            await self.on_stop()
            _log("info", "service.stopped", service=svc)

    def stop(self) -> None:
        self._running = False
        _log("info", "service.stop_requested", service=self.service_name)
