"""
Base class for all pipeline consumer services.
Handles: Kafka consume loop, metrics, graceful shutdown, DLQ routing,
         idempotency (SQLite), and correlation ID propagation.
"""
from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Histogram, Gauge, start_http_server

from app.config import settings
from app.logging import get_logger, bind_event_context, clear_event_context

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
        self._idempotency = None
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
        if self._idempotency is None:
            from app.idempotency import IdempotencyStore
            self._idempotency = IdempotencyStore()
        return self._idempotency

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
        idem = self._get_idempotency_store()

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

                # ── Idempotency check ──────────────────────────────────
                already = idem.check_and_mark(svc, event_id)
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

        except Exception as e:
            _log("error", "service.fatal_error", service=svc, error=str(e))
            self.metric_up.labels(service=svc).set(0)
            raise
        finally:
            if self._consumer:
                self._consumer.close()
            if self._idempotency:
                self._idempotency.close()
            await self.on_stop()
            _log("info", "service.stopped", service=svc)

    def stop(self) -> None:
        self._running = False
        _log("info", "service.stop_requested", service=self.service_name)
