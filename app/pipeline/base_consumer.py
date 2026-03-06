"""
Base class for all pipeline consumer services.
Handles: Kafka consume loop, metrics, graceful shutdown, DLQ routing.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Histogram, Gauge, start_http_server


def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


class BaseConsumer(ABC):
    """
    Base for all pipeline stage consumers.

    Subclasses implement:
      - input_topic: str
      - output_topic: str
      - service_name: str
      - process(record: dict) -> dict | None
          Return processed record to emit, or None to skip/drop.
    """

    def __init__(self) -> None:
        self._running = False
        self._consumer: Consumer | None = None
        self._setup_metrics()

        metrics_port = int(os.environ.get("METRICS_PORT", 8000))
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
        """
        Process one record.
        Return enriched/transformed dict to emit downstream, or None to drop.
        Raise exception to route to DLQ.
        """
        ...

    async def on_start(self) -> None:
        """Optional hook called once before consume loop starts."""
        pass

    async def on_stop(self) -> None:
        """Optional hook called after consume loop ends."""
        pass

    def _setup_metrics(self) -> None:
        svc = self.service_name
        self.metric_consumed = Counter(
            f"pipeline_consumed_total", "Messages consumed", ["service"])
        self.metric_emitted = Counter(
            f"pipeline_emitted_total", "Messages emitted", ["service"])
        self.metric_dropped = Counter(
            f"pipeline_dropped_total", "Messages dropped (dedup/filter)", ["service"])
        self.metric_errors = Counter(
            f"pipeline_errors_total", "Processing errors", ["service", "error_type"])
        self.metric_process_latency = Histogram(
            f"pipeline_process_latency_seconds", "Per-message processing time", ["service"])
        self.metric_lag = Gauge(
            f"pipeline_consumer_lag", "Estimated consumer lag", ["service"])
        self.metric_up = Gauge(
            f"pipeline_up", "1 if service healthy", ["service"])

    def _make_consumer(self) -> Consumer:
        from app.config import settings
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

        try:
            while self._running:
                # Poll in thread to not block event loop
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

                try:
                    with self.metric_process_latency.labels(service=svc).time():
                        result = await self.process(raw)

                    if result is None:
                        self.metric_dropped.labels(service=svc).inc()
                    else:
                        key = result.get("vendor_id") or result.get("id")
                        producer.produce(
                            topic=self.output_topic,
                            value=result,
                            key=str(key) if key else None,
                        )
                        self.metric_emitted.labels(service=svc).inc()

                except Exception as e:
                    _log("error", "service.process_error",
                         service=svc, error=str(e), msg_key=str(msg.key()))
                    self.metric_errors.labels(service=svc, error_type=type(e).__name__).inc()
                    # Route to DLQ
                    try:
                        producer.produce_to_dlq(
                            original_topic=self.input_topic,
                            value=raw,
                            error=str(e),
                        )
                    except Exception:
                        pass

                self._consumer.commit(msg)

        except Exception as e:
            _log("error", "service.fatal_error", service=svc, error=str(e))
            self.metric_up.labels(service=svc).set(0)
            raise
        finally:
            if self._consumer:
                self._consumer.close()
            await self.on_stop()
            _log("info", "service.stopped", service=svc)

    def stop(self) -> None:
        self._running = False
        _log("info", "service.stop_requested", service=self.service_name)
