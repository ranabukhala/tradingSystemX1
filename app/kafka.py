"""
Kafka/Redpanda producer and consumer wrappers.
Thin layer over confluent-kafka with:
  - JSON serialization
  - Prometheus metrics
  - Dead letter queue routing
  - Graceful shutdown
"""
from __future__ import annotations

import json
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from prometheus_client import Counter, Histogram

from app.config import settings
from app.logging import get_logger

log = get_logger(__name__)

# ── Metrics ───────────────────────────────────────────────────────────────────

kafka_messages_produced = Counter(
    "kafka_messages_produced_total",
    "Total messages produced to Kafka",
    ["topic"],
)
kafka_messages_consumed = Counter(
    "kafka_messages_consumed_total",
    "Total messages consumed from Kafka",
    ["topic", "group_id"],
)
kafka_produce_errors = Counter(
    "kafka_produce_errors_total",
    "Total Kafka produce errors",
    ["topic"],
)
kafka_consume_errors = Counter(
    "kafka_consume_errors_total",
    "Total Kafka consume errors",
    ["topic"],
)
kafka_produce_latency = Histogram(
    "kafka_produce_latency_seconds",
    "Kafka produce latency",
    ["topic"],
)


# ── Producer ──────────────────────────────────────────────────────────────────

class KafkaProducer:
    """Thread-safe JSON producer with metrics and DLQ support."""

    def __init__(self) -> None:
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 10,          # Small batching window
            "batch.size": 65536,
        })
        log.info("kafka.producer.created", brokers=settings.kafka_bootstrap_servers)

    def produce(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
    ) -> None:
        """
        Produce a JSON message to a topic.
        Uses fire-and-forget with delivery callback for metrics.
        """
        def delivery_callback(err, msg):
            if err:
                log.error("kafka.produce.failed", topic=topic, error=str(err))
                kafka_produce_errors.labels(topic=topic).inc()
            else:
                kafka_messages_produced.labels(topic=topic).inc()

        try:
            self._producer.produce(
                topic=topic,
                value=json.dumps(value, default=str).encode("utf-8"),
                key=key.encode("utf-8") if key else None,
                callback=delivery_callback,
            )
            self._producer.poll(0)  # Trigger delivery callbacks without blocking
        except KafkaException as e:
            log.error("kafka.produce.exception", topic=topic, error=str(e))
            kafka_produce_errors.labels(topic=topic).inc()
            raise

    def produce_to_dlq(self, original_topic: str, value: dict[str, Any], error: str) -> None:
        """Send failed message to dead letter queue."""
        dlq_topic = f"{original_topic}.dlq"
        dlq_payload = {"original_topic": original_topic, "error": error, "payload": value}
        self.produce(dlq_topic, dlq_payload)
        log.warning("kafka.dlq.sent", original_topic=original_topic, dlq=dlq_topic)

    def flush(self, timeout: float = 10.0) -> None:
        """Flush all pending messages."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            log.warning("kafka.flush.incomplete", remaining=remaining)

    def close(self) -> None:
        self.flush()
        log.info("kafka.producer.closed")


# ── Consumer ──────────────────────────────────────────────────────────────────

class KafkaConsumer:
    """JSON consumer with metrics and graceful shutdown."""

    def __init__(self, topics: list[str], group_id: str | None = None) -> None:
        self._group_id = group_id or settings.kafka_group_id
        self._topics = topics
        self._running = False

        self._consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": self._group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        })
        self._consumer.subscribe(topics)
        log.info("kafka.consumer.subscribed", topics=topics, group_id=self._group_id)

    def consume(
        self,
        handler: Callable[[str, dict], None],
        timeout: float = 1.0,
    ) -> None:
        """
        Blocking consume loop. Call handler(topic, message_dict) for each message.
        Run in a thread or async executor.
        """
        self._running = True
        log.info("kafka.consumer.started", topics=self._topics)

        try:
            while self._running:
                msg = self._consumer.poll(timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("kafka.consume.error", error=str(msg.error()))
                    kafka_consume_errors.labels(topic=msg.topic()).inc()
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    kafka_messages_consumed.labels(
                        topic=msg.topic(), group_id=self._group_id
                    ).inc()
                    handler(msg.topic(), value)
                except json.JSONDecodeError as e:
                    log.error("kafka.consume.json_error", topic=msg.topic(), error=str(e))
                    kafka_consume_errors.labels(topic=msg.topic()).inc()
                except Exception as e:
                    log.error("kafka.consume.handler_error", topic=msg.topic(), error=str(e))
                    kafka_consume_errors.labels(topic=msg.topic()).inc()

        finally:
            self._consumer.close()
            log.info("kafka.consumer.closed")

    def stop(self) -> None:
        self._running = False


# ── Module-level singleton producer ──────────────────────────────────────────

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer()
    return _producer
