"""
DLQ Inspector & Replayer CLI.

Usage:
  python -m app.dlq inspect <topic>           List DLQ messages
  python -m app.dlq inspect <topic> --limit 5 Limit output
  python -m app.dlq replay <topic> --event-id <id>  Replay one message
  python -m app.dlq replay <topic> --all      Replay all messages
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import click
from confluent_kafka import Consumer, KafkaError, Producer

from app.config import settings


def _make_consumer(topic: str, group_suffix: str = "dlq-inspector") -> Consumer:
    return Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": f"{settings.kafka_group_id}-{group_suffix}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
    })


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "all",
    })


@click.group()
def cli():
    """DLQ inspection and replay tools."""
    pass


@cli.command()
@click.argument("topic")
@click.option("--limit", default=50, help="Max messages to show")
@click.option("--event-id", default=None, help="Filter by event_id")
def inspect(topic: str, limit: int, event_id: str | None):
    """List DLQ messages with failure reasons."""
    # Ensure topic ends with .dlq
    if not topic.endswith(".dlq"):
        topic = f"{topic}.dlq"

    consumer = _make_consumer(topic, "dlq-inspector-peek")
    consumer.subscribe([topic])

    click.echo(f"Inspecting DLQ topic: {topic}")
    click.echo("-" * 80)

    count = 0
    empty_polls = 0

    while count < limit and empty_polls < 10:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
                continue
            click.echo(f"Error: {msg.error()}", err=True)
            continue

        empty_polls = 0

        try:
            value = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError:
            click.echo(f"[corrupt message at offset {msg.offset()}]")
            continue

        msg_event_id = value.get("event_id", "?")

        if event_id and msg_event_id != event_id:
            continue

        count += 1
        original = value.get("original_topic", "?")
        error = value.get("error", "?")
        error_type = value.get("error_type", "?")
        failed_at = value.get("failed_at", "?")

        click.echo(f"[{count}] offset={msg.offset()} partition={msg.partition()}")
        click.echo(f"  event_id:      {msg_event_id}")
        click.echo(f"  original_topic: {original}")
        click.echo(f"  error_type:    {error_type}")
        click.echo(f"  error:         {error[:200]}")
        click.echo(f"  failed_at:     {failed_at}")

        payload = value.get("payload", {})
        if payload:
            title = payload.get("title", payload.get("vendor_id", "?"))
            click.echo(f"  payload.title: {str(title)[:100]}")
        click.echo("")

    consumer.close()
    click.echo(f"Total: {count} messages shown")


@cli.command()
@click.argument("topic")
@click.option("--event-id", default=None, help="Replay specific event by ID")
@click.option("--all", "replay_all", is_flag=True, help="Replay all DLQ messages")
@click.option("--dry-run", is_flag=True, help="Show what would be replayed without producing")
def replay(topic: str, event_id: str | None, replay_all: bool, dry_run: bool):
    """Replay DLQ messages back to the original topic."""
    if not event_id and not replay_all:
        click.echo("Must specify --event-id or --all", err=True)
        sys.exit(1)

    if not topic.endswith(".dlq"):
        topic = f"{topic}.dlq"

    consumer = _make_consumer(topic, "dlq-replayer")
    consumer.subscribe([topic])
    producer = _make_producer() if not dry_run else None

    replayed = 0
    empty_polls = 0

    click.echo(f"Replaying from {topic}" + (" (dry-run)" if dry_run else ""))

    while empty_polls < 10:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
                continue
            continue

        empty_polls = 0

        try:
            value = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError:
            continue

        msg_event_id = value.get("event_id")
        if event_id and msg_event_id != event_id:
            continue

        original_topic = value.get("original_topic", "")
        payload = value.get("payload", {})

        if not original_topic or not payload:
            click.echo(f"  skip offset={msg.offset()}: missing original_topic or payload")
            continue

        # Bump retry count
        payload["_replay_count"] = value.get("retry_count", 0) + 1
        payload["_replayed_at"] = datetime.now(timezone.utc).isoformat()

        if dry_run:
            click.echo(f"  [dry-run] would replay event_id={msg_event_id} -> {original_topic}")
        else:
            producer.produce(
                topic=original_topic,
                value=json.dumps(payload, default=str).encode("utf-8"),
                key=msg_event_id.encode("utf-8") if msg_event_id else None,
            )
            producer.flush()
            click.echo(f"  replayed event_id={msg_event_id} -> {original_topic}")

        replayed += 1

        if event_id:
            break

    consumer.close()
    click.echo(f"Replayed {replayed} message(s)")


if __name__ == "__main__":
    cli()
