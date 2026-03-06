#!/bin/bash
# ─────────────────────────────────────────────
#  Create all Redpanda topics
#  Run after: docker-compose up -d
#  Safe to re-run (idempotent)
# ─────────────────────────────────────────────

set -e

BROKER="localhost:19092"
RPK="docker exec trading_redpanda rpk"

echo "⏳ Waiting for Redpanda to be ready..."
until docker exec trading_redpanda rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; do
  sleep 2
done
echo "✅ Redpanda is healthy"

echo ""
echo "📦 Creating topics..."

create_topic() {
  local NAME=$1
  local PARTITIONS=$2
  local RETENTION_MS=$3   # -1 = infinite

  $RPK topic create "$NAME" \
    --partitions "$PARTITIONS" \
    --replicas 1 \
    --topic-config "retention.ms=$RETENTION_MS" \
    --topic-config "compression.type=lz4" \
    2>&1 | grep -v "TOPIC_ALREADY_EXISTS" || true

  echo "  ✓ $NAME (partitions=$PARTITIONS, retention=${RETENTION_MS}ms)"
}

# news pipeline topics
create_topic "news.raw"          3   86400000     # 24h
create_topic "news.raw.dlq"      1   604800000    # 7d (DLQ kept longer)
create_topic "news.normalized"   3   172800000    # 48h
create_topic "news.deduped"      3   172800000    # 48h
create_topic "news.enriched"     3   604800000    # 7d
create_topic "news.summarized"   3   604800000    # 7d

# calendar events
create_topic "events.calendar"   1   2592000000   # 30d

# price data
create_topic "prices.bars"       6   604800000    # 7d

echo ""
echo "📋 Current topics:"
$RPK topic list

echo ""
echo "✅ All topics created successfully"
