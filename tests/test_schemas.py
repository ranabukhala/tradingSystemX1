"""
Schema validation tests — verify Pydantic models accept/reject correctly.
Covers every pipeline stage model + DLQ envelope.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from app.models.news import (
    BaseMessage,
    CatalystType,
    DedupedRecord,
    DLQMessage,
    EnrichedRecord,
    FactsJson,
    NormalizedRecord,
    NewsSource,
    RawNewsRecord,
    SummarizedRecord,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def load_fixture(name: str) -> dict:
    with open(os.path.join(FIXTURES_DIR, name)) as f:
        return json.load(f)


# ── BaseMessage ──────────────────────────────────────────────────────────────

class TestBaseMessage:

    def test_event_id_generated(self):
        msg = BaseMessage()
        assert msg.event_id
        assert len(msg.event_id) == 36  # UUID format

    def test_schema_version_default(self):
        msg = BaseMessage()
        assert msg.schema_version == "1"

    def test_parent_id_optional(self):
        msg = BaseMessage(parent_id="abc-123")
        assert msg.parent_id == "abc-123"

    def test_roundtrip(self):
        msg = BaseMessage(event_id="test-id", parent_id="parent-id")
        d = msg.to_kafka_dict()
        restored = BaseMessage.from_kafka_dict(d)
        assert restored.event_id == "test-id"
        assert restored.parent_id == "parent-id"


# ── RawNewsRecord ────────────────────────────────────────────────────────────

class TestRawNewsRecord:

    def test_from_fixture(self):
        data = load_fixture("news_raw.json")
        record = RawNewsRecord.from_kafka_dict(data)
        assert record.source == NewsSource.BENZINGA
        assert record.vendor_id == "bz_12345"
        assert record.event_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert "AAPL" in record.raw_tickers

    def test_valid_record(self):
        record = RawNewsRecord(
            source=NewsSource.BENZINGA,
            vendor_id="12345",
            published_at=datetime(2024, 1, 15, 8, 30, tzinfo=timezone.utc),
            url="https://example.com/article",
            title="Test Article",
        )
        assert isinstance(record.id, UUID)
        assert record.event_id  # Auto-generated

    def test_kafka_roundtrip(self):
        record = RawNewsRecord(
            source=NewsSource.POLYGON,
            vendor_id="abc123",
            published_at=datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc),
            url="https://polygon.io/news/abc123",
            title="Market Opens Higher",
            raw_tickers=["SPY", "QQQ"],
        )
        d = record.to_kafka_dict()
        restored = RawNewsRecord.from_kafka_dict(d)
        assert restored.vendor_id == record.vendor_id
        assert restored.raw_tickers == ["SPY", "QQQ"]
        assert restored.event_id == record.event_id

    def test_missing_required_field_fails(self):
        with pytest.raises(Exception):
            RawNewsRecord(
                source=NewsSource.BENZINGA,
                # Missing vendor_id, published_at, url, title
            )

    def test_invalid_source_fails(self):
        with pytest.raises(Exception):
            RawNewsRecord(
                source="invalid_source",
                vendor_id="x",
                published_at=datetime.now(timezone.utc),
                url="http://x.com",
                title="x",
            )


# ── NormalizedRecord ─────────────────────────────────────────────────────────

class TestNormalizedRecord:

    def test_content_hash_stable(self):
        h1 = NormalizedRecord.compute_hash("Apple Beats Earnings", "Strong sales")
        h2 = NormalizedRecord.compute_hash("Apple Beats Earnings", "Strong sales")
        assert h1 == h2

    def test_content_hash_case_insensitive(self):
        h1 = NormalizedRecord.compute_hash("APPLE BEATS", "STRONG")
        h2 = NormalizedRecord.compute_hash("apple beats", "strong")
        assert h1 == h2

    def test_content_hash_different(self):
        h1 = NormalizedRecord.compute_hash("Apple Beats", "Good")
        h2 = NormalizedRecord.compute_hash("Tesla Misses", "Bad")
        assert h1 != h2

    def test_content_hash_none_snippet(self):
        h = NormalizedRecord.compute_hash("Title", None)
        assert len(h) == 64


# ── DLQMessage ───────────────────────────────────────────────────────────────

class TestDLQMessage:

    def test_dlq_creation(self):
        dlq = DLQMessage(
            original_topic="news.raw",
            error="Validation failed: missing title",
            error_type="ValidationError",
            event_id="test-123",
            payload={"vendor_id": "x"},
        )
        assert dlq.original_topic == "news.raw"
        assert dlq.error_type == "ValidationError"
        assert dlq.retry_count == 0

    def test_dlq_roundtrip(self):
        dlq = DLQMessage(
            original_topic="news.enriched",
            error="timeout",
            event_id="ev-1",
            payload={"title": "test"},
        )
        d = dlq.to_kafka_dict()
        restored = DLQMessage.from_kafka_dict(d)
        assert restored.original_topic == "news.enriched"
        assert restored.event_id == "ev-1"


# ── FactsJson ────────────────────────────────────────────────────────────────

class TestFactsJson:

    def test_all_optional(self):
        facts = FactsJson()
        assert facts.eps_beat is None
        assert facts.deal_price is None

    def test_earnings_facts(self):
        facts = FactsJson(eps_beat=True, eps_actual=2.10, eps_estimate=2.08)
        assert facts.eps_beat is True
        assert facts.eps_actual == 2.10


# ── SummarizedRecord ─────────────────────────────────────────────────────────

class TestSummarizedRecord:

    def test_full_record(self):
        record = SummarizedRecord(
            id=uuid4(),
            source=NewsSource.BENZINGA,
            vendor_id="test",
            published_at=datetime.now(timezone.utc),
            received_at=datetime.now(timezone.utc),
            url="http://test.com",
            canonical_url="http://test.com",
            title="Test",
            content_hash="abc123",
            t1_summary="Beat earnings",
            impact_day=0.8,
            impact_swing=0.6,
            facts_json=FactsJson(eps_beat=True),
        )
        d = record.to_kafka_dict()
        assert d["impact_day"] == 0.8
        assert d["facts_json"]["eps_beat"] is True
        assert d["event_id"]  # Auto-generated
