"""
Connector integration tests.
These test the parsing logic with mock API responses.
No real API keys needed for unit tests.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from app.models.news import NewsSource, RawNewsRecord, NormalizedRecord
from app.models.events import EarningsEvent, EarningsTime, MacroEvent


# ── Benzinga Parser Tests ──────────────────────────────────────────────────────

class TestBenzingaParser:

    def setup_method(self):
        """Create connector without triggering __init__ API key validation."""
        with patch("app.config.settings") as mock_settings:
            mock_settings.benzinga_api_key = "test_key"
            mock_settings.benzinga_poll_interval_seconds = 30
            from app.connectors.benzinga import BenzingaConnector
            self.connector = BenzingaConnector.__new__(BenzingaConnector)
            self.connector._last_seen_id = None
            self.connector._session = None

    def test_parse_article_basic(self):
        article = {
            "id": 12345,
            "title": "Apple Reports Record Q4 Earnings",
            "teaser": "Apple beat estimates with strong iPhone sales",
            "url": "https://www.benzinga.com/news/12345",
            "created": 1704067200,  # Unix timestamp
            "author": "John Doe",
            "stocks": [{"name": "AAPL"}, {"name": "MSFT"}],
            "channels": [{"name": "earnings"}],
        }
        record = self.connector._parse_article(article)

        assert record.source == NewsSource.BENZINGA
        assert record.vendor_id == "12345"
        assert record.title == "Apple Reports Record Q4 Earnings"
        assert "AAPL" in record.raw_tickers
        assert "MSFT" in record.raw_tickers
        assert "earnings" in record.raw_categories
        assert record.published_at.tzinfo is not None  # Must be timezone-aware

    def test_parse_article_iso_timestamp(self):
        article = {
            "id": 99999,
            "title": "Test Article",
            "url": "https://benzinga.com/test",
            "created": "2024-01-15T08:30:00Z",
            "stocks": [],
            "channels": [],
        }
        record = self.connector._parse_article(article)
        assert record.published_at.year == 2024
        assert record.published_at.month == 1

    def test_parse_article_empty_tickers(self):
        article = {
            "id": 1,
            "title": "General Market Update",
            "url": "https://benzinga.com/market",
            "created": 1704067200,
            "stocks": [],
            "channels": [{"name": "markets"}],
        }
        record = self.connector._parse_article(article)
        assert record.raw_tickers == []

    def test_parse_article_analyst_category(self):
        article = {
            "id": 2,
            "title": "Goldman Upgrades Tesla to Buy",
            "url": "https://benzinga.com/analyst",
            "created": 1704067200,
            "stocks": [{"name": "TSLA"}],
            "channels": [{"name": "analyst-ratings"}],
        }
        record = self.connector._parse_article(article)
        assert "analyst" in record.raw_categories


# ── NormalizedRecord Hash Tests ───────────────────────────────────────────────

class TestNormalizedRecord:

    def test_content_hash_stable(self):
        """Same title+snippet must always produce same hash."""
        h1 = NormalizedRecord.compute_hash("Apple Beats Earnings", "Strong iPhone sales drove results")
        h2 = NormalizedRecord.compute_hash("Apple Beats Earnings", "Strong iPhone sales drove results")
        assert h1 == h2

    def test_content_hash_case_insensitive(self):
        """Hash should be case-insensitive."""
        h1 = NormalizedRecord.compute_hash("APPLE BEATS EARNINGS", "STRONG IPHONE SALES")
        h2 = NormalizedRecord.compute_hash("apple beats earnings", "strong iphone sales")
        assert h1 == h2

    def test_content_hash_different_content(self):
        """Different content must produce different hashes."""
        h1 = NormalizedRecord.compute_hash("Apple Beats Earnings", "Good results")
        h2 = NormalizedRecord.compute_hash("Tesla Misses Earnings", "Bad results")
        assert h1 != h2

    def test_content_hash_none_snippet(self):
        """None snippet should not cause error."""
        h = NormalizedRecord.compute_hash("Some Title", None)
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex


# ── Earnings Whispers Parser Tests ─────────────────────────────────────────────

class TestEarningsWhispersParser:

    def setup_method(self):
        with patch("app.config.settings") as mock_settings:
            mock_settings.earnings_whispers_api_key = "test_key"
            mock_settings.earnings_lookahead_days = 30
            mock_settings.benzinga_poll_interval_seconds = 30
            from app.connectors.earnings_whispers import EarningsWhispersConnector
            self.connector = EarningsWhispersConnector.__new__(EarningsWhispersConnector)
            self.connector._session = None

    def test_parse_earnings_event_bmo(self):
        item = {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "date": "2024-01-25",
            "time": "BMO",
            "fiscal_quarter": "Q1 2024",
            "whisper_eps": 2.10,
            "consensus_eps": 2.08,
        }
        event = self.connector._parse_event(item)
        assert event.ticker == "AAPL"
        assert event.event_time == EarningsTime.BMO
        assert event.whisper_eps == 2.10
        assert event.consensus_eps == 2.08

    def test_parse_earnings_event_amc(self):
        item = {
            "ticker": "TSLA",
            "name": "Tesla Inc.",
            "date": "2024-01-24",
            "time": "After Market Close",
        }
        event = self.connector._parse_event(item)
        assert event.event_time == EarningsTime.AMC

    def test_parse_earnings_event_unknown_time(self):
        item = {
            "ticker": "NVDA",
            "name": "NVIDIA",
            "date": "2024-02-21",
            "time": "",
        }
        event = self.connector._parse_event(item)
        assert event.event_time == EarningsTime.UNKNOWN

    def test_parse_float_none(self):
        assert EarningsWhispersConnector._parse_float(None) is None
        assert EarningsWhispersConnector._parse_float("") is None
        assert EarningsWhispersConnector._parse_float("1.50") == 1.50
        assert EarningsWhispersConnector._parse_float(2.0) == 2.0


# ── Model Validation Tests ────────────────────────────────────────────────────

class TestRawNewsRecord:

    def test_valid_record(self):
        record = RawNewsRecord(
            source=NewsSource.BENZINGA,
            vendor_id="12345",
            published_at=datetime(2024, 1, 15, 8, 30, tzinfo=timezone.utc),
            url="https://example.com/article",
            title="Test Article",
            snippet="Test snippet",
        )
        assert record.source == NewsSource.BENZINGA
        assert isinstance(record.id, UUID)

    def test_kafka_roundtrip(self):
        """Serialize to dict and back without data loss."""
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
        assert restored.source == NewsSource.POLYGON
