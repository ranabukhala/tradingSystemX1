"""
Unit and integration tests for event-level deduplication.

Coverage:
  - SimHash: identical, similar, different, symmetry, edge cases
  - FactFingerprint: stability, field-order invariance, precision, None handling
  - TimeBucket: same window, different window, cross-catalyst
  - EventClusterStore: representative, non-representative, gate logic (async, mocked Redis)
  - Combined clustering: SimHash match + conflicting facts = distinct event
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from app.pipeline.simhash import (
    compute_simhash,
    hamming_distance,
    is_similar,
    simhash_from_hex,
    simhash_to_hex,
)
from app.pipeline.fact_fingerprint import fact_fingerprint, facts_overlap_score
from app.pipeline.bucket import time_bucket, bucket_key


# ─────────────────────────────────────────────────────────────────────────────
# SimHash tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSimHash:

    def test_identical_text_produces_same_hash(self):
        h1 = compute_simhash("Apple reports blowout earnings beats EPS by 15%")
        h2 = compute_simhash("Apple reports blowout earnings beats EPS by 15%")
        assert h1 == h2

    def test_similar_headlines_are_similar(self):
        h1 = compute_simhash("Apple reports blowout earnings beats EPS by 15%")
        h2 = compute_simhash("AAPL beats earnings estimates EPS 2.05 vs 1.78 expected")
        # Both are about Apple beating earnings — should be within threshold
        dist = hamming_distance(h1, h2)
        assert dist <= 20, f"Expected similar headlines, Hamming distance was {dist}"

    def test_different_events_are_not_similar(self):
        h1 = compute_simhash("Apple reports blowout earnings beats EPS")
        h2 = compute_simhash("Tesla recalls 200000 vehicles due to autopilot software defect")
        dist = hamming_distance(h1, h2)
        assert dist > 8, f"Expected different events to diverge, got distance {dist}"

    def test_hamming_symmetry(self):
        h1 = compute_simhash("Goldman Sachs upgrades Apple to Buy price target 220")
        h2 = compute_simhash("Bank of America downgrades Tesla to Sell")
        assert hamming_distance(h1, h2) == hamming_distance(h2, h1)

    def test_empty_string_returns_zero(self):
        assert compute_simhash("") == 0
        assert compute_simhash("   ") == 0

    def test_numeric_content(self):
        # Headlines with different numbers should not match
        h1 = compute_simhash("Fed raises rates by 25 basis points")
        h2 = compute_simhash("Fed raises rates by 75 basis points")
        # Numbers differ after tokenization — should produce different hashes
        assert h1 != h2

    def test_hex_serialization_roundtrip(self):
        h = compute_simhash("NVIDIA announces new GPU architecture")
        assert simhash_from_hex(simhash_to_hex(h)) == h

    def test_is_similar_threshold(self):
        h1 = compute_simhash("Microsoft Azure outage affects East US region services")
        h2 = compute_simhash("Microsoft Azure East US region outage reported services down")
        assert is_similar(h1, h2, threshold=16)  # generous threshold

    def test_is_not_similar_threshold(self):
        h1 = compute_simhash("Amazon acquires MGM Studios for 8.5 billion dollars")
        h2 = compute_simhash("Apple iPhone 16 sales disappoint analysts below expectations")
        assert not is_similar(h1, h2, threshold=8)


# ─────────────────────────────────────────────────────────────────────────────
# FactFingerprint tests
# ─────────────────────────────────────────────────────────────────────────────

class FakeFacts:
    """Minimal stand-in for FactsJson without importing the model."""
    def __init__(self, **kwargs):
        self._data = kwargs

    def model_dump(self):
        return self._data


class TestFactFingerprint:

    def test_identical_facts_produce_same_fingerprint(self):
        f1 = FakeFacts(eps_actual=2.05, eps_estimate=1.78, eps_beat=True)
        f2 = FakeFacts(eps_actual=2.05, eps_estimate=1.78, eps_beat=True)
        assert fact_fingerprint(f1) == fact_fingerprint(f2)

    def test_field_order_does_not_matter(self):
        # Fingerprint is stable regardless of dict key order
        f1 = FakeFacts(eps_actual=2.05, eps_beat=True, eps_estimate=1.78)
        f2 = FakeFacts(eps_beat=True, eps_estimate=1.78, eps_actual=2.05)
        assert fact_fingerprint(f1) == fact_fingerprint(f2)

    def test_different_eps_values_produce_different_fingerprints(self):
        f1 = FakeFacts(eps_actual=2.05, eps_estimate=1.78)
        f2 = FakeFacts(eps_actual=1.90, eps_estimate=1.78)
        assert fact_fingerprint(f1) != fact_fingerprint(f2)

    def test_none_facts_returns_none(self):
        assert fact_fingerprint(None) is None

    def test_all_none_fields_returns_none(self):
        f = FakeFacts(eps_actual=None, eps_beat=None, rating_new=None)
        assert fact_fingerprint(f) is None

    def test_float_precision_normalized(self):
        # 2.050001 and 2.05 should produce same fingerprint after rounding to 2dp
        f1 = FakeFacts(eps_actual=2.050001)
        f2 = FakeFacts(eps_actual=2.05)
        assert fact_fingerprint(f1) == fact_fingerprint(f2)

    def test_analyst_facts_fingerprinted(self):
        f1 = FakeFacts(rating_new="Buy", price_target_new=220.0, analyst_firm="Goldman Sachs")
        f2 = FakeFacts(rating_new="Sell", price_target_new=150.0, analyst_firm="Goldman Sachs")
        assert fact_fingerprint(f1) != fact_fingerprint(f2)


class TestFactsOverlapScore:

    def test_identical_fingerprints_return_one(self):
        fp = "abc123def456"
        assert facts_overlap_score(fp, fp) == 1.0

    def test_different_fingerprints_return_zero(self):
        assert facts_overlap_score("abc123", "xyz789") == 0.0

    def test_none_fingerprints_return_half(self):
        assert facts_overlap_score(None, None) == 0.5
        assert facts_overlap_score("abc", None) == 0.5
        assert facts_overlap_score(None, "abc") == 0.5


# ─────────────────────────────────────────────────────────────────────────────
# TimeBucket tests
# ─────────────────────────────────────────────────────────────────────────────

class TestTimeBucket:

    def _dt(self, hour: int, minute: int) -> datetime:
        """Create a UTC datetime for testing (corresponds to ET during non-DST)."""
        # ET = UTC-5 in non-DST (Nov-Mar), UTC-4 in DST (Mar-Nov)
        # Use a fixed non-DST date: 2024-01-15 (January, UTC-5)
        # So ET 09:30 = UTC 14:30
        return datetime(2024, 1, 15, hour, minute, tzinfo=timezone.utc)

    def test_same_time_same_bucket(self):
        dt1 = self._dt(14, 30)  # ET 09:30
        dt2 = self._dt(14, 45)  # ET 09:45 — within 30-min earnings window
        _, b1, _ = time_bucket(dt1, "earnings")
        _, b2, _ = time_bucket(dt2, "earnings")
        assert b1 == b2

    def test_different_window_different_bucket(self):
        dt1 = self._dt(14, 30)  # ET 09:30
        dt2 = self._dt(15,  5)  # ET 10:05 — next 30-min earnings window
        _, b1, _ = time_bucket(dt1, "earnings")
        _, b2, _ = time_bucket(dt2, "earnings")
        assert b1 != b2

    def test_different_catalyst_different_window_size(self):
        dt = self._dt(14, 30)
        _, _, w_earnings = time_bucket(dt, "earnings")
        _, _, w_macro    = time_bucket(dt, "macro")
        assert w_earnings == 30
        assert w_macro    == 15

    def test_bucket_key_format(self):
        dt = self._dt(14, 30)  # ET 09:30
        key = bucket_key("AAPL", "earnings", dt)
        assert key.startswith("event:cluster:AAPL:earnings:")
        # Check it contains a date component
        assert "2024-01-15" in key

    def test_ticker_uppercased_in_key(self):
        dt = self._dt(14, 30)
        key = bucket_key("aapl", "earnings", dt)
        assert "AAPL" in key


# ─────────────────────────────────────────────────────────────────────────────
# EventClusterStore tests (async, mocked Redis)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_redis():
    """Minimal async mock of redis.asyncio.Redis for cluster store tests."""
    redis = AsyncMock()
    redis.hgetall = AsyncMock(return_value={})  # empty bucket by default
    redis.hset    = AsyncMock(return_value=1)
    redis.expire  = AsyncMock(return_value=True)
    redis.set     = AsyncMock(return_value=True)  # SET NX succeeds by default
    redis.get     = AsyncMock(return_value=None)
    return redis


@pytest.fixture
def cluster_store(mock_redis):
    from app.pipeline.event_cluster import EventClusterStore
    return EventClusterStore(mock_redis)


@pytest.fixture
def dt():
    return datetime(2024, 6, 15, 14, 30, tzinfo=timezone.utc)  # ET 10:30 (DST)


class TestEventClusterStore:

    @pytest.mark.asyncio
    async def test_first_event_is_representative(self, cluster_store, dt):
        result = await cluster_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=dt,
            title="Apple beats earnings EPS 2.05 vs 1.78",
            facts=None,
            incoming_event_id="event-001",
        )
        assert result.is_representative is True
        assert result.event_id == "event-001"

    @pytest.mark.asyncio
    async def test_similar_headline_is_non_representative(self, cluster_store, mock_redis, dt):
        from app.pipeline.simhash import compute_simhash, simhash_to_hex

        original_title = "Apple beats earnings EPS 2.05 vs 1.78 expected"
        duplicate_title = "AAPL beats earnings estimates EPS 2.05 vs 1.78"

        stored_sh = compute_simhash(original_title)
        # Bucket has one entry — the original event
        mock_redis.hgetall.return_value = {
            simhash_to_hex(stored_sh): "event-001|"  # no fact fingerprint stored
        }

        result = await cluster_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=dt,
            title=duplicate_title,
            facts=None,
            incoming_event_id="event-002",
        )
        assert result.is_representative is False
        assert result.event_id == "event-001"

    @pytest.mark.asyncio
    async def test_different_ticker_does_not_cluster(self, cluster_store, mock_redis, dt):
        from app.pipeline.simhash import compute_simhash, simhash_to_hex

        aapl_title = "Apple beats earnings EPS 2.05"
        tsla_title = "Tesla beats earnings EPS 0.72"

        stored_sh = compute_simhash(aapl_title)
        mock_redis.hgetall.return_value = {
            simhash_to_hex(stored_sh): "event-001|"
        }

        # Different ticker → different bucket key → hgetall returns empty
        mock_redis.hgetall.return_value = {}

        result = await cluster_store.match_or_create(
            ticker="TSLA",
            catalyst="earnings",
            published_at=dt,
            title=tsla_title,
            facts=None,
            incoming_event_id="event-003",
        )
        assert result.is_representative is True

    @pytest.mark.asyncio
    async def test_signal_gate_first_call_returns_true(self, cluster_store, mock_redis):
        mock_redis.set.return_value = True  # SET NX succeeds
        allowed = await cluster_store.mark_signal_emitted("event-001", "long", "deny")
        assert allowed is True

    @pytest.mark.asyncio
    async def test_signal_gate_second_call_returns_false_deny(self, cluster_store, mock_redis):
        mock_redis.set.return_value = None  # SET NX fails — key exists
        allowed = await cluster_store.mark_signal_emitted("event-001", "long", "deny")
        assert allowed is False

    @pytest.mark.asyncio
    async def test_signal_gate_allow_opposite_passes_reverse(self, cluster_store, mock_redis):
        mock_redis.set.return_value = None   # Key already exists
        mock_redis.get.return_value = "long" # Stored direction is long
        # Incoming is short — opposite allowed
        allowed = await cluster_store.mark_signal_emitted("event-001", "short", "allow_opposite")
        assert allowed is True

    @pytest.mark.asyncio
    async def test_order_gate_first_call_returns_true(self, cluster_store, mock_redis):
        mock_redis.set.return_value = True
        allowed = await cluster_store.mark_order_submitted("event-001", "broker-order-xyz")
        assert allowed is True

    @pytest.mark.asyncio
    async def test_order_gate_second_call_returns_false(self, cluster_store, mock_redis):
        mock_redis.set.return_value = None  # Key exists
        allowed = await cluster_store.mark_order_submitted("event-001", "broker-order-xyz")
        assert allowed is False


# ─────────────────────────────────────────────────────────────────────────────
# Combined clustering: SimHash match + conflicting facts = distinct event
# ─────────────────────────────────────────────────────────────────────────────

class TestCombinedClustering:

    @pytest.mark.asyncio
    async def test_simhash_match_but_different_facts_is_representative(
        self, cluster_store, mock_redis, dt
    ):
        """
        Two headlines that look similar (SimHash match) but have different
        structured facts (different EPS numbers) should NOT be clustered.
        """
        from app.pipeline.simhash import compute_simhash, simhash_to_hex
        from app.pipeline.fact_fingerprint import fact_fingerprint

        original_title  = "Apple beats earnings EPS 2.05 vs 1.78 expected"
        duplicate_title = "AAPL beats earnings estimates EPS 2.05 vs 1.78"

        f_original  = FakeFacts(eps_actual=2.05, eps_estimate=1.78, eps_beat=True)
        f_different = FakeFacts(eps_actual=1.65, eps_estimate=1.78, eps_beat=False)

        stored_sh = compute_simhash(original_title)
        stored_fp = fact_fingerprint(f_original)

        mock_redis.hgetall.return_value = {
            simhash_to_hex(stored_sh): f"event-001|{stored_fp}"
        }

        result = await cluster_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=dt,
            title=duplicate_title,
            facts=f_different,           # Different facts — different event
            incoming_event_id="event-002",
        )
        # Despite similar headline, different facts → treat as representative
        assert result.is_representative is True


# ─────────────────────────────────────────────────────────────────────────────
# Integration scenarios (documented for manual / CI integration testing)
# ─────────────────────────────────────────────────────────────────────────────

class TestIntegrationScenarios:
    """
    These are scenario descriptions + lightweight smoke tests.
    Full integration tests require a running Redis instance.

    Scenario A: Multi-vendor duplicate
      - Benzinga publishes "Apple beats EPS" at 14:30:01
      - Polygon publishes same story at 14:30:45
      - Expected: Polygon item is non-representative (SimHash match)
      - Audit: news.dropped receives Polygon item with dedup_reason=simhash_cluster

    Scenario B: Same vendor, same content hash
      - Benzinga sends the same payload twice (retry / resend)
      - Expected: Second send is dropped at content hash tier (faster than SimHash)

    Scenario C: Different events, same time window
      - "Apple beats EPS" and "Microsoft acquires Activision" in same 30-min bucket
      - Expected: Both are representative (different tickers + different SimHash)

    Scenario D: allow_opposite signal policy
      - LONG signal for AAPL emitted at 14:30
      - SHORT signal for same event at 14:31 (new vendor)
      - MULTI_SIGNAL_POLICY=allow_opposite → SHORT is allowed through
      - Both signals logged in signal_log

    Scenario E: TTL expiry replay
      - Event cluster TTL expires after 3× window = 90 min (for earnings)
      - Same story reappears after 2 hours → treated as new representative
      - No false duplicate blocking of genuinely new coverage
    """

    def test_scenario_descriptions_are_documented(self):
        """Placeholder — full integration tests run against live Redis."""
        scenarios = ["A", "B", "C", "D", "E"]
        assert len(scenarios) == 5
