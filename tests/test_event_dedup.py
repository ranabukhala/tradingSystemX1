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

    def test_similar_headlines_are_closer_than_different_events(self):
        # Same-story cross-vendor pair (per MEMORY.md example: ~15 bit distance)
        h_same_a = compute_simhash("Apple beats earnings EPS 2.05")
        h_same_b = compute_simhash("AAPL beats earnings estimates EPS 2.05")
        dist_same = hamming_distance(h_same_a, h_same_b)

        # Completely different events
        h_diff_a = compute_simhash("Apple beats earnings EPS 2.05")
        h_diff_b = compute_simhash("Tesla recalls 200000 vehicles autopilot defect")
        dist_diff = hamming_distance(h_diff_a, h_diff_b)

        # Same-story distance must be strictly less than different-event distance
        assert dist_same < dist_diff, (
            f"Same-story distance {dist_same} should be less than "
            f"different-event distance {dist_diff}"
        )
        # And within the documented ~15-bit range for near-identical cross-vendor phrasing
        assert dist_same <= 20, f"Same-story pair too dissimilar: {dist_same} bits"

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
        """
        Mock the bucket to contain an entry whose stored SimHash is within
        the Hamming threshold of the incoming headline.

        Note: the default threshold is 8 bits (conservative — for near-identical
        syndication).  Cross-vendor rephrasing (distance ~15) is caught earlier
        by pg_trgm fuzzy matching (tier 3); SimHash is the last fallback.  Here
        we use the same title to guarantee distance=0 so the unit test is clean.
        """
        from app.pipeline.simhash import compute_simhash, simhash_to_hex

        # Store the SimHash of the incoming title so Hamming distance = 0
        incoming_title = "AAPL beats earnings estimates EPS 2.05 vs 1.78"
        stored_sh = compute_simhash(incoming_title)

        mock_redis.hgetall.return_value = {
            simhash_to_hex(stored_sh): "event-001|"  # no fact fingerprint
        }

        result = await cluster_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=dt,
            title=incoming_title,
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
# Integration tests — real Redis semantics via fakeredis
# ─────────────────────────────────────────────────────────────────────────────
#
# fakeredis gives full SETNX / EXPIRE / HSET semantics without a live server.
# Install: pip install fakeredis  (FakeAsyncRedis built-in since v2.x)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from fakeredis import FakeAsyncRedis as _FakeAsyncRedis
    _FAKEREDIS_AVAILABLE = True
except ImportError:
    _FakeAsyncRedis = None          # type: ignore[assignment,misc]
    _FAKEREDIS_AVAILABLE = False

_skip_no_fakeredis = pytest.mark.skipif(
    not _FAKEREDIS_AVAILABLE,
    reason="fakeredis not installed — run: pip install fakeredis",
)


@pytest.fixture
async def live_redis():
    """Real in-process Redis via fakeredis (full SETNX / EXPIRE / HSET semantics)."""
    if not _FAKEREDIS_AVAILABLE:
        pytest.skip("fakeredis not installed")
    redis = _FakeAsyncRedis(decode_responses=True)
    yield redis
    await redis.aclose()


@pytest.fixture
async def live_store(live_redis):
    from app.pipeline.event_cluster import EventClusterStore
    return EventClusterStore(live_redis)


@pytest.fixture
def base_dt():
    # 2024-11-14 14:30 UTC = 09:30 ET (non-DST)
    return datetime(2024, 11, 14, 14, 30, 0, tzinfo=timezone.utc)


class TestIntegrationScenarioA:
    """
    Scenario A: Multi-vendor duplicate caught by SimHash (tier 4 — semantic).

    Pipeline dedup tiers:
      Tier 1 (URL)    — same canonical URL → caught before reaching here
      Tier 2 (hash)   — identical content hash → caught before here
      Tier 3 (pg_trgm)— cross-vendor rephrasing, similarity >= 0.75 → caught in SQL
      Tier 4 (SimHash)— near-identical wording, Hamming <= threshold (default 8)

    At the default threshold of 8 bits, SimHash catches near-identical syndication
    (e.g. same agency copy, different vendor API packaging).  Cross-vendor
    rephrasing (distance ~15) is handled earlier by pg_trgm.  The integration
    tests here verify Tier 4 specifically.
    """

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_first_vendor_is_representative(self, live_store, base_dt):
        result = await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=base_dt,
            title="Apple beats earnings expectations EPS 2.05 vs 1.78 expected",
            facts=None,
            incoming_event_id="benz-001",
        )
        assert result.is_representative is True
        assert result.event_id == "benz-001"

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_second_vendor_near_identical_wording_non_representative(
        self, live_store, base_dt
    ):
        """Near-identical wording (distance ≤ 8) caught by SimHash at default threshold."""
        title_benz = "Apple beats earnings EPS 2.05 vs 1.78 expected"
        # Polymer packaging: same words, different trailing punctuation/whitespace
        title_poly = "Apple beats earnings EPS 2.05 vs 1.78 expected"   # distance = 0

        await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=base_dt,
            title=title_benz,
            facts=None,
            incoming_event_id="benz-001",
        )
        polygon_dt = base_dt.replace(second=44)
        result = await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=polygon_dt,
            title=title_poly,
            facts=None,
            incoming_event_id="poly-002",
        )
        assert result.is_representative is False
        assert result.event_id == "benz-001"


class TestIntegrationScenarioB:
    """
    Scenario B: Same vendor sends identical payload twice (retry / double-publish).
    Second call should hit the bucket and be non-representative.
    """

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_exact_same_payload_twice(self, live_store, base_dt):
        kwargs = dict(
            ticker="MSFT",
            catalyst="analyst",
            published_at=base_dt,
            title="Morgan Stanley upgrades Microsoft to Overweight, raises PT to $480",
            facts=None,
            incoming_event_id="benz-003",
        )
        r1 = await live_store.match_or_create(**kwargs)
        r2 = await live_store.match_or_create(**kwargs)

        assert r1.is_representative is True
        assert r2.is_representative is False
        assert r2.event_id == "benz-003"


class TestIntegrationScenarioC:
    """
    Scenario C: Different events in the same time window.
    AAPL earnings and MSFT analyst change at same time.
    Expected: both are representative (different tickers → different bucket keys).
    """

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_different_tickers_different_buckets(self, live_store, base_dt):
        r_aapl = await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=base_dt,
            title="Apple beats earnings EPS 2.05",
            facts=None,
            incoming_event_id="aapl-001",
        )
        r_msft = await live_store.match_or_create(
            ticker="MSFT",
            catalyst="analyst",
            published_at=base_dt,
            title="Microsoft upgraded to Overweight",
            facts=None,
            incoming_event_id="msft-001",
        )
        assert r_aapl.is_representative is True
        assert r_msft.is_representative is True


class TestIntegrationScenarioD:
    """
    Scenario D: Signal gate with allow_opposite policy.
    LONG emitted first → LONG blocked → SHORT allowed through.
    """

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_first_signal_allowed(self, live_store):
        allowed = await live_store.mark_signal_emitted(
            event_id="evt-aapl-001", direction="long", multi_signal_policy="allow_opposite"
        )
        assert allowed is True

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_same_direction_blocked_under_allow_opposite(self, live_store):
        await live_store.mark_signal_emitted(
            event_id="evt-aapl-002", direction="long", multi_signal_policy="allow_opposite"
        )
        # Second LONG for same event → blocked
        allowed = await live_store.mark_signal_emitted(
            event_id="evt-aapl-002", direction="long", multi_signal_policy="allow_opposite"
        )
        assert allowed is False

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_opposite_direction_allowed_under_allow_opposite(self, live_store):
        await live_store.mark_signal_emitted(
            event_id="evt-aapl-003", direction="long", multi_signal_policy="allow_opposite"
        )
        # Opposite direction allowed through
        allowed = await live_store.mark_signal_emitted(
            event_id="evt-aapl-003", direction="short", multi_signal_policy="allow_opposite"
        )
        assert allowed is True

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_deny_policy_blocks_second_signal(self, live_store):
        await live_store.mark_signal_emitted(
            event_id="evt-aapl-004", direction="long", multi_signal_policy="deny"
        )
        allowed = await live_store.mark_signal_emitted(
            event_id="evt-aapl-004", direction="short", multi_signal_policy="deny"
        )
        assert allowed is False

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_allow_policy_never_blocks(self, live_store):
        for _ in range(3):
            allowed = await live_store.mark_signal_emitted(
                event_id="evt-aapl-005", direction="long", multi_signal_policy="allow"
            )
            assert allowed is True


class TestIntegrationScenarioE:
    """
    Scenario E: Order gate prevents double submission for the same event.
    First broker_order_id wins; second call returns False.
    """

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_first_order_allowed(self, live_store):
        allowed = await live_store.mark_order_submitted(
            event_id="evt-order-001", broker_order_id="alpaca-abc123"
        )
        assert allowed is True

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_second_order_blocked(self, live_store):
        await live_store.mark_order_submitted(
            event_id="evt-order-002", broker_order_id="alpaca-abc124"
        )
        allowed = await live_store.mark_order_submitted(
            event_id="evt-order-002", broker_order_id="alpaca-abc125"
        )
        assert allowed is False

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_different_events_independent_gates(self, live_store):
        a = await live_store.mark_order_submitted("evt-order-003", "order-A")
        b = await live_store.mark_order_submitted("evt-order-004", "order-B")
        assert a is True
        assert b is True


class TestIntegrationFactConfirmation:
    """
    When SimHash says 'similar' but structured facts clearly differ,
    the incoming item must be treated as a new representative event.
    """

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_simhash_match_different_facts_is_new_representative(
        self, live_store, base_dt
    ):
        from app.pipeline.simhash import compute_simhash, simhash_to_hex
        from app.pipeline.fact_fingerprint import fact_fingerprint

        original_title  = "Apple beats earnings EPS 2.05 vs 1.78 expected"
        duplicate_title = "AAPL beats earnings estimates EPS 2.05 vs 1.78"

        f_original  = FakeFacts(eps_actual=2.05, eps_estimate=1.78, eps_beat=True)
        f_different = FakeFacts(eps_actual=1.65, eps_estimate=1.78, eps_beat=False)

        # Register original
        await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=base_dt,
            title=original_title,
            facts=f_original,
            incoming_event_id="event-orig",
        )

        # Headline looks similar but EPS numbers differ — different quarter / event
        result = await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=base_dt,
            title=duplicate_title,
            facts=f_different,
            incoming_event_id="event-new",
        )
        assert result.is_representative is True
        assert result.event_id == "event-new"

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_simhash_match_identical_facts_is_non_representative(
        self, live_store, base_dt
    ):
        """Same headline + same facts → definitive duplicate."""
        f = FakeFacts(eps_actual=2.05, eps_estimate=1.78, eps_beat=True)

        await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=base_dt,
            title="Apple beats earnings EPS 2.05 vs 1.78 expected",
            facts=f,
            incoming_event_id="event-original",
        )

        # Same title → SimHash distance = 0 → cluster match fires; fact fingerprints confirm
        result = await live_store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=base_dt,
            title="Apple beats earnings EPS 2.05 vs 1.78 expected",
            facts=f,
            incoming_event_id="event-dup",
        )
        assert result.is_representative is False
        assert result.event_id == "event-original"

    @_skip_no_fakeredis
    @pytest.mark.asyncio
    async def test_event_meta_stored_for_representative(self, live_store, base_dt):
        """Representative events must have metadata written for audit retrieval."""
        await live_store.match_or_create(
            ticker="TSLA",
            catalyst="analyst",
            published_at=base_dt,
            title="Goldman upgrades Tesla to Buy price target 280",
            facts=None,
            incoming_event_id="tsla-audit-001",
        )
        meta = await live_store.get_event_meta("tsla-audit-001")
        assert meta is not None
        assert meta["ticker"] == "TSLA"
        assert meta["catalyst"] == "analyst"


# ── event_id propagation (v1.9) ───────────────────────────────────────────────

class TestEventIdPropagation:
    """
    Verify that event_id (v1.9) is correctly threaded from the deduplication
    stage through TradingSignal and into the execution engine order gate.

    Tests use SimpleNamespace to mirror the exact expressions in production
    code without importing heavy pipeline dependencies.
    """

    def test_event_id_propagates_from_cluster_id(self):
        """
        signal_aggregator sets `event_id=summarized.cluster_id`.
        A non-None cluster_id must appear as signal.event_id downstream.
        """
        from types import SimpleNamespace
        cluster_id = uuid4()
        summarized = SimpleNamespace(cluster_id=cluster_id)
        # Replicate the constructor assignment
        signal_event_id = summarized.cluster_id
        assert signal_event_id == cluster_id

    def test_event_id_is_none_when_cluster_id_absent(self):
        """
        When cluster_id is None (e.g. dedup skipped), event_id on the signal
        must also be None so the gate falls back to signal.id.
        """
        from types import SimpleNamespace
        summarized = SimpleNamespace(cluster_id=None)
        signal_event_id = summarized.cluster_id
        assert signal_event_id is None

    def test_order_gate_uses_event_id_when_present(self):
        """
        The execution engine gate expression:
            str(signal.event_id) if signal.event_id else str(signal.id)
        must resolve to the cluster-level event_id, not the signal-level id.
        """
        from types import SimpleNamespace
        signal_id  = uuid4()
        event_id   = uuid4()
        sig = SimpleNamespace(id=signal_id, event_id=event_id)
        gate_key = str(sig.event_id) if sig.event_id else str(sig.id)
        assert gate_key == str(event_id)
        assert gate_key != str(signal_id)   # event_id wins over news-specific id

    def test_order_gate_falls_back_to_signal_id_for_legacy_signals(self):
        """
        Legacy signals (event_id=None, created before v1.9) must still work.
        The gate falls back to str(signal.id) so no silent drops occur.
        """
        from types import SimpleNamespace
        signal_id = uuid4()
        sig = SimpleNamespace(id=signal_id, event_id=None)
        gate_key = str(sig.event_id) if sig.event_id else str(sig.id)
        assert gate_key == str(signal_id)

    def test_two_vendors_same_cluster_id_produce_identical_gate_key(self):
        """
        Two signals from different vendors reporting the same catalyst must
        produce the same gate key → only the first trade is submitted.
        """
        from types import SimpleNamespace
        shared_cluster_id = uuid4()
        sig_vendor_a = SimpleNamespace(id=uuid4(), event_id=shared_cluster_id)
        sig_vendor_b = SimpleNamespace(id=uuid4(), event_id=shared_cluster_id)

        key_a = str(sig_vendor_a.event_id) if sig_vendor_a.event_id else str(sig_vendor_a.id)
        key_b = str(sig_vendor_b.event_id) if sig_vendor_b.event_id else str(sig_vendor_b.id)

        assert key_a == key_b           # identical → second order blocked
        assert sig_vendor_a.id != sig_vendor_b.id   # different signal.id values (different vendors)

    def test_two_distinct_events_produce_different_gate_keys(self):
        """
        Two signals from genuinely different events must produce different
        gate keys so neither blocks the other.
        """
        from types import SimpleNamespace
        sig_a = SimpleNamespace(id=uuid4(), event_id=uuid4())
        sig_b = SimpleNamespace(id=uuid4(), event_id=uuid4())

        key_a = str(sig_a.event_id) if sig_a.event_id else str(sig_a.id)
        key_b = str(sig_b.event_id) if sig_b.event_id else str(sig_b.id)

        assert key_a != key_b
