"""
Tests for signal staleness guard and Kafka consumer lag blocking.

Coverage:
  TestComputeMaxAge              — per-catalyst / session / route policy maths
  TestCheckStaleness             — core check_staleness() logic
  TestStalenessSessionModifiers  — how session context scales the window
  TestStalenessRouteModifiers    — fast/slow route type scaling
  TestStalenessEdgeCases         — missing timestamps, future timestamps, disabled guard
  TestEnvVarOverrides            — STALENESS_MAX_AGE_OVERRIDE_SECONDS, per-catalyst overrides
  TestStalenessDictHelper        — staleness_from_signal_dict() convenience wrapper
  TestKafkaLagHelpers            — BaseConsumer._lag_too_high / _compute_lag_sync
  TestTimestampPropagation       — TradingSignal carries timestamps through aggregator
  TestPretradeFilterStaleness    — pretrade_filter blocks stale signals before I/O
  TestExecutionStaleness         — execution_engine blocks stale + lag-backlogged signals
  TestPipelineLatencyMath        — staleness age/limit ordering properties
"""
from __future__ import annotations

# ── Mock Docker-only dependencies BEFORE any pipeline imports ─────────────────
# confluent_kafka and prometheus_client are installed only in the Docker image.
# We stub them here so that the local test runner (py -3.12) can import all
# pipeline modules without a full container environment.

import sys
from unittest.mock import MagicMock as _MagicMock

if "confluent_kafka" not in sys.modules:
    _ck = _MagicMock()

    # TopicPartition must behave like the real class for lag tests
    class _TopicPartition:
        OFFSET_INVALID = -1001
        def __init__(self, topic="", partition=0, offset=-1001):
            self.topic = topic
            self.partition = partition
            self.offset = offset

    _ck.TopicPartition  = _TopicPartition
    _ck.OFFSET_INVALID  = -1001
    _ck.Consumer        = type("Consumer", (), {})
    _ck.KafkaError      = type("KafkaError", (Exception,), {"_PARTITION_EOF": None})
    _ck.KafkaException  = type("KafkaException", (Exception,), {})
    sys.modules["confluent_kafka"] = _ck

if "prometheus_client" not in sys.modules:
    _prom = _MagicMock()
    _prom.Counter   = lambda *a, **kw: _MagicMock()
    _prom.Histogram = lambda *a, **kw: _MagicMock()
    _prom.Gauge     = lambda *a, **kw: _MagicMock()
    _prom.start_http_server = _MagicMock()
    sys.modules["prometheus_client"] = _prom

# ── Standard imports ──────────────────────────────────────────────────────────

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from app.pipeline.staleness import (
    StalenessResult,
    _BASE_MAX_AGE,
    check_staleness,
    compute_max_age,
    staleness_from_signal_dict,
)

# ── helpers ───────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _ago(seconds: float) -> datetime:
    return _now() - timedelta(seconds=seconds)

def _future(seconds: float) -> datetime:
    return _now() + timedelta(seconds=seconds)

def _make_metrics_mock():
    """Return a dict of mock Prometheus metrics for BaseConsumer subclasses."""
    m = _MagicMock()
    return {
        "metric_consumed":         m,
        "metric_emitted":          m,
        "metric_dropped":          m,
        "metric_errors":           m,
        "metric_dlq":              m,
        "metric_dedup_skipped":    m,
        "metric_process_latency":  m,
        "metric_lag":              m,
        "metric_up":               m,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TestComputeMaxAge
# ═══════════════════════════════════════════════════════════════════════════════

class TestComputeMaxAge:
    """compute_max_age returns base × session × route."""

    def test_earnings_baseline(self):
        assert compute_max_age("earnings", "intraday", None) == 120.0

    def test_ma_baseline(self):
        assert compute_max_age("ma", "intraday", None) == 300.0

    def test_regulatory_baseline(self):
        assert compute_max_age("regulatory", "intraday", None) == 180.0

    def test_analyst_baseline(self):
        assert compute_max_age("analyst", "intraday", None) == 900.0

    def test_macro_baseline(self):
        assert compute_max_age("macro", "intraday", None) == 240.0

    def test_filing_baseline(self):
        assert compute_max_age("filing", "intraday", None) == 600.0

    def test_legal_baseline(self):
        assert compute_max_age("legal", "intraday", None) == 1800.0

    def test_other_baseline(self):
        assert compute_max_age("other", "intraday", None) == 600.0

    def test_unknown_catalyst_defaults_to_other(self):
        assert compute_max_age("unicorn", "intraday", None) == 600.0

    def test_premarket_doubles_window(self):
        base = compute_max_age("earnings", "intraday", None)
        assert compute_max_age("earnings", "premarket", None) == base * 2.0

    def test_open_halves_window(self):
        base = compute_max_age("earnings", "intraday", None)
        assert compute_max_age("earnings", "open", None) == base * 0.5

    def test_overnight_triples_window(self):
        base = compute_max_age("earnings", "intraday", None)
        assert compute_max_age("earnings", "overnight", None) == base * 3.0

    def test_afterhours_increases_window(self):
        base = compute_max_age("earnings", "intraday", None)
        assert compute_max_age("earnings", "afterhours", None) == base * 1.5

    def test_fast_route_tightens_window(self):
        base = compute_max_age("earnings", "intraday", None)
        assert compute_max_age("earnings", "intraday", "fast") == base * 0.8

    def test_slow_route_loosens_window(self):
        base = compute_max_age("earnings", "intraday", None)
        assert compute_max_age("earnings", "intraday", "slow") == base * 1.2

    def test_combined_session_and_route(self):
        # premarket × fast: 120 × 2.0 × 0.8 = 192
        assert compute_max_age("earnings", "premarket", "fast") == pytest.approx(192.0)

    def test_case_insensitive_catalyst(self):
        assert compute_max_age("EARNINGS", "intraday", None) == compute_max_age("earnings", "intraday", None)

    def test_case_insensitive_session(self):
        assert compute_max_age("earnings", "INTRADAY", None) == compute_max_age("earnings", "intraday", None)


# ═══════════════════════════════════════════════════════════════════════════════
# TestCheckStaleness
# ═══════════════════════════════════════════════════════════════════════════════

class TestCheckStaleness:
    """Core check_staleness() logic — boundary conditions and return shape."""

    def test_fresh_earnings_passes(self):
        r = check_staleness(_ago(60), "earnings", "intraday")
        assert not r.is_stale
        assert r.reason == "ok"
        assert r.age_seconds == pytest.approx(60.0, abs=1.0)
        assert r.max_age_seconds == 120.0

    def test_fresh_analyst_passes(self):
        r = check_staleness(_ago(300), "analyst", "intraday")
        assert not r.is_stale

    def test_zero_age_always_fresh(self):
        assert not check_staleness(_now(), "earnings", "open").is_stale

    def test_exactly_at_max_age_is_not_stale(self):
        max_age = compute_max_age("earnings", "intraday", None)
        r = check_staleness(_ago(max_age), "earnings", "intraday")
        assert not r.is_stale

    def test_one_second_past_max_age_is_stale(self):
        max_age = compute_max_age("earnings", "intraday", None)
        r = check_staleness(_ago(max_age + 1), "earnings", "intraday")
        assert r.is_stale
        assert r.reason == "signal_stale"

    def test_stale_earnings_blocked(self):
        r = check_staleness(_ago(300), "earnings", "intraday")
        assert r.is_stale
        assert r.age_seconds > r.max_age_seconds

    def test_stale_ma_blocked(self):
        r = check_staleness(_ago(600), "ma", "intraday")
        assert r.is_stale

    def test_open_session_earnings_stale_at_90s(self):
        # open: 120 × 0.5 = 60s limit
        r = check_staleness(_ago(90), "earnings", "open")
        assert r.is_stale
        assert r.max_age_seconds == 60.0

    def test_premarket_earnings_not_stale_at_200s(self):
        # premarket: 120 × 2.0 = 240s limit
        r = check_staleness(_ago(200), "earnings", "premarket")
        assert not r.is_stale
        assert r.max_age_seconds == 240.0

    def test_result_carries_context(self):
        r = check_staleness(_ago(50), "analyst", "afterhours", "fast")
        assert r.catalyst_type == "analyst"
        assert r.session == "afterhours"
        assert r.route_type == "fast"

    def test_stale_reason_code(self):
        r = check_staleness(_ago(9999), "earnings", "intraday")
        assert r.reason == "signal_stale"

    def test_fresh_reason_code(self):
        r = check_staleness(_ago(1), "earnings", "intraday")
        assert r.reason == "ok"


# ═══════════════════════════════════════════════════════════════════════════════
# TestStalenessEdgeCases
# ═══════════════════════════════════════════════════════════════════════════════

class TestStalenessEdgeCases:
    """Failure modes, malformed inputs, feature flag, clock skew."""

    def test_none_timestamp_fails_open(self):
        r = check_staleness(None, "earnings", "intraday")
        assert not r.is_stale
        assert r.reason == "no_published_at"

    def test_unparseable_string_fails_open(self):
        r = check_staleness("not-a-date", "earnings", "intraday")
        assert not r.is_stale
        assert r.reason == "unparseable_timestamp"

    def test_naive_datetime_treated_as_utc(self):
        # Build a naive datetime that represents "60 seconds ago in UTC".
        # datetime.now() would return LOCAL time (e.g. UTC+2), which would
        # skew the comparison; replace(tzinfo=None) drops tz while keeping
        # the UTC value so the guard treats it as UTC correctly.
        now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        ts_naive = now_utc_naive - timedelta(seconds=60)
        r = check_staleness(ts_naive, "earnings", "intraday")
        assert not r.is_stale
        assert r.age_seconds == pytest.approx(60.0, abs=2.0)

    def test_iso_string_parsing(self):
        r = check_staleness(_ago(60).isoformat(), "earnings", "intraday")
        assert not r.is_stale
        assert r.age_seconds == pytest.approx(60.0, abs=2.0)

    def test_iso_z_suffix_parsing(self):
        ts_str = _ago(60).strftime("%Y-%m-%dT%H:%M:%SZ")
        r = check_staleness(ts_str, "earnings", "intraday")
        assert not r.is_stale

    def test_future_timestamp_clock_skew_fails_open(self):
        r = check_staleness(_future(600), "earnings", "intraday")
        assert not r.is_stale
        assert r.reason == "future_timestamp_clock_skew"

    def test_slightly_future_timestamp_not_clock_skew(self):
        # 1 second ahead — within 300s skew tolerance, not blocked as clock skew
        r = check_staleness(_future(1), "earnings", "intraday")
        assert r.reason != "future_timestamp_clock_skew"

    def test_guard_disabled_always_passes(self):
        # Patch the module global directly — no reload needed
        with patch("app.pipeline.staleness._ENABLED", False):
            r = check_staleness(_ago(99999), "earnings", "intraday")
        assert not r.is_stale
        assert r.reason == "staleness_disabled"

    def test_guard_enabled_after_disable_patch_restored(self):
        # Ensure patching doesn't bleed into subsequent tests
        with patch("app.pipeline.staleness._ENABLED", False):
            pass  # context exited, _ENABLED restored
        r = check_staleness(_ago(9999), "earnings", "intraday")
        assert r.is_stale  # guard is back on

    def test_now_override_for_testing(self):
        published = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        fake_now  = datetime(2024, 1, 1, 12, 16, 40, tzinfo=timezone.utc)  # +1000s
        r = check_staleness(published, "earnings", "intraday", now=fake_now)
        assert r.age_seconds == pytest.approx(1000.0, abs=0.1)
        assert r.is_stale   # 1000 > 120


# ═══════════════════════════════════════════════════════════════════════════════
# TestEnvVarOverrides
# ═══════════════════════════════════════════════════════════════════════════════

class TestEnvVarOverrides:
    """STALENESS_MAX_AGE_OVERRIDE_SECONDS and per-catalyst env vars."""

    def test_flat_override_applies_to_all_catalysts(self):
        # Patch the module global directly — avoids reload contamination
        with patch("app.pipeline.staleness._FLAT_OVERRIDE", 999):
            for catalyst in ("earnings", "ma", "analyst", "macro", "other"):
                age = compute_max_age(catalyst, "intraday", None)
                assert age == 999.0, f"Expected 999 for {catalyst}, got {age}"

    def test_flat_override_removed_after_patch(self):
        with patch("app.pipeline.staleness._FLAT_OVERRIDE", 999):
            pass
        # After patch exits, normal policy applies
        assert compute_max_age("earnings", "intraday", None) == 120.0

    def test_per_catalyst_base_age_override(self):
        modified = {**_BASE_MAX_AGE, "earnings": 45}
        with patch("app.pipeline.staleness._BASE_MAX_AGE", modified):
            age = compute_max_age("earnings", "intraday", None)
            assert age == 45.0
            # Other catalysts unaffected
            assert compute_max_age("ma", "intraday", None) == 300.0

    def test_per_catalyst_override_does_not_affect_others(self):
        modified = {**_BASE_MAX_AGE, "earnings": 30}
        with patch("app.pipeline.staleness._BASE_MAX_AGE", modified):
            assert compute_max_age("analyst", "intraday", None) == 900.0
            assert compute_max_age("ma",      "intraday", None) == 300.0


# ═══════════════════════════════════════════════════════════════════════════════
# TestStalenessDictHelper
# ═══════════════════════════════════════════════════════════════════════════════

class TestStalenessDictHelper:
    """staleness_from_signal_dict() extracts correct fields from a signal dict."""

    def _signal(self, age: float = 60, catalyst: str = "earnings",
                session: str = "intraday", route: str | None = None) -> dict:
        return {
            "ticker": "AAPL",
            "direction": "long",
            "catalyst_type": catalyst,
            "session_context": session,
            "route_type": route,
            "news_published_at": _ago(age).isoformat(),
        }

    def test_fresh_signal_passes(self):
        r = staleness_from_signal_dict(self._signal(age=50))
        assert not r.is_stale

    def test_stale_signal_blocked(self):
        r = staleness_from_signal_dict(self._signal(age=300))
        assert r.is_stale

    def test_missing_published_at_key_fails_open(self):
        d = {"ticker": "AAPL", "catalyst_type": "earnings", "session_context": "intraday"}
        r = staleness_from_signal_dict(d)
        assert not r.is_stale
        assert r.reason == "no_published_at"

    def test_null_published_at_fails_open(self):
        d = {**self._signal(), "news_published_at": None}
        r = staleness_from_signal_dict(d)
        assert not r.is_stale

    def test_route_type_propagated(self):
        r = staleness_from_signal_dict(self._signal(age=50, route="fast"))
        assert r.route_type == "fast"

    def test_missing_catalyst_defaults_to_other(self):
        d = {"ticker": "AAPL", "session_context": "intraday",
             "news_published_at": _ago(50).isoformat()}
        r = staleness_from_signal_dict(d)
        assert r.catalyst_type == "other"

    def test_missing_session_defaults_to_intraday(self):
        d = {"ticker": "AAPL", "catalyst_type": "earnings",
             "news_published_at": _ago(50).isoformat()}
        r = staleness_from_signal_dict(d)
        assert r.session == "intraday"

    def test_open_session_from_dict(self):
        r = staleness_from_signal_dict(self._signal(age=90, session="open"))
        assert r.is_stale  # open × 120 = 60s limit; 90 > 60


# ═══════════════════════════════════════════════════════════════════════════════
# TestKafkaLagHelpers
# ═══════════════════════════════════════════════════════════════════════════════

class TestKafkaLagHelpers:
    """BaseConsumer._lag_too_high and _compute_lag_sync (lag calculation logic)."""

    def _make_service(self, lag: int = 0):
        """Build a minimal BaseConsumer subclass without calling __init__."""
        from app.pipeline.base_consumer import BaseConsumer

        class _Svc(BaseConsumer):
            @property
            def service_name(self): return "test_svc"
            @property
            def input_topic(self): return "test.in"
            @property
            def output_topic(self): return "test.out"
            async def process(self, record): return None

        svc = _Svc.__new__(_Svc)
        svc._running = False
        svc._consumer = None
        svc._idempotency = None
        svc._current_lag = lag
        svc._last_lag_check = 0.0
        # Mock all Prometheus metric objects
        for attr, val in _make_metrics_mock().items():
            setattr(svc, attr, val)
        return svc

    # ── _lag_too_high ─────────────────────────────────────────────────────────

    def test_lag_too_high_false_when_zero(self):
        assert not self._make_service(0)._lag_too_high(50)

    def test_lag_too_high_false_when_below_threshold(self):
        assert not self._make_service(30)._lag_too_high(50)

    def test_lag_too_high_false_at_exact_threshold(self):
        assert not self._make_service(50)._lag_too_high(50)   # strictly greater

    def test_lag_too_high_true_when_above_threshold(self):
        assert self._make_service(51)._lag_too_high(50)

    def test_lag_too_high_true_high_value(self):
        assert self._make_service(10000)._lag_too_high(50)

    # ── _compute_lag_sync ─────────────────────────────────────────────────────

    def test_compute_lag_no_consumer_returns_zero(self):
        svc = self._make_service()
        assert svc._compute_lag_sync() == 0

    def test_compute_lag_no_assignment_returns_zero(self):
        svc = self._make_service()
        svc._consumer = _MagicMock()
        svc._consumer.assignment.return_value = []
        assert svc._compute_lag_sync() == 0

    def test_compute_lag_single_partition(self):
        svc = self._make_service()
        tp = sys.modules["confluent_kafka"].TopicPartition("test.in", 0, offset=100)
        mc = _MagicMock()
        mc.assignment.return_value = [tp]
        mc.position.return_value   = [tp]
        mc.get_watermark_offsets.return_value = (0, 150)   # high = 150
        svc._consumer = mc
        assert svc._compute_lag_sync() == 50   # 150 - 100

    def test_compute_lag_multiple_partitions(self):
        svc = self._make_service()
        TP = sys.modules["confluent_kafka"].TopicPartition
        tp0 = TP("test.in", 0, offset=100)
        tp1 = TP("test.in", 1, offset=200)
        mc = _MagicMock()
        mc.assignment.return_value = [tp0, tp1]
        mc.position.return_value   = [tp0, tp1]

        def _watermarks(tp, timeout=2.0):
            return (0, 130) if tp.partition == 0 else (0, 210)

        mc.get_watermark_offsets.side_effect = _watermarks
        svc._consumer = mc
        assert svc._compute_lag_sync() == 40   # (130-100) + (210-200)

    def test_compute_lag_invalid_offset_skipped(self):
        svc = self._make_service()
        OFFSET_INVALID = sys.modules["confluent_kafka"].OFFSET_INVALID
        TP = sys.modules["confluent_kafka"].TopicPartition
        tp = TP("test.in", 0, offset=OFFSET_INVALID)  # not yet polled
        mc = _MagicMock()
        mc.assignment.return_value = [tp]
        mc.position.return_value   = [tp]
        svc._consumer = mc
        assert svc._compute_lag_sync() == 0

    def test_compute_lag_broker_error_returns_zero(self):
        svc = self._make_service()
        TP = sys.modules["confluent_kafka"].TopicPartition
        tp = TP("test.in", 0, offset=100)
        mc = _MagicMock()
        mc.assignment.return_value = [tp]
        mc.position.return_value   = [tp]
        mc.get_watermark_offsets.side_effect = RuntimeError("broker down")
        svc._consumer = mc
        # Should not raise; gracefully returns 0
        assert svc._compute_lag_sync() == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TestTimestampPropagation
# ═══════════════════════════════════════════════════════════════════════════════

class TestTimestampPropagation:
    """TradingSignal carries news_published_at, pipeline_entry_at, route_type."""

    def _make_signal(self, **kwargs):
        from app.signals.signal_aggregator import TradingSignal
        base = dict(
            ticker="AAPL",
            signal_type="event_driven",
            direction="long",
            conviction=0.75,
            catalyst_type="earnings",
            session_context="intraday",
            news_id=str(uuid4()),
            news_title="Apple Beats Earnings",
            decay_minutes=30,
            market_cap_tier="mega",
            float_sensitivity="normal",
            impact_day=0.8,
            impact_swing=0.55,
            source="benzinga",
        )
        base.update(kwargs)
        return TradingSignal(**base).to_kafka_dict()

    def test_signal_has_news_published_at_field(self):
        pub = _ago(60)
        d = self._make_signal(news_published_at=pub)
        assert d["news_published_at"] is not None

    def test_signal_has_pipeline_entry_at_field(self):
        d = self._make_signal(pipeline_entry_at=_ago(65))
        assert d["pipeline_entry_at"] is not None

    def test_signal_has_route_type_field(self):
        d = self._make_signal(route_type="fast")
        assert d["route_type"] == "fast"

    def test_slow_route_type(self):
        d = self._make_signal(route_type="slow")
        assert d["route_type"] == "slow"

    def test_missing_timestamps_serialize_as_none(self):
        d = self._make_signal()
        assert d.get("news_published_at") is None
        assert d.get("pipeline_entry_at") is None

    def test_sympathy_signal_inherits_timestamps(self):
        from app.signals.signal_aggregator import TradingSignal, build_sympathy_signal
        pub   = _ago(60)
        entry = _ago(65)
        primary = TradingSignal(
            ticker="AAPL",
            signal_type="event_driven",
            direction="long",
            conviction=0.75,
            catalyst_type="earnings",
            session_context="intraday",
            news_id=str(uuid4()),
            news_title="Apple Beats",
            decay_minutes=30,
            market_cap_tier="mega",
            float_sensitivity="normal",
            impact_day=0.8,
            impact_swing=0.55,
            source="benzinga",
            news_published_at=pub,
            pipeline_entry_at=entry,
            route_type="slow",
        )
        sym = build_sympathy_signal(primary, "MSFT")
        assert sym.news_published_at == pub
        assert sym.pipeline_entry_at == entry
        assert sym.route_type == "slow"


# ═══════════════════════════════════════════════════════════════════════════════
# TestPretradeFilterStaleness
# ═══════════════════════════════════════════════════════════════════════════════

class TestPretradeFilterStaleness:
    """PreTradeFilterService blocks stale signals before any filter I/O."""

    def _make_service(self):
        from app.filters.pretrade_filter import PreTradeFilterService
        svc = PreTradeFilterService.__new__(PreTradeFilterService)
        svc._redis       = _MagicMock()
        svc._http        = _MagicMock()
        svc._polygon_key = ""
        svc._current_lag = 0
        svc._last_lag_check = 0.0
        for attr, val in _make_metrics_mock().items():
            setattr(svc, attr, val)
        return svc

    def _record(self, age: float = 60, catalyst: str = "earnings",
                session: str = "intraday") -> dict:
        return {
            "ticker": "AAPL",
            "direction": "long",
            "conviction": 0.75,
            "catalyst_type": catalyst,
            "session_context": session,
            "news_published_at": _ago(age).isoformat(),
            "news_id": str(uuid4()),
            "route_type": None,
        }

    @pytest.mark.asyncio
    async def test_stale_signal_blocked_before_filter_tasks(self):
        """Stale signal dropped before regime/tech/options/squeeze are called."""
        svc = self._make_service()
        record = self._record(age=9999, catalyst="earnings")

        with patch("app.filters.pretrade_filter.get_current_regime") as mock_regime, \
             patch("app.filters.pretrade_filter.score_technicals")   as mock_tech, \
             patch("app.filters.pretrade_filter.score_options_flow") as mock_opts, \
             patch("app.filters.pretrade_filter.score_squeeze")      as mock_sq, \
             patch.object(svc, "_emit_blocked",       new=AsyncMock()), \
             patch.object(svc, "_log_staleness_block", new=AsyncMock()):

            result = await svc.process(record)

        assert result is None
        mock_regime.assert_not_called()
        mock_tech.assert_not_called()
        mock_opts.assert_not_called()
        mock_sq.assert_not_called()

    @pytest.mark.asyncio
    async def test_fresh_signal_proceeds_to_filters(self):
        """Fresh signal must reach the filter tasks."""
        svc = self._make_service()
        record = self._record(age=30, catalyst="earnings")

        regime_result = {
            "regime": "risk_on", "conviction_scale": 1.0,
            "block_longs": False, "block_shorts": False,
            "vix": 15.0, "spy_chg_pct": 0.5,
        }
        from app.filters.technicals import TechnicalScore
        from app.filters.options_flow import OptionsFlowResult
        from app.filters.short_interest import SqueezeResult
        tech_result    = TechnicalScore(ticker="AAPL", direction="long",
                                        technical_score=8, blocked=False)
        options_result = OptionsFlowResult(ticker="AAPL", direction="long")
        squeeze_result = SqueezeResult(ticker="AAPL", direction="long")

        with patch("app.filters.pretrade_filter.get_current_regime",
                   new=AsyncMock(return_value=regime_result)), \
             patch("app.filters.pretrade_filter.score_technicals",
                   new=AsyncMock(return_value=tech_result)), \
             patch("app.filters.pretrade_filter.score_options_flow",
                   new=AsyncMock(return_value=options_result)), \
             patch("app.filters.pretrade_filter.score_squeeze",
                   new=AsyncMock(return_value=squeeze_result)), \
             patch.object(svc, "_get_stock_context",  new=AsyncMock(return_value=None)), \
             patch.object(svc, "_update_signal_log",  new=AsyncMock()):

            result = await svc.process(record)

        assert result is not None   # Signal passed through filter

    @pytest.mark.asyncio
    async def test_neutral_direction_dropped_before_staleness(self):
        """Neutral signals exit immediately; staleness check never runs."""
        svc = self._make_service()
        record = {"ticker": "AAPL", "direction": "neutral", "conviction": 0.5,
                  "news_published_at": _ago(99999).isoformat()}

        with patch.object(svc, "_log_staleness_block", new=AsyncMock()) as mock_stale:
            result = await svc.process(record)

        assert result is None
        mock_stale.assert_not_called()

    @pytest.mark.asyncio
    async def test_staleness_block_emits_to_blocked_topic(self):
        """Stale signal must trigger _emit_blocked() with block_reason set."""
        svc = self._make_service()
        record = self._record(age=9999)

        with patch.object(svc, "_emit_blocked",        new=AsyncMock()) as mock_emit, \
             patch.object(svc, "_log_staleness_block",  new=AsyncMock()):
            await svc.process(record)

        mock_emit.assert_called_once()
        emitted = mock_emit.call_args[0][0]
        assert emitted.get("blocked") is True
        assert emitted.get("block_reason") == "signal_stale"


# ═══════════════════════════════════════════════════════════════════════════════
# TestExecutionStaleness
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecutionStaleness:
    """ExecutionEngine blocks on staleness and consumer lag."""

    def _make_engine(self, lag: int = 0):
        """Build an ExecutionEngine with all I/O mocked using __new__."""
        from app.execution.execution_engine import ExecutionEngine
        eng = ExecutionEngine.__new__(ExecutionEngine)
        eng._running       = False
        eng._consumer      = None
        eng._idempotency   = None
        eng._current_lag   = lag
        eng._last_lag_check = 0.0
        eng._broker        = _MagicMock()
        eng._risk          = _MagicMock()
        eng._http          = _MagicMock()
        eng._redis         = None
        eng._cluster_store = None
        eng._telegram_token = ""
        eng._telegram_chat  = ""
        for attr, val in _make_metrics_mock().items():
            setattr(eng, attr, val)
        return eng

    def _record(self, age: float = 30, catalyst: str = "earnings",
                session: str = "intraday") -> dict:
        return {
            "id":              str(uuid4()),
            "ticker":          "AAPL",
            "direction":       "long",
            "conviction":      0.80,
            "catalyst_type":   catalyst,
            "session_context": session,
            "signal_type":     "event_driven",
            "news_id":         str(uuid4()),
            "news_title":      "Apple Beats",
            "t1_summary":      "AAPL beat EPS.",
            "decay_minutes":   30,
            "market_cap_tier": "mega",
            "float_sensitivity": "normal",
            "impact_day":      0.8,
            "impact_swing":    0.55,
            "source":          "benzinga",
            "route_type":      None,
            "regime_flag":     None,
            "priced_in":       None,
            "priced_in_reason": None,
            "sympathy_plays":  [],
            "is_sympathy":     False,
            "key_levels":      [],
            "time_window_label":   None,
            "time_window_emoji":   None,
            "time_window_mult":    1.0,
            "time_window_quality": "good",
            "created_at":      datetime.now(timezone.utc).isoformat(),
            "prompt_version":  None,
            "news_published_at": _ago(age).isoformat(),
            "pipeline_entry_at": _ago(age + 5).isoformat(),
        }

    @pytest.mark.asyncio
    async def test_stale_signal_blocked(self):
        eng = self._make_engine()
        with patch.object(eng, "_log_staleness_block", new=AsyncMock()) as mock_log:
            result = await eng.process(self._record(age=9999))
        assert result is None
        mock_log.assert_called_once()

    @pytest.mark.asyncio
    async def test_fresh_signal_not_stale_blocked(self):
        eng = self._make_engine()

        eng._broker.get_quote             = AsyncMock(return_value=150.0)
        eng._broker.get_account           = AsyncMock(return_value={"equity": 100000})
        eng._broker.get_positions         = AsyncMock(return_value={})
        eng._broker.get_order_flow_context = AsyncMock(return_value={})
        eng._broker.submit_order           = AsyncMock(return_value=_MagicMock(
            broker_order_id="order-1",
            qty_requested=10,
            status=_MagicMock(value="filled"),
        ))
        eng._broker.name  = "alpaca_paper"
        eng._broker.paper = True

        eng._risk.evaluate = AsyncMock(return_value=_MagicMock(
            approved=True, qty=10, position_value=1500.0,
            take_profit=160.0, stop_loss=145.0, risk_reward=2.0, reason="",
        ))
        eng._risk.build_order = _MagicMock(return_value=_MagicMock(qty=10))

        with patch.object(eng, "_send_trade_alert",   new=AsyncMock()), \
             patch.object(eng, "_save_trade",         new=AsyncMock()), \
             patch.object(eng, "_log_staleness_block", new=AsyncMock()) as mock_log:
            await eng.process(self._record(age=30))

        mock_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_lag_blocked_when_consumer_lag_high(self):
        eng = self._make_engine(lag=100)  # above default threshold 50
        with patch.object(eng, "_log_lag_block", new=AsyncMock()) as mock_log:
            result = await eng.process(self._record(age=30))
        assert result is None
        mock_log.assert_called_once()

    @pytest.mark.asyncio
    async def test_lag_not_blocked_at_exact_threshold(self):
        """lag == threshold should NOT block (strictly greater)."""
        eng = self._make_engine(lag=50)

        eng._broker.get_quote             = AsyncMock(return_value=150.0)
        eng._broker.get_account           = AsyncMock(return_value={"equity": 100000})
        eng._broker.get_positions         = AsyncMock(return_value={})
        eng._broker.get_order_flow_context = AsyncMock(return_value={})
        eng._broker.submit_order           = AsyncMock(return_value=_MagicMock(
            broker_order_id="order-2", qty_requested=10,
            status=_MagicMock(value="filled"),
        ))
        eng._broker.name  = "alpaca_paper"
        eng._broker.paper = True
        eng._risk.evaluate = AsyncMock(return_value=_MagicMock(
            approved=True, qty=10, position_value=1500.0,
            take_profit=160.0, stop_loss=145.0, risk_reward=2.0, reason="",
        ))
        eng._risk.build_order = _MagicMock(return_value=_MagicMock(qty=10))

        with patch.object(eng, "_send_trade_alert", new=AsyncMock()), \
             patch.object(eng, "_save_trade",       new=AsyncMock()), \
             patch.object(eng, "_log_lag_block",    new=AsyncMock()) as mock_lag:
            await eng.process(self._record(age=30))

        mock_lag.assert_not_called()

    @pytest.mark.asyncio
    async def test_lag_guard_fires_before_staleness(self):
        """Lag guard short-circuits; staleness block is never reached."""
        eng = self._make_engine(lag=999)

        with patch.object(eng, "_log_lag_block",       new=AsyncMock()) as mock_lag, \
             patch.object(eng, "_log_staleness_block",  new=AsyncMock()) as mock_stale:
            result = await eng.process(self._record(age=9999))

        assert result is None
        mock_lag.assert_called_once()
        mock_stale.assert_not_called()

    @pytest.mark.asyncio
    async def test_neutral_direction_bypasses_all_guards(self):
        eng = self._make_engine(lag=999)
        record = {**self._record(), "direction": "neutral"}

        with patch.object(eng, "_log_lag_block",       new=AsyncMock()) as mock_lag, \
             patch.object(eng, "_log_staleness_block",  new=AsyncMock()) as mock_stale:
            result = await eng.process(record)

        assert result is None
        mock_lag.assert_not_called()
        mock_stale.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════════
# TestPipelineLatencyMath
# ═══════════════════════════════════════════════════════════════════════════════

class TestPipelineLatencyMath:
    """Ordering properties of the staleness policy."""

    def test_age_grows_with_older_timestamp(self):
        r1 = check_staleness(_ago(60),  "analyst", "intraday")
        r2 = check_staleness(_ago(120), "analyst", "intraday")
        assert r2.age_seconds > r1.age_seconds

    def test_session_changes_max_not_age(self):
        ts = _ago(100)
        r_intraday  = check_staleness(ts, "earnings", "intraday")
        r_premarket = check_staleness(ts, "earnings", "premarket")
        assert abs(r_intraday.age_seconds - r_premarket.age_seconds) < 2.0
        assert r_premarket.max_age_seconds > r_intraday.max_age_seconds

    def test_all_catalysts_have_defined_ages(self):
        catalysts = ["earnings", "ma", "regulatory", "analyst",
                     "macro", "filing", "legal", "other"]
        for c in catalysts:
            assert compute_max_age(c, "intraday", None) > 0

    def test_session_ordering_open_to_overnight(self):
        """open < intraday < afterhours < premarket < overnight."""
        def w(session):
            return compute_max_age("earnings", session, None)

        assert w("open") < w("intraday")
        assert w("intraday") < w("afterhours")
        assert w("afterhours") < w("premarket")
        assert w("premarket") < w("overnight")

    def test_route_ordering_fast_to_slow(self):
        """fast < None < slow."""
        fast = compute_max_age("earnings", "intraday", "fast")
        base = compute_max_age("earnings", "intraday", None)
        slow = compute_max_age("earnings", "intraday", "slow")
        assert fast < base < slow

    def test_legal_has_largest_base_window(self):
        """Legal is a lagging catalyst — longest window."""
        legal_age = compute_max_age("legal", "intraday", None)
        for c in ("earnings", "ma", "regulatory", "analyst", "macro", "other"):
            assert legal_age >= compute_max_age(c, "intraday", None)
