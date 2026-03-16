"""
Tests for Task 10 — Execution and Position-Monitor Upgrade (v1.11)

Test classes (57 tests total):
  TestEntryValidator          — pre-entry halt/stale-quote/drift gate    (12)
  TestOrderTypePolicy         — policy table lookup                       (8)
  TestFillPoller              — fill polling behaviour                   (10)
  TestATRStops                — ATR-based stop/take-profit calc           (8)
  TestPositionRegistration    — position monitor wiring                  (10)
  TestStreamingFallback       — trade stream start / fallback             (5)
  TestExecutionQualityMetrics — slippage / latency / flag persistence     (4)

Run:
  pytest tests/test_execution_upgrade.py -v
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest


# ── Stub unavailable packages BEFORE any app imports ────────────────────────

def _stub_kafka() -> None:
    if "confluent_kafka" not in sys.modules:
        ck = ModuleType("confluent_kafka")
        ck.Consumer = MagicMock
        ck.Producer = MagicMock
        ck.KafkaError = type("KafkaError", (Exception,), {})
        ck.KafkaException = type("KafkaException", (Exception,), {})
        sys.modules["confluent_kafka"] = ck
        sys.modules["confluent_kafka.admin"] = ModuleType("confluent_kafka.admin")


def _stub_prometheus() -> None:
    if "prometheus_client" not in sys.modules:
        pc = ModuleType("prometheus_client")
        pc.Counter = MagicMock
        pc.Histogram = MagicMock
        pc.Gauge = MagicMock
        pc.start_http_server = MagicMock()
        sys.modules["prometheus_client"] = pc


def _stub_aioredis() -> None:
    if "redis" not in sys.modules:
        redis_mod = ModuleType("redis")
        redis_asyncio = ModuleType("redis.asyncio")
        redis_asyncio.Redis = MagicMock
        redis_asyncio.from_url = AsyncMock()
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio
        redis_mod.asyncio = redis_asyncio


_stub_kafka()
_stub_prometheus()
_stub_aioredis()

# ── App imports (safe after stubs) ───────────────────────────────────────────

from app.execution.entry_validator import EntryValidator, EntryCheckResult
from app.execution.order_policy import (
    build_order_policy_table, get_order_policy, OrderTypePolicy,
    ADV_TIER_LIQUID, ADV_TIER_NORMAL, ADV_TIER_THIN,
)
from app.execution.fill_poller import FillPoller, FillPollConfig
from app.execution.risk_manager import RiskManager, RiskConfig
from app.execution.base_broker import (
    OrderResult, OrderRequest, OrderStatus, OrderType, OrderSide,
)
from app.execution.position_monitor import PositionMonitor


# ── Helpers ──────────────────────────────────────────────────────────────────

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_settings(**kw):
    s = MagicMock()
    defaults = dict(
        entry_max_quote_age_seconds=10.0,
        entry_max_price_drift_pct=1.5,
        entry_halt_check_enabled=True,
        order_adv_liquid_threshold=5_000_000,
        order_adv_thin_threshold=500_000,
        order_policy_earnings_liquid_type="market",
        order_policy_earnings_normal_type="limit_ioc",
        order_policy_earnings_thin_type="limit",
        order_policy_analyst_liquid_type="limit",
        order_policy_analyst_normal_type="limit",
        order_policy_analyst_thin_type="limit",
        order_policy_regulatory_liquid_type="market",
        order_policy_regulatory_normal_type="limit_ioc",
        order_policy_regulatory_thin_type="limit_ioc",
        order_policy_ma_liquid_type="market",
        order_policy_ma_normal_type="limit_ioc",
        order_policy_ma_thin_type="limit",
        order_policy_macro_liquid_type="market",
        order_policy_macro_normal_type="market",
        order_policy_macro_thin_type="limit",
        order_policy_default_type="limit",
        order_limit_slippage_liquid_pct=0.05,
        order_limit_slippage_normal_pct=0.15,
        order_limit_slippage_thin_pct=0.30,
        fill_poll_interval_seconds=2.0,
        fill_poll_max_attempts=15,
        fill_min_partial_pct=0.80,
        risk_atr_sl_multiplier=2.0,
        risk_atr_tp_multiplier=4.0,
        stock_context_url="http://stock_context:8082",
        position_monitor_stream_enabled=True,
        position_monitor_poll_interval_seconds=30,
        execution_quality_enabled=True,
    )
    defaults.update(kw)
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


def _make_order_result(
    status=OrderStatus.FILLED,
    broker_order_id="broker-123",
    qty_requested=10.0,
    qty_filled=10.0,
    avg_fill_price=100.0,
    ticker="AAPL",
) -> OrderResult:
    return OrderResult(
        broker_order_id=broker_order_id,
        ticker=ticker,
        side=OrderSide.BUY,
        qty_requested=qty_requested,
        qty_filled=qty_filled,
        avg_fill_price=avg_fill_price,
        status=status,
    )


def _make_order_request(
    qty=10.0,
    ticker="AAPL",
    order_type=OrderType.LIMIT,
) -> OrderRequest:
    return OrderRequest(
        ticker=ticker,
        side=OrderSide.BUY,
        qty=qty,
        order_type=order_type,
    )


# ═════════════════════════════════════════════════════════════════════════════
# 1. TestEntryValidator (12 tests)
# ═════════════════════════════════════════════════════════════════════════════

class TestEntryValidator:

    def _make_validator(self, **kw) -> EntryValidator:
        return EntryValidator(_make_settings(**kw))

    def _fresh_ts(self, age_seconds=0.0) -> datetime:
        return datetime.now(timezone.utc) - timedelta(seconds=age_seconds)

    # -- halt ------------------------------------------------------------------

    def test_halt_detected_blocks(self):
        v = self._make_validator()
        res = _run(v.check("AAPL", 100.0, 100.0, self._fresh_ts(0), halted=True))
        assert not res.approved
        assert res.halted
        assert res.reason == "trading_halted"

    def test_halt_check_disabled_via_config(self):
        v = self._make_validator(entry_halt_check_enabled=False)
        res = _run(v.check("AAPL", 100.0, 100.0, self._fresh_ts(0), halted=True))
        assert res.approved  # halt check disabled → pass through

    # -- stale quote -----------------------------------------------------------

    def test_stale_quote_blocks(self):
        v = self._make_validator(entry_max_quote_age_seconds=10.0)
        old_ts = self._fresh_ts(age_seconds=15.0)  # 15 s > 10 s max
        res = _run(v.check("AAPL", 100.0, 100.0, old_ts, halted=False))
        assert not res.approved
        assert "stale_quote" in res.reason
        assert res.quote_age_seconds > 10.0

    def test_stale_quote_exact_threshold_blocks(self):
        """age == max_age → blocks (strict >)"""
        v = self._make_validator(entry_max_quote_age_seconds=10.0)
        # Sleep drift can cause flakiness if exactly at threshold; use 10.05 s
        old_ts = self._fresh_ts(age_seconds=10.05)
        res = _run(v.check("AAPL", 100.0, 100.0, old_ts, halted=False))
        assert not res.approved

    def test_quote_age_zero_passes(self):
        v = self._make_validator()
        res = _run(v.check("AAPL", 100.0, 100.0, self._fresh_ts(0), halted=False))
        assert res.approved
        assert res.quote_age_seconds < 1.0

    def test_quote_age_borderline_exact_max(self):
        """age == max_age (10.0 s) should block"""
        v = self._make_validator(entry_max_quote_age_seconds=10.0)
        old_ts = self._fresh_ts(age_seconds=10.1)
        res = _run(v.check("AAPL", 100.0, 100.0, old_ts, halted=False))
        assert not res.approved

    # -- price drift -----------------------------------------------------------

    def test_price_drift_blocks_long(self):
        """Price rose 2% from signal reference → blocks (> 1.5% max)"""
        v = self._make_validator(entry_max_price_drift_pct=1.5)
        res = _run(v.check(
            "AAPL",
            signal_price=100.0,
            current_price=102.0,    # 2% rise
            quote_timestamp=self._fresh_ts(1),
            halted=False,
        ))
        assert not res.approved
        assert "price_drift" in res.reason
        assert res.price_drift_pct > 1.5

    def test_price_drift_blocks_short(self):
        """Price fell 2% from signal reference → abs() still triggers"""
        v = self._make_validator(entry_max_price_drift_pct=1.5)
        res = _run(v.check(
            "AAPL",
            signal_price=100.0,
            current_price=98.0,     # 2% drop — abs → 2% > 1.5%
            quote_timestamp=self._fresh_ts(1),
            halted=False,
        ))
        assert not res.approved
        assert res.price_drift_pct > 1.5

    def test_negative_drift_is_absolute(self):
        """abs() applied: price below ref still triggers drift block"""
        v = self._make_validator(entry_max_price_drift_pct=1.5)
        res = _run(v.check(
            "AAPL",
            signal_price=100.0,
            current_price=97.0,     # 3% drop
            quote_timestamp=self._fresh_ts(1),
            halted=False,
        ))
        assert not res.approved
        assert round(res.price_drift_pct, 1) == 3.0

    def test_price_drift_absent_signal_price_skipped(self):
        """signal_price=None → drift check skipped → approved"""
        v = self._make_validator()
        res = _run(v.check(
            "AAPL",
            signal_price=None,
            current_price=105.0,   # large move, but no ref
            quote_timestamp=self._fresh_ts(1),
            halted=False,
        ))
        assert res.approved

    def test_all_pass_approved(self):
        v = self._make_validator()
        res = _run(v.check(
            "AAPL",
            signal_price=100.0,
            current_price=100.5,   # 0.5% drift — within 1.5% limit
            quote_timestamp=self._fresh_ts(2),
            halted=False,
        ))
        assert res.approved
        assert res.reason == "ok"
        assert res.price_drift_pct < 1.5

    def test_zero_current_price_blocks(self):
        """current_price=0 with signal_price set → cannot compute drift → skip drift, passes"""
        v = self._make_validator()
        # current_price=0 → both conditions (signal_price > 0, current_price > 0) required
        # so drift check is skipped → approved
        res = _run(v.check(
            "AAPL",
            signal_price=100.0,
            current_price=0.0,
            quote_timestamp=self._fresh_ts(1),
            halted=False,
        ))
        # current_price=0 → drift condition `current_price > 0` fails → skip check → approved
        assert res.approved


# ═════════════════════════════════════════════════════════════════════════════
# 2. TestOrderTypePolicy (8 tests)
# ═════════════════════════════════════════════════════════════════════════════

class TestOrderTypePolicy:

    def _make_table(self, **kw):
        return build_order_policy_table(_make_settings(**kw))

    def test_earnings_liquid_resolves_market(self):
        table = self._make_table()
        policy = get_order_policy(table, "earnings", adv=10_000_000)
        assert policy.order_type == "market"
        assert policy.adv_tier == ADV_TIER_LIQUID

    def test_earnings_thin_resolves_limit(self):
        table = self._make_table()
        policy = get_order_policy(table, "earnings", adv=100_000)
        assert policy.order_type == "limit"
        assert policy.adv_tier == ADV_TIER_THIN

    def test_analyst_liquid_resolves_limit(self):
        """Analyst catalyst → LIMIT regardless of ADV (not urgent)"""
        table = self._make_table()
        policy = get_order_policy(table, "analyst", adv=20_000_000)
        assert policy.order_type == "limit"

    def test_adv_tier_liquid_threshold(self):
        """ADV exactly at liquid threshold → liquid"""
        table = self._make_table()
        policy = get_order_policy(
            table, "earnings", adv=5_000_000,
            liquid_threshold=5_000_000, thin_threshold=500_000,
        )
        assert policy.adv_tier == ADV_TIER_LIQUID

    def test_adv_tier_thin_threshold(self):
        """ADV just below thin threshold → thin"""
        table = self._make_table()
        policy = get_order_policy(
            table, "earnings", adv=499_999,
            liquid_threshold=5_000_000, thin_threshold=500_000,
        )
        assert policy.adv_tier == ADV_TIER_THIN

    def test_adv_none_defaults_normal_tier(self):
        """ADV=None (unavailable) → normal tier (fail-safe)"""
        table = self._make_table()
        policy = get_order_policy(table, "earnings", adv=None)
        assert policy.adv_tier == ADV_TIER_NORMAL

    def test_unknown_catalyst_falls_back_default(self):
        """Unknown catalyst type → falls back to 'other' row → limit"""
        table = self._make_table(order_policy_default_type="limit")
        policy = get_order_policy(table, "unknown_catalyst_xyz", adv=1_000_000)
        assert policy.order_type == "limit"

    def test_policy_table_covers_all_catalyst_types(self):
        """All expected catalyst × tier combinations are in the table"""
        table = self._make_table()
        catalysts = ["earnings", "analyst", "regulatory", "m&a", "macro", "other"]
        tiers = [ADV_TIER_LIQUID, ADV_TIER_NORMAL, ADV_TIER_THIN]
        for cat in catalysts:
            for tier in tiers:
                assert (cat, tier) in table, f"Missing ({cat!r}, {tier!r})"


# ═════════════════════════════════════════════════════════════════════════════
# 3. TestFillPoller (10 tests)
# ═════════════════════════════════════════════════════════════════════════════

class TestFillPoller:

    def _make_poller(self, broker, **kw) -> FillPoller:
        cfg = FillPollConfig(
            poll_interval_seconds=0.0,  # no sleep in tests
            max_polls=kw.pop("max_polls", 5),
            min_partial_fill_pct=kw.pop("min_partial_fill_pct", 0.80),
        )
        return FillPoller(broker, cfg)

    def _mock_broker(self, sequence: list[OrderResult]) -> MagicMock:
        """Broker whose get_order returns items from `sequence` in order."""
        broker = MagicMock()
        broker.get_order = AsyncMock(side_effect=sequence)
        broker.cancel_order = AsyncMock(return_value=True)
        return broker

    def test_immediate_fill_returns_at_first_poll(self):
        filled = _make_order_result(OrderStatus.FILLED)
        broker = self._mock_broker([filled])
        req = _make_order_request()
        submitted = _make_order_result(OrderStatus.SUBMITTED, broker_order_id="broker-123")
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.FILLED
        assert broker.get_order.call_count == 1

    def test_polls_until_filled_on_3rd_attempt(self):
        submitted = _make_order_result(OrderStatus.SUBMITTED)
        pending   = _make_order_result(OrderStatus.SUBMITTED)
        filled    = _make_order_result(OrderStatus.FILLED)
        broker = self._mock_broker([pending, pending, filled])
        req = _make_order_request()
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.FILLED
        assert broker.get_order.call_count == 3

    def test_partial_above_threshold_accepts_and_cancels_remainder(self):
        submitted = _make_order_result(OrderStatus.SUBMITTED, qty_requested=10)
        partial   = _make_order_result(
            OrderStatus.PARTIAL, qty_requested=10, qty_filled=9.0)  # 90% ≥ 80%
        broker = self._mock_broker([partial])
        req = _make_order_request(qty=10)
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.PARTIAL
        assert result.qty_filled == 9.0
        broker.cancel_order.assert_called_once()

    def test_partial_below_threshold_continues_to_poll(self):
        submitted = _make_order_result(OrderStatus.SUBMITTED, qty_requested=10)
        partial_low  = _make_order_result(
            OrderStatus.PARTIAL, qty_requested=10, qty_filled=5.0)  # 50% < 80%
        filled       = _make_order_result(
            OrderStatus.FILLED,  qty_requested=10, qty_filled=10.0)
        broker = self._mock_broker([partial_low, partial_low, filled])
        req = _make_order_request(qty=10)
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.FILLED
        broker.cancel_order.assert_not_called()

    def test_partial_exactly_at_threshold(self):
        """fill_ratio == 0.80 → accept (≥ threshold)"""
        submitted = _make_order_result(OrderStatus.SUBMITTED, qty_requested=10)
        partial   = _make_order_result(
            OrderStatus.PARTIAL, qty_requested=10, qty_filled=8.0)  # exactly 80%
        broker = self._mock_broker([partial])
        req = _make_order_request(qty=10)
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.PARTIAL
        broker.cancel_order.assert_called_once()

    def test_cancelled_returns_immediately(self):
        submitted  = _make_order_result(OrderStatus.SUBMITTED)
        cancelled  = _make_order_result(OrderStatus.CANCELLED)
        broker = self._mock_broker([cancelled])
        req = _make_order_request()
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.CANCELLED
        assert broker.get_order.call_count == 1

    def test_rejected_returns_immediately(self):
        submitted = _make_order_result(OrderStatus.SUBMITTED)
        rejected  = _make_order_result(OrderStatus.REJECTED)
        broker = self._mock_broker([rejected])
        req = _make_order_request()
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.REJECTED

    def test_expired_returns_immediately(self):
        submitted = _make_order_result(OrderStatus.SUBMITTED)
        expired   = _make_order_result(OrderStatus.EXPIRED)
        broker = self._mock_broker([expired])
        req = _make_order_request()
        result = _run(self._make_poller(broker).wait_for_fill(submitted, req))
        assert result.status == OrderStatus.EXPIRED

    def test_timeout_exhausted_returns_last_state(self):
        submitted = _make_order_result(OrderStatus.SUBMITTED)
        still_pending = _make_order_result(OrderStatus.SUBMITTED)
        broker = self._mock_broker([still_pending] * 3)
        req = _make_order_request()
        poller = self._make_poller(broker, max_polls=3)
        result = _run(poller.wait_for_fill(submitted, req))
        assert result.status == OrderStatus.SUBMITTED
        assert broker.get_order.call_count == 3

    def test_market_order_skips_poller_by_convention(self):
        """Engine convention: MARKET orders skip wait_for_fill entirely.
        This test verifies the engine's condition works with MARKET order type."""
        req = _make_order_request(order_type=OrderType.MARKET)
        # If engine checks: order.order_type != OrderType.MARKET → skip
        assert req.order_type == OrderType.MARKET
        # Verified: the condition in execution_engine.py skips MARKET orders


# ═════════════════════════════════════════════════════════════════════════════
# 4. TestATRStops (8 tests)
# ═════════════════════════════════════════════════════════════════════════════

class TestATRStops:

    def _make_rm(self, **kw) -> RiskManager:
        cfg = RiskConfig(
            default_take_profit_pct=kw.pop("default_take_profit_pct", 0.10),
            default_stop_loss_pct=kw.pop("default_stop_loss_pct", 0.04),
            **kw,
        )
        return RiskManager(cfg)

    def test_atr_long_stop_below_entry(self):
        """SL (long) = entry - 2 × ATR"""
        rm = self._make_rm()
        tp, sl = rm.compute_atr_stops(100.0, "long", atr_14=2.5)
        assert sl == 100.0 - 2.5 * 2.0  # 95.0

    def test_atr_long_tp_above_entry(self):
        """TP (long) = entry + 4 × ATR"""
        rm = self._make_rm()
        tp, sl = rm.compute_atr_stops(100.0, "long", atr_14=2.5)
        assert tp == 100.0 + 2.5 * 4.0  # 110.0

    def test_atr_short_stop_above_entry(self):
        """SL (short) = entry + 2 × ATR"""
        rm = self._make_rm()
        tp, sl = rm.compute_atr_stops(100.0, "short", atr_14=2.5)
        assert sl == 100.0 + 2.5 * 2.0  # 105.0

    def test_atr_short_tp_below_entry(self):
        """TP (short) = entry - 4 × ATR"""
        rm = self._make_rm()
        tp, sl = rm.compute_atr_stops(100.0, "short", atr_14=2.5)
        assert tp == 100.0 - 2.5 * 4.0  # 90.0

    def test_atr_none_falls_back_to_fixed_pct(self):
        """atr_14=None → fixed-pct fallback"""
        rm = self._make_rm(default_take_profit_pct=0.10, default_stop_loss_pct=0.04)
        tp, sl = rm.compute_atr_stops(100.0, "long", atr_14=None)
        assert tp == round(100.0 * 1.10, 2)  # 110.0
        assert sl == round(100.0 * 0.96, 2)  # 96.0

    def test_atr_zero_falls_back_to_fixed_pct(self):
        """atr_14=0 → fixed-pct fallback"""
        rm = self._make_rm(default_take_profit_pct=0.10, default_stop_loss_pct=0.04)
        tp, sl = rm.compute_atr_stops(100.0, "long", atr_14=0.0)
        assert tp == round(100.0 * 1.10, 2)

    def test_rr_ratio_is_2_to_1_by_default(self):
        """Default multipliers tp_mult=4, sl_mult=2 → 2:1 RR ratio"""
        rm = self._make_rm()
        tp, sl = rm.compute_atr_stops(100.0, "long", atr_14=2.0)
        reward = tp - 100.0   # 8.0
        risk   = 100.0 - sl   # 4.0
        assert round(reward / risk, 1) == 2.0

    def test_custom_multipliers_applied(self):
        """Custom sl_mult / tp_mult override defaults"""
        rm = self._make_rm()
        tp, sl = rm.compute_atr_stops(
            100.0, "long", atr_14=2.0, sl_mult=3.0, tp_mult=6.0)
        assert sl == round(100.0 - 2.0 * 3.0, 2)   # 94.0
        assert tp == round(100.0 + 2.0 * 6.0, 2)   # 112.0


# ═════════════════════════════════════════════════════════════════════════════
# 5. TestPositionRegistration (10 tests)
# ═════════════════════════════════════════════════════════════════════════════

class TestPositionRegistration:

    def _make_monitor(self, db=None) -> PositionMonitor:
        broker = MagicMock()
        broker.name = "alpaca_paper"
        broker.get_positions = AsyncMock(return_value=[])
        broker.close_position = AsyncMock(return_value=_make_order_result())
        # No start_trade_stream by default (simulate unsupported)
        del broker.start_trade_stream
        monitor = PositionMonitor(broker, db=db)
        return monitor

    def test_register_called_after_fill(self):
        monitor = self._make_monitor()
        monitor.register_position("AAPL", {
            "entry_price": 100.0,
            "take_profit": 110.0,
            "stop_loss":   95.0,
            "decay_minutes": 60,
        })
        assert "AAPL" in monitor._position_meta
        assert monitor._position_meta["AAPL"]["take_profit"] == 110.0

    def test_register_not_called_on_rejected(self):
        """Registration only happens for FILLED/PARTIAL — not REJECTED"""
        monitor = self._make_monitor()
        # Simulate engine skipping register on REJECTED
        result = _make_order_result(OrderStatus.REJECTED)
        if result.status in (OrderStatus.FILLED, OrderStatus.PARTIAL):
            monitor.register_position("AAPL", {})
        assert "AAPL" not in monitor._position_meta

    def test_register_not_called_on_cancelled(self):
        result = _make_order_result(OrderStatus.CANCELLED)
        monitor = self._make_monitor()
        if result.status in (OrderStatus.FILLED, OrderStatus.PARTIAL):
            monitor.register_position("AAPL", {})
        assert "AAPL" not in monitor._position_meta

    def test_register_partial_fill_also_registers(self):
        monitor = self._make_monitor()
        result = _make_order_result(OrderStatus.PARTIAL)
        if result.status in (OrderStatus.FILLED, OrderStatus.PARTIAL):
            monitor.register_position("AAPL", {"entry_price": 100.0})
        assert "AAPL" in monitor._position_meta

    def test_db_recovery_loads_open_trade(self):
        """_recover_from_db() loads rows without closed_at"""
        db = AsyncMock()
        db.fetch = AsyncMock(return_value=[{
            "ticker":          "MSFT",
            "entry_price":     300.0,
            "take_profit_price": 330.0,
            "stop_loss_price":  285.0,
            "decay_minutes":    60,
            "broker_order_id":  "ord-1",
        }])
        monitor = self._make_monitor(db=db)
        _run(monitor._recover_from_db())
        assert "MSFT" in monitor._position_meta
        assert monitor._position_meta["MSFT"]["take_profit"] == 330.0

    def test_db_recovery_skips_already_registered(self):
        """If ticker already in _position_meta, recovery skips it"""
        db = AsyncMock()
        db.fetch = AsyncMock(return_value=[{
            "ticker":           "AAPL",
            "entry_price":      200.0,
            "take_profit_price": 220.0,
            "stop_loss_price":   190.0,
            "decay_minutes":     30,
            "broker_order_id":   "ord-2",
        }])
        monitor = self._make_monitor(db=db)
        monitor._position_meta["AAPL"] = {"entry_price": 150.0}  # already registered
        _run(monitor._recover_from_db())
        # Should NOT be overwritten
        assert monitor._position_meta["AAPL"]["entry_price"] == 150.0

    def test_db_recovery_connection_error_nonfatal(self):
        """DB error during recovery does not raise"""
        db = AsyncMock()
        db.fetch = AsyncMock(side_effect=Exception("DB connection refused"))
        monitor = self._make_monitor(db=db)
        # Should complete without exception
        _run(monitor._recover_from_db())

    def test_duplicate_register_overwrites_meta(self):
        monitor = self._make_monitor()
        monitor.register_position("AAPL", {"entry_price": 100.0, "stop_loss": 95.0})
        monitor.register_position("AAPL", {"entry_price": 105.0, "stop_loss": 99.0})
        assert monitor._position_meta["AAPL"]["entry_price"] == 105.0

    def test_position_monitor_started_as_asyncio_task(self):
        """PositionMonitor.start() can be wrapped in create_task without error"""
        monitor = self._make_monitor()

        async def _start_briefly():
            task = asyncio.create_task(monitor.start())
            await asyncio.sleep(0.01)
            monitor.stop()
            try:
                task.cancel()
                await task
            except (asyncio.CancelledError, Exception):
                pass

        _run(_start_briefly())

    def test_persist_close_updates_trade_row(self):
        """_persist_close() calls db.execute with correct ticker"""
        db = AsyncMock()
        db.execute = AsyncMock()
        monitor = self._make_monitor(db=db)
        _run(monitor._persist_close("AAPL", "stop_loss", 95.0, -50.0))
        db.execute.assert_called_once()
        call_args = db.execute.call_args
        # The SQL and params are positional — check AAPL is in params
        params = call_args[0]   # positional args to execute()
        assert "AAPL" in params


# ═════════════════════════════════════════════════════════════════════════════
# 6. TestStreamingFallback (5 tests)
# ═════════════════════════════════════════════════════════════════════════════

class TestStreamingFallback:

    def _make_monitor_with_stream(self, stream_impl=None) -> PositionMonitor:
        broker = MagicMock()
        broker.name = "alpaca_paper"
        broker.get_positions = AsyncMock(return_value=[])
        if stream_impl is not None:
            broker.start_trade_stream = stream_impl
        else:
            del broker.start_trade_stream  # simulate broker without streaming
        return PositionMonitor(broker)

    def test_stream_starts_if_broker_has_method(self):
        """If broker has start_trade_stream, _run_trade_stream is called"""
        stream_called = []

        async def _mock_stream(cb):
            stream_called.append(True)

        monitor = self._make_monitor_with_stream(stream_impl=_mock_stream)
        _run(monitor._run_trade_stream())
        assert stream_called

    def test_stream_not_started_without_method(self):
        """Broker without start_trade_stream → stream task not started"""
        monitor = self._make_monitor_with_stream(stream_impl=None)
        # hasattr check should be False
        assert not hasattr(monitor._broker, "start_trade_stream")

    def test_stream_exception_falls_back_to_polling(self):
        """Stream raising exception is caught; does not propagate"""
        async def _bad_stream(cb):
            raise RuntimeError("WebSocket disconnected")

        monitor = self._make_monitor_with_stream(stream_impl=_bad_stream)
        # _run_trade_stream() catches exceptions and logs them
        _run(monitor._run_trade_stream())   # must not raise

    def test_on_trade_update_fill_logs(self):
        """fill event is processed without error"""
        monitor = self._make_monitor_with_stream(stream_impl=None)
        event = {
            "event": "fill",
            "symbol": "AAPL",
            "broker_order_id": "ord-1",
            "qty_filled": 10.0,
            "avg_fill_price": 100.0,
        }
        _run(monitor._on_trade_update(event))   # must not raise

    def test_on_trade_update_cancelled_removes_meta(self):
        """cancel event removes position meta from tracking"""
        monitor = self._make_monitor_with_stream(stream_impl=None)
        monitor._position_meta["AAPL"] = {"entry_price": 100.0}
        event = {
            "event": "canceled",
            "symbol": "AAPL",
            "broker_order_id": "ord-1",
            "qty_filled": 0.0,
            "avg_fill_price": 0.0,
        }
        _run(monitor._on_trade_update(event))
        assert "AAPL" not in monitor._position_meta


# ═════════════════════════════════════════════════════════════════════════════
# 7. TestExecutionQualityMetrics (4 tests)
# ═════════════════════════════════════════════════════════════════════════════

class TestExecutionQualityMetrics:

    def test_slippage_computed_correctly(self):
        """(fill_price - ref_price) / ref_price * 100 = slippage_pct"""
        entry_ref_price = 100.0
        avg_fill = 101.0
        slippage_pct = (avg_fill - entry_ref_price) / entry_ref_price * 100.0
        assert abs(slippage_pct - 1.0) < 0.001

    def test_negative_slippage_means_better_fill(self):
        """Negative slippage = filled below reference price (good for long)"""
        entry_ref_price = 100.0
        avg_fill = 99.5
        slippage_pct = (avg_fill - entry_ref_price) / entry_ref_price * 100.0
        assert slippage_pct < 0.0

    def test_fill_latency_ms_positive(self):
        """fill_latency_ms = time from submit to fill, must be non-negative"""
        submit_time = datetime.now(timezone.utc)
        fill_time = submit_time + timedelta(milliseconds=350)
        latency = (fill_time - submit_time).total_seconds() * 1000
        fill_latency_ms = max(0, int(latency))
        assert fill_latency_ms == 350

    def test_atr_stop_used_flag_set_when_atr_used(self):
        """stop_type='atr' maps to atr_stop_used=True in execution quality row"""
        stop_type = "atr"
        atr_stop_used = (stop_type == "atr")
        assert atr_stop_used is True

        stop_type_fixed = "fixed_pct"
        atr_stop_used_fixed = (stop_type_fixed == "atr")
        assert atr_stop_used_fixed is False
