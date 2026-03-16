"""
Tests for app/risk/ package — Portfolio Risk Manager (v1.10)

Test classes:
  TestRiskCodes                 — risk_codes module constants (2)
  TestRiskStateStore            — Redis-backed state store (10)
  TestKillSwitchGates           — kill switch blocks all signals (6)
  TestPortfolioConstraints      — daily loss / rate / position constraints (14)
  TestCorrelationAndEventChecks — correlation proxy + event cluster dedup (6)
  TestPortfolioRiskIntegration  — end-to-end process() (12)
  TestAutoHaltTriggers          — auto-halt set on drawdown / loss-streak (5)

Run:
  pytest tests/test_portfolio_risk.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone, timedelta
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

# ── Stub Docker-only / unavailable packages before any app imports ─────────────
# portfolio_risk → BaseConsumer → confluent_kafka / prometheus_client
# portfolio_risk → import redis.asyncio as aioredis


def _stub_kafka() -> None:
    if "confluent_kafka" not in sys.modules:
        ck = ModuleType("confluent_kafka")
        ck.Consumer = MagicMock          # type: ignore[attr-defined]
        ck.Producer = MagicMock          # type: ignore[attr-defined]
        ck.KafkaError = type("KafkaError", (Exception,), {})       # type: ignore
        ck.KafkaException = type("KafkaException", (Exception,), {})  # type: ignore
        sys.modules["confluent_kafka"] = ck
        sys.modules["confluent_kafka.admin"] = ModuleType("confluent_kafka.admin")


def _stub_prometheus() -> None:
    if "prometheus_client" not in sys.modules:
        pc = ModuleType("prometheus_client")
        pc.Counter = MagicMock           # type: ignore[attr-defined]
        pc.Histogram = MagicMock         # type: ignore[attr-defined]
        pc.Gauge = MagicMock             # type: ignore[attr-defined]
        pc.start_http_server = MagicMock()  # type: ignore[attr-defined]
        sys.modules["prometheus_client"] = pc


def _stub_aioredis() -> None:
    if "redis" not in sys.modules:
        redis_mod = ModuleType("redis")
        redis_asyncio = ModuleType("redis.asyncio")
        redis_asyncio.Redis = MagicMock  # type: ignore[attr-defined]
        redis_asyncio.from_url = AsyncMock()  # type: ignore[attr-defined]
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio
        redis_mod.asyncio = redis_asyncio  # type: ignore[attr-defined]


_stub_kafka()
_stub_prometheus()
_stub_aioredis()

# ── App imports (safe after stubs) ─────────────────────────────────────────────
import app.risk.risk_codes as rc
from app.risk.risk_state import RiskStateStore
from app.risk.portfolio_risk import (
    PortfolioRiskService,
    _position_sector,
    _position_direction,
    _catalyst_positions_count,
)


# ─────────────────────────────────────────────────────────────────────────────
# Shared test helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_redis(key_values: dict | None = None, zcard_result: int = 0) -> MagicMock:
    """Redis mock where get() returns values from a key-value map."""
    kv = key_values or {}
    r = MagicMock()

    async def _get(key):
        return kv.get(key)

    r.get = _get
    r.set = AsyncMock()
    r.setex = AsyncMock()
    r.delete = AsyncMock()
    r.incr = AsyncMock(return_value=1)
    r.expire = AsyncMock()
    r.zadd = AsyncMock()
    r.zremrangebyscore = AsyncMock()
    r.zcard = AsyncMock(return_value=zcard_result)
    return r


def _make_settings(**overrides) -> MagicMock:
    """Settings mock with production defaults for all v1.10 risk fields."""
    s = MagicMock()
    s.risk_kill_switch_redis_key          = "risk:kill_switch"
    s.risk_halt_drawdown_pct              = 0.08
    s.risk_halt_consecutive_losses        = 5
    s.risk_max_daily_loss_pct             = 0.05
    s.risk_max_open_positions             = 10
    s.risk_max_orders_per_hour            = 20
    s.risk_max_orders_per_day             = 120
    s.risk_cooldown_after_loss_minutes    = 30
    s.risk_max_portfolio_heat_pct         = 0.20
    s.risk_max_ticker_exposure_usd        = 10_000.0
    s.risk_max_ticker_exposure_pct        = 0.05
    s.risk_max_sector_pct                 = 0.25
    s.risk_max_catalyst_earnings_positions  = 3
    s.risk_max_catalyst_analyst_positions   = 4
    s.risk_max_catalyst_regulatory_positions = 2
    s.risk_max_catalyst_ma_positions        = 2
    s.risk_max_catalyst_macro_positions     = 3
    s.risk_max_catalyst_default_positions   = 5
    s.risk_max_correlated_positions         = 3
    s.risk_max_trades_per_cluster           = 1
    s.risk_cluster_ttl_seconds              = 3600
    s.risk_position_cache_ttl_seconds       = 30
    s.enable_portfolio_risk_manager         = True
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _make_account(equity: float = 100_000.0, daily_pnl: float = 0.0) -> MagicMock:
    a = MagicMock()
    a.equity     = equity
    a.daily_pnl  = daily_pnl
    return a


def _make_position(
    ticker: str = "AAPL",
    market_value: float = 5_000.0,
    qty: int = 10,
    sector: str = "Technology",
    catalyst_type: str = "earnings",
) -> MagicMock:
    p = MagicMock()
    p.ticker        = ticker
    p.market_value  = market_value
    p.qty           = qty
    p.sector        = sector
    p.catalyst_type = catalyst_type
    return p


def _make_record(**overrides) -> dict:
    base: dict[str, Any] = {
        "ticker":        "MSFT",
        "direction":     "long",
        "conviction":    0.70,
        "catalyst_type": "earnings",
        "cluster_id":    "cluster-001",
        "sectors":       ["Technology"],
        "news_id":       "news-001",
    }
    base.update(overrides)
    return base


def _make_svc(
    account=None,
    positions=None,
) -> tuple[PortfolioRiskService, MagicMock, list]:
    """
    Returns (svc, mock_state, blocked_calls).
    _block() is replaced with a collector that appends to blocked_calls.
    """
    svc = PortfolioRiskService.__new__(PortfolioRiskService)

    # Mock RiskStateStore
    state = MagicMock()
    state.is_kill_switch_active   = AsyncMock(return_value=(False, ""))
    state.get_consecutive_losses  = AsyncMock(return_value=0)
    state.is_in_cooldown          = AsyncMock(return_value=False)
    state.get_hourly_order_count  = AsyncMock(return_value=0)
    state.get_daily_order_count   = AsyncMock(return_value=0)
    state.get_event_trade_count   = AsyncMock(return_value=0)
    state.get_equity_hwm          = AsyncMock(return_value=0.0)
    state.record_order_submitted  = AsyncMock()
    state.increment_daily_orders  = AsyncMock()
    state.record_event_trade      = AsyncMock()
    state.update_equity_hwm       = AsyncMock()
    state.set_auto_halt           = AsyncMock()
    state.clear_auto_halt         = AsyncMock()
    svc._state = state

    # Mock _get_account_and_positions
    _acct = account if account is not None else _make_account()
    _pos  = positions if positions is not None else []

    async def _get_ap():
        return _acct, _pos

    svc._get_account_and_positions = _get_ap

    # Capture _block calls
    blocked: list[dict] = []

    async def _fake_block(record, code, human_reason, **kwargs):
        blocked.append({
            "code":   code,
            "reason": human_reason,
            "record": dict(record),
            **kwargs,
        })
        return None

    svc._block = _fake_block
    return svc, state, blocked


def _run(svc: PortfolioRiskService, record: dict,
         settings_overrides: dict | None = None) -> dict | None:
    """Run svc.process(record) with patched module-level _settings."""
    s = _make_settings(**(settings_overrides or {}))
    with patch("app.risk.portfolio_risk._settings", s):
        return asyncio.get_event_loop().run_until_complete(svc.process(record))


# ─────────────────────────────────────────────────────────────────────────────
# 1 · TestRiskCodes
# ─────────────────────────────────────────────────────────────────────────────

class TestRiskCodes:
    """Machine-readable risk codes are well-formed and complete."""

    def test_all_codes_have_risk_prefix(self):
        for code in rc.ALL_CODES:
            assert code.startswith("RISK:"), f"Code missing RISK: prefix: {code!r}"

    def test_codes_are_unique(self):
        all_list = [
            rc.KILL_SWITCH_MANUAL,
            rc.AUTO_HALT_DRAWDOWN,
            rc.AUTO_HALT_LOSS_STREAK,
            rc.AUTO_HALT_ORDER_RATE,
            rc.DAILY_LOSS_HALT,
            rc.CONSECUTIVE_LOSS_COOLDOWN,
            rc.ORDER_RATE_LIMIT,
            rc.DAILY_ORDER_CAP,
            rc.MAX_POSITIONS,
            rc.TICKER_EXPOSURE,
            rc.SECTOR_CONCENTRATION,
            rc.CATALYST_CONCENTRATION,
            rc.CORRELATION_RISK,
            rc.EVENT_ALREADY_TRADED,
        ]
        assert len(all_list) == len(set(all_list)), "Duplicate risk codes found"


# ─────────────────────────────────────────────────────────────────────────────
# 2 · TestRiskStateStore
# ─────────────────────────────────────────────────────────────────────────────

class TestRiskStateStore:
    """RiskStateStore Redis operations — all methods use mock Redis."""

    def _store(self, **kv) -> RiskStateStore:
        return RiskStateStore(_make_redis(kv), MagicMock())

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_kill_switch_absent_returns_false(self):
        store = self._store()  # all keys return None
        active, reason = self._run(store.is_kill_switch_active())
        assert active is False
        assert reason == ""

    def test_kill_switch_set_returns_true_with_key(self):
        store = self._store(**{"risk:kill_switch": "1"})
        active, reason = self._run(store.is_kill_switch_active())
        assert active is True
        assert reason == rc.KILL_SWITCH_MANUAL

    def test_auto_halt_set_returns_reason(self):
        # No manual kill switch, but drawdown auto-halt is active
        store = self._store(**{"risk:auto_halt:drawdown": rc.AUTO_HALT_DRAWDOWN})
        active, reason = self._run(store.is_kill_switch_active())
        assert active is True
        assert reason == rc.AUTO_HALT_DRAWDOWN

    def test_auto_halt_clear_removes(self):
        r = _make_redis()
        store = RiskStateStore(r, MagicMock())
        self._run(store.clear_auto_halt("drawdown"))
        r.delete.assert_called_once_with("risk:auto_halt:drawdown")

    def test_consecutive_losses_increment_and_reset(self):
        # record_loss increments and returns new count
        r = _make_redis({"risk:consecutive_losses": "3"})
        r.incr = AsyncMock(return_value=4)
        store = RiskStateStore(r, MagicMock())

        count = self._run(store.record_loss())
        assert count == 4

        # reset_consecutive_losses clears both keys
        self._run(store.reset_consecutive_losses())
        assert r.delete.call_count == 2

    def test_cooldown_active_within_window(self):
        five_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        store = self._store(**{"risk:last_loss_at": five_min_ago})
        result = self._run(store.is_in_cooldown(30))
        assert result is True

    def test_cooldown_expired_after_window(self):
        forty_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=40)).isoformat()
        store = self._store(**{"risk:last_loss_at": forty_min_ago})
        result = self._run(store.is_in_cooldown(30))
        assert result is False

    def test_hourly_order_count_increments(self):
        r = _make_redis(zcard_result=7)
        store = RiskStateStore(r, MagicMock())
        count = self._run(store.get_hourly_order_count())
        assert count == 7

    def test_event_trade_count_tracks_cluster(self):
        store = self._store(**{"risk:event_trades:cluster-abc": "2"})
        count = self._run(store.get_event_trade_count("cluster-abc"))
        assert count == 2

    def test_positions_cache_roundtrip(self):
        now_iso = datetime.now(timezone.utc).isoformat()
        positions_data = {"AAPL": {"ticker": "AAPL", "market_value": 5000.0}}
        store = self._store(**{
            "risk:positions_cache_at": now_iso,
            "risk:positions_cache":    json.dumps(positions_data),
        })
        result = asyncio.get_event_loop().run_until_complete(
            store.get_positions_cache(30)
        )
        assert result is not None
        assert "AAPL" in result


# ─────────────────────────────────────────────────────────────────────────────
# 3 · TestKillSwitchGates
# ─────────────────────────────────────────────────────────────────────────────

class TestKillSwitchGates:
    """Kill switch (manual + auto-halt) blocks signals before broker calls."""

    def test_manual_kill_switch_blocks_signal(self):
        svc, state, blocked = _make_svc()
        state.is_kill_switch_active.return_value = (True, rc.KILL_SWITCH_MANUAL)

        result = _run(svc, _make_record())

        assert result is None
        assert len(blocked) == 1
        assert blocked[0]["code"] == rc.KILL_SWITCH_MANUAL

    def test_auto_halt_drawdown_blocks(self):
        svc, state, blocked = _make_svc()
        state.is_kill_switch_active.return_value = (True, rc.AUTO_HALT_DRAWDOWN)

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.AUTO_HALT_DRAWDOWN

    def test_auto_halt_loss_streak_blocks(self):
        svc, state, blocked = _make_svc()
        state.is_kill_switch_active.return_value = (True, rc.AUTO_HALT_LOSS_STREAK)

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.AUTO_HALT_LOSS_STREAK

    def test_no_kill_switch_allows(self):
        svc, state, blocked = _make_svc()
        # Defaults: no kill switch, no losses, 0 positions

        result = _run(svc, _make_record())

        assert result is not None
        assert len(blocked) == 0

    def test_kill_switch_emits_machine_reason_code(self):
        svc, state, blocked = _make_svc()
        state.is_kill_switch_active.return_value = (True, rc.KILL_SWITCH_MANUAL)

        _run(svc, _make_record())

        assert blocked[0]["code"].startswith("RISK:")

    def test_kill_switch_blocks_before_broker_call(self):
        """Kill switch path should not call _get_account_and_positions."""
        svc, state, blocked = _make_svc()
        state.is_kill_switch_active.return_value = (True, rc.KILL_SWITCH_MANUAL)

        broker_called = []

        async def _get_ap():
            broker_called.append(True)
            return _make_account(), []

        svc._get_account_and_positions = _get_ap
        _run(svc, _make_record())

        assert len(broker_called) == 0, "Broker should not be called when kill switch active"


# ─────────────────────────────────────────────────────────────────────────────
# 4 · TestPortfolioConstraints
# ─────────────────────────────────────────────────────────────────────────────

class TestPortfolioConstraints:
    """Constraint checks 2–9 (daily loss through sector concentration)."""

    # ── Check 2: Daily loss ────────────────────────────────────────────────────

    def test_daily_loss_halt_blocks(self):
        # daily_pnl = -6000 on equity=100000 → 6% ≥ 5% max
        svc, state, blocked = _make_svc(account=_make_account(
            equity=100_000.0, daily_pnl=-6_000.0
        ))
        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.DAILY_LOSS_HALT

    def test_daily_loss_below_threshold_allows(self):
        # daily_pnl = -4000 on equity=100000 → 4% < 5% max
        svc, state, blocked = _make_svc(account=_make_account(
            equity=100_000.0, daily_pnl=-4_000.0
        ))
        result = _run(svc, _make_record())

        assert result is not None
        assert not any(b["code"] == rc.DAILY_LOSS_HALT for b in blocked)

    # ── Check 3: Consecutive-loss cooldown ─────────────────────────────────────

    def test_consecutive_loss_cooldown_blocks(self):
        svc, state, blocked = _make_svc()
        state.get_consecutive_losses.return_value = 5  # == halt_consecutive_losses
        state.is_in_cooldown.return_value = True

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.CONSECUTIVE_LOSS_COOLDOWN

    def test_consecutive_loss_cooldown_expired_allows(self):
        svc, state, blocked = _make_svc()
        state.get_consecutive_losses.return_value = 5
        state.is_in_cooldown.return_value = False  # cooldown window passed

        result = _run(svc, _make_record())

        # Not blocked by cooldown (check 3 passes)
        assert not any(b["code"] == rc.CONSECUTIVE_LOSS_COOLDOWN for b in blocked)

    # ── Check 4: Hourly order rate ─────────────────────────────────────────────

    def test_hourly_rate_limit_blocks(self):
        svc, state, blocked = _make_svc()
        state.get_hourly_order_count.return_value = 20  # == max_orders_per_hour

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.ORDER_RATE_LIMIT

    def test_hourly_rate_below_limit_allows(self):
        svc, state, blocked = _make_svc()
        state.get_hourly_order_count.return_value = 5

        result = _run(svc, _make_record())

        assert not any(b["code"] == rc.ORDER_RATE_LIMIT for b in blocked)

    # ── Check 5: Daily order cap ───────────────────────────────────────────────

    def test_daily_order_cap_blocks(self):
        svc, state, blocked = _make_svc()
        state.get_daily_order_count.return_value = 120  # == max_orders_per_day

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.DAILY_ORDER_CAP

    # ── Check 6: Max open positions ────────────────────────────────────────────

    def test_max_positions_blocks(self):
        svc, state, blocked = _make_svc(
            positions=[_make_position(f"T{i}") for i in range(10)]
        )
        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.MAX_POSITIONS

    def test_max_positions_at_limit_minus_one_allows(self):
        svc, state, blocked = _make_svc(
            positions=[_make_position(f"T{i}") for i in range(9)]
        )
        result = _run(svc, _make_record())

        assert not any(b["code"] == rc.MAX_POSITIONS for b in blocked)

    # ── Check 7: Per-ticker exposure ───────────────────────────────────────────

    def test_ticker_exposure_blocks(self):
        # Existing MSFT position worth 15000 (≥ min(10000, 100000 * 0.05=5000) = 5000)
        existing = _make_position(ticker="MSFT", market_value=15_000.0)
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=[existing],
        )
        result = _run(svc, _make_record(ticker="MSFT"))

        assert result is None
        assert blocked[0]["code"] == rc.TICKER_EXPOSURE

    def test_ticker_new_allows(self):
        # No existing position in MSFT
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=[_make_position(ticker="AAPL", market_value=2_000.0)],
        )
        result = _run(svc, _make_record(ticker="MSFT"))

        assert not any(b["code"] == rc.TICKER_EXPOSURE for b in blocked)

    # ── Check 8: Sector concentration ─────────────────────────────────────────

    def test_sector_concentration_blocks(self):
        # 3 positions each worth 10000 in Technology → 30000/100000 = 30% ≥ 25% limit
        positions = [
            _make_position(f"T{i}", market_value=10_000.0, sector="Technology")
            for i in range(3)
        ]
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=positions,
        )
        result = _run(svc, _make_record(sectors=["Technology"]))

        assert result is None
        assert blocked[0]["code"] == rc.SECTOR_CONCENTRATION

    def test_sector_below_limit_allows(self):
        # 1 position worth 10000 in Technology → 10% < 25% limit
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=[_make_position("AAPL", market_value=10_000.0, sector="Technology")],
        )
        result = _run(svc, _make_record(sectors=["Technology"]))

        assert not any(b["code"] == rc.SECTOR_CONCENTRATION for b in blocked)

    # ── Check 9: Catalyst concentration ───────────────────────────────────────

    def test_catalyst_earnings_concentration_blocks(self):
        # 3 open earnings positions == max (risk_max_catalyst_earnings_positions=3)
        positions = [_make_position(f"E{i}", catalyst_type="earnings") for i in range(3)]
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=positions,
        )
        result = _run(svc, _make_record(catalyst_type="earnings"))

        assert result is None
        assert blocked[0]["code"] == rc.CATALYST_CONCENTRATION

    def test_catalyst_below_limit_allows(self):
        # 2 open earnings positions < max 3
        positions = [_make_position(f"E{i}", catalyst_type="earnings") for i in range(2)]
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=positions,
        )
        result = _run(svc, _make_record(catalyst_type="earnings"))

        assert not any(b["code"] == rc.CATALYST_CONCENTRATION for b in blocked)


# ─────────────────────────────────────────────────────────────────────────────
# 5 · TestCorrelationAndEventChecks
# ─────────────────────────────────────────────────────────────────────────────

class TestCorrelationAndEventChecks:
    """Constraint 10 (correlation proxy) and constraint 11 (event cluster dedup)."""

    def test_correlation_proxy_blocks_same_sector_direction(self):
        # 3 long Technology positions == max_correlated_positions(3).
        # Use catalyst_type="analyst" on positions (max=4, 3 < 4 → catalyst check passes)
        # and catalyst_type="other" on signal (0 "other" positions → catalyst check passes).
        # Correlation check then sees 3 same-sector/same-direction → blocked.
        positions = [
            _make_position(f"T{i}", qty=10, sector="Technology", catalyst_type="analyst")
            for i in range(3)
        ]
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=positions,
        )
        result = _run(svc, _make_record(
            direction="long", sectors=["Technology"], catalyst_type="other"
        ))

        assert result is None
        assert blocked[0]["code"] == rc.CORRELATION_RISK

    def test_correlation_proxy_different_sector_allows(self):
        # 3 long Technology positions, but signal is in Healthcare — no correlation
        positions = [
            _make_position(f"T{i}", qty=10, sector="Technology")
            for i in range(3)
        ]
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=positions,
        )
        result = _run(svc, _make_record(direction="long", sectors=["Healthcare"]))

        assert not any(b["code"] == rc.CORRELATION_RISK for b in blocked)

    def test_event_cluster_already_traded_blocks(self):
        svc, state, blocked = _make_svc()
        state.get_event_trade_count.return_value = 1  # == max_trades_per_cluster

        result = _run(svc, _make_record(cluster_id="cluster-xyz"))

        assert result is None
        assert blocked[0]["code"] == rc.EVENT_ALREADY_TRADED

    def test_event_cluster_new_allows(self):
        svc, state, blocked = _make_svc()
        state.get_event_trade_count.return_value = 0

        result = _run(svc, _make_record(cluster_id="cluster-new"))

        assert not any(b["code"] == rc.EVENT_ALREADY_TRADED for b in blocked)

    def test_event_cluster_count_recorded_on_approve(self):
        svc, state, blocked = _make_svc()
        state.get_event_trade_count.return_value = 0

        _run(svc, _make_record(cluster_id="cluster-rec"))

        state.record_event_trade.assert_called_once()
        call_args = state.record_event_trade.call_args
        assert call_args[0][0] == "cluster-rec"

    def test_no_cluster_id_skips_event_check(self):
        """Records without a cluster_id should never be blocked for event dedup."""
        svc, state, blocked = _make_svc()
        state.get_event_trade_count.return_value = 999  # would block if checked

        result = _run(svc, _make_record(cluster_id=None))

        # Should still pass — no cluster means no event check
        assert not any(b["code"] == rc.EVENT_ALREADY_TRADED for b in blocked)


# ─────────────────────────────────────────────────────────────────────────────
# 6 · TestPortfolioRiskIntegration
# ─────────────────────────────────────────────────────────────────────────────

class TestPortfolioRiskIntegration:
    """
    End-to-end process() tests.
    Broker, Redis state, and Kafka are all mocked.
    """

    def test_process_neutral_skips_all_checks(self):
        svc, state, blocked = _make_svc()
        result = _run(svc, _make_record(direction="neutral"))

        assert result is None
        # No checks should have run — kill switch not consulted
        state.is_kill_switch_active.assert_not_called()

    def test_process_all_checks_pass_emits_risk_approved(self):
        svc, state, blocked = _make_svc()
        result = _run(svc, _make_record())

        assert result is not None
        assert len(blocked) == 0

    def test_process_kill_switch_blocks_before_broker_call(self):
        svc, state, blocked = _make_svc()
        state.is_kill_switch_active.return_value = (True, rc.KILL_SWITCH_MANUAL)

        ap_called = []

        async def _get_ap():
            ap_called.append(True)
            return _make_account(), []

        svc._get_account_and_positions = _get_ap
        _run(svc, _make_record())

        assert len(ap_called) == 0

    def test_process_daily_loss_blocks(self):
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0, daily_pnl=-7_000.0)
        )
        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.DAILY_LOSS_HALT

    def test_process_max_positions_blocks(self):
        positions = [_make_position(f"X{i}") for i in range(10)]
        svc, state, blocked = _make_svc(positions=positions)
        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.MAX_POSITIONS

    def test_process_sector_blocks(self):
        positions = [
            _make_position(f"T{i}", market_value=12_000.0, sector="Technology")
            for i in range(3)
        ]
        svc, state, blocked = _make_svc(
            account=_make_account(equity=100_000.0),
            positions=positions,
        )
        result = _run(svc, _make_record(sectors=["Technology"]))

        assert result is None
        assert blocked[0]["code"] == rc.SECTOR_CONCENTRATION

    def test_process_catalyst_blocks(self):
        positions = [_make_position(f"E{i}", catalyst_type="earnings") for i in range(3)]
        svc, state, blocked = _make_svc(
            account=_make_account(),
            positions=positions,
        )
        result = _run(svc, _make_record(catalyst_type="earnings"))

        assert result is None
        assert blocked[0]["code"] == rc.CATALYST_CONCENTRATION

    def test_process_event_cluster_blocks(self):
        svc, state, blocked = _make_svc()
        state.get_event_trade_count.return_value = 1

        result = _run(svc, _make_record(cluster_id="dup-cluster"))

        assert result is None
        assert blocked[0]["code"] == rc.EVENT_ALREADY_TRADED

    def test_process_block_has_block_code_field(self):
        """Every block entry must contain a machine-readable code."""
        svc, state, blocked = _make_svc()
        state.is_kill_switch_active.return_value = (True, rc.KILL_SWITCH_MANUAL)

        _run(svc, _make_record())

        assert "code" in blocked[0]
        assert blocked[0]["code"] in rc.ALL_CODES

    def test_process_pass_enriches_record_with_debug_fields(self):
        svc, state, blocked = _make_svc()
        result = _run(svc, _make_record())

        assert result is not None
        assert result["portfolio_risk_checked"] is True
        assert "risk_positions_count" in result
        assert "risk_hourly_orders" in result

    def test_process_pass_increments_counters(self):
        """On approval, rate counters and event trade must be recorded."""
        svc, state, blocked = _make_svc()
        _run(svc, _make_record(cluster_id="track-me"))

        state.record_order_submitted.assert_called_once()
        state.increment_daily_orders.assert_called_once()
        state.record_event_trade.assert_called_once()

    def test_process_no_cluster_id_skips_event_recording(self):
        svc, state, blocked = _make_svc()
        _run(svc, _make_record(cluster_id=None))

        state.record_event_trade.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 7 · TestAutoHaltTriggers
# ─────────────────────────────────────────────────────────────────────────────

class TestAutoHaltTriggers:
    """
    Auto-halt state is written / cleared by the risk service itself.
    Drawdown: set when equity < HWM × (1 − halt_drawdown_pct).
    Cooldown: auto-halt loss_streak key is cleared when cooldown window expires.
    """

    def test_drawdown_breach_sets_auto_halt(self):
        # equity=85000, HWM=100000 → drawdown=15% ≥ 8% → set_auto_halt("drawdown")
        svc, state, blocked = _make_svc(
            account=_make_account(equity=85_000.0, daily_pnl=0.0)
        )
        state.get_equity_hwm.return_value = 100_000.0

        _run(svc, _make_record())

        state.set_auto_halt.assert_called_once()
        call_args = state.set_auto_halt.call_args[0]
        assert call_args[0] == "drawdown"

    def test_drawdown_breach_blocks_with_auto_halt_code(self):
        svc, state, blocked = _make_svc(
            account=_make_account(equity=85_000.0, daily_pnl=0.0)
        )
        state.get_equity_hwm.return_value = 100_000.0

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.AUTO_HALT_DRAWDOWN

    def test_loss_streak_cooldown_blocks_signal(self):
        # consecutive_losses=5, in_cooldown=True → CONSECUTIVE_LOSS_COOLDOWN
        svc, state, blocked = _make_svc()
        state.get_consecutive_losses.return_value = 5
        state.is_in_cooldown.return_value = True

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.CONSECUTIVE_LOSS_COOLDOWN

    def test_cooldown_expired_clears_loss_streak_auto_halt(self):
        # consecutive_losses=5, cooldown expired → clear_auto_halt("loss_streak") called
        svc, state, blocked = _make_svc()
        state.get_consecutive_losses.return_value = 5
        state.is_in_cooldown.return_value = False

        _run(svc, _make_record())

        state.clear_auto_halt.assert_called_once_with("loss_streak")

    def test_manual_kill_switch_overrides_auto_halt(self):
        # Both manual kill switch and auto-halt active — manual takes priority
        svc, state, blocked = _make_svc()
        # is_kill_switch_active returns the FIRST match (manual > auto-halt)
        state.is_kill_switch_active.return_value = (True, rc.KILL_SWITCH_MANUAL)

        result = _run(svc, _make_record())

        assert result is None
        assert blocked[0]["code"] == rc.KILL_SWITCH_MANUAL


# ─────────────────────────────────────────────────────────────────────────────
# 8 · TestHelperFunctions
# ─────────────────────────────────────────────────────────────────────────────

class TestHelperFunctions:
    """Module-level helper functions used by PortfolioRiskService."""

    def test_position_sector_returns_attribute(self):
        p = _make_position(sector="Healthcare")
        assert _position_sector(p) == "Healthcare"

    def test_position_sector_missing_returns_empty(self):
        # Plain object with no sector attribute
        class Bare:
            pass
        assert _position_sector(Bare()) == ""

    def test_position_direction_long_for_positive_qty(self):
        p = _make_position(qty=10)
        assert _position_direction(p) == "long"

    def test_position_direction_short_for_negative_qty(self):
        p = _make_position(qty=-5)
        assert _position_direction(p) == "short"

    def test_position_direction_zero_qty_treated_as_long(self):
        p = _make_position(qty=0)
        assert _position_direction(p) == "long"

    def test_catalyst_positions_count_exact_match(self):
        positions = [
            _make_position(catalyst_type="earnings"),
            _make_position(catalyst_type="earnings"),
            _make_position(catalyst_type="analyst"),
        ]
        assert _catalyst_positions_count(positions, "earnings") == 2

    def test_catalyst_positions_count_no_match_returns_zero(self):
        positions = [_make_position(catalyst_type="analyst")]
        assert _catalyst_positions_count(positions, "earnings") == 0
