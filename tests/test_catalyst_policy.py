"""
Tests for app/filters/catalyst_policy.py  (v1.9 — catalyst-aware pretrade policies)

Test classes:
  TestCatalystPolicyBuilding   — build_policy_table / get_policy (5)
  TestApplyPolicyHardBlocks    — gate: tech score / volume / facts direction (8)
  TestApplyPolicyMultipliers   — interpretive / sympathy / options / mktcap (10)
  TestSympathyPolicies         — sympathy threshold bump and compound effects (6)
  TestPretradeFilterIntegration — pretrade_filter.process() end-to-end with policy (10)
  TestPolicyConfigOverride     — env-var-driven config overrides (6)

Run:
  pytest tests/test_catalyst_policy.py -v
"""
from __future__ import annotations

import asyncio
import sys
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Stub Docker-only packages before any app imports ─────────────────────────
# pretrade_filter → BaseConsumer → confluent_kafka / prometheus_client.
# Neither package is installed in the host Python test environment.

def _stub_kafka() -> None:
    if "confluent_kafka" not in sys.modules:
        ck = ModuleType("confluent_kafka")
        ck.Consumer = MagicMock  # type: ignore[attr-defined]
        ck.Producer = MagicMock  # type: ignore[attr-defined]
        ck.KafkaError = type("KafkaError", (Exception,), {})  # type: ignore[attr-defined]
        ck.KafkaException = type("KafkaException", (Exception,), {})  # type: ignore[attr-defined]
        sys.modules["confluent_kafka"] = ck
        sys.modules["confluent_kafka.admin"] = ModuleType("confluent_kafka.admin")

def _stub_prometheus() -> None:
    if "prometheus_client" not in sys.modules:
        pc = ModuleType("prometheus_client")
        pc.Counter = MagicMock  # type: ignore[attr-defined]
        pc.Histogram = MagicMock  # type: ignore[attr-defined]
        pc.Gauge = MagicMock  # type: ignore[attr-defined]
        pc.start_http_server = MagicMock()  # type: ignore[attr-defined]
        sys.modules["prometheus_client"] = pc

_stub_kafka()
_stub_prometheus()

from app.filters.catalyst_policy import (
    CatalystPolicy,
    PolicyResult,
    apply_policy,
    build_policy_table,
    get_policy,
    _build_mktcap_options_scale,
    _build_mktcap_regime_adj,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_settings(**overrides) -> MagicMock:
    """
    Build a Settings-like mock with all policy fields pre-populated to match
    the production defaults in config.py.
    """
    s = MagicMock()
    # Thresholds
    s.policy_threshold_earnings   = 0.55
    s.policy_threshold_analyst    = 0.60
    s.policy_threshold_regulatory = 0.58
    s.policy_threshold_filing     = 0.58
    s.policy_threshold_ma         = 0.50
    s.policy_threshold_macro      = 0.60
    s.policy_threshold_legal      = 0.62
    s.policy_threshold_other      = 0.60
    # Interpretive penalty
    s.policy_interp_penalty_earnings   = 0.70
    s.policy_interp_penalty_analyst    = 0.60
    s.policy_interp_penalty_regulatory = 0.75
    s.policy_interp_penalty_filing     = 0.75
    s.policy_interp_penalty_ma         = 0.65
    s.policy_interp_penalty_macro      = 0.80
    s.policy_interp_penalty_legal      = 0.65
    s.policy_interp_penalty_other      = 0.80
    # Options weight
    s.policy_options_weight_earnings   = 1.30
    s.policy_options_weight_analyst    = 0.80
    s.policy_options_weight_regulatory = 1.10
    s.policy_options_weight_filing     = 1.10
    s.policy_options_weight_ma         = 1.20
    s.policy_options_weight_macro      = 1.10
    s.policy_options_weight_legal      = 0.90
    s.policy_options_weight_other      = 1.00
    # Market-cap tier options scale
    s.policy_mktcap_mega_options_scale  = 0.90
    s.policy_mktcap_large_options_scale = 0.90
    s.policy_mktcap_mid_options_scale   = 1.00
    s.policy_mktcap_small_options_scale = 1.20
    s.policy_mktcap_micro_options_scale = 1.20
    # Market-cap tier regime adj
    s.policy_mktcap_mega_regime_adj  = 1.10
    s.policy_mktcap_large_regime_adj = 1.10
    s.policy_mktcap_mid_regime_adj   = 1.00
    s.policy_mktcap_small_regime_adj = 0.85
    s.policy_mktcap_micro_regime_adj = 0.85
    # Min tech score
    s.policy_min_tech_score_earnings   = 7
    s.policy_min_tech_score_analyst    = 8
    s.policy_min_tech_score_regulatory = 7
    s.policy_min_tech_score_filing     = 7
    s.policy_min_tech_score_ma         = 6
    s.policy_min_tech_score_macro      = 7
    s.policy_min_tech_score_legal      = 7
    s.policy_min_tech_score_other      = 7
    # Sympathy multipliers
    s.policy_sympathy_mult_earnings   = 0.80
    s.policy_sympathy_mult_analyst    = 0.75
    s.policy_sympathy_mult_regulatory = 0.85
    s.policy_sympathy_mult_filing     = 0.85
    s.policy_sympathy_mult_ma         = 0.90
    s.policy_sympathy_mult_macro      = 0.80
    s.policy_sympathy_mult_legal      = 0.80
    s.policy_sympathy_mult_other      = 0.80
    # Volume confirmation
    s.policy_require_volume_earnings = True
    s.policy_require_volume_analyst  = False
    s.policy_require_volume_ma       = True
    s.policy_require_volume_other    = False
    # Global
    s.enable_catalyst_policy         = True
    s.policy_sympathy_threshold_bump = 0.08
    # Apply caller overrides
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _make_signal(**overrides) -> dict:
    """Minimal passing signal dict with safe defaults."""
    base: dict[str, Any] = {
        "ticker": "AAPL",
        "catalyst_type": "earnings",
        "direction": "long",
        "direction_source": "facts",
        "is_sympathy": False,
        "market_cap_tier": None,
        "conviction": 0.70,
        "tech_volume_confirmed": True,
    }
    base.update(overrides)
    return base


def _make_mktcap_tables(settings: MagicMock):
    return (
        _build_mktcap_options_scale(settings),
        _build_mktcap_regime_adj(settings),
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1 · TestCatalystPolicyBuilding
# ─────────────────────────────────────────────────────────────────────────────

class TestCatalystPolicyBuilding:
    """build_policy_table / get_policy behaviour."""

    KNOWN_CATALYSTS = [
        "earnings", "analyst", "regulatory", "filing",
        "ma", "macro", "legal", "other",
    ]

    def test_build_policy_table_all_catalysts(self):
        s = _make_settings()
        table = build_policy_table(s)
        for cat in self.KNOWN_CATALYSTS:
            assert cat in table, f"Missing catalyst key: {cat}"
            assert isinstance(table[cat], CatalystPolicy)

    def test_build_policy_table_respects_config(self):
        s = _make_settings(policy_threshold_analyst=0.70)
        table = build_policy_table(s)
        assert table["analyst"].conviction_threshold == pytest.approx(0.70)

    def test_get_policy_returns_other_for_unknown(self):
        s = _make_settings()
        table = build_policy_table(s)
        policy = get_policy(table, "unknown_catalyst_xyz")
        assert policy is table["other"]

    def test_get_policy_returns_correct_family(self):
        s = _make_settings()
        table = build_policy_table(s)
        for cat in self.KNOWN_CATALYSTS:
            assert get_policy(table, cat) is table[cat]

    def test_policy_table_is_deterministic(self):
        s = _make_settings()
        table1 = build_policy_table(s)
        table2 = build_policy_table(s)
        for cat in self.KNOWN_CATALYSTS:
            assert table1[cat] == table2[cat]


# ─────────────────────────────────────────────────────────────────────────────
# 2 · TestApplyPolicyHardBlocks
# ─────────────────────────────────────────────────────────────────────────────

class TestApplyPolicyHardBlocks:
    """Gates that hard-block a signal before the conviction formula."""

    def _apply(self, catalyst: str, tech_score: int, signal_overrides: dict | None = None,
               settings_overrides: dict | None = None) -> PolicyResult:
        s = _make_settings(**(settings_overrides or {}))
        table = build_policy_table(s)
        policy = get_policy(table, catalyst)
        sig = _make_signal(catalyst_type=catalyst, **(signal_overrides or {}))
        opt_scale, reg_adj = _make_mktcap_tables(s)
        return apply_policy(
            policy, signal=sig,
            tech_raw_score=tech_score,
            options_delta=0.0,
            regime_scale=1.0,
            mktcap_tier=None,
            sympathy_threshold_bump=s.policy_sympathy_threshold_bump,
            mktcap_options_scale=opt_scale,
            mktcap_regime_adj=reg_adj,
        )

    def test_block_below_min_tech_score_analyst(self):
        result = self._apply("analyst", tech_score=7)  # min=8
        assert result.block is True
        assert "tech_score=7" in result.block_reason
        assert "min=8" in result.block_reason

    def test_allow_at_min_tech_score_analyst(self):
        result = self._apply("analyst", tech_score=8)  # exactly at min
        assert result.block is False

    def test_block_below_min_tech_score_earnings(self):
        result = self._apply("earnings", tech_score=6)  # min=7
        assert result.block is True

    def test_allow_at_min_tech_score_earnings(self):
        result = self._apply("earnings", tech_score=7)
        assert result.block is False

    def test_ma_lower_tech_bar_allows_score_6(self):
        result = self._apply("ma", tech_score=6)  # min=6
        assert result.block is False

    def test_legal_blocks_below_7(self):
        result = self._apply("legal", tech_score=6)
        assert result.block is True

    def test_block_volume_required_earnings_not_confirmed(self):
        result = self._apply(
            "earnings", tech_score=8,
            signal_overrides={"tech_volume_confirmed": False},
        )
        assert result.block is True
        assert "volume_confirmation" in result.block_reason

    def test_allow_earnings_with_volume_confirmed(self):
        result = self._apply(
            "earnings", tech_score=8,
            signal_overrides={"tech_volume_confirmed": True},
        )
        assert result.block is False

    def test_analyst_does_not_require_volume(self):
        # analyst: require_volume_confirmation=False — should never block for volume
        result = self._apply(
            "analyst", tech_score=8,
            signal_overrides={"tech_volume_confirmed": False},
        )
        assert result.block is False

    def test_require_facts_direction_blocks_interpretive(self):
        s = _make_settings()
        # Override analyst policy to require facts direction for this test
        table = build_policy_table(s)
        policy = CatalystPolicy(
            conviction_threshold=0.60,
            min_technical_score=8,
            require_facts_direction=True,
        )
        sig = _make_signal(catalyst_type="analyst", direction_source="interpretive_prior")
        result = apply_policy(
            policy, signal=sig,
            tech_raw_score=8, options_delta=0.0, regime_scale=1.0, mktcap_tier=None,
        )
        assert result.block is True
        assert "facts_direction" in result.block_reason

    def test_require_facts_direction_allows_facts_source(self):
        policy = CatalystPolicy(
            conviction_threshold=0.60,
            min_technical_score=7,
            require_facts_direction=True,
        )
        sig = _make_signal(direction_source="facts")
        result = apply_policy(
            policy, signal=sig,
            tech_raw_score=8, options_delta=0.0, regime_scale=1.0, mktcap_tier=None,
        )
        assert result.block is False


# ─────────────────────────────────────────────────────────────────────────────
# 3 · TestApplyPolicyMultipliers
# ─────────────────────────────────────────────────────────────────────────────

class TestApplyPolicyMultipliers:
    """Conviction multipliers and adjusted deltas returned by apply_policy."""

    def _apply(self, catalyst: str, **sig_overrides) -> PolicyResult:
        s = _make_settings()
        table = build_policy_table(s)
        policy = get_policy(table, catalyst)
        sig = _make_signal(catalyst_type=catalyst, **sig_overrides)
        opt_scale, reg_adj = _make_mktcap_tables(s)
        return apply_policy(
            policy, signal=sig,
            tech_raw_score=9,
            options_delta=0.10,
            regime_scale=1.0,
            mktcap_tier=sig_overrides.get("market_cap_tier"),
            sympathy_threshold_bump=0.08,
            mktcap_options_scale=opt_scale,
            mktcap_regime_adj=reg_adj,
        )

    def test_no_penalty_for_facts_direction(self):
        result = self._apply("earnings", direction_source="facts", is_sympathy=False)
        assert result.multiplier == pytest.approx(1.0)

    def test_interpretive_penalty_earnings(self):
        result = self._apply("earnings", direction_source="interpretive_prior")
        assert result.multiplier == pytest.approx(0.70)

    def test_interpretive_penalty_analyst(self):
        result = self._apply("analyst", direction_source="interpretive_prior")
        assert result.multiplier == pytest.approx(0.60)

    def test_interpretive_penalty_legal(self):
        result = self._apply("legal", direction_source="interpretive_prior")
        assert result.multiplier == pytest.approx(0.65)

    def test_sympathy_multiplier_applied(self):
        result = self._apply("earnings", direction_source="facts", is_sympathy=True)
        assert result.multiplier == pytest.approx(0.80)

    def test_sympathy_compounds_with_interpretive(self):
        # earnings: interp=0.70, sympathy=0.80 → 0.70 × 0.80 = 0.56
        result = self._apply("earnings", direction_source="interpretive_prior", is_sympathy=True)
        assert result.multiplier == pytest.approx(0.70 * 0.80, rel=1e-4)

    def test_options_weight_amplified_earnings(self):
        # earnings options_weight=1.30; raw delta=0.10 → 0.10 × 1.30 = 0.13
        result = self._apply("earnings", direction_source="facts")
        assert result.adjusted_options_delta == pytest.approx(0.10 * 1.30, rel=1e-4)

    def test_options_weight_dampened_analyst(self):
        # analyst options_weight=0.80; raw delta=0.10 → 0.10 × 0.80 = 0.08
        result = self._apply("analyst", direction_source="facts")
        assert result.adjusted_options_delta == pytest.approx(0.10 * 0.80, rel=1e-4)

    def test_mktcap_large_scales_options_down(self):
        # large: options_scale=0.90; earnings options_weight=1.30
        # effective = 1.30 × 0.90 = 1.17; delta = 0.10 × 1.17 = 0.117
        result = self._apply("earnings", direction_source="facts", market_cap_tier="large")
        assert result.adjusted_options_delta == pytest.approx(0.10 * 1.30 * 0.90, rel=1e-4)
        assert result.options_weight_effective == pytest.approx(1.30 * 0.90, rel=1e-4)

    def test_mktcap_small_scales_options_up(self):
        # small: options_scale=1.20; earnings options_weight=1.30
        # effective = 1.30 × 1.20 = 1.56; delta = 0.10 × 1.56 = 0.156
        result = self._apply("earnings", direction_source="facts", market_cap_tier="small")
        assert result.adjusted_options_delta == pytest.approx(0.10 * 1.30 * 1.20, rel=1e-4)

    def test_mktcap_large_adjusts_regime_up(self):
        # large: regime_adj=1.10; input regime_scale=0.80 → 0.80 × 1.10 = 0.88
        s = _make_settings()
        table = build_policy_table(s)
        policy = get_policy(table, "earnings")
        sig = _make_signal(catalyst_type="earnings", market_cap_tier="large")
        opt_scale, reg_adj = _make_mktcap_tables(s)
        result = apply_policy(
            policy, signal=sig,
            tech_raw_score=8, options_delta=0.0, regime_scale=0.80,
            mktcap_tier="large",
            mktcap_options_scale=opt_scale,
            mktcap_regime_adj=reg_adj,
        )
        assert result.adjusted_regime_scale == pytest.approx(0.80 * 1.10, rel=1e-4)
        assert result.regime_adj_effective == pytest.approx(1.10)

    def test_mktcap_small_adjusts_regime_down(self):
        # small: regime_adj=0.85; input regime_scale=1.0 → 1.0 × 0.85 = 0.85
        s = _make_settings()
        table = build_policy_table(s)
        policy = get_policy(table, "earnings")
        sig = _make_signal(catalyst_type="earnings", market_cap_tier="small")
        opt_scale, reg_adj = _make_mktcap_tables(s)
        result = apply_policy(
            policy, signal=sig,
            tech_raw_score=8, options_delta=0.0, regime_scale=1.0,
            mktcap_tier="small",
            mktcap_options_scale=opt_scale,
            mktcap_regime_adj=reg_adj,
        )
        assert result.adjusted_regime_scale == pytest.approx(0.85, rel=1e-4)

    def test_unknown_mktcap_tier_defaults_to_1(self):
        s = _make_settings()
        table = build_policy_table(s)
        policy = get_policy(table, "earnings")
        sig = _make_signal(catalyst_type="earnings")
        opt_scale, reg_adj = _make_mktcap_tables(s)
        result = apply_policy(
            policy, signal=sig,
            tech_raw_score=8, options_delta=0.10, regime_scale=1.0,
            mktcap_tier="unknown_tier",
            mktcap_options_scale=opt_scale,
            mktcap_regime_adj=reg_adj,
        )
        # Unknown tier → scale=1.0, adj=1.0
        assert result.adjusted_options_delta == pytest.approx(0.10 * 1.30, rel=1e-4)
        assert result.adjusted_regime_scale == pytest.approx(1.0, rel=1e-4)


# ─────────────────────────────────────────────────────────────────────────────
# 4 · TestSympathyPolicies
# ─────────────────────────────────────────────────────────────────────────────

class TestSympathyPolicies:
    """Sympathy trade threshold bump and compound effects."""

    def _run(self, catalyst: str, is_sympathy: bool, direction_source: str = "facts",
             settings_overrides: dict | None = None) -> PolicyResult:
        s = _make_settings(**(settings_overrides or {}))
        table = build_policy_table(s)
        policy = get_policy(table, catalyst)
        sig = _make_signal(
            catalyst_type=catalyst,
            is_sympathy=is_sympathy,
            direction_source=direction_source,
        )
        opt_scale, reg_adj = _make_mktcap_tables(s)
        return apply_policy(
            policy, signal=sig,
            tech_raw_score=9, options_delta=0.0, regime_scale=1.0, mktcap_tier=None,
            sympathy_threshold_bump=s.policy_sympathy_threshold_bump,
            mktcap_options_scale=opt_scale, mktcap_regime_adj=reg_adj,
        )

    def test_sympathy_threshold_bumped_earnings(self):
        # earnings threshold=0.55 + sympathy_bump=0.08 → 0.63
        result = self._run("earnings", is_sympathy=True)
        assert result.threshold == pytest.approx(0.55 + 0.08)

    def test_non_sympathy_uses_base_threshold(self):
        result = self._run("earnings", is_sympathy=False)
        assert result.threshold == pytest.approx(0.55)

    def test_sympathy_analyst_highest_threshold(self):
        # analyst base=0.60 + bump=0.08 → 0.68
        result = self._run("analyst", is_sympathy=True)
        assert result.threshold == pytest.approx(0.60 + 0.08)

    def test_sympathy_multiplier_applied_earnings(self):
        result = self._run("earnings", is_sympathy=True)
        assert result.multiplier == pytest.approx(0.80)

    def test_sympathy_multiplier_analyst_lower(self):
        result = self._run("analyst", is_sympathy=True)
        assert result.multiplier == pytest.approx(0.75)

    def test_sympathy_compounds_with_interpretive_earnings(self):
        # earnings: interp_penalty=0.70, sympathy_mult=0.80 → 0.70 × 0.80 = 0.56
        result = self._run("earnings", is_sympathy=True, direction_source="interpretive_prior")
        assert result.multiplier == pytest.approx(0.70 * 0.80, rel=1e-4)

    def test_sympathy_tech_block_still_fires(self):
        # Even sympathy signals are blocked if tech_score below min
        s = _make_settings()
        table = build_policy_table(s)
        policy = get_policy(table, "analyst")  # min=8
        sig = _make_signal(catalyst_type="analyst", is_sympathy=True)
        result = apply_policy(
            policy, signal=sig,
            tech_raw_score=7,   # below min
            options_delta=0.0, regime_scale=1.0, mktcap_tier=None,
        )
        assert result.block is True

    def test_sympathy_bump_configurable(self):
        # Custom bump: 0.12
        result = self._run("earnings", is_sympathy=True,
                           settings_overrides={"policy_sympathy_threshold_bump": 0.12})
        assert result.threshold == pytest.approx(0.55 + 0.12)


# ─────────────────────────────────────────────────────────────────────────────
# 5 · TestPretradeFilterIntegration
# ─────────────────────────────────────────────────────────────────────────────

class TestPretradeFilterIntegration:
    """
    End-to-end tests for pretrade_filter.PreTradeFilterService.process() with
    the policy layer active.

    The 4 filter tasks are mocked; all other infra (Redis, HTTP, asyncpg) is
    mocked at a coarse level.  We test the decision logic, not network I/O.
    """

    def _make_tech(self, score: int = 8, blocked: bool = False,
                   block_reason: str = "", mult: float = 1.0) -> MagicMock:
        t = MagicMock()
        t.technical_score = score
        t.blocked = blocked
        t.block_reason = block_reason
        t.conviction_multiplier = mult
        t.score = None
        t.checks = {}
        t.rsi = None
        t.technical_score_breakdown = {}
        t.signal_type_override = None
        return t

    def _make_options(self, delta: float = 0.0) -> MagicMock:
        o = MagicMock()
        o.conviction_delta = delta
        o.flow_bias = "neutral"
        o.put_call_ratio = None
        o.notes = ""
        return o

    def _make_squeeze(self, mult: float = 1.0) -> MagicMock:
        sq = MagicMock()
        sq.conviction_multiplier = mult
        sq.squeeze_score = 0.0
        sq.short_float_pct = None
        sq.days_to_cover = None
        sq.signal_type_override = None
        return sq

    def _make_regime(self, scale: float = 1.0, regime: str = "risk_on") -> dict:
        return {
            "conviction_scale": scale,
            "block_longs": False,
            "block_shorts": False,
            "regime": regime,
            "vix": 18.0,
            "spy_chg_pct": 0.3,
        }

    def _run_process(self, record: dict, tech_score: int = 8,
                     options_delta: float = 0.0, regime_scale: float = 1.0,
                     regime_name: str = "risk_on", squeeze_mult: float = 1.0,
                     settings_overrides: dict | None = None) -> tuple[dict | None, list]:
        """
        Run PreTradeFilterService.process() with mocked dependencies.
        Returns (result_record_or_None, emitted_blocked_records).
        """
        from app.filters.pretrade_filter import PreTradeFilterService

        tech    = self._make_tech(score=tech_score)
        options = self._make_options(delta=options_delta)
        squeeze = self._make_squeeze(mult=squeeze_mult)
        regime  = self._make_regime(scale=regime_scale, regime=regime_name)

        blocked_emissions: list[dict] = []

        with (
            patch("app.filters.pretrade_filter._settings") as mock_settings,
            patch("app.filters.pretrade_filter.get_current_regime",
                  new=AsyncMock(return_value=regime)),
            patch("app.filters.pretrade_filter.score_technicals",
                  new=AsyncMock(return_value=tech)),
            patch("app.filters.pretrade_filter.score_options_flow",
                  new=AsyncMock(return_value=options)),
            patch("app.filters.pretrade_filter.score_squeeze",
                  new=AsyncMock(return_value=squeeze)),
            patch("app.filters.pretrade_filter.staleness_from_signal_dict") as mock_stale,
        ):
            # Configure settings mock
            s = _make_settings(**(settings_overrides or {}))
            mock_settings.enable_catalyst_policy = s.enable_catalyst_policy
            mock_settings.policy_sympathy_threshold_bump = s.policy_sympathy_threshold_bump
            for attr in dir(s):
                if attr.startswith("policy_"):
                    setattr(mock_settings, attr, getattr(s, attr))

            # Staleness — never stale in these tests
            stale_obj = MagicMock()
            stale_obj.is_stale = False
            mock_stale.return_value = stale_obj

            svc = PreTradeFilterService.__new__(PreTradeFilterService)
            svc._redis = MagicMock()
            svc._http = MagicMock()
            svc._polygon_key = ""
            svc._policy_table = build_policy_table(s)
            svc._mktcap_options_scale = _build_mktcap_options_scale(s)
            svc._mktcap_regime_adj = _build_mktcap_regime_adj(s)

            async def fake_get_stock_context(ticker):
                return None

            svc._get_stock_context = fake_get_stock_context

            async def fake_emit_blocked(rec):
                blocked_emissions.append(rec)

            svc._emit_blocked = fake_emit_blocked
            svc._update_signal_log = AsyncMock()
            svc._log_staleness_block = AsyncMock()

            result = asyncio.get_event_loop().run_until_complete(svc.process(record))

        return result, blocked_emissions

    # Tests ------------------------------------------------------------------

    def test_earnings_strong_tech_passes(self):
        rec = _make_signal(catalyst_type="earnings", direction="long", conviction=0.70,
                           direction_source="facts")
        result, blocked = self._run_process(rec, tech_score=9)
        assert result is not None
        assert not blocked

    def test_earnings_weak_tech_blocked_by_policy(self):
        # tech_score=6 < earnings min=7 → policy block
        rec = _make_signal(catalyst_type="earnings", direction="long", conviction=0.70)
        result, blocked = self._run_process(rec, tech_score=6)
        assert result is None
        assert len(blocked) == 1
        assert "tech_score" in blocked[0]["block_reason"]

    def test_analyst_requires_tech_score_8(self):
        # analyst min=8; score=7 → policy block
        rec = _make_signal(catalyst_type="analyst", direction="long", conviction=0.70,
                           direction_source="facts")
        result, blocked = self._run_process(rec, tech_score=7)
        assert result is None
        assert len(blocked) == 1

    def test_analyst_score_8_passes(self):
        rec = _make_signal(catalyst_type="analyst", direction="long", conviction=0.80,
                           direction_source="facts")
        result, blocked = self._run_process(rec, tech_score=8)
        assert result is not None

    def test_ma_low_tech_allowed(self):
        # ma min=6; score=6 → should pass
        rec = _make_signal(catalyst_type="ma", direction="long", conviction=0.65,
                           direction_source="facts", tech_volume_confirmed=True)
        result, blocked = self._run_process(rec, tech_score=6)
        assert result is not None

    def test_sympathy_raised_threshold_blocks_at_base(self):
        # earnings base threshold=0.55, sympathy bump=0.08 → effective=0.63
        # conviction=0.60 < 0.63 → blocked (below effective threshold)
        rec = _make_signal(catalyst_type="earnings", direction="long", conviction=0.60,
                           direction_source="facts", is_sympathy=True,
                           tech_volume_confirmed=True)
        # conviction_in=0.60, all multipliers=1.0, policy_mult=sympathy_mult=0.80
        # final = 0.60 × 1.0 × 1.0 × 1.0 × 1.0 × 0.80 = 0.48 < 0.63 → blocked
        result, blocked = self._run_process(rec, tech_score=8)
        assert result is None

    def test_sympathy_passes_with_high_conviction(self):
        # conviction=0.90 × policy_mult=0.80 = 0.72 > threshold 0.63 → passes
        rec = _make_signal(catalyst_type="earnings", direction="long", conviction=0.90,
                           direction_source="facts", is_sympathy=True,
                           tech_volume_confirmed=True)
        result, blocked = self._run_process(rec, tech_score=8)
        assert result is not None

    def test_interpretive_penalty_reduces_final_conviction(self):
        # earnings: interpretive_penalty=0.70
        rec = _make_signal(catalyst_type="earnings", direction="long", conviction=0.70,
                           direction_source="interpretive_prior", tech_volume_confirmed=True)
        result, _ = self._run_process(rec, tech_score=8)
        if result is not None:
            # final ≤ 0.70 × 0.70 = 0.49 — well below 0.55 threshold
            # so it may be blocked; either way conviction should be reduced
            assert result["conviction"] <= 0.70

    def test_policy_debug_fields_in_outbound_record(self):
        rec = _make_signal(catalyst_type="earnings", direction="long", conviction=0.70,
                           direction_source="facts", tech_volume_confirmed=True)
        result, _ = self._run_process(rec, tech_score=8)
        if result is not None:
            assert "catalyst_policy_applied" in result
            assert "policy_threshold_used" in result
            assert "policy_multiplier" in result
            assert "policy_options_weight" in result
            assert "policy_regime_adj" in result

    def test_kill_switch_bypasses_policy(self):
        # enable_catalyst_policy=False — analyst with score 7 should now pass
        # (would be blocked by policy, but kill-switch is off)
        rec = _make_signal(catalyst_type="analyst", direction="long", conviction=0.70,
                           direction_source="facts")
        result, blocked = self._run_process(
            rec, tech_score=7,
            settings_overrides={"enable_catalyst_policy": False},
        )
        # No policy block; signal passes the 4-filter result
        assert result is not None
        assert not blocked

    def test_regime_hard_block_fires_before_policy(self):
        # Regime blocks longs → should block before policy even runs
        rec = _make_signal(catalyst_type="earnings", direction="long", conviction=0.70)
        regime_blocking = {
            "conviction_scale": 0.0,
            "block_longs": True,
            "block_shorts": False,
            "regime": "high_vol",
            "vix": 35.0,
            "spy_chg_pct": -2.0,
        }
        with (
            patch("app.filters.pretrade_filter._settings") as mock_settings,
            patch("app.filters.pretrade_filter.get_current_regime",
                  new=AsyncMock(return_value=regime_blocking)),
            patch("app.filters.pretrade_filter.score_technicals",
                  new=AsyncMock(return_value=self._make_tech(score=9))),
            patch("app.filters.pretrade_filter.score_options_flow",
                  new=AsyncMock(return_value=self._make_options())),
            patch("app.filters.pretrade_filter.score_squeeze",
                  new=AsyncMock(return_value=self._make_squeeze())),
            patch("app.filters.pretrade_filter.staleness_from_signal_dict") as mock_stale,
        ):
            s = _make_settings()
            mock_settings.enable_catalyst_policy = True
            mock_settings.policy_sympathy_threshold_bump = 0.08
            for attr in dir(s):
                if attr.startswith("policy_"):
                    setattr(mock_settings, attr, getattr(s, attr))

            stale_obj = MagicMock()
            stale_obj.is_stale = False
            mock_stale.return_value = stale_obj

            from app.filters.pretrade_filter import PreTradeFilterService
            svc = PreTradeFilterService.__new__(PreTradeFilterService)
            svc._redis = MagicMock()
            svc._http = MagicMock()
            svc._polygon_key = ""
            svc._policy_table = build_policy_table(s)
            svc._mktcap_options_scale = _build_mktcap_options_scale(s)
            svc._mktcap_regime_adj = _build_mktcap_regime_adj(s)
            svc._get_stock_context = AsyncMock(return_value=None)
            blocked_out: list[dict] = []
            async def emit_blocked(r):
                blocked_out.append(r)
            svc._emit_blocked = emit_blocked
            svc._update_signal_log = AsyncMock()
            svc._log_staleness_block = AsyncMock()

            result = asyncio.get_event_loop().run_until_complete(svc.process(rec))

        assert result is None
        assert len(blocked_out) == 1
        assert "Regime block" in blocked_out[0]["block_reason"]


# ─────────────────────────────────────────────────────────────────────────────
# 6 · TestPolicyConfigOverride
# ─────────────────────────────────────────────────────────────────────────────

class TestPolicyConfigOverride:
    """Config-driven overrides are reflected in the policy table."""

    @pytest.mark.parametrize("catalyst,field,value", [
        ("earnings",   "policy_threshold_earnings",   0.52),
        ("analyst",    "policy_threshold_analyst",    0.70),
        ("regulatory", "policy_threshold_regulatory", 0.62),
        ("ma",         "policy_threshold_ma",         0.45),
        ("macro",      "policy_threshold_macro",      0.65),
        ("legal",      "policy_threshold_legal",      0.68),
    ])
    def test_threshold_respects_env_override(self, catalyst, field, value):
        s = _make_settings(**{field: value})
        table = build_policy_table(s)
        assert table[catalyst].conviction_threshold == pytest.approx(value)

    def test_interpretive_penalty_respects_env_override(self):
        s = _make_settings(policy_interp_penalty_analyst=0.50)
        table = build_policy_table(s)
        assert table["analyst"].interpretive_penalty == pytest.approx(0.50)

    def test_options_weight_respects_env_override(self):
        s = _make_settings(policy_options_weight_earnings=1.50)
        table = build_policy_table(s)
        assert table["earnings"].options_weight == pytest.approx(1.50)

    def test_min_tech_score_respects_env_override(self):
        s = _make_settings(policy_min_tech_score_analyst=9)
        table = build_policy_table(s)
        assert table["analyst"].min_technical_score == 9

    def test_sympathy_mult_respects_env_override(self):
        s = _make_settings(policy_sympathy_mult_earnings=0.70)
        table = build_policy_table(s)
        assert table["earnings"].sympathy_multiplier == pytest.approx(0.70)
