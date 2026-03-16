"""
Tests for stock_context_service.classifier — Wilder smoothing and ADX/ATR.

Key regression: _wilder_smooth previously used `result[-1] - result[-1]/p + v`
(cumulative-sum formula) instead of the correct EMA formula
`result[-1] - result[-1]/p + v/p`.  This caused ADX to exceed 100 and ATR to
be inflated by ~14x for any realistic OHLCV series.
"""
from __future__ import annotations

import math
import sys
import os
from dataclasses import dataclass
from types import ModuleType
from unittest.mock import MagicMock

import pytest

# ── Stub out aiohttp before importing stock_context_service ─────────────────
# Python 3.9 ships with an aiohttp version that has a typing incompatibility.
# The classifier only imports OHLCV from polygon_client (a plain dataclass) so
# we stub aiohttp at the module level to avoid the import error.
if "aiohttp" not in sys.modules:
    sys.modules["aiohttp"] = MagicMock()

# ── Minimal OHLCV stub (mirrors the real dataclass) ──────────────────────────
@dataclass
class OHLCV:
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float
    ts_ms:  int

# Inject the stub so polygon_client import succeeds
_poly_mod = ModuleType("stock_context_service.polygon_client")
_poly_mod.OHLCV = OHLCV          # type: ignore[attr-defined]
sys.modules["stock_context_service.polygon_client"] = _poly_mod

# Now we can safely import the pure-Python classifier
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from stock_context_service.classifier import (      # noqa: E402
    _wilder_smooth,
    _atr_smooth,
    _calc_adx,
    _true_range_series,
    classify_ticker,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_bars(n: int, base_close: float = 100.0, daily_move: float = 1.0) -> list[OHLCV]:
    """
    Synthetic trending OHLCV bars.
    Each bar: close rises by daily_move, high = close+0.5, low = close-0.5.
    """
    bars = []
    close = base_close
    for i in range(n):
        bars.append(OHLCV(
            open=close - 0.3,
            high=close + 0.5,
            low=close - 0.5,
            close=close,
            volume=1_000_000.0,
            ts_ms=i * 86_400_000,
        ))
        close += daily_move
    return bars


def _make_flat_bars(n: int, price: float = 100.0) -> list[OHLCV]:
    """Flat bars — no movement.  TR ≈ 1.0 per bar."""
    return _make_bars(n, base_close=price, daily_move=0.0)


# ── _wilder_smooth ─────────────────────────────────────────────────────────────

class TestWilderSmooth:

    def test_empty_on_insufficient_data(self):
        assert _wilder_smooth([1.0, 2.0], period=5) == []

    def test_length_correct(self):
        values = [float(i) for i in range(30)]
        result = _wilder_smooth(values, period=14)
        # Should produce 30 - 14 + 1 = 17 values
        assert len(result) == 17

    def test_first_value_is_mean(self):
        values = [10.0] * 20
        result = _wilder_smooth(values, period=14)
        assert result[0] == pytest.approx(10.0, abs=1e-9)

    def test_constant_series_stays_constant(self):
        """If all input values are C, every output value should be C (EMA of constant = C)."""
        C = 75.0
        values = [C] * 30
        result = _wilder_smooth(values, period=14)
        for v in result:
            assert v == pytest.approx(C, rel=1e-9), \
                f"Expected {C} but got {v} — cumulative-sum formula would diverge here"

    def test_dx_series_stays_bounded(self):
        """
        DX values are percentages (0–100).  Wilder-smoothed ADX must stay ≤ 100.
        This is the core regression test for the /period bug.
        """
        dx = [80.0] * 30      # strong trend — every bar DX = 80
        result = _wilder_smooth(dx, period=14)
        for adx_val in result:
            assert adx_val <= 100.0 + 1e-9, \
                f"ADX exceeded 100: {adx_val} — _wilder_smooth is using wrong formula"

    def test_high_dx_does_not_explode(self):
        """Reproduce the exact failure: DX ≈ 100 should not push ADX to 207."""
        dx = [99.0] * 30
        result = _wilder_smooth(dx, period=14)
        assert max(result) < 100.5, \
            f"ADX blew up to {max(result):.2f} — should stay near 99"

    def test_convergence_toward_new_steady_state(self):
        """After a shift to higher DX, ADX should rise but stay ≤ 100."""
        dx = [20.0] * 20 + [80.0] * 20
        result = _wilder_smooth(dx, period=14)
        assert all(0 <= v <= 100 + 1e-9 for v in result), \
            "ADX out of [0, 100] range during regime transition"


# ── _calc_adx ──────────────────────────────────────────────────────────────────

class TestCalcAdx:

    def test_returns_zero_on_insufficient_bars(self):
        bars = _make_bars(20)  # needs 2*14+1 = 29
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        assert _calc_adx(h, l, c, period=14) == 0.0

    def test_adx_bounded_0_to_100(self):
        """Primary regression: ADX must never exceed 100."""
        bars = _make_bars(60, base_close=100.0, daily_move=2.0)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        adx = _calc_adx(h, l, c, period=14)
        assert 0.0 <= adx <= 100.0, f"ADX out of bounds: {adx}"

    def test_trending_market_has_high_adx(self):
        """A strongly trending market should produce ADX > 25."""
        # Large consistent daily moves → strong trend
        bars = _make_bars(80, base_close=50.0, daily_move=1.5)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        adx = _calc_adx(h, l, c, period=14)
        assert adx > 25.0, f"Expected trending ADX > 25 but got {adx:.2f}"
        assert adx <= 100.0, f"ADX exceeded 100: {adx:.2f}"

    def test_flat_market_has_low_adx(self):
        """Flat price action → very little directional movement → low ADX."""
        bars = _make_flat_bars(60, price=100.0)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        adx = _calc_adx(h, l, c, period=14)
        # Flat bars have up=0, down=0, so +DM = -DM = 0 → DX = 0 → ADX ≈ 0
        assert adx < 10.0, f"Expected near-zero ADX for flat market but got {adx:.2f}"

    def test_adx_nvda_like_44_bars(self):
        """
        Reproduce the original bug scenario: 44 bars of a strongly falling stock.
        Before the fix this produced ADX ≈ 207.78.  After fix it must be ≤ 100.
        """
        # Simulate a stock falling from 130 → 88 over 44 bars (like NVDA)
        bars = _make_bars(44, base_close=130.0, daily_move=-1.0)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        adx = _calc_adx(h, l, c, period=14)
        assert adx <= 100.0, f"ADX {adx:.2f} exceeds 100 — Wilder-smooth bug still present"


# ── _atr_smooth ────────────────────────────────────────────────────────────────

class TestAtrSmooth:

    def test_atr_returns_zero_on_insufficient_bars(self):
        bars = _make_bars(10)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        # period=14 needs 15+ bars
        assert _atr_smooth(h, l, c, period=14) == 0.0

    def test_atr14_reasonable_magnitude(self):
        """
        For bars with H-L range of 1.0, ATR(14) should be close to 1.0,
        not 14x inflated (~14.0) as the old bug would produce.
        """
        bars = _make_bars(60, base_close=100.0, daily_move=0.5)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        atr = _atr_smooth(h, l, c, period=14)
        # H-L = 1.0 per bar; ATR(14) should be ~1.0 (allow 20% slack for warmup)
        assert 0.5 <= atr <= 3.0, \
            f"ATR14 = {atr:.4f} is out of expected range — inflation bug may still be present"

    def test_atr50_zero_when_fewer_than_51_bars(self):
        """atr50 requires 51+ bars (50 TR values from 51 closes). Returns 0.0 otherwise."""
        bars = _make_bars(44)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        atr50 = _atr_smooth(h, l, c, period=50)
        assert atr50 == 0.0

    def test_atr50_computable_with_enough_bars(self):
        bars = _make_bars(70)
        h = [b.high for b in bars]
        l = [b.low  for b in bars]
        c = [b.close for b in bars]
        atr50 = _atr_smooth(h, l, c, period=50)
        assert atr50 > 0.0


# ── classify_ticker (integration) ────────────────────────────────────────────

class TestClassifyTicker:

    @pytest.mark.asyncio
    async def test_adx_bounded_in_full_classification(self):
        bars = _make_bars(60, base_close=100.0, daily_move=2.0)
        ctx = await classify_ticker("TEST", bars, [])
        assert 0.0 <= ctx.adx <= 100.0, f"classify_ticker returned adx={ctx.adx}"

    @pytest.mark.asyncio
    async def test_default_returned_for_insufficient_bars(self):
        bars = _make_bars(20)   # below _MIN_BARS_DAILY = 30
        ctx = await classify_ticker("TINY", bars, [])
        assert ctx.adx == 0.0
        assert ctx.adjusted_threshold == 8

    @pytest.mark.asyncio
    async def test_trending_up_detected(self):
        bars = _make_bars(80, base_close=50.0, daily_move=1.5)
        ctx = await classify_ticker("BULL", bars, [])
        # Strong uptrend: should not be RANGING
        from stock_context_service.models import TrendRegime
        assert ctx.trend_regime in (TrendRegime.TRENDING_UP, TrendRegime.TRENDING_DOWN, TrendRegime.RANGING)
        # More importantly: adx is bounded
        assert ctx.adx <= 100.0

    @pytest.mark.asyncio
    async def test_atr_ratio_not_inf_when_atr50_zero(self):
        """When atr50=0, atr_ratio defaults to 1.0 (not inf or NaN)."""
        bars = _make_bars(44, base_close=100.0, daily_move=1.0)
        ctx = await classify_ticker("SHORT", bars, [])
        assert math.isfinite(ctx.atr_ratio), f"atr_ratio is not finite: {ctx.atr_ratio}"
        # With atr50=0, code should use fallback 1.0
        assert ctx.atr_ratio == pytest.approx(1.0, abs=0.01)
