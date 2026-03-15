"""
Core classification logic for the Stock Context Classifier.

classify_ticker() is the single public entry point.  It accepts lists of
OHLCV bars (daily + hourly) and returns a fully populated StockContext.

Classification pipeline
-----------------------
1. Trend detection   — ADX, 20-day MA slope, higher-highs / higher-lows
2. Volatility regime — ATR(14)/ATR(50) ratio, Bollinger-Band-width trend
3. Cleanliness score — MA-respect count, wick ratio, return std-dev
4. Threshold mapping — combine classifications → adjusted_threshold (7/8/9)
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Sequence

from .models import (
    Cleanliness,
    StockContext,
    TrendRegime,
    VolatilityRegime,
)
from .polygon_client import OHLCV

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
_ADX_TRENDING       = 25.0
_MA_SLOPE_TRENDING  = 0.3     # pct; above → up-trend, below negative → down-trend
_ATR_EXPAND         = 1.3     # ATR(14)/ATR(50) above this → expanding vol
_ATR_CONTRACT       = 0.8     # … below this → contracting vol
_MA_RESPECT_CLEAN   = 14      # out of last 20 daily closes
_MA_RESPECT_CHOPPY  = 10
_WICK_RATIO_CLEAN   = 0.45
_WICK_RATIO_CHOPPY  = 0.60
_RETURN_STD_CLEAN   = 0.025
_RETURN_STD_CHOPPY  = 0.040
_MIN_BARS_DAILY     = 30      # minimum bars to attempt classification


# ── Public entry point ────────────────────────────────────────────────────────

async def classify_ticker(
    ticker:       str,
    daily_bars:   list[OHLCV],
    hourly_bars:  list[OHLCV],
) -> StockContext:
    """
    Classify a ticker's current market context.

    Parameters
    ----------
    ticker      : Equity symbol (used for logging only).
    daily_bars  : At least 30 daily OHLCV bars (60 preferred).
    hourly_bars : Recent hourly bars (used for future extensions; not
                  currently required for classification).

    Returns
    -------
    StockContext with all fields populated.  Falls back to conservative
    defaults (RANGING / NORMAL / INSUFFICIENT_DATA / threshold=8) when
    there are insufficient bars to compute reliable statistics.
    """
    if len(daily_bars) < _MIN_BARS_DAILY:
        logger.warning(
            "classifier.insufficient_bars",
            extra={"ticker": ticker, "bars": len(daily_bars), "min": _MIN_BARS_DAILY},
        )
        return StockContext.default(ticker)

    closes  = [b.close  for b in daily_bars]
    highs   = [b.high   for b in daily_bars]
    lows    = [b.low    for b in daily_bars]
    opens_  = [b.open   for b in daily_bars]

    # ── 1. Technical indicators ───────────────────────────────────────────────
    adx_val          = _calc_adx(highs, lows, closes, period=14)
    ma20             = _sma(closes, 20)
    ma_slope         = _ma_slope(closes, 20, lookback=5)
    atr14            = _atr_smooth(highs, lows, closes, period=14)
    atr50            = _atr_smooth(highs, lows, closes, period=50)
    atr_ratio        = (atr14 / atr50) if atr50 > 0 else 1.0
    bb_width_series  = _bb_width_series(closes, period=20, num_std=2.0, length=22)
    bb_width_trend   = _bb_width_direction(bb_width_series)
    hh_hl            = _higher_highs_higher_lows(highs, lows, window=20)

    # ── 2. Trend regime ───────────────────────────────────────────────────────
    trend = _classify_trend(adx_val, ma_slope, hh_hl)

    # ── 3. Volatility regime ─────────────────────────────────────────────────
    vol = _classify_volatility(atr_ratio, bb_width_trend)

    # ── 4. Cleanliness ────────────────────────────────────────────────────────
    last20_closes = closes[-20:]
    last20_highs  = highs[-20:]
    last20_lows   = lows[-20:]
    last20_opens  = opens_[-20:]
    last20_ma20   = _rolling_sma(closes, 20)[-20:]

    ma_respect     = _ma_respect_count(last20_closes, last20_ma20, trend)
    avg_wick_ratio = _avg_wick_ratio(last20_opens, last20_closes, last20_highs, last20_lows)
    return_std     = _return_std(last20_closes)
    clean          = _classify_cleanliness(ma_respect, avg_wick_ratio, return_std)

    # ── 5. Adjusted threshold ────────────────────────────────────────────────
    threshold = _adjusted_threshold(trend, clean)

    raw_metrics = {
        "adx":            round(adx_val, 2),
        "ma_slope_20":    round(ma_slope, 4),
        "atr14":          round(atr14, 4),
        "atr50":          round(atr50, 4),
        "atr_ratio":      round(atr_ratio, 3),
        "bb_width_trend": bb_width_trend,
        "hh_hl":          hh_hl,
        "ma_respect":     ma_respect,
        "avg_wick_ratio": round(avg_wick_ratio, 3),
        "return_std":     round(return_std, 5),
        "daily_bars":     len(daily_bars),
    }

    logger.info(
        "classifier.result",
        extra={
            "ticker":    ticker,
            "trend":     trend.value,
            "vol":       vol.value,
            "clean":     clean.value,
            "threshold": threshold,
            "adx":       round(adx_val, 1),
            "ma_slope":  round(ma_slope, 3),
            "atr_ratio": round(atr_ratio, 2),
        },
    )

    return StockContext(
        ticker             = ticker,
        trend_regime       = trend,
        volatility_regime  = vol,
        cleanliness        = clean,
        adx                = round(adx_val, 2),
        ma_slope_20        = round(ma_slope, 4),
        bb_width_trend     = bb_width_trend,
        atr_ratio          = round(atr_ratio, 3),
        adjusted_threshold = threshold,
        calculated_at      = datetime.now(timezone.utc),
        raw_metrics        = raw_metrics,
    )


# ── Classification rules ──────────────────────────────────────────────────────

def _classify_trend(adx: float, slope: float, hh_hl: bool) -> TrendRegime:
    """
    Determine trend direction from ADX strength, MA slope, and pivot structure.

    TRENDING_UP   : ADX > 25 AND slope > +0.3% AND higher-highs-higher-lows
    TRENDING_DOWN : ADX > 25 AND slope < -0.3% AND NOT higher-highs
    RANGING       : everything else
    """
    if adx > _ADX_TRENDING and slope > _MA_SLOPE_TRENDING and hh_hl:
        return TrendRegime.TRENDING_UP
    if adx > _ADX_TRENDING and slope < -_MA_SLOPE_TRENDING and not hh_hl:
        return TrendRegime.TRENDING_DOWN
    return TrendRegime.RANGING


def _classify_volatility(atr_ratio: float, bb_trend: str) -> VolatilityRegime:
    """
    Classify volatility regime from ATR ratio and BB width direction.

    EXPANDING   : ATR ratio > 1.3 AND BB expanding
    CONTRACTING : ATR ratio < 0.8 AND BB contracting
    NORMAL      : everything else
    """
    if atr_ratio > _ATR_EXPAND and bb_trend == "EXPANDING":
        return VolatilityRegime.EXPANDING
    if atr_ratio < _ATR_CONTRACT and bb_trend == "CONTRACTING":
        return VolatilityRegime.CONTRACTING
    return VolatilityRegime.NORMAL


def _classify_cleanliness(
    ma_respect: int,
    avg_wick_ratio: float,
    return_std: float,
) -> Cleanliness:
    """
    Determine how "clean" the price action is.

    CLEAN             : respect ≥ 14, wick ≤ 0.45, std ≤ 0.025
    CHOPPY            : respect < 10 OR wick > 0.60 OR std > 0.04
    INSUFFICIENT_DATA : borderline — treat conservatively
    """
    if (
        ma_respect >= _MA_RESPECT_CLEAN
        and avg_wick_ratio < _WICK_RATIO_CLEAN
        and return_std < _RETURN_STD_CLEAN
    ):
        return Cleanliness.CLEAN

    if (
        ma_respect < _MA_RESPECT_CHOPPY
        or avg_wick_ratio > _WICK_RATIO_CHOPPY
        or return_std > _RETURN_STD_CHOPPY
    ):
        return Cleanliness.CHOPPY

    return Cleanliness.INSUFFICIENT_DATA


def _adjusted_threshold(trend: TrendRegime, clean: Cleanliness) -> int:
    """
    Map (trend, cleanliness) → minimum technical score (7, 8, or 9).

    Priority (highest first):
      CHOPPY → 9  (most strict; choppy stocks need very strong setups)
      RANGING → 8
      TRENDING + CLEAN → 7  (best conditions; allow lower threshold)
      default → 8
    """
    if clean == Cleanliness.CHOPPY:
        return 9
    if trend == TrendRegime.RANGING:
        return 8
    if trend in (TrendRegime.TRENDING_UP, TrendRegime.TRENDING_DOWN) and clean == Cleanliness.CLEAN:
        return 7
    return 8   # conservative default (INSUFFICIENT_DATA or mixed)


# ── Indicator calculations ────────────────────────────────────────────────────

def _sma(values: Sequence[float], period: int) -> float:
    """Return the most-recent simple moving average over *period* values."""
    if len(values) < period:
        return mean(values) if values else 0.0
    return mean(values[-period:])


def _rolling_sma(values: Sequence[float], period: int) -> list[float]:
    """
    Return a list of SMA values aligned with *values*.
    First *period-1* entries are padded with the first computable SMA.
    """
    result: list[float] = []
    for i in range(len(values)):
        start = max(0, i - period + 1)
        result.append(mean(values[start : i + 1]))
    return result


def _ma_slope(values: Sequence[float], period: int, lookback: int = 5) -> float:
    """
    Return the percentage slope of the *period*-day SMA over the last
    *lookback* bars: (sma_now - sma_then) / sma_then * 100.
    """
    if len(values) < period + lookback:
        return 0.0
    sma_now  = mean(values[-period:])
    sma_then = mean(values[-(period + lookback) : -lookback])
    if sma_then == 0:
        return 0.0
    return (sma_now - sma_then) / sma_then * 100.0


def _true_range_series(
    highs:  Sequence[float],
    lows:   Sequence[float],
    closes: Sequence[float],
) -> list[float]:
    """
    Compute the True Range for each bar (starting at index 1).
    TR = max(H-L, |H-prev_C|, |L-prev_C|)
    """
    tr: list[float] = []
    for i in range(1, len(closes)):
        hl  = highs[i]  - lows[i]
        hpc = abs(highs[i]  - closes[i - 1])
        lpc = abs(lows[i]   - closes[i - 1])
        tr.append(max(hl, hpc, lpc))
    return tr


def _wilder_smooth(values: list[float], period: int) -> list[float]:
    """
    Wilder's smoothed moving average (used for ATR, ADX):
      smooth[0] = mean(values[:period])
      smooth[i] = smooth[i-1] - smooth[i-1]/period + values[i]
    Returns a series aligned with values[period-1:].
    """
    if len(values) < period:
        return []
    first = sum(values[:period]) / period
    result = [first]
    for v in values[period:]:
        result.append(result[-1] - result[-1] / period + v)
    return result


def _atr_smooth(
    highs:  Sequence[float],
    lows:   Sequence[float],
    closes: Sequence[float],
    period: int,
) -> float:
    """Return the most-recent Wilder-smoothed ATR(*period*)."""
    tr = _true_range_series(highs, lows, closes)
    smoothed = _wilder_smooth(tr, period)
    return smoothed[-1] if smoothed else 0.0


def _calc_adx(
    highs:  Sequence[float],
    lows:   Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> float:
    """
    Calculate the Average Directional Index (Wilder method).

    Returns the most-recent ADX value, or 0.0 if insufficient data.
    Requires at least 2*period+1 bars for a stable reading.
    """
    n = len(closes)
    if n < 2 * period + 1:
        return 0.0

    plus_dm:  list[float] = []
    minus_dm: list[float] = []
    for i in range(1, n):
        up   = highs[i]  - highs[i - 1]
        down = lows[i - 1] - lows[i]
        pdm  = up   if (up > down and up > 0)   else 0.0
        mdm  = down if (down > up and down > 0) else 0.0
        plus_dm.append(pdm)
        minus_dm.append(mdm)

    tr = _true_range_series(highs, lows, closes)

    s_tr   = _wilder_smooth(tr,       period)
    s_pdm  = _wilder_smooth(plus_dm,  period)
    s_mdm  = _wilder_smooth(minus_dm, period)

    dx_series: list[float] = []
    for s, p, m in zip(s_tr, s_pdm, s_mdm):
        if s == 0:
            dx_series.append(0.0)
            continue
        plus_di  = 100 * p / s
        minus_di = 100 * m / s
        denom    = plus_di + minus_di
        dx_series.append(100 * abs(plus_di - minus_di) / denom if denom else 0.0)

    adx_series = _wilder_smooth(dx_series, period)
    return adx_series[-1] if adx_series else 0.0


def _higher_highs_higher_lows(
    highs: Sequence[float],
    lows:  Sequence[float],
    window: int = 20,
) -> bool:
    """
    Detect an uptrend structure (HH + HL) using 3-bar pivot points.

    A pivot high occurs at bar[i] when bar[i].high > bar[i-1].high
    AND bar[i].high > bar[i+1].high.
    Returns True when the last two pivot highs are ascending AND
    the last two pivot lows are ascending.
    """
    h = list(highs[-window:])
    l = list(lows[-window:])
    n = len(h)

    pivot_highs: list[float] = []
    pivot_lows:  list[float] = []

    for i in range(1, n - 1):
        if h[i] > h[i - 1] and h[i] > h[i + 1]:
            pivot_highs.append(h[i])
        if l[i] < l[i - 1] and l[i] < l[i + 1]:
            pivot_lows.append(l[i])

    hh = len(pivot_highs) >= 2 and pivot_highs[-1] > pivot_highs[-2]
    hl = len(pivot_lows)  >= 2 and pivot_lows[-1]  > pivot_lows[-2]
    return hh and hl


def _bb_width_series(
    closes: Sequence[float],
    period: int     = 20,
    num_std: float  = 2.0,
    length: int     = 22,
) -> list[float]:
    """
    Compute the last *length* Bollinger Band width values.
    BB width = (upper - lower) / middle.
    """
    widths: list[float] = []
    n = len(closes)
    for i in range(max(0, n - length), n):
        start = max(0, i - period + 1)
        window = closes[start : i + 1]
        if len(window) < period:
            widths.append(0.0)
            continue
        m   = mean(window)
        std = _pstdev(window)
        widths.append((2 * num_std * std) / m if m else 0.0)
    return widths


def _pstdev(values: Sequence[float]) -> float:
    """Population standard deviation (uses N, not N-1)."""
    if len(values) < 2:
        return 0.0
    m   = mean(values)
    var = sum((v - m) ** 2 for v in values) / len(values)
    return math.sqrt(var)


def _bb_width_direction(widths: list[float]) -> str:
    """
    Classify whether BB width is expanding, contracting, or stable.

    Compare current width to 10-bar average to determine direction.
    """
    if len(widths) < 5:
        return "STABLE"
    current = widths[-1]
    baseline = mean(widths[:-3]) if len(widths) > 3 else mean(widths)
    if baseline == 0:
        return "STABLE"
    change_pct = (current - baseline) / baseline * 100
    if change_pct > 5:
        return "EXPANDING"
    if change_pct < -5:
        return "CONTRACTING"
    return "STABLE"


def _ma_respect_count(
    closes: Sequence[float],
    ma20:   Sequence[float],
    trend:  TrendRegime,
) -> int:
    """
    Count how many of the last 20 closes respected the 20-day MA.

    In an uptrend: close above MA = respect.
    In a downtrend: close below MA = respect.
    In RANGING: we measure absolute deviation ≤ 1% of MA.
    """
    count = 0
    for c, m in zip(closes, ma20):
        if m == 0:
            continue
        if trend == TrendRegime.TRENDING_UP:
            if c >= m:
                count += 1
        elif trend == TrendRegime.TRENDING_DOWN:
            if c <= m:
                count += 1
        else:
            # RANGING: count close if within 1% of MA (touching / hugging band)
            if abs(c - m) / m <= 0.01:
                count += 1
    return count


def _avg_wick_ratio(
    opens_:  Sequence[float],
    closes:  Sequence[float],
    highs:   Sequence[float],
    lows:    Sequence[float],
) -> float:
    """
    Average daily wick ratio over the given bars.

    wick_ratio = wick_size / total_range
    wick_size  = (high - low) - abs(close - open)
    total_range = high - low

    A low wick ratio (≈0) indicates clean directional candles.
    A high wick ratio (≈1) indicates indecision / choppy wicks.
    """
    ratios: list[float] = []
    for o, c, h, l in zip(opens_, closes, highs, lows):
        total = h - l
        if total == 0:
            continue
        body  = abs(c - o)
        wicks = total - body
        ratios.append(wicks / total)
    return mean(ratios) if ratios else 0.5


def _return_std(closes: Sequence[float]) -> float:
    """
    Standard deviation of daily percentage returns.

    return_i = (close_i - close_{i-1}) / close_{i-1}
    """
    if len(closes) < 2:
        return 0.0
    returns = [
        (closes[i] - closes[i - 1]) / closes[i - 1]
        for i in range(1, len(closes))
        if closes[i - 1] > 0
    ]
    try:
        return stdev(returns) if len(returns) >= 2 else 0.0
    except Exception:
        return 0.0
