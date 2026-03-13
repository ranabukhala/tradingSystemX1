"""
Technical Confluence Filter.

Scores technical setup quality for a given ticker at signal time.
Prevents trading news events against the technical trend.

Legacy scoring (-1.0 to +1.0):
  Preserved for backward compatibility with pretrade_filter.py orchestrator.
  Written to TechnicalScore.score and DB filter_tech_score column.

New 0-10 composite scoring (7 components, min 7/10 to pass):
  1. Volume confirmation    (2pts): volume > 1.5x 20-day avg + OBV trending in direction
  2. Multi-timeframe align  (2pts): daily 50MA + 1hr 20MA both agree with direction
  3. Key level proximity    (2pts): price within 1.5% of swing high/low or round number
  4. VWAP positioning       (1pt):  price above/below VWAP (intraday sessions)
  5. ATR volatility check   (1pt):  today's move is 1.0–2.5x ATR (sweet spot)
  6. RSI divergence absence (1pt):  no opposing bearish/bullish divergence
  7. Relative strength      (1pt):  5-day return outperforms SPY in trade direction

Score stored in result.checks["_technical_score"] and
result.checks["_technical_score_breakdown"] so the pretrade_filter orchestrator
automatically includes them in the Kafka payload without code changes.

Configurable: TECHNICAL_SCORE_THRESHOLD env var (default 7).

RSI source priority:
  1. fmp:technical:{ticker}     Redis key (written by fmp_technical connector)
  2. finnhub:technical:{ticker} Redis key (written by finnhub connector)
  3. Polygon /v1/indicators/rsi/{ticker} — primary live source; cached 1hr
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import redis.asyncio as aioredis


def _log(level: str, event: str, **kw) -> None:
    import json as _j
    print(_j.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                    "level": level, "event": event, **kw}), flush=True)


POLYGON_BASE = "https://api.polygon.io"

# Minimum 0-10 score required to pass; override via env var
SCORE_THRESHOLD = int(os.environ.get("TECHNICAL_SCORE_THRESHOLD", "7"))


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class TechnicalScore:
    ticker: str
    direction: str                  # long | short

    # Raw values — basic
    price: float = 0.0
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    rsi: Optional[float] = None
    macd_bias: Optional[str] = None  # bullish | bearish | neutral
    volume: Optional[float] = None
    avg_volume: Optional[float] = None

    # Extended data for 0-10 composite scoring
    vwap: Optional[float] = None                       # Intraday VWAP from snapshot
    sma20_1hr: Optional[float] = None                  # 1-hour 20-period SMA
    daily_bars: list = field(default_factory=list)     # Last 20 daily OHLCV bars
    rsi_history: list = field(default_factory=list)    # Last 3 RSI values (oldest-first)
    spy_5d_return: Optional[float] = None              # SPY 5-day return %
    stock_5d_return: Optional[float] = None            # Stock 5-day return %

    # Computed — legacy fields (pretrade_filter.py reads these, do not rename)
    score: float = 0.0              # -1.0 to +1.0 legacy score
    conviction_multiplier: float = 1.0
    blocked: bool = False
    block_reason: str = ""
    checks: dict = field(default_factory=dict)

    # New 0-10 composite score (surfaced through checks dict for Kafka payload)
    technical_score: int = 0
    technical_score_breakdown: dict = field(default_factory=dict)


# ── Main entry point ───────────────────────────────────────────────────────────

async def score_technicals(
    ticker: str,
    direction: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
    api_key: str,
    session_context: str = "intraday",
) -> TechnicalScore:
    """
    Score technical setup for a ticker against a proposed trade direction.

    Returns TechnicalScore with:
      - Legacy .score (-1.0/+1.0) for orchestrator backward compatibility
      - New .technical_score (0-10) injected into .checks for Kafka payload
      - .blocked / .conviction_multiplier derived from composite score
    """
    result = TechnicalScore(ticker=ticker, direction=direction)

    # ── Pull all data ──────────────────────────────────────────────────────────
    await _populate_price_data(result, ticker, redis_conn, http, api_key)
    await _populate_extended_data(result, ticker, redis_conn, http, api_key)

    # Nothing at all — pass through neutral without blocking
    if result.price <= 0 and result.rsi is None:
        _log("warning", "technical_filter.no_data",
             ticker=ticker,
             api_key_present=bool(api_key),
             note="No price and no RSI — passing neutral")
        result.conviction_multiplier = 1.0
        return result

    # Price missing but RSI exists — log and continue
    if result.price <= 0:
        _log("warning", "technical_filter.no_price_data",
             ticker=ticker, rsi=result.rsi,
             note="No price (pre-market?) — RSI-only scoring")

    # ── Legacy -1.0/+1.0 score (backward compat) ──────────────────────────────
    _compute_legacy_score(result)

    # ── New 0-10 composite score ───────────────────────────────────────────────
    tech_score, breakdown = calculate_technical_score(result, direction, session_context)
    result.technical_score = tech_score
    result.technical_score_breakdown = breakdown

    # Inject into checks → orchestrator passes these into Kafka record naturally
    result.checks["_technical_score"] = tech_score
    result.checks["_technical_score_breakdown"] = breakdown

    # ── Blocking + conviction multiplier ──────────────────────────────────────
    alignment = result.score if direction == "long" else -result.score

    if alignment < -0.55:
        result.blocked = True
        result.block_reason = (
            f"Technical setup strongly opposes {direction}: "
            f"legacy score {result.score:.2f} "
            f"(RSI {round(result.rsi, 0) if result.rsi else '?'})"
        )
        result.conviction_multiplier = 0.0

    elif tech_score < SCORE_THRESHOLD:
        result.blocked = True
        result.block_reason = (
            f"Technical confluence too low: {tech_score}/{SCORE_THRESHOLD} "
            f"(need {SCORE_THRESHOLD}/10 — "
            f"vol={breakdown.get('volume', {}).get('pts', '?')}/2 "
            f"mtf={breakdown.get('multi_timeframe', {}).get('pts', '?')}/2 "
            f"lvl={breakdown.get('key_levels', {}).get('pts', '?')}/2)"
        )
        result.conviction_multiplier = 0.0

    else:
        result.conviction_multiplier = _score_to_multiplier(tech_score)

    _log("info", "technical_filter.scored",
         ticker=ticker,
         direction=direction,
         legacy_score=result.score,
         technical_score=tech_score,
         threshold=SCORE_THRESHOLD,
         multiplier=result.conviction_multiplier,
         blocked=result.blocked,
         breakdown=breakdown)

    return result


# ── Legacy scoring (preserved for orchestrator) ────────────────────────────────

def _compute_legacy_score(result: TechnicalScore) -> None:
    """Compute legacy -1.0/+1.0 score from MA/RSI/MACD.  Fills result.score and result.checks."""
    raw_score = 0.0
    weight_total = 0.0

    # 1. Price vs MA20 (weight 0.20)
    if result.ma20 and result.price > 0:
        pct = (result.price - result.ma20) / result.ma20 * 100
        contrib = _clamp(pct / 3.0, -1, 1)
        result.checks["ma20"] = {"above": pct > 0, "pct": round(pct, 2), "contrib": round(contrib, 2)}
        raw_score   += contrib * 0.20
        weight_total += 0.20

    # 2. Price vs MA50 (weight 0.25)
    if result.ma50 and result.price > 0:
        pct = (result.price - result.ma50) / result.ma50 * 100
        contrib = _clamp(pct / 5.0, -1, 1)
        result.checks["ma50"] = {"above": pct > 0, "pct": round(pct, 2), "contrib": round(contrib, 2)}
        raw_score   += contrib * 0.25
        weight_total += 0.25

    # 3. Price vs MA200 (weight 0.20)
    if result.ma200 and result.price > 0:
        pct = (result.price - result.ma200) / result.ma200 * 100
        contrib = _clamp(pct / 8.0, -1, 1)
        result.checks["ma200"] = {"above": pct > 0, "pct": round(pct, 2), "contrib": round(contrib, 2)}
        raw_score   += contrib * 0.20
        weight_total += 0.20

    # 4. RSI zone (weight 0.20)
    if result.rsi is not None:
        rsi = result.rsi
        if rsi > 70:
            contrib = -0.6   # Overbought — headwind for longs
        elif rsi > 60:
            contrib = 0.2
        elif rsi > 40:
            contrib = 0.4    # Healthy mid-range — good for longs
        elif rsi > 30:
            contrib = 0.1
        else:
            contrib = 0.6    # Oversold — bounce candidate for longs
        result.checks["rsi"] = {"value": round(rsi, 1), "contrib": contrib}
        raw_score   += contrib * 0.20
        weight_total += 0.20

    # 5. MACD bias (weight 0.15)
    if result.macd_bias:
        contrib = (1.0  if result.macd_bias == "bullish" else
                   -1.0 if result.macd_bias == "bearish" else 0.0)
        result.checks["macd"] = {"bias": result.macd_bias, "contrib": contrib}
        raw_score   += contrib * 0.15
        weight_total += 0.15

    result.score = round(raw_score / weight_total, 3) if weight_total > 0 else 0.0


# ── 0-10 composite scoring ─────────────────────────────────────────────────────

def calculate_technical_score(
    result: TechnicalScore,
    direction: str,
    session_context: str = "intraday",
) -> tuple[int, dict]:
    """
    Compute 0-10 composite score across 7 technical components.
    Returns (score: int, breakdown: dict).
    """
    breakdown: dict = {}
    total = 0
    is_long = direction.lower() == "long"

    # ── 1. Volume Confirmation (2pts) ──────────────────────────────────────────
    # 2pts: volume > 1.5x avg AND OBV trending in direction
    # 1pt:  only one condition met
    # 0pts: both fail
    vol_pts = 0
    vol_detail: dict = {}

    if result.volume and result.avg_volume and result.avg_volume > 0:
        vol_ratio = result.volume / result.avg_volume
        vol_detail["vol_ratio"] = round(vol_ratio, 2)
        vol_detail["vol_above_1_5x"] = vol_ratio >= 1.5
        if vol_ratio >= 1.5:
            vol_pts += 1

    obv_ok = _calc_obv_trend(result.daily_bars, is_long)
    vol_detail["obv_trending_with_direction"] = obv_ok
    if obv_ok:
        vol_pts += 1

    breakdown["volume"] = {"pts": vol_pts, "max": 2, **vol_detail}
    total += vol_pts

    # ── 2. Multi-Timeframe Alignment (2pts) ───────────────────────────────────
    # 2pts: daily 50MA AND 1hr 20MA both side with direction
    # 1pt:  only one agrees
    # 0pts: neither agrees
    mtf_pts = 0
    mtf_detail: dict = {}

    if result.ma50 and result.price > 0:
        d50_ok = (result.price > result.ma50) if is_long else (result.price < result.ma50)
        mtf_detail["daily_50ma_aligned"] = d50_ok
        mtf_detail["price"] = round(result.price, 2)
        mtf_detail["ma50"] = round(result.ma50, 2)
        if d50_ok:
            mtf_pts += 1

    if result.sma20_1hr and result.price > 0:
        h20_ok = (result.price > result.sma20_1hr) if is_long else (result.price < result.sma20_1hr)
        mtf_detail["hourly_20ma_aligned"] = h20_ok
        mtf_detail["sma20_1hr"] = round(result.sma20_1hr, 2)
        if h20_ok:
            mtf_pts += 1

    breakdown["multi_timeframe"] = {"pts": mtf_pts, "max": 2, **mtf_detail}
    total += mtf_pts

    # ── 3. Key Level Proximity (2pts) ─────────────────────────────────────────
    # 2pts: price within 1.5% of swing high (long) / swing low (short)
    #       OR within 1.5% of a round-number level
    # 1pt:  within 3.0% (looser proximity) or near-round without tight proximity
    # 0pts: away from all key levels
    level_pts = 0
    level_detail: dict = {}

    if result.price > 0 and result.daily_bars:
        highs = [b["h"] for b in result.daily_bars if b.get("h")]
        lows  = [b["l"] for b in result.daily_bars if b.get("l")]

        if highs and lows:
            swing_high = max(highs)
            swing_low  = min(lows)
            level_detail["swing_high"] = round(swing_high, 2)
            level_detail["swing_low"]  = round(swing_low, 2)

            if is_long:
                pct = abs(result.price - swing_high) / swing_high * 100
                level_detail["pct_from_swing_high"] = round(pct, 2)
                if pct <= 1.5:
                    level_pts = 2
                elif pct <= 3.0:
                    level_pts = 1
            else:
                pct = abs(result.price - swing_low) / swing_low * 100
                level_detail["pct_from_swing_low"] = round(pct, 2)
                if pct <= 1.5:
                    level_pts = 2
                elif pct <= 3.0:
                    level_pts = 1

        near_round = _near_round_number(result.price)
        level_detail["near_round_number"] = near_round
        if near_round and level_pts < 2:
            level_pts = min(level_pts + 1, 2)

    breakdown["key_levels"] = {"pts": level_pts, "max": 2, **level_detail}
    total += level_pts

    # ── 4. VWAP Positioning (1pt) ─────────────────────────────────────────────
    # 1pt:  price above VWAP for longs / below VWAP for shorts
    # 0pts: wrong side (skipped for pre-market/overnight sessions)
    vwap_pts = 0
    vwap_detail: dict = {}

    if session_context in ("premarket", "overnight"):
        # VWAP not meaningful outside regular hours — auto-pass
        vwap_pts = 1
        vwap_detail["note"] = f"session_{session_context}_vwap_skipped"
    elif result.vwap and result.vwap > 0 and result.price > 0:
        vwap_ok = (result.price >= result.vwap) if is_long else (result.price <= result.vwap)
        vwap_detail["vwap"] = round(result.vwap, 2)
        vwap_detail["price_vs_vwap"] = "above" if result.price >= result.vwap else "below"
        vwap_detail["aligned"] = vwap_ok
        if vwap_ok:
            vwap_pts = 1
    else:
        # VWAP unavailable — don't penalize for missing data
        vwap_pts = 1
        vwap_detail["note"] = "vwap_unavailable_auto_pass"

    breakdown["vwap"] = {"pts": vwap_pts, "max": 1, **vwap_detail}
    total += vwap_pts

    # ── 5. ATR Volatility Check (1pt) ─────────────────────────────────────────
    # 1pt: today's open-to-close move is between 1.0x and 2.5x ATR
    #      (too small = news not driving; too large = move may be exhausted)
    # 0pts: outside that range
    atr_pts = 0
    atr_detail: dict = {}

    if result.price > 0 and len(result.daily_bars) >= 5:
        atr = _calc_atr(result.daily_bars, period=14)
        if atr and atr > 0:
            today_bar = result.daily_bars[-1] if result.daily_bars else None
            if today_bar and today_bar.get("o") and today_bar.get("c"):
                today_move = abs(today_bar["c"] - today_bar["o"])
                atr_detail["atr"] = round(atr, 4)
                atr_detail["today_move"] = round(today_move, 4)
                if today_move > 0:
                    ratio = today_move / atr
                    atr_detail["move_atr_ratio"] = round(ratio, 2)
                    atr_detail["in_sweet_spot"] = 1.0 <= ratio <= 2.5
                    if atr_detail["in_sweet_spot"]:
                        atr_pts = 1
                else:
                    atr_pts = 1
                    atr_detail["note"] = "zero_move_auto_pass"
            else:
                atr_pts = 1
                atr_detail["note"] = "no_today_bar_auto_pass"
        else:
            atr_pts = 1
            atr_detail["note"] = "atr_unavailable_auto_pass"
    else:
        atr_pts = 1
        atr_detail["note"] = "insufficient_bars_auto_pass"

    breakdown["atr"] = {"pts": atr_pts, "max": 1, **atr_detail}
    total += atr_pts

    # ── 6. RSI Divergence Absence (1pt) ───────────────────────────────────────
    # 1pt: no opposing divergence detected (bearish for longs / bullish for shorts)
    # 0pts: opposing divergence found
    rsi_div_pts = 0
    rsi_div_detail: dict = {}

    if result.rsi is not None:
        rsi_div_detail["current_rsi"] = round(result.rsi, 1)
        divergence = _check_rsi_divergence(result.daily_bars, result.rsi_history, is_long)
        rsi_div_detail["opposing_divergence"] = divergence
        if not divergence:
            rsi_div_pts = 1
    else:
        rsi_div_pts = 1
        rsi_div_detail["note"] = "rsi_unavailable_auto_pass"

    breakdown["rsi_divergence"] = {"pts": rsi_div_pts, "max": 1, **rsi_div_detail}
    total += rsi_div_pts

    # ── 7. Relative Strength vs SPY (1pt) ─────────────────────────────────────
    # 1pt: stock's 5-day return outperforms SPY in the direction of the trade
    # 0pts: underperforms (or data unavailable → auto-pass)
    rs_pts = 0
    rs_detail: dict = {}

    if result.stock_5d_return is not None and result.spy_5d_return is not None:
        rs_detail["stock_5d_pct"] = round(result.stock_5d_return, 3)
        rs_detail["spy_5d_pct"]   = round(result.spy_5d_return, 3)
        outperforms = (
            result.stock_5d_return > result.spy_5d_return if is_long
            else result.stock_5d_return < result.spy_5d_return
        )
        rs_detail["outperforms_spy"] = outperforms
        if outperforms:
            rs_pts = 1
    else:
        rs_pts = 1
        rs_detail["note"] = "rs_data_unavailable_auto_pass"

    breakdown["relative_strength"] = {"pts": rs_pts, "max": 1, **rs_detail}
    total += rs_pts

    # ── Summary ────────────────────────────────────────────────────────────────
    breakdown["total"]     = total
    breakdown["threshold"] = SCORE_THRESHOLD
    breakdown["passed"]    = total >= SCORE_THRESHOLD

    return total, breakdown


# ── Score → multiplier mapping ─────────────────────────────────────────────────

def _score_to_multiplier(score: int) -> float:
    """Map 0-10 technical score to conviction multiplier."""
    if score >= 10:
        return 1.20
    elif score >= 9:
        return 1.10
    elif score >= 8:
        return 1.00
    elif score >= 7:
        return 0.90
    else:
        # Blocked before reaching here; defensive fallback
        return 0.70


# ── Scoring helper functions ───────────────────────────────────────────────────

def _calc_obv_trend(bars: list, is_long: bool) -> bool:
    """
    Check if On-Balance Volume favours the trade direction over last 5 bars.
    Compares cumulative volume on up-days vs down-days.
    Returns True if OBV trend aligns with direction (or data insufficient).
    """
    if not bars or len(bars) < 3:
        return True  # Not enough data — auto-pass

    recent = bars[-5:] if len(bars) >= 5 else bars
    up_vol   = sum(b.get("v", 0) for b in recent if (b.get("c") or 0) > (b.get("o") or 0))
    down_vol = sum(b.get("v", 0) for b in recent if (b.get("c") or 0) < (b.get("o") or 0))

    if up_vol + down_vol == 0:
        return True  # No directional volume — auto-pass

    return up_vol >= down_vol if is_long else down_vol >= up_vol


def _calc_atr(bars: list, period: int = 14) -> Optional[float]:
    """
    Average True Range over last `period` bars.
    TR = max(high-low, |high-prev_close|, |low-prev_close|)
    """
    if not bars or len(bars) < 2:
        return None

    use = bars[-(period + 1):]
    trs = []
    for i in range(1, len(use)):
        h  = use[i].get("h") or 0
        l  = use[i].get("l") or 0
        pc = use[i - 1].get("c") or 0
        if h and l and pc:
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    return sum(trs) / len(trs) if trs else None


def _near_round_number(price: float) -> bool:
    """
    True if price is within 1.5% of a psychologically significant round number.
    Steps: $1 below $20, $5 for $20-100, $10 for $100-500, $50 above $500.
    """
    if price <= 0:
        return False
    if price < 20:
        step = 1.0
    elif price < 100:
        step = 5.0
    elif price < 500:
        step = 10.0
    else:
        step = 50.0

    nearest = round(price / step) * step
    return abs(price - nearest) / price <= 0.015


def _check_rsi_divergence(bars: list, rsi_history: list, is_long: bool) -> bool:
    """
    Detect opposing RSI divergence (returns True = divergence found = bad).

    Bearish divergence (bad for longs):  price higher high + RSI lower high
    Bullish divergence (bad for shorts): price lower low  + RSI higher low

    Requires at least 3 price bars and 2 RSI values (oldest-first).
    """
    if not bars or len(bars) < 3 or not rsi_history or len(rsi_history) < 2:
        return False  # Insufficient data — assume clean

    recent_bars = bars[-5:] if len(bars) >= 5 else bars
    rsi_vals    = rsi_history[-3:] if len(rsi_history) >= 3 else rsi_history

    if is_long:
        price_new_high = (recent_bars[-1].get("h") or 0) > (recent_bars[0].get("h") or 0)
        rsi_declining  = rsi_vals[-1] < rsi_vals[0]
        return price_new_high and rsi_declining
    else:
        price_new_low  = (recent_bars[-1].get("l") or 0) < (recent_bars[0].get("l") or 0)
        rsi_rising     = rsi_vals[-1] > rsi_vals[0]
        return price_new_low and rsi_rising


# ── Data population ────────────────────────────────────────────────────────────

async def _populate_price_data(
    result: TechnicalScore,
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
    api_key: str,
) -> None:
    """Fill basic price + indicator fields from Redis caches and Polygon."""

    # ── RSI + MACD from FMP Redis cache ───────────────────────────────────────
    try:
        raw = await redis_conn.get(f"fmp:technical:{ticker}")
        if raw:
            tech = json.loads(raw)
            result.rsi       = tech.get("rsi")
            result.macd_bias = tech.get("macd_bias")
    except Exception as e:
        _log("warning", "technical_filter.fmp_cache_error", ticker=ticker, error=str(e))

    # ── Finnhub RSI fallback ──────────────────────────────────────────────────
    if result.rsi is None:
        try:
            raw = await redis_conn.get(f"finnhub:technical:{ticker}")
            if raw:
                fh = json.loads(raw)
                result.rsi = fh.get("rsi")
        except Exception as e:
            _log("warning", "technical_filter.finnhub_cache_error", ticker=ticker, error=str(e))

    # ── Polygon RSI — primary live source ─────────────────────────────────────
    if result.rsi is None and api_key:
        try:
            ck = f"poly:rsi:{ticker}"
            cached = await redis_conn.get(ck)
            if cached:
                result.rsi = float(cached)
            else:
                resp = await http.get(
                    f"{POLYGON_BASE}/v1/indicators/rsi/{ticker}",
                    params={"timespan": "day", "window": 14,
                            "series_type": "close", "limit": 1,
                            "apiKey": api_key},
                    timeout=8.0,
                )
                if resp.status_code == 200:
                    vals = resp.json().get("results", {}).get("values", [])
                    if vals:
                        result.rsi = round(float(vals[0]["value"]), 2)
                        await redis_conn.setex(ck, 3600, str(result.rsi))
                        _log("debug", "technical_filter.rsi_fetched",
                             ticker=ticker, rsi=result.rsi, source="polygon")
                    else:
                        _log("warning", "technical_filter.rsi_empty", ticker=ticker)
                else:
                    _log("warning", "technical_filter.rsi_error",
                         ticker=ticker, status=resp.status_code)
        except Exception as e:
            _log("warning", "technical_filter.rsi_exception", ticker=ticker, error=str(e))

    # ── Polygon snapshot: price, volume, VWAP ─────────────────────────────────
    try:
        resp = await http.get(
            f"{POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}",
            params={"apiKey": api_key},
            timeout=8.0,
        )
        if resp.status_code == 200:
            snap     = resp.json().get("ticker", {})
            day      = snap.get("day", {})
            prev_day = snap.get("prevDay", {})
            last     = snap.get("lastTrade", {})
            result.price = float(
                day.get("c") or last.get("p") or prev_day.get("c") or 0
            )
            result.volume     = float(day.get("v") or 0)
            result.avg_volume = float(prev_day.get("v") or 0)
            vwap_raw = day.get("vw")
            result.vwap = float(vwap_raw) if vwap_raw else None
        else:
            _log("warning", "technical_filter.snapshot_error",
                 ticker=ticker, status=resp.status_code)
    except Exception as e:
        _log("warning", "technical_filter.snapshot_exception", ticker=ticker, error=str(e))

    # ── Polygon SMA: 20 / 50 / 200 ────────────────────────────────────────────
    if result.price > 0 and api_key:
        for period, attr in [(20, "ma20"), (50, "ma50"), (200, "ma200")]:
            try:
                ck = f"poly:sma:{ticker}:{period}"
                cached = await redis_conn.get(ck)
                if cached:
                    setattr(result, attr, float(cached))
                    continue
                resp = await http.get(
                    f"{POLYGON_BASE}/v1/indicators/sma/{ticker}",
                    params={"timespan": "day", "window": period,
                            "series_type": "close", "limit": 1,
                            "apiKey": api_key},
                    timeout=8.0,
                )
                if resp.status_code == 200:
                    vals = resp.json().get("results", {}).get("values", [])
                    if vals:
                        ma = float(vals[0]["value"])
                        setattr(result, attr, ma)
                        await redis_conn.setex(ck, 3600, str(ma))
                    else:
                        _log("warning", "technical_filter.sma_empty",
                             ticker=ticker, period=period)
                else:
                    _log("warning", "technical_filter.sma_error",
                         ticker=ticker, period=period, status=resp.status_code)
            except Exception as e:
                _log("warning", "technical_filter.sma_exception",
                     ticker=ticker, period=period, error=str(e))


async def _populate_extended_data(
    result: TechnicalScore,
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
    api_key: str,
) -> None:
    """Fetch extended data for 0-10 scoring: daily bars, 1hr SMA20, RSI history, SPY returns."""
    if not api_key:
        return

    today     = datetime.now(timezone.utc)
    from_date = (today - timedelta(days=35)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")

    # ── Daily OHLCV bars (last 20 sessions) ───────────────────────────────────
    ck_daily = f"poly:ohlcv:{ticker}:daily"
    try:
        cached = await redis_conn.get(ck_daily)
        if cached:
            result.daily_bars = json.loads(cached)
        else:
            resp = await http.get(
                f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}",
                params={"adjusted": "true", "sort": "asc", "limit": 20, "apiKey": api_key},
                timeout=8.0,
            )
            if resp.status_code == 200:
                bars = resp.json().get("results", [])
                result.daily_bars = bars
                if bars:
                    await redis_conn.setex(ck_daily, 14400, json.dumps(bars))  # 4h TTL
            else:
                _log("warning", "technical_filter.daily_bars_error",
                     ticker=ticker, status=resp.status_code)
    except Exception as e:
        _log("warning", "technical_filter.daily_bars_exception", ticker=ticker, error=str(e))

    # ── 1-hour SMA20 for multi-timeframe alignment ────────────────────────────
    ck_sma1h = f"poly:sma1h:{ticker}:20"
    try:
        cached = await redis_conn.get(ck_sma1h)
        if cached:
            result.sma20_1hr = float(cached)
        else:
            resp = await http.get(
                f"{POLYGON_BASE}/v1/indicators/sma/{ticker}",
                params={"timespan": "hour", "window": 20,
                        "series_type": "close", "limit": 1,
                        "apiKey": api_key},
                timeout=8.0,
            )
            if resp.status_code == 200:
                vals = resp.json().get("results", {}).get("values", [])
                if vals:
                    result.sma20_1hr = round(float(vals[0]["value"]), 4)
                    await redis_conn.setex(ck_sma1h, 1800, str(result.sma20_1hr))  # 30min TTL
            else:
                _log("warning", "technical_filter.sma1h_error",
                     ticker=ticker, status=resp.status_code)
    except Exception as e:
        _log("warning", "technical_filter.sma1h_exception", ticker=ticker, error=str(e))

    # ── RSI history (last 3 values, oldest-first) for divergence check ────────
    ck_rsi_hist = f"poly:rsi_hist:{ticker}"
    try:
        cached = await redis_conn.get(ck_rsi_hist)
        if cached:
            result.rsi_history = json.loads(cached)
        else:
            resp = await http.get(
                f"{POLYGON_BASE}/v1/indicators/rsi/{ticker}",
                params={"timespan": "day", "window": 14,
                        "series_type": "close", "limit": 3,
                        "apiKey": api_key},
                timeout=8.0,
            )
            if resp.status_code == 200:
                vals = resp.json().get("results", {}).get("values", [])
                if vals:
                    # Polygon returns newest-first — reverse to oldest-first
                    rsi_vals = [round(v["value"], 2) for v in reversed(vals)]
                    result.rsi_history = rsi_vals
                    await redis_conn.setex(ck_rsi_hist, 3600, json.dumps(rsi_vals))  # 1h TTL
    except Exception as e:
        _log("warning", "technical_filter.rsi_hist_exception", ticker=ticker, error=str(e))

    # ── SPY daily bars for relative-strength baseline ─────────────────────────
    ck_spy = "poly:ohlcv:SPY:daily"
    try:
        cached = await redis_conn.get(ck_spy)
        spy_bars = json.loads(cached) if cached else []
        if not spy_bars:
            resp = await http.get(
                f"{POLYGON_BASE}/v2/aggs/ticker/SPY/range/1/day/{from_date}/{to_date}",
                params={"adjusted": "true", "sort": "asc", "limit": 10, "apiKey": api_key},
                timeout=8.0,
            )
            if resp.status_code == 200:
                spy_bars = resp.json().get("results", [])
                if spy_bars:
                    await redis_conn.setex(ck_spy, 14400, json.dumps(spy_bars))  # 4h TTL
        if spy_bars and len(spy_bars) >= 5:
            result.spy_5d_return = _calc_5d_return(spy_bars)
    except Exception as e:
        _log("warning", "technical_filter.spy_bars_exception", error=str(e))

    # ── Stock 5-day return (derived from daily_bars) ──────────────────────────
    if result.daily_bars and len(result.daily_bars) >= 5:
        result.stock_5d_return = _calc_5d_return(result.daily_bars)


# ── Utility ────────────────────────────────────────────────────────────────────

def _calc_5d_return(bars: list) -> Optional[float]:
    """5-day percentage return from a list of OHLCV bars (oldest-first)."""
    if not bars or len(bars) < 5:
        return None
    c_old = bars[-5].get("c")
    c_new = bars[-1].get("c")
    if c_old and c_new and c_old > 0:
        return round((c_new - c_old) / c_old * 100, 4)
    return None


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))
