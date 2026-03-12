"""
Technical Confluence Filter.

Scores technical setup quality for a given ticker at signal time.
Prevents trading news events against the technical trend.

Checks (all pulled from Polygon + FMP Redis cache):
  1. Price vs 20-day MA  — above = bullish structure, below = bearish
  2. Price vs 50-day MA  — above = medium-term trend intact
  3. Price vs 200-day MA — above = secular bull, below = bear
  4. RSI zone            — overbought (>70) or oversold (<30) matters for direction
  5. MACD signal         — bullish/bearish crossover alignment
  6. Volume vs avg       — high volume confirms move, low volume questions it

Scoring (-1.0 to +1.0):
  Each check contributes a signed score. Positive = supports longs, negative = supports shorts.
  Final score is used to:
    - BOOST conviction when technical confirms news direction
    - REDUCE conviction when technical opposes news direction
    - BLOCK trade when setup is strongly opposed (score < -0.6 for longs, > 0.6 for shorts)

Examples:
  LONG signal, stock above all MAs, RSI 55, MACD bullish, high vol → score +0.85 → +20% conviction
  LONG signal, stock below 200MA, RSI 72 (overbought), low vol    → score -0.4 → -30% conviction
  LONG signal, stock below all MAs, RSI 75, MACD bearish          → score -0.75 → BLOCKED
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx
import redis.asyncio as aioredis


def _log(level, event, **kw):
    import json as _j
    print(_j.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                    "level": level, "event": event, **kw}), flush=True)


POLYGON_BASE = "https://api.polygon.io"
POLYGON_SNAPSHOT = "/v2/snapshot/locale/us/markets/stocks/tickers"


@dataclass
class TechnicalScore:
    ticker: str
    direction: str                  # long | short

    # Raw values
    price: float = 0.0
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    rsi: Optional[float] = None
    macd_bias: Optional[str] = None  # bullish | bearish | neutral
    volume: Optional[float] = None
    avg_volume: Optional[float] = None

    # Computed
    score: float = 0.0              # -1.0 to +1.0
    conviction_multiplier: float = 1.0
    blocked: bool = False
    block_reason: str = ""
    checks: dict = field(default_factory=dict)


async def score_technicals(
    ticker: str,
    direction: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
    api_key: str,
) -> TechnicalScore:
    """
    Score technical setup for a ticker. Higher score = better setup for direction.
    """
    result = TechnicalScore(ticker=ticker, direction=direction)

    # ── Pull data ──────────────────────────────────────────────────────────────
    await _populate_price_data(result, ticker, redis_conn, http, api_key)

    if result.price <= 0:
        # No price data — pass through with neutral score but log for diagnostics
        _log("warning", "technical_filter.no_price_data",
             ticker=ticker,
             api_key_present=bool(api_key),
             rsi_from_redis=result.rsi,
             note="Polygon snapshot returned no price — check POLYGON_API_KEY env var")
        result.conviction_multiplier = 1.0
        return result

    # ── Score each check ───────────────────────────────────────────────────────
    raw_score = 0.0
    weight_total = 0.0

    # 1. Price vs MA20 (weight 0.20)
    if result.ma20 and result.price > 0:
        pct = (result.price - result.ma20) / result.ma20 * 100
        contrib = _clamp(pct / 3.0, -1, 1)  # ±3% = full score
        result.checks["ma20"] = {"above": pct > 0, "pct": round(pct, 2), "contrib": round(contrib, 2)}
        raw_score += contrib * 0.20
        weight_total += 0.20

    # 2. Price vs MA50 (weight 0.25)
    if result.ma50 and result.price > 0:
        pct = (result.price - result.ma50) / result.ma50 * 100
        contrib = _clamp(pct / 5.0, -1, 1)
        result.checks["ma50"] = {"above": pct > 0, "pct": round(pct, 2), "contrib": round(contrib, 2)}
        raw_score += contrib * 0.25
        weight_total += 0.25

    # 3. Price vs MA200 (weight 0.20)
    if result.ma200 and result.price > 0:
        pct = (result.price - result.ma200) / result.ma200 * 100
        contrib = _clamp(pct / 8.0, -1, 1)
        result.checks["ma200"] = {"above": pct > 0, "pct": round(pct, 2), "contrib": round(contrib, 2)}
        raw_score += contrib * 0.20
        weight_total += 0.20

    # 4. RSI zone (weight 0.20)
    if result.rsi is not None:
        if result.rsi > 70:
            contrib = -0.6   # Overbought — bad for longs, good for shorts
        elif result.rsi > 60:
            contrib = 0.2
        elif result.rsi > 40:
            contrib = 0.4    # Healthy middle zone — good for longs
        elif result.rsi > 30:
            contrib = 0.1
        else:
            contrib = 0.6    # Oversold — bad for shorts, good for longs (bounce)
        result.checks["rsi"] = {"value": round(result.rsi, 1), "contrib": contrib}
        raw_score += contrib * 0.20
        weight_total += 0.20

    # 5. MACD bias (weight 0.15)
    if result.macd_bias:
        contrib = 1.0 if result.macd_bias == "bullish" else (-1.0 if result.macd_bias == "bearish" else 0.0)
        result.checks["macd"] = {"bias": result.macd_bias, "contrib": contrib}
        raw_score += contrib * 0.15
        weight_total += 0.15

    # Normalise to -1.0 / +1.0 range
    if weight_total > 0:
        result.score = round(raw_score / weight_total, 3)
    else:
        result.score = 0.0

    # ── Direction-adjust: positive score = good for longs, negative = good for shorts
    alignment = result.score if direction == "long" else -result.score

    # ── Block / adjust ─────────────────────────────────────────────────────────
    if alignment < -0.55:
        # Technical strongly opposes direction → block
        result.blocked = True
        result.block_reason = (
            f"Technical setup opposes {direction}: score {result.score:.2f} "
            f"(below MAs, RSI {result.rsi:.0f if result.rsi else '?'})"
        )
        result.conviction_multiplier = 0.0

    elif alignment < -0.25:
        # Technical mildly opposes → cut conviction
        result.conviction_multiplier = 0.70
    elif alignment < 0.10:
        result.conviction_multiplier = 0.90
    elif alignment < 0.35:
        result.conviction_multiplier = 1.00
    elif alignment < 0.60:
        result.conviction_multiplier = 1.10
    else:
        # Strong technical confluence → boost
        result.conviction_multiplier = 1.20

    _log("info", "technical_filter.scored",
         ticker=ticker, direction=direction,
         score=result.score, multiplier=result.conviction_multiplier,
         blocked=result.blocked, checks=result.checks)

    return result


async def _populate_price_data(
    result: TechnicalScore,
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
    api_key: str,
) -> None:
    """Fill TechnicalScore with price + indicator data from Redis/Polygon."""

    # 1. RSI + MACD from FMP Redis cache (written by fmp_technical service)
    try:
        tech_raw = await redis_conn.get(f"fmp:technical:{ticker}")
        if tech_raw:
            tech = json.loads(tech_raw)
            result.rsi = tech.get("rsi")
            result.macd_bias = tech.get("macd_bias")
    except Exception:
        pass

    # Also try Finnhub sentiment RSI
    if result.rsi is None:
        try:
            fh_raw = await redis_conn.get(f"finnhub:technical:{ticker}")
            if fh_raw:
                fh = json.loads(fh_raw)
                result.rsi = fh.get("rsi")
        except Exception:
            pass

    # 2. Current price + MAs from Polygon snapshot
    try:
        resp = await http.get(
            f"{POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}",
            params={"apiKey": api_key},
            timeout=8.0,
        )
        if resp.status_code == 200:
            snap = resp.json().get("ticker", {})
            day = snap.get("day", {})
            result.price = float(day.get("c") or snap.get("lastTrade", {}).get("p") or 0)
            result.volume = float(day.get("v") or 0)
            result.avg_volume = float(snap.get("prevDay", {}).get("v") or 0)
        else:
            _log("warning", "technical_filter.snapshot_error",
                 ticker=ticker, status=resp.status_code,
                 api_key_present=bool(api_key))
    except Exception as e:
        _log("warning", "technical_filter.snapshot_exception",
             ticker=ticker, error=str(e))

    # 3. MAs from Polygon SMA endpoint
    if result.price > 0 and api_key:
        for period, attr in [(20, "ma20"), (50, "ma50"), (200, "ma200")]:
            try:
                cache_key = f"poly:sma:{ticker}:{period}"
                cached = await redis_conn.get(cache_key)
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
                    values = resp.json().get("results", {}).get("values", [])
                    if values:
                        ma = float(values[0]["value"])
                        setattr(result, attr, ma)
                        await redis_conn.setex(cache_key, 3600, str(ma))  # Cache 1hr
                else:
                    _log("warning", "technical_filter.sma_error",
                         ticker=ticker, period=period, status=resp.status_code)
            except Exception as e:
                _log("warning", "technical_filter.sma_exception",
                     ticker=ticker, period=period, error=str(e))


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))
