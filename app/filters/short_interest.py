"""
Short Interest & Squeeze Detector.

Short squeezes are the most explosive moves in equities.
A positive news catalyst on a heavily shorted stock can produce
30-100% moves in hours. This filter identifies squeeze setups.

Data sources:
  - Polygon — regulatory filing data (bi-monthly FINRA reports, requires higher plan)
  - Finnhub — insider transactions (SEC Form 4, real-time; supplements short data)
  Note: IEX Cloud removed — API shut down in 2024
  Note: Finviz Elite export API is screener-only, not suitable for per-ticker short lookups

Squeeze score (0.0 – 1.0):
  0.0 = No squeeze potential
  1.0 = Extreme squeeze candidate

Factors:
  - Short float > 20%      → high base score
  - Short float > 30%      → very high
  - Days to cover > 5      → harder to cover = more violent squeeze
  - Recent price increase  → shorts already under pressure
  - High borrow rate       → expensive to stay short = capitulation pressure

What we do with the score:
  - squeeze_score > 0.6 + long signal → flag as "SQUEEZE" type, +25% conviction, wider TP
  - squeeze_score > 0.8               → flag as "HIGH SQUEEZE" — special sizing rules
  - short signal + squeeze_score > 0.5 → reduce conviction -20% (fighting a potential squeeze)

Note: We DON'T block shorts on squeezable stocks — if the news is bad enough,
even heavily shorted stocks can fall. But we do reduce size.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
import redis.asyncio as aioredis


def _log(level, event, **kw):
    import json as _j
    print(_j.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                    "level": level, "event": event, **kw}), flush=True)


@dataclass
class SqueezeResult:
    ticker: str
    direction: str

    # Raw data
    short_float_pct: Optional[float] = None   # % of float that is short
    days_to_cover: Optional[float] = None     # Short interest / avg daily volume
    borrow_rate: Optional[float] = None       # Annual % cost to borrow shares

    # Derived
    squeeze_score: float = 0.0               # 0.0 – 1.0
    is_squeeze_candidate: bool = False
    signal_type_override: Optional[str] = None  # "squeeze" | "high_squeeze"
    conviction_multiplier: float = 1.0
    notes: str = ""
    data_available: bool = False


async def score_squeeze(
    ticker: str,
    direction: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
) -> SqueezeResult:
    result = SqueezeResult(ticker=ticker, direction=direction)

    data = await _get_short_data(ticker, redis_conn, http)

    if not data:
        result.notes = "No short data available"
        return result

    result.data_available = True
    result.short_float_pct = data.get("short_float_pct")
    result.days_to_cover = data.get("days_to_cover")
    result.borrow_rate = data.get("borrow_rate")

    si = result.short_float_pct or 0.0
    dtc = result.days_to_cover or 0.0
    br = result.borrow_rate or 0.0

    # ── Compute squeeze score ──────────────────────────────────────────────────
    score = 0.0

    # Short float contribution (max 0.50)
    if si >= 40:
        score += 0.50
    elif si >= 30:
        score += 0.40
    elif si >= 20:
        score += 0.28
    elif si >= 15:
        score += 0.15
    elif si >= 10:
        score += 0.05

    # Days to cover contribution (max 0.30)
    if dtc >= 10:
        score += 0.30
    elif dtc >= 7:
        score += 0.22
    elif dtc >= 5:
        score += 0.14
    elif dtc >= 3:
        score += 0.06

    # Borrow rate contribution (max 0.20)
    if br >= 100:
        score += 0.20
    elif br >= 50:
        score += 0.14
    elif br >= 20:
        score += 0.08
    elif br >= 10:
        score += 0.04

    result.squeeze_score = round(min(score, 1.0), 3)
    result.is_squeeze_candidate = result.squeeze_score >= 0.50

    # ── Apply to conviction ────────────────────────────────────────────────────
    if direction == "long":
        if result.squeeze_score >= 0.80:
            result.conviction_multiplier = 1.30
            result.signal_type_override = "high_squeeze"
            result.notes = (
                f"HIGH SQUEEZE: {si:.0f}% short float, {dtc:.1f} days to cover, "
                f"{br:.0f}% borrow — catalyst could trigger violent squeeze"
            )
        elif result.squeeze_score >= 0.60:
            result.conviction_multiplier = 1.20
            result.signal_type_override = "squeeze"
            result.notes = (
                f"Squeeze candidate: {si:.0f}% short float, "
                f"{dtc:.1f} days to cover"
            )
        elif result.squeeze_score >= 0.40:
            result.conviction_multiplier = 1.08
            result.notes = f"Moderate short interest: {si:.0f}% — some squeeze potential"
        else:
            result.conviction_multiplier = 1.00

    elif direction == "short":
        # Shorting into a heavily shorted stock = crowded trade + squeeze risk
        if result.squeeze_score >= 0.60:
            result.conviction_multiplier = 0.75
            result.notes = (
                f"Caution: heavily shorted ({si:.0f}%) — short is crowded, "
                f"squeeze risk if news reverses"
            )
        elif result.squeeze_score >= 0.40:
            result.conviction_multiplier = 0.88
        else:
            result.conviction_multiplier = 1.00

    _log("info", "squeeze_filter.scored",
         ticker=ticker, direction=direction,
         squeeze_score=result.squeeze_score,
         short_float=si, dtc=dtc, borrow_rate=br,
         multiplier=result.conviction_multiplier)

    return result


async def _get_short_data(
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
) -> dict | None:
    cache_key = f"short:{ticker}"
    try:
        cached = await redis_conn.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    # Polygon: regulatory filing data (bi-monthly FINRA). Requires higher plan.
    data = await _try_polygon_short(ticker, http)

    # Finnhub: insider transactions (SEC Form 4) — supplements short data.
    # Even when Polygon returns no short interest, insider sentiment adds signal.
    insider = await _try_finnhub_insider(ticker, redis_conn, http)
    if insider:
        if data is None:
            data = {}
        data["insider_buys"]          = insider.get("total_buy_value", 0)
        data["insider_sells"]         = insider.get("total_sell_value", 0)
        data["insider_net_sentiment"] = insider.get("net_sentiment", "neutral")
        data["insider_notable"]       = insider.get("notable_transactions", False)

    if data:
        try:
            await redis_conn.setex(cache_key, 3600 * 4, json.dumps(data))  # Cache 4hr
        except Exception:
            pass

    return data


# IEX Cloud removed — API shut down in 2024.
# Finviz Elite removed — export.ashx is a screener bulk export, not a per-ticker quote API.
#   Short float data lives on quote.ashx (HTML scrape) which is fragile.
#   Polygon regulatory filing data covers this need adequately.



async def _try_finnhub_insider(
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
) -> dict | None:
    """Insider transactions from Finnhub /stock/insider-transactions (SEC Form 4).

    Filters to last 90 days, P (Purchase) and S (Sale) codes only.
    Returns:
        total_buy_value   — sum of (|change| * price) for Purchase codes
        total_sell_value  — sum of (|change| * price) for Sale codes
        net_sentiment     — "bullish" | "bearish" | "neutral"
        notable_transactions — True if any single trade > $1M notional
    """
    cache_key = f"insider:finnhub:{ticker}"
    try:
        cached = await redis_conn.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    finnhub_key = os.environ.get("FINNHUB_API_KEY", "")
    if not finnhub_key:
        return None

    try:
        resp = await http.get(
            "https://finnhub.io/api/v1/stock/insider-transactions",
            params={"symbol": ticker, "token": finnhub_key},
            timeout=8.0,
        )
        if resp.status_code != 200:
            return None

        txns = resp.json().get("data", [])
        if not txns:
            return None

        # Filter: last 90 days, Purchase (P) and Sale (S) only
        cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
        buy_value  = 0.0
        sell_value = 0.0
        notable    = False

        for t in txns:
            code  = t.get("transactionCode", "")
            if code not in ("P", "S"):
                continue
            txn_date = t.get("transactionDate", "") or t.get("filingDate", "")
            if txn_date < cutoff:
                continue

            shares = abs(t.get("change", 0) or 0)
            price  = float(t.get("transactionPrice", 0) or 0)
            value  = shares * price

            if code == "P":
                buy_value += value
            else:
                sell_value += value

            if value >= 1_000_000:
                notable = True

        if buy_value == 0 and sell_value == 0:
            return None

        if buy_value > sell_value * 1.5:
            sentiment = "bullish"
        elif sell_value > buy_value * 1.5:
            sentiment = "bearish"
        else:
            sentiment = "neutral"

        result = {
            "total_buy_value":      round(buy_value, 2),
            "total_sell_value":     round(sell_value, 2),
            "net_sentiment":        sentiment,
            "notable_transactions": notable,
        }

        try:
            await redis_conn.setex(cache_key, 3600 * 4, json.dumps(result))
        except Exception:
            pass

        _log("debug", "squeeze_filter.finnhub_insider_ok",
             ticker=ticker, sentiment=sentiment,
             buys=round(buy_value / 1e6, 2), sells=round(sell_value / 1e6, 2))
        return result

    except Exception as e:
        _log("warning", "squeeze_filter.finnhub_insider_error",
             ticker=ticker, error=str(e))
        return None


async def _try_polygon_short(ticker: str, http: httpx.AsyncClient) -> dict | None:
    """Polygon short interest from regulatory filings (bi-monthly FINRA data).

    NOTE: The /v3/trades/{ticker}/short-interest endpoint returns 404 on the current
    Polygon plan (Starter tier). FINRA short interest data requires a higher-tier
    subscription. The function is preserved for when the plan is upgraded — it will
    begin returning data automatically. Until then, returns None silently.
    """
    api_key = os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        return None
    try:
        resp = await http.get(
            f"https://api.polygon.io/v3/trades/{ticker}/short-interest",
            params={"apiKey": api_key, "limit": 1},
            timeout=8.0,
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                r = results[0]
                data = {
                    "short_float_pct": float(r.get("short_percent_of_float", 0)) * 100,
                    "days_to_cover":   float(r.get("days_to_cover", 0)),
                    "borrow_rate":     0.0,
                    "source":          "polygon",
                }
                _log("debug", "squeeze_filter.polygon_short_ok",
                     ticker=ticker,
                     short_float=data["short_float_pct"],
                     dtc=data["days_to_cover"])
                return data
            else:
                _log("debug", "squeeze_filter.polygon_short_empty",
                     ticker=ticker,
                     note="No short interest filings found — ticker may be too new or small")
        else:
            # 404 = endpoint not available on current plan; log at debug, not warning,
            # to avoid flooding logs every cycle for every ticker.
            _log("debug", "squeeze_filter.polygon_short_unavailable",
                 ticker=ticker, status=resp.status_code,
                 note="FINRA short interest requires higher Polygon tier")
    except Exception as e:
        _log("warning", "squeeze_filter.polygon_short_exception",
             ticker=ticker, error=str(e))
    return None