"""
Options Flow Filter.

Detects unusual options activity that confirms or contradicts news signals.
Large options flow often precedes or coincides with big moves — it's the
most direct read of where informed money is positioned.

Data sources (in order of preference):
  1. Unusual Whales API — best retail-accessible options flow feed
  2. Tradier API        — free tier, real-time options chains
  3. Polygon options    — included in most plans

What we look for:
  - Unusual call buying  → bullish confirmation for long signals
  - Unusual put buying   → bearish confirmation / warning against longs
  - Put/Call ratio spike → extreme fear, potential reversal
  - Large single-leg sweeps at ask → aggressive directional bet

Scoring:
  Strong call flow + long signal  → +0.20 conviction boost
  Strong put flow  + long signal  → -0.30 conviction cut (smart money warning)
  Strong put flow  + short signal → +0.15 confirmation
  No unusual flow                 → 0.00 (neutral, no adjustment)

Flow is cached in Redis for 15 minutes.
Falls back gracefully if API unavailable (no block on missing data).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx
import redis.asyncio as aioredis


def _log(level, event, **kw):
    import json as _j
    print(_j.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                    "level": level, "event": event, **kw}), flush=True)


@dataclass
class OptionsFlowResult:
    ticker: str
    direction: str

    # Raw signals
    unusual_calls: bool = False
    unusual_puts: bool = False
    put_call_ratio: Optional[float] = None
    largest_flow_side: Optional[str] = None  # "calls" | "puts"
    largest_flow_premium: float = 0.0        # $ value of largest single flow

    # Derived
    flow_bias: str = "neutral"              # bullish | bearish | neutral | mixed
    conviction_delta: float = 0.0          # additive change to conviction
    notes: str = ""
    data_available: bool = False


async def score_options_flow(
    ticker: str,
    direction: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
) -> OptionsFlowResult:
    """
    Fetch and score options flow for ticker.
    Returns neutral result (no change) if data unavailable.
    """
    result = OptionsFlowResult(ticker=ticker, direction=direction)

    # Try each data source
    flow = await _try_unusual_whales(ticker, redis_conn, http)
    if not flow:
        flow = await _try_tradier(ticker, redis_conn, http)
    if not flow:
        flow = await _try_polygon_options(ticker, redis_conn, http)

    if not flow:
        result.notes = "No options data available"
        return result

    result.data_available = True
    result.put_call_ratio = flow.get("put_call_ratio")
    result.unusual_calls = flow.get("unusual_calls", False)
    result.unusual_puts = flow.get("unusual_puts", False)
    result.largest_flow_side = flow.get("largest_flow_side")
    result.largest_flow_premium = flow.get("largest_flow_premium", 0)

    # ── Classify flow bias ─────────────────────────────────────────────────────
    pcr = result.put_call_ratio or 1.0
    if result.unusual_calls and not result.unusual_puts:
        result.flow_bias = "bullish"
    elif result.unusual_puts and not result.unusual_calls:
        result.flow_bias = "bearish"
    elif pcr > 2.0:
        result.flow_bias = "bearish"   # Extreme put buying
    elif pcr < 0.5:
        result.flow_bias = "bullish"   # Extreme call buying
    elif result.unusual_calls and result.unusual_puts:
        result.flow_bias = "mixed"     # Volatility play — no directional edge
    else:
        result.flow_bias = "neutral"

    # ── Compute conviction delta ───────────────────────────────────────────────
    if result.flow_bias == "neutral" or result.flow_bias == "mixed":
        result.conviction_delta = 0.0

    elif direction == "long":
        if result.flow_bias == "bullish":
            # Smart money also buying calls → strong confirmation
            base = 0.08
            if result.largest_flow_premium > 500_000:    # >$500K single flow
                base += 0.07
            if result.largest_flow_side == "calls" and pcr < 0.7:
                base += 0.05
            result.conviction_delta = base
            result.notes = f"Unusual call buying confirms long (PCR {pcr:.2f})"

        elif result.flow_bias == "bearish":
            # Smart money buying puts against our long → warning
            penalty = -0.15
            if result.largest_flow_premium > 1_000_000:  # >$1M — serious hedge
                penalty -= 0.10
            result.conviction_delta = penalty
            result.notes = f"Unusual PUT buying warns against long (PCR {pcr:.2f})"

    elif direction == "short":
        if result.flow_bias == "bearish":
            base = 0.08
            if result.largest_flow_premium > 500_000:
                base += 0.05
            result.conviction_delta = base
            result.notes = f"Unusual put buying confirms short (PCR {pcr:.2f})"

        elif result.flow_bias == "bullish":
            result.conviction_delta = -0.10
            result.notes = f"Call buying warns against short (PCR {pcr:.2f})"

    _log("info", "options_filter.scored",
         ticker=ticker, direction=direction,
         flow_bias=result.flow_bias, delta=result.conviction_delta,
         pcr=result.put_call_ratio, notes=result.notes)

    return result


# ── Data source connectors ─────────────────────────────────────────────────────

async def _try_unusual_whales(
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
) -> dict | None:
    """Fetch from Unusual Whales API."""
    api_key = os.environ.get("UNUSUAL_WHALES_API_KEY", "")
    if not api_key:
        return None

    cache_key = f"options:uw:{ticker}"
    try:
        cached = await redis_conn.get(cache_key)
        if cached:
            return json.loads(cached)

        resp = await http.get(
            f"https://api.unusualwhales.com/api/stock/{ticker}/flow-alerts",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 20},
            timeout=8.0,
        )
        if resp.status_code != 200:
            return None

        data = resp.json().get("data", [])
        if not data:
            return None

        # Parse UW flow alerts
        calls = [f for f in data if f.get("put_call") == "CALL"]
        puts  = [f for f in data if f.get("put_call") == "PUT"]

        call_prem = sum(float(f.get("premium", 0)) for f in calls)
        put_prem  = sum(float(f.get("premium", 0)) for f in puts)
        total_prem = call_prem + put_prem

        pcr = put_prem / call_prem if call_prem > 0 else (2.0 if put_prem > 0 else 1.0)

        largest = max(data, key=lambda f: float(f.get("premium", 0)), default={})

        result = {
            "put_call_ratio":       round(pcr, 2),
            "unusual_calls":        call_prem > 200_000 and len(calls) >= 2,
            "unusual_puts":         put_prem  > 200_000 and len(puts) >= 2,
            "largest_flow_side":    "calls" if call_prem > put_prem else "puts",
            "largest_flow_premium": float(largest.get("premium", 0)),
            "source":               "unusual_whales",
        }

        await redis_conn.setex(cache_key, 900, json.dumps(result))
        return result

    except Exception as e:
        _log("warning", "options_filter.uw_error", ticker=ticker, error=str(e))
        return None


async def _try_tradier(
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
) -> dict | None:
    """Fetch options chain from Tradier and compute PCR."""
    api_key = os.environ.get("TRADIER_API_KEY", "")
    if not api_key:
        return None

    cache_key = f"options:tradier:{ticker}"
    try:
        cached = await redis_conn.get(cache_key)
        if cached:
            return json.loads(cached)

        # Get nearest expiry options chain
        resp = await http.get(
            "https://api.tradier.com/v1/markets/options/chains",
            headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            params={"symbol": ticker, "expiration": _next_friday(),
                    "greeks": "false"},
            timeout=10.0,
        )
        if resp.status_code != 200:
            return None

        options = resp.json().get("options", {}).get("option", []) or []
        if not options:
            return None

        calls = [o for o in options if o.get("option_type") == "call"]
        puts  = [o for o in options if o.get("option_type") == "put"]

        call_vol = sum(int(o.get("volume", 0)) for o in calls)
        put_vol  = sum(int(o.get("volume", 0)) for o in puts)

        # High volume relative to open interest = unusual activity
        call_oi = sum(int(o.get("open_interest", 1)) for o in calls)
        put_oi  = sum(int(o.get("open_interest", 1)) for o in puts)

        call_vol_ratio = call_vol / call_oi if call_oi > 0 else 0
        put_vol_ratio  = put_vol / put_oi  if put_oi > 0 else 0

        pcr = put_vol / call_vol if call_vol > 0 else 1.0

        result = {
            "put_call_ratio":       round(pcr, 2),
            "unusual_calls":        call_vol_ratio > 2.0 and call_vol > 500,
            "unusual_puts":         put_vol_ratio > 2.0 and put_vol > 500,
            "largest_flow_side":    "calls" if call_vol > put_vol else "puts",
            "largest_flow_premium": 0.0,   # Tradier doesn't give premium directly
            "source":               "tradier",
        }

        await redis_conn.setex(cache_key, 900, json.dumps(result))
        return result

    except Exception as e:
        _log("warning", "options_filter.tradier_error", ticker=ticker, error=str(e))
        return None


async def _try_polygon_options(
    ticker: str,
    redis_conn: aioredis.Redis,
    http: httpx.AsyncClient,
) -> dict | None:
    """Fallback: use Polygon options snapshot for PCR."""
    api_key = os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        return None

    cache_key = f"options:polygon:{ticker}"
    try:
        cached = await redis_conn.get(cache_key)
        if cached:
            return json.loads(cached)

        resp = await http.get(
            f"https://api.polygon.io/v3/snapshot/options/{ticker}",
            params={"apiKey": api_key, "limit": 250,
                    "expiration_date.gte": datetime.now(timezone.utc).strftime("%Y-%m-%d")},
            timeout=10.0,
        )
        if resp.status_code != 200:
            return None

        results = resp.json().get("results", [])
        if not results:
            return None

        calls = [r for r in results if r.get("details", {}).get("contract_type") == "call"]
        puts  = [r for r in results if r.get("details", {}).get("contract_type") == "put"]

        call_vol = sum(r.get("day", {}).get("volume", 0) for r in calls)
        put_vol  = sum(r.get("day", {}).get("volume", 0) for r in puts)
        call_oi  = sum(r.get("open_interest", 1) for r in calls)
        put_oi   = sum(r.get("open_interest", 1) for r in puts)

        pcr = put_vol / call_vol if call_vol > 0 else 1.0
        call_ratio = call_vol / call_oi if call_oi > 0 else 0
        put_ratio  = put_vol / put_oi if put_oi > 0 else 0

        result = {
            "put_call_ratio":       round(pcr, 2),
            "unusual_calls":        call_ratio > 2.5,
            "unusual_puts":         put_ratio > 2.5,
            "largest_flow_side":    "calls" if call_vol > put_vol else "puts",
            "largest_flow_premium": 0.0,
            "source":               "polygon",
        }

        await redis_conn.setex(cache_key, 900, json.dumps(result))
        return result

    except Exception as e:
        _log("warning", "options_filter.polygon_error", ticker=ticker, error=str(e))
        return None


def _next_friday() -> str:
    from datetime import timedelta
    today = datetime.now(timezone.utc)
    days_ahead = (4 - today.weekday()) % 7  # 4 = Friday
    if days_ahead == 0:
        days_ahead = 7
    return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
