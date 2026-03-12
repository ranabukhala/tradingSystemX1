"""
Market Regime Filter.

Classifies the current macro environment as risk-on / risk-off / high-vol.
Runs every 5 minutes and writes state to Redis.

Inputs:
  - SPY intraday % change  (from Polygon WebSocket prices already in Redis)
  - VXX price level        (VIX proxy ETF — covered under Stocks Starter plan)
  - SPY vs 20-day MA       (computed from 20-bar close history in Redis)

Note: VXX is used as a VIX proxy because Polygon's Indices product (required
for I:VIX) is a separate paid tier. VXX tracks VIX directionally but trades
at a different absolute scale (~$60-90 range vs VIX's 12-40). Thresholds
below are calibrated for VXX price levels.

VXX → VIX approximate mapping:
  VXX < 65    ≈ VIX < 15  (very low vol / compression)
  VXX 65-80   ≈ VIX 15-20 (normal)
  VXX 80-95   ≈ VIX 20-25 (elevated)
  VXX 95-120  ≈ VIX 25-35 (high vol)
  VXX > 120   ≈ VIX > 35  (extreme — block longs)

Output (Redis key "regime:current"):
  {
    "regime":      "risk_on" | "risk_off" | "high_vol" | "compression",
    "vix":         75.2,       # stores VXX price, labelled vix for downstream compat
    "spy_chg_pct": -0.8,
    "spy_vs_ma20": -0.3,       # % above/below 20-day MA
    "conviction_scale": 1.0,   # multiplier applied to every signal
    "block_longs":  false,
    "block_shorts": false,
    "updated_at":  "..."
  }

Conviction scale matrix (VXX levels):
  risk_on   + VXX < 80   → 1.15  (ideal — trade full size)
  risk_on   + VXX 80-95  → 1.00
  risk_off  + VXX 80-95  → 0.75  (reduce size)
  high_vol  + VXX 95-120 → 0.50  (half size only)
  high_vol  + VXX > 120  → 0.00  → block all new longs
  compression             → 0.80  (low vol, low follow-through)

Hard blocks:
  - SPY down > 1.5% intraday → block_longs = True
  - SPY up > 2.0% intraday   → block_shorts = True
  - VXX > 120                → block_longs = True
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from collections import deque

import httpx
import redis.asyncio as aioredis


def _log(level, event, **kw):
    import json as _json
    print(_json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                       "level": level, "event": event, **kw}), flush=True)


POLYGON_BASE = "https://api.polygon.io"
SPY_HISTORY_KEY = "regime:spy_closes"   # Redis list of last 25 SPY closes
REGIME_KEY      = "regime:current"


class RegimeFilter:
    """
    Polls Polygon for VIX + SPY data and maintains current regime state in Redis.
    """

    def __init__(self) -> None:
        self._api_key = os.environ.get("POLYGON_API_KEY", "")
        self._redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
        self._http: httpx.AsyncClient | None = None
        self._redis: aioredis.Redis | None = None
        self._running = False
        self._poll_interval = int(os.environ.get("REGIME_POLL_INTERVAL", "300"))  # 5 min

    async def run(self) -> None:
        self._running = True
        self._http = httpx.AsyncClient(base_url=POLYGON_BASE, timeout=10.0)
        self._redis = await aioredis.from_url(self._redis_url, decode_responses=True)

        _log("info", "regime_filter.starting", poll_interval=self._poll_interval)

        while self._running:
            try:
                await self._update()
            except Exception as e:
                _log("error", "regime_filter.update_error", error=str(e))
            await asyncio.sleep(self._poll_interval)

    def stop(self) -> None:
        self._running = False

    async def _update(self) -> None:
        spy_open, spy_last = await self._get_spy_prices()
        vix = await self._get_vix()

        # SPY intraday % change
        spy_chg = ((spy_last - spy_open) / spy_open * 100) if spy_open else 0.0

        # SPY vs 20-day MA
        spy_vs_ma20 = await self._compute_ma20_deviation(spy_last)

        # ── Classify regime ────────────────────────────────────────────────────
        # vix variable holds VXX price; thresholds calibrated to VXX scale
        # VXX > 120 ≈ VIX > 35 | VXX > 95 ≈ VIX > 25 | VXX < 65 ≈ VIX < 15
        if vix > 120:
            regime = "high_vol"
        elif vix > 95:
            regime = "high_vol"
        elif spy_chg < -1.0 or spy_vs_ma20 < -3.0:
            regime = "risk_off"
        elif spy_chg > 0.3 and spy_vs_ma20 > -1.0 and vix < 85:
            regime = "risk_on"
        elif vix < 65 and abs(spy_chg) < 0.3:
            regime = "compression"   # Low vol, low follow-through — fade regime
        else:
            regime = "risk_on"       # Default neutral-positive

        # ── Conviction scale ───────────────────────────────────────────────────
        if vix > 120:
            scale = 0.0
        elif vix > 95:
            scale = 0.50
        elif regime == "risk_off" and vix > 80:
            scale = 0.70
        elif regime == "risk_off":
            scale = 0.80
        elif regime == "compression":
            scale = 0.80
        elif regime == "risk_on" and vix < 80:
            scale = 1.15
        else:
            scale = 1.00

        # ── Hard blocks ────────────────────────────────────────────────────────
        block_longs  = spy_chg < -1.5 or vix > 120
        block_shorts = spy_chg > 2.0

        state = {
            "regime":           regime,
            "vix":              round(vix, 2),
            "spy_chg_pct":      round(spy_chg, 3),
            "spy_vs_ma20":      round(spy_vs_ma20, 3),
            "conviction_scale": round(scale, 2),
            "block_longs":      block_longs,
            "block_shorts":     block_shorts,
            "updated_at":       datetime.now(timezone.utc).isoformat(),
        }

        await self._redis.setex(REGIME_KEY, 600, json.dumps(state))   # TTL 10 min

        _log("info", "regime_filter.updated",
             regime=regime, vix=vix, spy_chg=spy_chg,
             scale=scale, block_longs=block_longs)

    async def _get_spy_prices(self) -> tuple[float, float]:
        """Get SPY open and latest price from today's aggregate."""
        try:
            # Try Redis first (polygon_prices connector writes here)
            raw = await self._redis.get("price:SPY:last")
            if raw:
                d = json.loads(raw)
                last = float(d.get("c") or d.get("close") or 0)
                open_ = float(d.get("o") or d.get("open") or last)
                if last > 0:
                    return open_, last
        except Exception:
            pass

        # Fallback: Polygon REST
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            resp = await self._http.get(
                f"/v2/aggs/ticker/SPY/range/1/day/{today}/{today}",
                params={"apiKey": self._api_key, "adjusted": "true"},
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    bar = results[-1]
                    return float(bar["o"]), float(bar["c"])
        except Exception:
            pass

        return 0.0, 0.0

    async def _get_vix(self) -> float:
        """Get current VXX price as VIX proxy from Polygon.

        VXX (iPath Series B S&P 500 VIX Short-Term Futures ETN) tracks VIX
        directionally and is available under the Stocks Starter plan, unlike
        I:VIX which requires a separate Indices subscription.
        """
        try:
            raw = await self._redis.get("price:VXX:last")
            if raw:
                d = json.loads(raw)
                v = float(d.get("c") or d.get("close") or 0)
                if v > 0:
                    return v
        except Exception:
            pass

        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            resp = await self._http.get(
                f"/v2/aggs/ticker/VXX/range/1/day/{today}/{today}",
                params={"apiKey": self._api_key, "adjusted": "true"},
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    return float(results[-1]["c"])
            else:
                _log("warning", "regime_filter.vxx_fetch_failed",
                     status=resp.status_code, ticker="VXX")
        except Exception as e:
            _log("warning", "regime_filter.vxx_exception", error=str(e))

        return 75.0   # Neutral fallback ≈ VIX ~18 equivalent

    async def _compute_ma20_deviation(self, current_price: float) -> float:
        """Compute % deviation of current SPY from 20-day MA using stored closes."""
        if current_price <= 0:
            return 0.0
        try:
            closes_raw = await self._redis.lrange(SPY_HISTORY_KEY, 0, -1)
            if len(closes_raw) >= 10:
                closes = [float(c) for c in closes_raw[-20:]]
                ma20 = sum(closes) / len(closes)
                deviation = (current_price - ma20) / ma20 * 100
                # Store today's close
                await self._redis.rpush(SPY_HISTORY_KEY, str(current_price))
                await self._redis.ltrim(SPY_HISTORY_KEY, -25, -1)  # Keep last 25
                return deviation
        except Exception:
            pass
        return 0.0


async def get_current_regime(redis_conn: aioredis.Redis) -> dict:
    """
    Read current regime from Redis. Called by pre-trade filter.
    Returns safe defaults if Redis unavailable.
    """
    try:
        raw = await redis_conn.get(REGIME_KEY)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    # Safe default: neutral regime, no blocks
    return {
        "regime": "risk_on",
        "vix": 18.0,
        "spy_chg_pct": 0.0,
        "conviction_scale": 1.0,
        "block_longs": False,
        "block_shorts": False,
    }