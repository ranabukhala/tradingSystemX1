"""
Finnhub Fundamentals Connector.

Provides two data streams written to Redis for the AI summarizer:

1. Earnings Surprises — historical beat/miss rate per ticker
   Endpoint: /stock/earnings
   Value: "Has AAPL beaten EPS estimates 7 of last 8 quarters (+$0.08 avg)"
   → Dramatically improves earnings catalyst analysis

2. Analyst Recommendations — consensus rating trend
   Endpoint: /stock/recommendation
   Value: buy/hold/sell counts, trend over 3 months (upgrading vs downgrading)
   → Adds "analyst consensus shifting bullish" context to T2

Both are cached in Redis and pulled by the AI summarizer when
catalyst_type = earnings or analyst.

Poll: every 6 hours for earnings surprises, every 2 hours for analyst.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import httpx
import redis.asyncio as aioredis

from app.config import settings
from app.connectors.base import BaseConnector, _log
from app.connectors.finnhub_rate_limiter import try_acquire, CACHE_TTL

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Tickers to pre-fetch fundamentals for
TRACKED_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA",
    "AMD", "INTC", "QCOM", "COIN", "MSTR", "PLTR", "SOFI",
    "JPM", "GS", "BAC", "MS", "WFC",
    "PFE", "MRNA", "ABBV", "GILD", "REGN",
    "COST", "TGT", "SBUX", "MCD", "NKE",
]


class FinnhubFundamentalsConnector(BaseConnector):
    """
    Polls earnings surprises + analyst recommendations.
    Writes to Redis for AI summarizer to pull during T2 analysis.
    """

    @property
    def source_name(self) -> str:
        return "finnhub_fundamentals"

    @property
    def poll_interval_seconds(self) -> int:
        return 3600 * 6  # Every 6 hours

    def validate_config(self) -> None:
        if not settings.finnhub_api_key:
            _log("warning", "finnhub_fundamentals.no_key")

    async def fetch(self) -> int:
        api_key = settings.finnhub_api_key
        if not api_key:
            return 0

        redis_conn = await aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
        )
        http = httpx.AsyncClient(
            base_url=FINNHUB_BASE,
            headers={"X-Finnhub-Token": api_key},
            timeout=10.0,
        )

        updated = 0
        for ticker in TRACKED_TICKERS:
            try:
                # ── Earnings surprises ────────────────────────────────────────
                earnings_key = f"finnhub:earnings:{ticker}"
                cached_earnings = await redis_conn.get(earnings_key)
                if cached_earnings is not None:
                    _log("debug", "finnhub.cache.hit", ticker=ticker, data_type="earnings")
                else:
                    _log("debug", "finnhub.cache.miss", ticker=ticker, data_type="earnings")

                    # Per-minute rate limit check before API call
                    allowed, count = await try_acquire(
                        redis_conn, settings.finnhub_per_minute_call_limit
                    )
                    if not allowed:
                        _log("warning", "finnhub.rate_limit.per_minute_cap_reached",
                             ticker=ticker, count=count,
                             limit=settings.finnhub_per_minute_call_limit)
                        break  # Skip remaining tickers for this cycle

                    earnings = await self._get_earnings_surprises(http, ticker)
                    if earnings:
                        await redis_conn.setex(
                            earnings_key, CACHE_TTL["earnings"], json.dumps(earnings)
                        )
                        updated += 1

                await asyncio.sleep(0.3)  # Rate limit

                # ── Analyst recommendations ───────────────────────────────────
                analyst_key = f"finnhub:analyst:{ticker}"
                cached_analyst = await redis_conn.get(analyst_key)
                if cached_analyst is not None:
                    _log("debug", "finnhub.cache.hit", ticker=ticker, data_type="analyst")
                else:
                    _log("debug", "finnhub.cache.miss", ticker=ticker, data_type="analyst")

                    # Per-minute rate limit check before API call
                    allowed, count = await try_acquire(
                        redis_conn, settings.finnhub_per_minute_call_limit
                    )
                    if not allowed:
                        _log("warning", "finnhub.rate_limit.per_minute_cap_reached",
                             ticker=ticker, count=count,
                             limit=settings.finnhub_per_minute_call_limit)
                        break  # Skip remaining tickers for this cycle

                    analyst = await self._get_analyst_consensus(http, ticker)
                    if analyst:
                        await redis_conn.setex(
                            analyst_key, CACHE_TTL["analyst"], json.dumps(analyst)
                        )

                await asyncio.sleep(0.3)

            except Exception as e:
                _log("error", "finnhub_fundamentals.error",
                     ticker=ticker, error=str(e))

        _log("info", "finnhub_fundamentals.updated", tickers=updated)
        await http.aclose()
        await redis_conn.aclose()
        return updated

    async def _get_earnings_surprises(
        self, http: httpx.AsyncClient, ticker: str
    ) -> dict | None:
        """Get last 8 quarters of EPS surprises."""
        resp = await http.get(
            "/stock/earnings",
            params={"symbol": ticker, "limit": 8},
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not data:
            return None

        beats  = sum(1 for q in data if (q.get("actual") or 0) > (q.get("estimate") or 0))
        misses = sum(1 for q in data if (q.get("actual") or 0) < (q.get("estimate") or 0))
        total  = len(data)

        # Average surprise magnitude (actual - estimate)
        surprises = [
            (q.get("actual") or 0) - (q.get("estimate") or 0)
            for q in data
            if q.get("actual") is not None and q.get("estimate") is not None
        ]
        avg_surprise = sum(surprises) / len(surprises) if surprises else 0

        # Most recent quarter
        latest = data[0] if data else {}

        return {
            "ticker":        ticker,
            "quarters":      total,
            "beats":         beats,
            "misses":        misses,
            "beat_rate_pct": round(beats / total * 100, 0) if total else 0,
            "avg_surprise":  round(avg_surprise, 4),
            "latest_actual": latest.get("actual"),
            "latest_est":    latest.get("estimate"),
            "latest_period": latest.get("period", ""),
            "summary":       _earnings_summary(beats, total, avg_surprise),
        }

    async def _get_analyst_consensus(
        self, http: httpx.AsyncClient, ticker: str
    ) -> dict | None:
        """Get analyst buy/hold/sell consensus + trend."""
        resp = await http.get(
            "/stock/recommendation",
            params={"symbol": ticker},
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not data or len(data) < 1:
            return None

        # Latest month
        latest = data[0]
        buy      = latest.get("buy", 0)
        hold     = latest.get("hold", 0)
        sell     = latest.get("sell", 0)
        strong_buy  = latest.get("strongBuy", 0)
        strong_sell = latest.get("strongSell", 0)

        total = buy + hold + sell + strong_buy + strong_sell
        if total == 0:
            return None

        bullish = buy + strong_buy
        bearish = sell + strong_sell
        consensus = (
            "strong_buy" if strong_buy > total * 0.4 else
            "buy"        if bullish > total * 0.5 else
            "strong_sell" if strong_sell > total * 0.3 else
            "sell"       if bearish > total * 0.4 else
            "hold"
        )

        # Trend: compare latest vs 1 month ago
        trend = "stable"
        if len(data) >= 2:
            prev = data[1]
            prev_bull = prev.get("buy", 0) + prev.get("strongBuy", 0)
            if bullish > prev_bull + 2:
                trend = "upgrading"
            elif bullish < prev_bull - 2:
                trend = "downgrading"

        return {
            "ticker":    ticker,
            "consensus": consensus,
            "trend":     trend,
            "buy":       bullish,
            "hold":      hold,
            "sell":      bearish,
            "total":     total,
            "period":    latest.get("period", ""),
        }


def _earnings_summary(beats: int, total: int, avg_surprise: float) -> str:
    """Human-readable earnings history summary for AI prompt injection."""
    if total == 0:
        return "No earnings history"
    beat_rate = int(beats / total * 100)
    direction = "above" if avg_surprise > 0 else "below"
    return (
        f"Beat EPS estimates {beats}/{total} quarters ({beat_rate}%), "
        f"avg ${abs(avg_surprise):.2f} {direction} consensus"
    )
