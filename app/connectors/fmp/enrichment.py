"""
FMP Enrichment Service — enhances signals with fundamental context.

Consumes: news.enriched
Enriches each record with:
  1. Shares float & float sensitivity (replaces static map)
  2. Company profile: sector, market cap, beta
  3. Analyst consensus: rating, avg price target, # of analysts
  4. Recent analyst price target changes (last 5)
  5. Insider trading activity (last 30 days)

Emits: news.enriched (pass-through with extra fields added)
       → downstream AI summarizer gets much better context

This runs between entity_resolver and ai_summarizer.
New topic: news.fmp_enriched
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import redis.asyncio as aioredis

from app.connectors.fmp.client import FMPClient
from app.pipeline.base_consumer import BaseConsumer, _log


# Float thresholds (shares)
FLOAT_HIGH_THRESHOLD  = 20_000_000    # < 20M shares = high sensitivity
FLOAT_MICRO_THRESHOLD = 5_000_000     # < 5M shares = extreme sensitivity


class FMPEnrichmentService(BaseConsumer):
    """
    Sits between entity_resolver → ai_summarizer.
    Adds fundamental context to each enriched news record.
    """

    def __init__(self) -> None:
        self._fmp: FMPClient | None = None
        self._redis: aioredis.Redis | None = None
        super().__init__()

    @property
    def service_name(self) -> str:
        return "fmp_enrichment"

    @property
    def input_topic(self) -> str:
        return "news.enriched"

    @property
    def output_topic(self) -> str:
        return "news.fmp_enriched"

    async def on_start(self) -> None:
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        api_key = os.environ.get("FMP_API_KEY", "")
        plan = os.environ.get("FMP_PLAN", "free")

        if not api_key:
            _log("warning", "fmp_enrichment.no_api_key",
                 msg="FMP_API_KEY not set — enrichment will pass through without FMP data")

        self._fmp = FMPClient(api_key=api_key, redis=self._redis, plan=plan)
        _log("info", "fmp_enrichment.ready", plan=plan,
             daily_limit=self._fmp._daily_limit)

    async def on_stop(self) -> None:
        if self._fmp:
            await self._fmp.close()

    async def process(self, record: dict) -> dict | None:
        tickers = record.get("tickers", [])
        catalyst = record.get("catalyst_type", "other")

        if not tickers or not self._fmp or not self._fmp._api_key:
            # Pass through unchanged if no tickers or no API key
            record["fmp_enriched"] = False
            return record

        primary = tickers[0]

        # Run enrichment tasks in parallel
        float_data, profile, analyst, insider = await asyncio.gather(
            self._get_float(primary),
            self._get_profile(primary),
            self._get_analyst(primary) if catalyst in ("analyst", "earnings") else asyncio.coroutine(lambda: None)(),
            self._get_insider(primary) if catalyst in ("earnings", "ma", "regulatory") else asyncio.coroutine(lambda: None)(),
            return_exceptions=True,
        )

        # ── Float sensitivity (replaces static map) ────────────────────────────
        if float_data and not isinstance(float_data, Exception):
            float_shares = float_data.get("floatShares", 0)
            record["float_shares"] = float_shares
            if float_shares and float_shares < FLOAT_MICRO_THRESHOLD:
                record["float_sensitivity"] = "extreme"
            elif float_shares and float_shares < FLOAT_HIGH_THRESHOLD:
                record["float_sensitivity"] = "high"
            else:
                record["float_sensitivity"] = "normal"
            _log("debug", "fmp.float_resolved",
                 ticker=primary,
                 float_shares=float_shares,
                 sensitivity=record["float_sensitivity"])

        # ── Company profile ────────────────────────────────────────────────────
        if profile and not isinstance(profile, Exception):
            record["fmp_sector"]      = profile.get("sector", "")
            record["fmp_industry"]    = profile.get("industry", "")
            record["fmp_beta"]        = profile.get("beta", 1.0)
            record["fmp_mkt_cap"]     = profile.get("mktCap", 0)
            record["fmp_description"] = profile.get("description", "")[:500]

            # Override market_cap_tier with live data
            mkt_cap = profile.get("mktCap", 0)
            if mkt_cap:
                record["market_cap_tier"] = _classify_market_cap(mkt_cap)

        # ── Analyst context ────────────────────────────────────────────────────
        if analyst and not isinstance(analyst, Exception):
            record["fmp_analyst"] = analyst

        # ── Insider activity ───────────────────────────────────────────────────
        if insider and not isinstance(insider, Exception):
            record["fmp_insider"] = insider

        record["fmp_enriched"] = True
        record["fmp_requests_remaining"] = self._fmp.daily_requests_remaining

        return record

    async def _get_float(self, ticker: str) -> dict | None:
        data = await self._fmp.get("/stable/shares-float", symbol=ticker)
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    async def _get_profile(self, ticker: str) -> dict | None:
        data = await self._fmp.get("/stable/profile", symbol=ticker)
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    async def _get_analyst(self, ticker: str) -> dict | None:
        """Get analyst consensus + recent price target changes."""
        # Latest ratings
        ratings = await self._fmp.get(
            "/stable/grades-latest",
            symbol=ticker,
            limit=10,
        )

        # Price target summary
        pt_summary = await self._fmp.get(
            "/stable/price-target-summary",
            symbol=ticker,
        )

        if not ratings and not pt_summary:
            return None

        result = {}

        if pt_summary and isinstance(pt_summary, list) and pt_summary:
            pt = pt_summary[0]
            result["avg_price_target"]  = pt.get("targetMedian", 0)
            result["high_price_target"] = pt.get("targetHigh", 0)
            result["low_price_target"]  = pt.get("targetLow", 0)
            result["num_analysts"]      = pt.get("numberOfAnalysts", 0)

        if ratings and isinstance(ratings, list):
            # Last 5 ratings
            recent = ratings[:5]
            result["recent_ratings"] = [
                {
                    "firm":       r.get("gradingCompany", ""),
                    "from_grade": r.get("previousGrade", ""),
                    "to_grade":   r.get("newGrade", ""),
                    "action":     r.get("action", ""),
                    "date":       r.get("date", ""),
                }
                for r in recent
            ]
            # Sentiment score: upgrades - downgrades in last 5
            upgrades   = sum(1 for r in recent if r.get("action", "").lower() == "upgrade")
            downgrades = sum(1 for r in recent if r.get("action", "").lower() == "downgrade")
            result["analyst_sentiment"] = upgrades - downgrades  # +ve = bullish

        return result

    async def _get_insider(self, ticker: str) -> dict | None:
        """Get insider trading activity last 30 days."""
        data = await self._fmp.get(
            "/stable/insider-trading",
            symbol=ticker,
            limit=20,
        )
        if not data or not isinstance(data, list):
            return None

        # Filter last 30 days
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        recent = []
        for item in data:
            try:
                trade_date = datetime.fromisoformat(
                    item.get("transactionDate", "").replace("Z", "+00:00"))
                if trade_date >= cutoff:
                    recent.append(item)
            except Exception:
                continue

        if not recent:
            return None

        buys  = [r for r in recent if r.get("transactionType", "").upper() in ("P-PURCHASE", "BUY")]
        sells = [r for r in recent if r.get("transactionType", "").upper() in ("S-SALE", "SELL")]

        total_buy_value  = sum(float(r.get("value", 0) or 0) for r in buys)
        total_sell_value = sum(float(r.get("value", 0) or 0) for r in sells)

        return {
            "buy_count":         len(buys),
            "sell_count":        len(sells),
            "total_buy_value":   round(total_buy_value, 0),
            "total_sell_value":  round(total_sell_value, 0),
            "net_sentiment":     "bullish" if total_buy_value > total_sell_value else "bearish",
            "notable":           total_buy_value > 500_000 or total_sell_value > 1_000_000,
        }


def _classify_market_cap(mkt_cap: float) -> str:
    if mkt_cap >= 200e9:  return "mega"
    if mkt_cap >= 10e9:   return "large"
    if mkt_cap >= 2e9:    return "mid"
    if mkt_cap >= 300e6:  return "small"
    return "micro"
