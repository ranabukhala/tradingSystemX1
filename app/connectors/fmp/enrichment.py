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
import json
import os
from datetime import datetime, timezone
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
        plan = os.environ.get("FMP_PLAN", "starter")

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
        async def _noop():
            return None

        float_data, profile, analyst, insider, quote = await asyncio.gather(
            self._get_float(primary),
            self._get_profile(primary),
            self._get_analyst(primary) if catalyst in ("analyst", "earnings") else _noop(),
            self._get_insider(primary) if catalyst in ("earnings", "ma", "regulatory") else _noop(),
            self._get_quote(primary),
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

        # ── Quote data: price context for technical setup analysis ──────────────
        if quote and not isinstance(quote, Exception):
            record["fmp_quote"] = quote
            _log("debug", "fmp.quote_resolved",
                 ticker=primary,
                 prev_close=quote.get("prev_close"),
                 week52_high=quote.get("week52_high"),
                 avg_volume=quote.get("avg_volume"))

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
        """Get analyst consensus + FMP composite rating + price target summary.

        Endpoint changes (Aug 2025):
          REMOVED: /stable/grades-latest → 404
          REMOVED: /api/v3/grade/{symbol} → 403 (legacy, blocked)
          ADDED:   /stable/ratings-snapshot — overall FMP letter rating + component scores
        """
        # ── FMP composite ratings snapshot ──────────────────────────────────
        # Returns: rating (A-F letter), overallScore (1-5), and individual
        # component scores for DCF, ROE, ROA, D/E, P/E, P/B (each 1-5).
        ratings_snapshot = await self._fmp.get(
            "/stable/ratings-snapshot",
            symbol=ticker,
        )

        # ── Price target summary (unchanged — still works) ───────────────────
        pt_summary = await self._fmp.get(
            "/stable/price-target-summary",
            symbol=ticker,
        )

        if not ratings_snapshot and not pt_summary:
            return None

        result: dict = {}

        # ── Price targets ────────────────────────────────────────────────────
        if pt_summary and isinstance(pt_summary, list) and pt_summary:
            pt = pt_summary[0]
            result["avg_price_target"]  = pt.get("targetMedian", 0)
            result["high_price_target"] = pt.get("targetHigh", 0)
            result["low_price_target"]  = pt.get("targetLow", 0)
            result["num_analysts"]      = pt.get("numberOfAnalysts", 0)

        # ── FMP composite rating ─────────────────────────────────────────────
        if ratings_snapshot and isinstance(ratings_snapshot, list) and ratings_snapshot:
            snap = ratings_snapshot[0]
            overall_score = int(snap.get("overallScore") or 0)

            result["fmp_rating"]       = snap.get("rating", "")            # e.g. "B"
            result["fmp_overall_score"] = overall_score                    # 1-5
            result["fmp_dcf_score"]    = snap.get("discountedCashFlowScore", 0)
            result["fmp_roe_score"]    = snap.get("returnOnEquityScore", 0)
            result["fmp_roa_score"]    = snap.get("returnOnAssetsScore", 0)
            result["fmp_de_score"]     = snap.get("debtToEquityScore", 0)
            result["fmp_pe_score"]     = snap.get("priceToEarningsScore", 0)
            result["fmp_pb_score"]     = snap.get("priceToBookScore", 0)

            # Derive analyst_sentiment from overall score for backward compatibility
            # with downstream consumers (signal_aggregator, ai_summarizer, etc.)
            #   5 → +2 (strong buy), 4 → +1 (buy), 3 → 0 (neutral),
            #   2 → -1 (underperform), 1 → -2 (sell)
            result["analyst_sentiment"] = overall_score - 3  # maps 1-5 → -2..+2

        # recent_ratings is no longer populated (grades-latest endpoint removed).
        # Kept as empty list to avoid KeyError in any code using .get("recent_ratings", []).
        result.setdefault("recent_ratings", [])
        # Ensure analyst_sentiment always has a value (backward compat)
        result.setdefault("analyst_sentiment", 0)

        return result

    async def _get_insider(self, ticker: str) -> dict | None:
        """Get insider trading activity from Finnhub (replaces dead FMP endpoint Aug 2025).

        History:
          /stable/insider-trading       → 404 Not Found
          /api/v4/insider-trading       → 403 Forbidden (blocked Aug 2025)
          Finnhub /stock/insider-transactions → active, SEC Form 4 data

        Filters to last 90 days, Purchase (P) and Sale (S) codes only.
        Returns net_sentiment, buy/sell totals, and notable_transactions flag.
        Cached in Redis for 4 hours (insider data changes infrequently).
        """
        finnhub_key = os.environ.get("FINNHUB_API_KEY", "")
        if not finnhub_key:
            return None

        cache_key = f"insider:finnhub:{ticker}"
        try:
            cached = await self._redis.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

        try:
            import httpx as _httpx
            from datetime import timedelta

            async with _httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    "https://finnhub.io/api/v1/stock/insider-transactions",
                    params={"symbol": ticker, "token": finnhub_key},
                )
            if resp.status_code != 200:
                return None

            txns = resp.json().get("data", [])
            if not txns:
                return None

            from datetime import datetime, timezone
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=90)
            ).strftime("%Y-%m-%d")

            buy_value = sell_value = 0.0
            notable = False

            for t in txns:
                code = t.get("transactionCode", "")
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
                "source":               "finnhub",
            }

            try:
                await self._redis.setex(cache_key, 3600 * 4, json.dumps(result))
            except Exception:
                pass

            _log("debug", "fmp_enrichment.insider_resolved",
                 ticker=ticker, sentiment=sentiment,
                 buys_m=round(buy_value / 1e6, 2),
                 sells_m=round(sell_value / 1e6, 2))
            return result

        except Exception as e:
            _log("warning", "fmp_enrichment.insider_error",
                 ticker=ticker, error=str(e))
            return None


    async def _get_quote(self, ticker: str) -> dict | None:
        """
        Fetch intraday quote for technical setup context.
        Returns: prev_close, price, 52w high/low, avg_volume, change_pct, volume
        Single FMP call to /stable/quote
        """
        data = await self._fmp.get("/stable/quote", symbol=ticker)
        if not data or not isinstance(data, list) or not data:
            return None
        q = data[0]
        try:
            prev_close   = float(q.get("previousClose") or 0)
            price        = float(q.get("price") or 0)
            week52_high  = float(q.get("yearHigh") or 0)
            week52_low   = float(q.get("yearLow") or 0)
            avg_volume   = int(q.get("avgVolume") or 0)
            volume       = int(q.get("volume") or 0)
            change_pct   = float(q.get("changesPercentage") or 0)

            # Gap % = (current price - prev close) / prev close * 100
            gap_pct = round(((price - prev_close) / prev_close * 100), 2) if prev_close else 0.0

            # Volume ratio vs average
            vol_ratio = round(volume / avg_volume, 2) if avg_volume else 0.0

            # 52w position: where is price in the 52w range (0=at low, 1=at high)
            week52_range = week52_high - week52_low
            week52_position = round((price - week52_low) / week52_range, 2) if week52_range else 0.5

            return {
                "price":           round(price, 2),
                "prev_close":      round(prev_close, 2),
                "gap_pct":         gap_pct,
                "week52_high":     round(week52_high, 2),
                "week52_low":      round(week52_low, 2),
                "week52_position": week52_position,   # 0.0–1.0
                "avg_volume":      avg_volume,
                "volume":          volume,
                "vol_ratio":       vol_ratio,         # current vol / avg vol
                "change_pct":      round(change_pct, 2),
            }
        except (TypeError, ValueError, ZeroDivisionError):
            return None


def _classify_market_cap(mkt_cap: float) -> str:
    if mkt_cap >= 200e9:  return "mega"
    if mkt_cap >= 10e9:   return "large"
    if mkt_cap >= 2e9:    return "mid"
    if mkt_cap >= 300e6:  return "small"
    return "micro"
