"""
Finnhub Sentiment Enrichment Service.

Consumes: news.fmp_enriched  (or news.enriched if FMP not configured)
Enriches: adds Finnhub NLP sentiment scores as conviction modifier
Emits:    news.sentiment_enriched

Two sentiment sources per article:
  1. Finnhub company news sentiment — per-article bullish/bearish score
  2. Finnhub social sentiment (MSPR) — insider + social combined signal

Conviction adjustment logic:
  - Claude says LONG + Finnhub sentiment bullish → conviction × 1.15 boost
  - Claude says LONG + Finnhub sentiment bearish → conviction × 0.80 penalty
  - Disagreement logged as "sentiment_conflict" — valuable for review

Also polls:
  - /stock/insider-sentiment — MSPR (Monthly Share Purchase Ratio)
    MSPR > 0 = insiders net buying, < 0 = net selling
  - /news-sentiment — aggregate market buzz score

Free tier: all these endpoints available at 60 req/min.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone, timedelta

import httpx
import redis.asyncio as aioredis

from app.pipeline.base_consumer import BaseConsumer, _log

FINNHUB_BASE = "https://finnhub.io/api/v1"

# How much to boost/cut conviction based on sentiment alignment
SENTIMENT_BOOST   = 1.15   # Claude + Finnhub agree = 15% conviction boost
SENTIMENT_PENALTY = 0.80   # Claude + Finnhub disagree = 20% cut
SENTIMENT_MIN_SCORE = 0.55  # Finnhub score threshold to consider "strong" signal


class FinnhubSentimentService(BaseConsumer):
    """
    Adds Finnhub NLP sentiment as a cross-validation layer on top of Claude.
    """

    def __init__(self) -> None:
        self._http: httpx.AsyncClient | None = None
        self._redis: aioredis.Redis | None = None
        self._api_key = os.environ.get("FINNHUB_API_KEY", "")
        super().__init__()

    @property
    def service_name(self) -> str:
        return "finnhub_sentiment"

    @property
    def input_topic(self) -> str:
        # Consume from FMP-enriched if available, else plain enriched
        return os.environ.get("FINNHUB_INPUT_TOPIC", "news.fmp_enriched")

    @property
    def output_topic(self) -> str:
        return "news.sentiment_enriched"

    async def on_start(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=FINNHUB_BASE,
            headers={"X-Finnhub-Token": self._api_key},
            timeout=10.0,
        )
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        _log("info", "finnhub_sentiment.ready",
             api_key_set=bool(self._api_key))

    async def on_stop(self) -> None:
        if self._http:
            await self._http.aclose()
        if self._redis:
            await self._redis.aclose()

    async def process(self, record: dict) -> dict | None:
        tickers = record.get("tickers", [])

        if not tickers or not self._api_key:
            record["finnhub_enriched"] = False
            return record

        primary = tickers[0]

        # Run in parallel
        news_sentiment, insider_sentiment = await asyncio.gather(
            self._get_news_sentiment(primary),
            self._get_insider_sentiment(primary),
            return_exceptions=True,
        )

        # ── News sentiment ─────────────────────────────────────────────────────
        finnhub_bias = None
        finnhub_score = None

        if news_sentiment and not isinstance(news_sentiment, Exception):
            buzz = news_sentiment.get("buzz", {})
            score = news_sentiment.get("companyNewsScore", 0.0)
            bullish_pct = news_sentiment.get("sentiment", {}).get("bullishPercent", 0.5)
            bearish_pct = news_sentiment.get("sentiment", {}).get("bearishPercent", 0.5)

            finnhub_score = score
            if bullish_pct > 0.55:
                finnhub_bias = "bullish"
            elif bearish_pct > 0.55:
                finnhub_bias = "bearish"
            else:
                finnhub_bias = "neutral"

            record["finnhub_news_sentiment"] = {
                "score":       round(score, 3),
                "bias":        finnhub_bias,
                "bullish_pct": round(bullish_pct, 3),
                "bearish_pct": round(bearish_pct, 3),
                "buzz":        buzz.get("buzz", 0),
                "articles_in_week": buzz.get("weeklyAverage", 0),
            }

        # ── Insider sentiment (MSPR) ───────────────────────────────────────────
        if insider_sentiment and not isinstance(insider_sentiment, Exception):
            record["finnhub_insider_mspr"] = insider_sentiment

        # ── Conviction adjustment ──────────────────────────────────────────────
        if finnhub_bias and finnhub_score and finnhub_score > 0.3:
            signal_bias = record.get("signal_bias", "neutral")  # From T2 Claude output
            conviction = record.get("conviction", 0.0)

            alignment = _check_alignment(signal_bias, finnhub_bias)

            if alignment == "agree" and finnhub_score >= SENTIMENT_MIN_SCORE:
                new_conviction = min(conviction * SENTIMENT_BOOST, 1.0)
                record["conviction"] = round(new_conviction, 3)
                record["sentiment_alignment"] = "agree"
                _log("info", "finnhub_sentiment.boost",
                     ticker=primary,
                     old_conviction=conviction,
                     new_conviction=new_conviction,
                     claude_bias=signal_bias,
                     finnhub_bias=finnhub_bias)

            elif alignment == "conflict":
                new_conviction = conviction * SENTIMENT_PENALTY
                record["conviction"] = round(new_conviction, 3)
                record["sentiment_alignment"] = "conflict"
                _log("warning", "finnhub_sentiment.conflict",
                     ticker=primary,
                     old_conviction=conviction,
                     new_conviction=new_conviction,
                     claude_bias=signal_bias,
                     finnhub_bias=finnhub_bias,
                     finnhub_score=finnhub_score)
            else:
                record["sentiment_alignment"] = "neutral"

        record["finnhub_enriched"] = True
        return record

    async def _get_news_sentiment(self, ticker: str) -> dict | None:
        """Get Finnhub company news sentiment score."""
        # Check Redis cache first (TTL 15min)
        cache_key = f"finnhub:sentiment:{ticker}"
        if self._redis:
            cached = await self._redis.get(cache_key)
            if cached:
                return json.loads(cached)

        try:
            resp = await self._http.get(
                "/news-sentiment",
                params={"symbol": ticker},
            )
            if resp.status_code == 200:
                data = resp.json()
                if self._redis:
                    await self._redis.setex(cache_key, 900, json.dumps(data))
                return data
        except Exception as e:
            _log("warning", "finnhub_sentiment.news_error",
                 ticker=ticker, error=str(e))
        return None

    async def _get_insider_sentiment(self, ticker: str) -> dict | None:
        """Get MSPR — Monthly Share Purchase Ratio (insider sentiment)."""
        cache_key = f"finnhub:insider_mspr:{ticker}"
        if self._redis:
            cached = await self._redis.get(cache_key)
            if cached:
                return json.loads(cached)

        try:
            # Last 3 months
            now = datetime.now(timezone.utc)
            from_date = (now - timedelta(days=90)).strftime("%Y-%m-%d")
            to_date = now.strftime("%Y-%m-%d")

            resp = await self._http.get(
                "/stock/insider-sentiment",
                params={"symbol": ticker, "from": from_date, "to": to_date},
            )
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("data", [])
                if not items:
                    return None

                # Average MSPR over recent months
                mspr_values = [
                    item.get("mspr", 0)
                    for item in items
                    if item.get("mspr") is not None
                ]
                avg_mspr = sum(mspr_values) / len(mspr_values) if mspr_values else 0

                result = {
                    "avg_mspr": round(avg_mspr, 2),
                    "bias": "bullish" if avg_mspr > 10 else "bearish" if avg_mspr < -10 else "neutral",
                    "months": len(items),
                }

                if self._redis:
                    await self._redis.setex(cache_key, 3600, json.dumps(result))
                return result

        except Exception as e:
            _log("warning", "finnhub_sentiment.mspr_error",
                 ticker=ticker, error=str(e))
        return None


def _check_alignment(claude_bias: str, finnhub_bias: str) -> str:
    """Check if Claude and Finnhub agree on direction."""
    if finnhub_bias == "neutral":
        return "neutral"

    agree_pairs = {
        ("long", "bullish"),
        ("short", "bearish"),
    }
    conflict_pairs = {
        ("long", "bearish"),
        ("short", "bullish"),
    }

    pair = (claude_bias, finnhub_bias)
    if pair in agree_pairs:
        return "agree"
    if pair in conflict_pairs:
        return "conflict"
    return "neutral"
