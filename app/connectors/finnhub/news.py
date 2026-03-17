"""
Finnhub News Connector (REST polling fallback).

Polls /news and /company-news every 30s for breaking news.
Also enriches each article with Finnhub's pre-computed NLP sentiment score.

Finnhub free tier: 60 req/min — very generous.
Sentiment score: -1.0 (bearish) to +1.0 (bullish), pre-computed by Finnhub NLP.

This connector runs alongside Benzinga/Polygon as an additional news source.
Deduplicator downstream handles cross-source duplicates automatically.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import random
from datetime import datetime, timezone, timedelta

import httpx
import redis.asyncio as aioredis

from app.config import settings
from app.connectors.base import BaseConnector, _log
from app.connectors.finnhub_rate_limiter import try_acquire, CACHE_TTL
from app.kafka import get_producer
from app.models.news import NewsSource, RawNewsRecord

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Map Finnhub categories to our catalyst types
CATEGORY_MAP = {
    "earnings":         "earnings",
    "ipo":              "other",
    "mergers":          "ma",
    "acquisitions":     "ma",
    "analyst":          "analyst",
    "upgrade":          "analyst",
    "downgrade":        "analyst",
    "fda":              "regulatory",
    "clinical":         "regulatory",
    "macro":            "macro",
    "economy":          "macro",
    "federal reserve":  "macro",
    "sec":              "filing",
    "filing":           "filing",
}

# High-priority tickers to always poll company-specific news for
WATCHLIST_TICKERS = [
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "TSLA",
    "AMD", "COIN", "MSTR", "PLTR", "SOFI", "HOOD", "RIVN",
    "SPY", "QQQ", "IWM",
]


class FinnhubNewsConnector(BaseConnector):
    """
    REST polling connector for Finnhub general + company-specific news.
    Attaches Finnhub's NLP sentiment score to every article.
    """

    def __init__(self) -> None:
        self._seen_ids: set[str] = set()
        self._http: httpx.AsyncClient | None = None
        self._api_key = settings.finnhub_api_key
        super().__init__()

    @property
    def source_name(self) -> str:
        return "finnhub"

    @property
    def poll_interval_seconds(self) -> int:
        return settings.finnhub_poll_interval

    def validate_config(self) -> None:
        if not self._api_key:
            _log("warning", "finnhub.no_api_key",
                 msg="FINNHUB_API_KEY not set — connector disabled")

    def _get_http(self) -> httpx.AsyncClient:
        if not self._http:
            self._http = httpx.AsyncClient(
                base_url=FINNHUB_BASE,
                headers={"X-Finnhub-Token": self._api_key},
                timeout=15.0,
            )
        return self._http

    async def fetch(self) -> int:
        if not self._api_key:
            return 0

        # Startup jitter: spread first fetch randomly across 0–8 s to avoid
        # coincident bursts with finnhub_fundamentals and finnhub_sentiment at
        # minute boundaries. Subsequent fetches are unaffected (jitter only on
        # first call via the flag set in BaseConnector / on container start).
        if not getattr(self, "_jitter_done", False):
            self._jitter_done = True
            await asyncio.sleep(random.uniform(0, 8))

        http = self._get_http()
        producer = get_producer()
        emitted = 0

        redis_conn = await aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
        )

        # General news disabled - geopolitical noise

        # ── 2. Company-specific news for watchlist (sequential) ───────────────
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            for ticker in WATCHLIST_TICKERS:
                try:
                    cache_key = f"finnhub:news:{ticker}:{today}"

                    # 1. Cache check — skip API call entirely on hit
                    cached = await redis_conn.get(cache_key)
                    if cached is not None:
                        _log("info", "finnhub.cache.hit", ticker=ticker)
                        articles = json.loads(cached)
                        emitted += await self._process_articles(articles, producer, ticker=ticker)
                        await asyncio.sleep(0.35)
                        continue

                    _log("debug", "finnhub.cache.miss", ticker=ticker)

                    # 2. Per-minute rate limit check before API call
                    allowed, count = await try_acquire(
                        redis_conn, settings.finnhub_per_minute_call_limit
                    )
                    if not allowed:
                        _log("warning", "finnhub.rate_limit.per_minute_cap_reached",
                             ticker=ticker, count=count,
                             limit=settings.finnhub_per_minute_call_limit)
                        break  # Skip remaining tickers for this cycle

                    # 3. API call
                    resp = await http.get("/company-news", params={
                        "symbol": ticker,
                        "from": yesterday,
                        "to": today,
                    })
                    if resp.status_code == 200:
                        articles = resp.json() or []
                        await redis_conn.setex(
                            cache_key, CACHE_TTL["news"], json.dumps(articles)
                        )
                        emitted += await self._process_articles(articles, producer, ticker=ticker)
                    else:
                        _log("warning", "finnhub.company_news_http_error",
                             ticker=ticker, status=resp.status_code)
                    await asyncio.sleep(0.35)  # 350ms: ~2 calls/s burst cap, 34 calls/min

                except Exception as e:
                    _log("error", "finnhub.company_news_error",
                         ticker=ticker, error=str(e) or repr(e),
                         error_type=type(e).__name__)
        finally:
            await redis_conn.aclose()

        return emitted

    async def _process_articles(
        self,
        articles: list,
        producer,
        ticker: str | None = None,
    ) -> int:
        emitted = 0
        # Limit seen_ids set size
        if len(self._seen_ids) > 10000:
            self._seen_ids = set(list(self._seen_ids)[-5000:])

        for article in articles:
            article_id = str(article.get("id", ""))
            if not article_id:
                # Fallback: hash of headline + datetime
                article_id = hashlib.md5(
                    f"{article.get('headline','')}{article.get('datetime','')}".encode()
                ).hexdigest()

            if article_id in self._seen_ids:
                continue
            self._seen_ids.add(article_id)

            try:
                record = self._parse_article(article, article_id, ticker)
                producer.produce(
                    topic="news.raw",
                    value=record.to_kafka_dict(),
                    key=record.vendor_id,
                )
                emitted += 1
            except Exception as e:
                _log("error", "finnhub.parse_error",
                     article_id=article_id, error=str(e))

        return emitted

    def _parse_article(
        self, article: dict, article_id: str, hint_ticker: str | None
    ) -> RawNewsRecord:
        # Timestamp — Finnhub uses Unix epoch int
        ts = article.get("datetime", 0)
        published_at = (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            if ts else datetime.now(timezone.utc)
        )

        # Tickers — Finnhub embeds 'related' as comma-separated string
        raw_tickers: list[str] = []
        if related := article.get("related", ""):
            raw_tickers = [t.strip().upper() for t in related.split(",") if t.strip()]
        if hint_ticker and hint_ticker not in raw_tickers:
            raw_tickers.insert(0, hint_ticker)

        # Category → catalyst type
        category = (article.get("category") or "").lower()
        mapped = CATEGORY_MAP.get(category, "other")
        raw_categories = [mapped] if mapped != "other" else []

        # Sentiment score — attach as extra field in raw_payload
        # Ranges -1.0 to +1.0, computed by Finnhub NLP
        sentiment = article.get("sentiment", {})

        return RawNewsRecord(
            source=NewsSource.FINNHUB,   # We'll extend NewsSource below
            vendor_id=f"finnhub_{article_id}",
            published_at=published_at,
            url=article.get("url", ""),
            title=article.get("headline", "").strip(),
            snippet=(article.get("summary") or "")[:500],
            author=article.get("source", ""),
            raw_tickers=raw_tickers,
            raw_categories=raw_categories,
            raw_payload={
                **article,
                "_finnhub_sentiment": sentiment,
                "_source": "finnhub",
            },
        )
