"""
Alpha Vantage News & Sentiment Connector
─────────────────────────────────────────
Polls /query?function=NEWS_SENTIMENT for financial news with built-in
sentiment scores and ticker-level relevance.

Rate limits:
  Free tier : 25 req/day  → use ALPHAVANTAGE_POLL_INTERVAL_SECONDS=3600
  Premium   : unlimited   → default 300s (5 min) is fine
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone, timedelta

import aiohttp

from app.config import settings
from app.connectors.base import BaseConnector
from app.kafka import get_producer
from app.logging import get_logger
from app.models.news import NewsSource, RawNewsRecord

log = get_logger(__name__)

ALPHAVANTAGE_NEWS_URL = "https://www.alphavantage.co/query"


class AlphaVantageConnector(BaseConnector):

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        # Track seen URLs to avoid re-emitting the same article
        self._seen_urls: set[str] = set()
        super().__init__()

    @property
    def source_name(self) -> str:
        return "alphavantage"

    @property
    def poll_interval_seconds(self) -> int:
        return settings.alphavantage_poll_interval_seconds

    def validate_config(self) -> None:
        if not settings.alphavantage_api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY is not set in .env")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20)
            )
        return self._session

    async def fetch(self) -> int:
        session = await self._get_session()
        producer = get_producer()

        # Rolling 10-minute window — AV format: YYYYMMDDTHHMM
        time_from = (
            datetime.now(timezone.utc) - timedelta(minutes=10)
        ).strftime("%Y%m%dT%H%M")

        params = {
            "function": "NEWS_SENTIMENT",
            "apikey": settings.alphavantage_api_key,
            "limit": 50,
            "sort": "LATEST",
            "time_from": time_from,
        }

        try:
            async with session.get(ALPHAVANTAGE_NEWS_URL, params=params) as resp:
                if resp.status == 429:
                    log.warning("alphavantage.rate_limited")
                    return 0
                if resp.status != 200:
                    raise ConnectionError(f"AlphaVantage returned HTTP {resp.status}")
                data = await resp.json(content_type=None)
        except aiohttp.ClientError as e:
            raise ConnectionError(f"AlphaVantage HTTP error: {e}") from e

        # AV signals quota exhaustion or errors via 200 JSON body
        if "Information" in data or "Note" in data:
            msg = data.get("Information") or data.get("Note", "")
            log.warning("alphavantage.api_notice", message=msg)
            return 0

        feed = data.get("feed", [])
        emitted = 0

        for article in feed:
            url = article.get("url", "")
            if not url or url in self._seen_urls:
                continue

            try:
                record = self._parse_article(article)
                producer.produce(
                    topic=settings.topic_news_raw,
                    value=record.to_kafka_dict(),
                    key=record.vendor_id,
                )
                self._seen_urls.add(url)
                emitted += 1
            except Exception as e:
                log.error("alphavantage.parse_error", error=str(e))
                producer.produce_to_dlq(
                    original_topic=settings.topic_news_raw,
                    value=article,
                    error=str(e),
                )

        # Bound the seen-URL cache to avoid unbounded memory growth
        if len(self._seen_urls) > 5_000:
            self._seen_urls = set(list(self._seen_urls)[-2_500:])

        if emitted > 0:
            log.info("alphavantage.emitted", count=emitted)

        return emitted

    def _parse_article(self, article: dict) -> RawNewsRecord:
        time_str = article.get("time_published", "")
        try:
            # AV format: YYYYMMDDTHHMMSS
            published_at = datetime.strptime(time_str, "%Y%m%dT%H%M%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            published_at = datetime.now(timezone.utc)

        url = article.get("url", "")
        # AV has no per-article ID — use MD5 of the URL as a stable vendor_id
        vendor_id = hashlib.md5(url.encode()).hexdigest()

        raw_tickers = [
            ts["ticker"].upper()
            for ts in article.get("ticker_sentiment", [])
            if ts.get("ticker")
        ]

        raw_categories = [
            t["topic"]
            for t in article.get("topics", [])
            if t.get("topic")
        ]

        authors = article.get("authors", [])
        author = authors[0] if authors else article.get("source", "")

        return RawNewsRecord(
            source=NewsSource.ALPHAVANTAGE,
            vendor_id=vendor_id,
            published_at=published_at,
            url=url,
            title=article.get("title", "").strip(),
            snippet=article.get("summary", "")[:500],
            author=author,
            raw_tickers=raw_tickers,
            raw_categories=raw_categories,
            raw_payload=article,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
