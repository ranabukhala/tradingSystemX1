"""
Polygon.io News Connector
─────────────────────────────────────────────
Polls Polygon /v2/reference/news for coverage breadth.
Lower priority than Benzinga — runs every 60s.
Deduplication handled downstream.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import aiohttp

from app.config import settings
from app.connectors.base import BaseConnector
from app.kafka import get_producer
from app.logging import get_logger
from app.models.news import NewsSource, RawNewsRecord

log = get_logger(__name__)

POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"


class PolygonNewsConnector(BaseConnector):

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._last_published: str | None = None
        super().__init__()

    @property
    def source_name(self) -> str:
        return "polygon_news"

    @property
    def poll_interval_seconds(self) -> int:
        return settings.polygon_news_poll_interval_seconds

    def validate_config(self) -> None:
        if not settings.polygon_api_key:
            raise ValueError("POLYGON_API_KEY is not set in .env")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def fetch(self) -> int:
        session = await self._get_session()
        producer = get_producer()

        published_gte = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        params = {
            "apiKey": settings.polygon_api_key,
            "limit": 100,
            "sort": "published_utc",
            "order": "desc",
            "published_utc.gte": published_gte,
        }

        try:
            async with session.get(POLYGON_NEWS_URL, params=params) as resp:
                if resp.status == 429:
                    log.warning("polygon_news.rate_limited")
                    return 0
                if resp.status != 200:
                    raise ConnectionError(f"Polygon returned HTTP {resp.status}")
                data = await resp.json()
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Polygon HTTP error: {e}") from e

        results = data.get("results", [])
        emitted = 0

        for article in results:
            try:
                record = self._parse_article(article)
                producer.produce(
                    topic=settings.topic_news_raw,
                    value=record.to_kafka_dict(),
                    key=record.vendor_id,
                )
                emitted += 1
            except Exception as e:
                log.error("polygon_news.parse_error", error=str(e))
                producer.produce_to_dlq(
                    original_topic=settings.topic_news_raw,
                    value=article,
                    error=str(e),
                )

        return emitted

    def _parse_article(self, article: dict) -> RawNewsRecord:
        published_str = article.get("published_utc", "")
        try:
            published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
        except ValueError:
            published_at = datetime.now(timezone.utc)

        raw_tickers = article.get("tickers", [])

        return RawNewsRecord(
            source=NewsSource.POLYGON,
            vendor_id=article.get("id", ""),
            published_at=published_at,
            url=article.get("article_url", ""),
            title=article.get("title", "").strip(),
            snippet=article.get("description", "")[:500],
            author=article.get("author", ""),
            raw_tickers=[t.upper() for t in raw_tickers],
            raw_categories=article.get("keywords", []),
            raw_payload=article,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
