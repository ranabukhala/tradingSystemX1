"""
Benzinga News Connector
Polls /v2/news every 30s and emits RawNewsRecord to news.raw.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta

import aiohttp

from app.config import settings
from app.connectors.base import BaseConnector
from app.kafka import get_producer
from app.logging import get_logger
from app.models.news import NewsSource, RawNewsRecord

log = get_logger(__name__)

BENZINGA_NEWS_URL = "https://api.benzinga.com/api/v2/news"

CATEGORY_MAP = {
    "earnings": "earnings",
    "analyst-ratings": "analyst",
    "m-a": "ma",
    "fda": "regulatory",
    "economics": "macro",
    "sec": "filing",
}


class BenzingaConnector(BaseConnector):

    def __init__(self) -> None:
        self._last_seen_id: str | None = None
        self._session: aiohttp.ClientSession | None = None
        super().__init__()

    @property
    def source_name(self) -> str:
        return "benzinga"

    @property
    def poll_interval_seconds(self) -> int:
        return settings.benzinga_poll_interval_seconds

    def validate_config(self) -> None:
        if not settings.benzinga_api_key:
            raise ValueError("BENZINGA_API_KEY is not set in .env")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._session

    async def fetch(self) -> int:
        session = await self._get_session()
        producer = get_producer()

        # Benzinga updatedSince must be a Unix timestamp integer (not ISO string)
        updated_since_unix = int(
            (datetime.now(timezone.utc) - timedelta(minutes=2)).timestamp()
        )

        params = {
            "token": settings.benzinga_api_key,
            "pageSize": 100,
            "displayOutput": "full",
            "updatedSince": updated_since_unix,
            "sort": "created:desc",
        }

        try:
            async with session.get(BENZINGA_NEWS_URL, params=params) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    log.warning("benzinga.rate_limited", retry_after=retry_after)
                    await asyncio.sleep(retry_after)
                    return 0

                if resp.status != 200:
                    body = await resp.text()
                    log.error("benzinga.bad_response", status=resp.status, body=body)
                    raise ConnectionError(f"Benzinga returned HTTP {resp.status}")

                data = await resp.json()

        except aiohttp.ClientError as e:
            raise ConnectionError(f"Benzinga HTTP error: {e}") from e

        articles = data if isinstance(data, list) else data.get("result", [])

        if not articles:
            return 0

        new_items = []
        for article in articles:
            article_id = str(article.get("id", ""))
            if article_id == self._last_seen_id:
                break
            new_items.append(article)

        if new_items:
            self._last_seen_id = str(new_items[0].get("id", ""))

        emitted = 0
        for article in new_items:
            try:
                record = self._parse_article(article)
                producer.produce(
                    topic=settings.topic_news_raw,
                    value=record.to_kafka_dict(),
                    key=record.vendor_id,
                )
                emitted += 1
            except Exception as e:
                log.error("benzinga.parse_error", article_id=article.get("id"), error=str(e))
                producer.produce_to_dlq(
                    original_topic=settings.topic_news_raw,
                    value=article,
                    error=str(e),
                )

        if emitted > 0:
            log.info("benzinga.emitted", count=emitted)

        return emitted

    def _parse_article(self, article: dict) -> RawNewsRecord:
        published_raw = article.get("created") or article.get("date", "")
        if isinstance(published_raw, (int, float)):
            published_at = datetime.fromtimestamp(published_raw, tz=timezone.utc)
        else:
            try:
                published_at = datetime.fromisoformat(
                    str(published_raw).replace("Z", "+00:00")
                )
                if published_at.tzinfo is None:
                    published_at = published_at.replace(tzinfo=timezone.utc)
            except ValueError:
                published_at = datetime.now(timezone.utc)

        raw_tickers = []
        for stock in article.get("stocks", []):
            if ticker := stock.get("name", "").strip().upper():
                raw_tickers.append(ticker)

        raw_categories = []
        for channel in article.get("channels", []):
            cat = channel.get("name", "").lower()
            raw_categories.append(CATEGORY_MAP.get(cat, cat))

        return RawNewsRecord(
            source=NewsSource.BENZINGA,
            vendor_id=str(article.get("id", "")),
            published_at=published_at,
            url=article.get("url", ""),
            title=article.get("title", "").strip(),
            snippet=article.get("teaser") or article.get("body", "")[:500],
            author=article.get("author", ""),
            raw_tickers=raw_tickers,
            raw_categories=raw_categories,
            raw_payload=article,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
