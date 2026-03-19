"""
Finlight Real-Time News WebSocket Connector.

Connects to wss://wss.finlight.me via the finlight-client library.
Receives articles with pre-computed sentiment + entity extraction,
normalizes to RawNewsRecord, and produces to news.raw Kafka topic.

Key advantages over Finnhub WebSocket:
  - Pre-computed sentiment (positive/neutral/negative + confidence 0-1)
  - Company entities with ticker, exchange, sector, industry, confidence
  - Sub-second latency
  - finlight-client handles auto-reconnect on the 2-hour AWS API Gateway limit
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone

from app.connectors.base import _log
from app.kafka import get_producer
from app.models.news import NewsSource, RawNewsRecord

# Redis TTL for cached sentiment (1 hour — fresh overwrites stale)
_SENTIMENT_TTL_SECONDS = 3_600

# Map Finlight categories to pipeline catalyst types
_CATEGORY_MAP = {
    "earnings":    "earnings",
    "analyst":     "analyst",
    "mergers":     "ma",
    "acquisitions": "ma",
    "fda":         "regulatory",
    "regulatory":  "regulatory",
    "economy":     "macro",
    "macro":       "macro",
    "sec":         "filing",
    "filing":      "filing",
    "legal":       "legal",
}

# Finlight sentiment → pipeline bias
_SENTIMENT_BIAS_MAP = {
    "positive": "bullish",
    "negative": "bearish",
    "neutral":  "neutral",
}


class FinlightConnector:
    """
    Long-lived WebSocket connector for Finlight news.

    Not a polling connector — uses event-driven WebSocket callbacks.
    Has run() / stop() interface matching FinnhubWebSocketConnector.
    """

    def __init__(self) -> None:
        # Prefer dedicated WS key; fall back to general API key
        self._api_key: str = os.environ.get("FINLIGHT_WS_KEY", "") or os.environ.get("FINLIGHT_API_KEY", "")
        self._language: str = os.environ.get("FINLIGHT_LANGUAGE", "en")
        self._countries: str = os.environ.get("FINLIGHT_COUNTRIES", "US")
        self._query: str = os.environ.get("FINLIGHT_QUERY", "")
        self._tickers_env: str = os.environ.get("FINLIGHT_TICKERS", "")
        self._use_known_tickers: bool = (
            os.environ.get("FINLIGHT_USE_KNOWN_TICKERS", "true").lower() == "true"
        )
        self._redis_url: str = os.environ.get("REDIS_URL", "redis://redis:6379/0")
        self._enable_redis_sentiment: bool = (
            os.environ.get("ENABLE_REDIS_SENTIMENT", "true").lower() == "true"
        )
        self._running = False
        self._reconnect_delay = 5
        self._max_reconnect_delay = 300
        self._redis = None

        # Stats
        self._received = 0
        self._produced = 0
        self._errors = 0

    async def run(self) -> None:
        if not self._api_key:
            _log("warning", "finlight_ws.no_api_key",
                 msg="FINLIGHT_API_KEY not set — connector disabled")
            return

        self._running = True
        _log("info", "finlight_ws.starting",
             language=self._language,
             countries=self._countries,
             query=self._query or "(none)")

        # Lazy Redis connection for sentiment caching
        if self._enable_redis_sentiment:
            try:
                import redis.asyncio as aioredis
                self._redis = await aioredis.from_url(
                    self._redis_url,
                    decode_responses=True,
                    max_connections=3,
                )
                _log("info", "finlight_ws.redis_connected")
            except Exception as e:
                _log("warning", "finlight_ws.redis_unavailable", error=str(e))
                self._redis = None

        # Heartbeat task
        _hb_task = asyncio.create_task(self._heartbeat_loop(), name="finlight-heartbeat")

        # Outer reconnect loop — finlight-client handles reconnect internally, but if
        # connect() ever raises or returns, we restart after a delay.
        while self._running:
            try:
                await self._connect_and_stream()
                self._reconnect_delay = 5  # clean disconnect → reset
            except Exception as e:
                _log("error", "finlight_ws.connection_error",
                     error=str(e),
                     reconnect_in=self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._max_reconnect_delay,
                )

        _hb_task.cancel()
        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass

        _log("info", "finlight_ws.stopped",
             received=self._received,
             produced=self._produced,
             errors=self._errors)

    def stop(self) -> None:
        self._running = False
        _log("info", "finlight_ws.stop_requested")

    def _resolve_tickers(self) -> list[str]:
        """Resolve ticker list for WebSocket subscription filter."""
        # 1. Explicit env var takes priority
        if self._tickers_env:
            tickers = [t.strip() for t in self._tickers_env.split(",") if t.strip()]
            _log("info", "finlight_ws.ticker_filter",
                 source="env", count=len(tickers))
            return tickers

        # 2. Use shared known-ticker universe
        if self._use_known_tickers:
            from app.known_tickers import KNOWN_TICKERS
            tickers = sorted(KNOWN_TICKERS)
            _log("info", "finlight_ws.ticker_filter",
                 source="known_tickers", count=len(tickers))
            return tickers

        # 3. Wildcard fallback (not recommended — high volume + cost)
        _log("warning", "finlight_ws.ticker_filter",
             source="wildcard", count=0,
             msg="Using wildcard — consider enabling FINLIGHT_USE_KNOWN_TICKERS")
        return ["*"]

    async def _connect_and_stream(self) -> None:
        from finlight_client import ApiConfig, FinlightApi, WebSocketOptions
        from finlight_client.models import GetArticlesWebSocketParams

        config = ApiConfig(api_key=self._api_key)
        client = FinlightApi(
            config,
            websocket_options=WebSocketOptions(takeover=True),
        )

        tickers = self._resolve_tickers()

        params = GetArticlesWebSocketParams(
            language=self._language,
            includeEntities=True,
        )
        if tickers != ["*"]:
            params.tickers = tickers
        if self._query:
            params.query = self._query
        if self._countries:
            params.countries = [c.strip() for c in self._countries.split(",")]

        _log("info", "finlight_ws.connecting",
             language=self._language,
             countries=self._countries,
             ticker_count=len(tickers))

        # finlight-client handles the 2-hour AWS API Gateway reconnect internally
        await client.websocket.connect(
            request_payload=params,
            on_article=self._on_article_sync,
        )

    def _on_article_sync(self, article) -> None:
        """Sync callback from finlight-client — schedule async processing."""
        asyncio.ensure_future(self._process_article(article))

    async def _process_article(self, article) -> None:
        self._received += 1
        try:
            record = self._normalize(article)
            producer = get_producer()
            producer.produce(
                topic="news.raw",
                value=record.to_kafka_dict(),
                key=record.vendor_id,
            )
            self._produced += 1

            sentiment_raw = record.raw_payload.get("_finlight_sentiment", {})
            _log("info", "finlight_ws.article_received",
                 vendor_id=record.vendor_id,
                 tickers=record.raw_tickers[:5],
                 sentiment=sentiment_raw.get("sentiment"),
                 confidence=sentiment_raw.get("confidence"),
                 title=record.title[:70])

            # Cache per-ticker sentiment in Redis for T2 context
            if self._redis and record.raw_tickers:
                sentiment_cache = self._extract_sentiment_cache(article)
                if sentiment_cache:
                    payload = json.dumps(sentiment_cache)
                    for ticker in record.raw_tickers[:3]:
                        try:
                            await self._redis.setex(
                                f"finlight:sentiment:{ticker}",
                                _SENTIMENT_TTL_SECONDS,
                                payload,
                            )
                        except Exception as e:
                            _log("debug", "finlight_ws.redis_cache_error",
                                 ticker=ticker, error=str(e))

        except Exception as e:
            self._errors += 1
            _log("error", "finlight_ws.process_error",
                 error=str(e),
                 title=getattr(article, "title", "unknown")[:60])

    def _normalize(self, article) -> RawNewsRecord:
        """Convert Finlight article object → RawNewsRecord."""
        # Tickers from Finlight entity extraction
        raw_tickers: list[str] = []
        companies_data: list[dict] = []
        if hasattr(article, "companies") and article.companies:
            for c in article.companies:
                ticker = getattr(c, "ticker", None)
                if ticker and isinstance(ticker, str):
                    raw_tickers.append(ticker.upper())
                companies_data.append({
                    "ticker": ticker,
                    "name": getattr(c, "name", None),
                    "sector": getattr(c, "sector", None),
                    "industry": getattr(c, "industry", None),
                    "exchange": getattr(c, "exchange", None),
                    "confidence": getattr(c, "confidence", None),
                    "country": getattr(c, "country", None),
                })

        # Categories → catalyst hints
        raw_categories: list[str] = []
        if hasattr(article, "categories") and article.categories:
            for cat in article.categories:
                cat_lower = str(cat).lower()
                mapped = _CATEGORY_MAP.get(cat_lower)
                if mapped and mapped not in raw_categories:
                    raw_categories.append(mapped)
            # If no mapping found, keep raw categories for entity resolver
            if not raw_categories:
                raw_categories = [str(c).lower() for c in article.categories[:3]]

        # Published timestamp
        published_at: datetime
        raw_pub = getattr(article, "publishDate", None)
        if isinstance(raw_pub, datetime):
            published_at = raw_pub if raw_pub.tzinfo else raw_pub.replace(tzinfo=timezone.utc)
        elif isinstance(raw_pub, str) and raw_pub:
            try:
                published_at = datetime.fromisoformat(raw_pub.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                published_at = datetime.now(timezone.utc)
        else:
            published_at = datetime.now(timezone.utc)

        # Stable vendor_id from URL hash
        link = getattr(article, "link", "") or ""
        link_hash = hashlib.md5(link.encode()).hexdigest()[:12]
        vendor_id = f"finlight_{link_hash}"

        sentiment = getattr(article, "sentiment", None)
        confidence = getattr(article, "confidence", None)

        return RawNewsRecord(
            source=NewsSource.FINLIGHT,
            vendor_id=vendor_id,
            published_at=published_at,
            url=link,
            title=(getattr(article, "title", "") or "").strip(),
            snippet=(getattr(article, "summary", "") or "")[:500].strip(),
            author=(getattr(article, "source", "") or "").strip(),
            raw_tickers=raw_tickers,
            raw_categories=raw_categories,
            raw_payload={
                "_source": "finlight",
                "_realtime": True,
                "_finlight_sentiment": {
                    "sentiment": sentiment,
                    "confidence": float(confidence) if confidence is not None else None,
                },
                "companies": companies_data,
                "categories": raw_categories,
                "language": getattr(article, "language", None),
            },
        )

    @staticmethod
    def _extract_sentiment_cache(article) -> dict | None:
        """Return sentiment dict for Redis caching, or None if unavailable."""
        sentiment = getattr(article, "sentiment", None)
        confidence = getattr(article, "confidence", None)
        if not sentiment:
            return None
        return {
            "bias": _SENTIMENT_BIAS_MAP.get(str(sentiment).lower(), "neutral"),
            "score": float(confidence) if confidence is not None else 0.0,
            "source": "finlight",
        }

    async def _heartbeat_loop(self) -> None:
        """Write Redis heartbeat every 30s so the watchdog can monitor this service."""
        try:
            import redis.asyncio as aioredis
            r = await aioredis.from_url(self._redis_url, decode_responses=True,
                                        max_connections=2, socket_connect_timeout=2)
            key = "service:heartbeat:finlight_connector"
            while self._running:
                try:
                    await r.setex(key, 90, "1")
                except Exception:
                    pass
                await asyncio.sleep(30)
            await r.delete(key)
            await r.aclose()
        except Exception:
            # Heartbeat is best-effort — never crash the main loop
            while self._running:
                await asyncio.sleep(30)
