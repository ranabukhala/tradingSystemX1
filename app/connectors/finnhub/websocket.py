"""
Finnhub WebSocket News Connector.

Streams real-time news via Finnhub WebSocket API.
Latency: sub-second vs 30s polling.
Critical for premarket catalysts where speed = edge.

Protocol:
  - Connect to wss://ws.finnhub.io?token=YOUR_KEY
  - Subscribe to news: {"type": "subscribe-news", "symbol": "AAPL"}
  - Subscribe to general: {"type": "subscribe-news", "symbol": "*"}
  - Messages arrive as JSON with type="news"

Free tier: supports news streaming.
Reconnects automatically on disconnect with exponential backoff.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from app.connectors.base import _log
from app.kafka import get_producer
from app.models.news import NewsSource, RawNewsRecord

FINNHUB_WS_URL = "wss://ws.finnhub.io"

# Symbols to subscribe for real-time news
# "*" = all general news, individual tickers = company-specific
WS_SUBSCRIPTIONS = [
    "*",           # All general market news
    "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "TSLA",
    "AMD", "COIN", "MSTR", "PLTR", "SOFI", "SPY", "QQQ",
]

CATEGORY_MAP = {
    "earnings": "earnings",
    "ipo": "other",
    "mergers": "ma",
    "analyst": "analyst",
    "fda": "regulatory",
    "macro": "macro",
    "economy": "macro",
    "sec": "filing",
}


class FinnhubWebSocketConnector:
    """
    Real-time WebSocket news stream from Finnhub.
    Runs as a long-lived async task, not a polling connector.
    """

    def __init__(self) -> None:
        from app.config import settings
        self._api_key = settings.finnhub_api_key
        self._running = False
        self._seen_ids: set[str] = set()
        self._reconnect_delay = 5   # seconds, doubles on each failure
        self._max_reconnect_delay = 300

    async def run(self) -> None:
        if not self._api_key:
            _log("warning", "finnhub_ws.no_api_key",
                 msg="FINNHUB_API_KEY not set — WebSocket connector disabled")
            return

        self._running = True
        _log("info", "finnhub_ws.starting")

        while self._running:
            try:
                await self._connect_and_stream()
                self._reconnect_delay = 5  # Reset on clean disconnect
            except Exception as e:
                _log("error", "finnhub_ws.connection_error",
                     error=str(e),
                     reconnect_in=self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._max_reconnect_delay
                )

    def stop(self) -> None:
        self._running = False

    async def _connect_and_stream(self) -> None:
        url = f"{FINNHUB_WS_URL}?token={self._api_key}"

        async with websockets.connect(
            url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            _log("info", "finnhub_ws.connected")

            # Subscribe to all symbols
            for symbol in WS_SUBSCRIPTIONS:
                await ws.send(json.dumps({
                    "type": "subscribe-news",
                    "symbol": symbol,
                }))
                await asyncio.sleep(0.05)  # Small delay between subs

            _log("info", "finnhub_ws.subscribed",
                 symbols=len(WS_SUBSCRIPTIONS))

            producer = get_producer()

            async for raw_msg in ws:
                if not self._running:
                    break

                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("type", "")

                if msg_type == "news":
                    data = msg.get("data", [])
                    if isinstance(data, list):
                        for item in data:
                            await self._handle_news_item(item, producer)
                    elif isinstance(data, dict):
                        await self._handle_news_item(data, producer)

                elif msg_type == "error":
                    _log("error", "finnhub_ws.server_error",
                         msg=msg.get("msg", ""))

                elif msg_type == "ping":
                    await ws.send(json.dumps({"type": "pong"}))

    async def _handle_news_item(self, item: dict, producer) -> None:
        # Dedup
        article_id = str(item.get("id", ""))
        if not article_id:
            article_id = hashlib.md5(
                f"{item.get('headline','')}{item.get('datetime','')}".encode()
            ).hexdigest()

        if article_id in self._seen_ids:
            return

        # Prune seen set
        if len(self._seen_ids) > 10000:
            self._seen_ids = set(list(self._seen_ids)[-5000:])
        self._seen_ids.add(article_id)

        try:
            record = self._parse_item(item, article_id)

            producer.produce(
                topic="news.raw",
                value=record.to_kafka_dict(),
                key=record.vendor_id,
            )

            _log("info", "finnhub_ws.news_received",
                 ticker=record.raw_tickers[:3] if record.raw_tickers else [],
                 title=record.title[:60],
                 sentiment=item.get("sentiment", {}).get("bullishPercent"))

        except Exception as e:
            _log("error", "finnhub_ws.parse_error",
                 article_id=article_id, error=str(e))

    def _parse_item(self, item: dict, article_id: str) -> RawNewsRecord:
        ts = item.get("datetime", 0)
        published_at = (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            if ts else datetime.now(timezone.utc)
        )

        raw_tickers: list[str] = []
        if related := item.get("related", ""):
            raw_tickers = [t.strip().upper() for t in related.split(",") if t.strip()]

        category = (item.get("category") or "").lower()
        mapped = CATEGORY_MAP.get(category, "other")
        raw_categories = [mapped] if mapped != "other" else []

        return RawNewsRecord(
            source=NewsSource.UNKNOWN,
            vendor_id=f"finnhub_ws_{article_id}",
            published_at=published_at,
            url=item.get("url", ""),
            title=item.get("headline", "").strip(),
            snippet=(item.get("summary") or "")[:500],
            author=item.get("source", ""),
            raw_tickers=raw_tickers,
            raw_categories=raw_categories,
            raw_payload={
                **item,
                "_finnhub_sentiment": item.get("sentiment", {}),
                "_source": "finnhub_ws",
                "_realtime": True,
            },
        )
