"""
Polygon.io Price Connector
─────────────────────────────────────────────
WebSocket for real-time 1-minute OHLCV aggregates.
REST backfill for historical bars.
Emits PriceBar to prices.bars Kafka topic.

WebSocket protocol: wss://socket.polygon.io/stocks
Subscribes to A.* (aggregate per minute) for watchlist tickers.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, date, timedelta
from typing import AsyncGenerator

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from app.config import settings
from app.kafka import get_producer
from app.logging import get_logger
from app.models.price_bar import PriceBar

log = get_logger(__name__)

POLYGON_WS_URL = "wss://socket.polygon.io/stocks"
POLYGON_REST_URL = "https://api.polygon.io"


def load_watchlist() -> list[str]:
    """Load ticker watchlist from config/watchlist.txt"""
    try:
        with open("config/watchlist.txt") as f:
            return [line.strip().upper() for line in f if line.strip() and not line.startswith("#")]
    except FileNotFoundError:
        # Default watchlist if file not found
        log.warning("prices.watchlist_not_found", using="defaults")
        return ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "SPY", "QQQ", "IWM"]


class PolygonPriceConnector:
    """
    Manages WebSocket connection to Polygon for real-time price bars.
    Separate from BaseConnector since it uses WebSocket, not polling.
    """

    def __init__(self) -> None:
        if not settings.polygon_api_key:
            raise ValueError("POLYGON_API_KEY is not set in .env")

        self._watchlist = load_watchlist()
        self._producer = get_producer()
        self._running = False
        log.info("prices.connector.initialized", tickers=len(self._watchlist))

    async def run(self) -> None:
        """Main WebSocket loop with auto-reconnect."""
        self._running = True
        log.info("prices.websocket.starting")

        while self._running:
            try:
                await self._connect_and_stream()
            except ConnectionClosed as e:
                log.warning("prices.websocket.disconnected", reason=str(e))
                if self._running:
                    await asyncio.sleep(5)  # Brief pause before reconnect
            except Exception as e:
                log.error("prices.websocket.error", error=str(e), exc_info=True)
                if self._running:
                    await asyncio.sleep(10)

    async def _connect_and_stream(self) -> None:
        """Connect, authenticate, subscribe, and stream."""
        async with websockets.connect(
            POLYGON_WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            # Step 1: Wait for connected message
            msg = json.loads(await ws.recv())
            if not any(m.get("status") == "connected" for m in (msg if isinstance(msg, list) else [msg])):
                raise ConnectionError("Polygon WS: expected 'connected' status")

            # Step 2: Authenticate
            await ws.send(json.dumps({"action": "auth", "params": settings.polygon_api_key}))
            auth_resp = json.loads(await ws.recv())
            if not any(m.get("status") == "auth_success" for m in (auth_resp if isinstance(auth_resp, list) else [auth_resp])):
                raise ConnectionError("Polygon WS: authentication failed")
            log.info("prices.websocket.authenticated")

            # Step 3: Subscribe to minute aggregates for all watchlist tickers
            # Subscribe in batches of 100 to avoid message size limits
            for i in range(0, len(self._watchlist), 100):
                batch = self._watchlist[i:i+100]
                subs = ",".join(f"A.{ticker}" for ticker in batch)
                await ws.send(json.dumps({"action": "subscribe", "params": subs}))
                await asyncio.sleep(0.1)

            log.info("prices.websocket.subscribed", tickers=len(self._watchlist))

            # Step 4: Stream
            while self._running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    messages = json.loads(raw)
                    if not isinstance(messages, list):
                        messages = [messages]

                    for msg in messages:
                        if msg.get("ev") == "A":  # Aggregate event
                            self._handle_aggregate(msg)

                except asyncio.TimeoutError:
                    # Send a ping to keep connection alive
                    await ws.ping()

    def _handle_aggregate(self, msg: dict) -> None:
        """Parse a Polygon aggregate message and emit to Kafka."""
        try:
            bar = PriceBar(
                ticker=msg["sym"],
                timestamp=datetime.fromtimestamp(msg["s"] / 1000, tz=timezone.utc),
                timeframe="1min",
                open=msg["o"],
                high=msg["h"],
                low=msg["l"],
                close=msg["c"],
                volume=msg["v"],
                vwap=msg.get("vw"),
                transactions=msg.get("z"),
            )
            self._producer.produce(
                topic=settings.topic_prices_bars,
                value=bar.to_kafka_dict(),
                key=bar.ticker,
            )
        except (KeyError, TypeError) as e:
            log.error("prices.parse_error", error=str(e), msg=msg)

    async def backfill(
        self,
        ticker: str,
        from_date: date,
        to_date: date | None = None,
        timeframe: str = "minute",
        multiplier: int = 1,
    ) -> int:
        """
        Backfill historical bars via REST for a single ticker.
        Returns number of bars emitted.
        """
        to_date = to_date or date.today()
        url = f"{POLYGON_REST_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timeframe}/{from_date}/{to_date}"

        params = {
            "apiKey": settings.polygon_api_key,
            "limit": 50000,
            "sort": "asc",
            "adjusted": "true",
        }

        emitted = 0
        async with aiohttp.ClientSession() as session:
            while url:
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(60)
                        continue
                    if resp.status != 200:
                        log.error("prices.backfill.error", ticker=ticker, status=resp.status)
                        break
                    data = await resp.json()

                results = data.get("results", [])
                for r in results:
                    bar = PriceBar(
                        ticker=ticker,
                        timestamp=datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc),
                        timeframe=f"{multiplier}{timeframe[0]}",
                        open=r["o"], high=r["h"], low=r["l"], close=r["c"],
                        volume=r["v"], vwap=r.get("vw"), transactions=r.get("n"),
                    )
                    self._producer.produce(
                        topic=settings.topic_prices_bars,
                        value=bar.to_kafka_dict(),
                        key=bar.ticker,
                    )
                    emitted += 1

                # Pagination
                url = data.get("next_url")
                params = {"apiKey": settings.polygon_api_key}  # next_url has params baked in

        log.info("prices.backfill.complete", ticker=ticker, bars=emitted)
        return emitted

    def stop(self) -> None:
        self._running = False
