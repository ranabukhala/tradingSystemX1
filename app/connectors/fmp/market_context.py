"""
FMP Market Context Connector.

Provides two data streams:

1. Technical Indicators (per ticker, on-demand via Redis)
   - RSI, MACD, EMA, SMA, Bollinger Bands
   - Stored in Redis keyed by ticker for AI summarizer to pull

2. Sector Performance (market-wide, every 60 min during market hours)
   - Sector returns for today
   - Used by regime detection and conviction scoring

These aren't Kafka consumers — they're background workers that
pre-populate Redis with context data that other services read.
"""
from __future__ import annotations

import asyncio
import json
import zoneinfo
from datetime import datetime, timezone

import redis.asyncio as aioredis

from app.config import settings
from app.connectors.fmp.client import FMPClient
from app.connectors.base import BaseConnector, _log

_ET = zoneinfo.ZoneInfo("America/New_York")


class FMPTechnicalConnector(BaseConnector):
    """
    Polls technical indicators for all active signal tickers.
    Writes to Redis: fmp:technical:{ticker} with TTL 24 h.
    AI summarizer reads this to add RSI/MACD context to prompts.
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._fmp: FMPClient | None = None
        super().__init__()

    @property
    def source_name(self) -> str:
        return "fmp_technical"

    @property
    def poll_interval_seconds(self) -> int:
        return 43_200  # Every 12 hours
        # Math: 20 tickers × 2 endpoints = 40 calls/cycle.
        # 43,200 s → 2 cycles/day → 80 calls/day within the shared 200-call budget.

    def validate_config(self) -> None:
        pass

    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = await aioredis.from_url(
                settings.redis_url,
                decode_responses=True,
                max_connections=3,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        return self._redis

    def _get_fmp(self, redis) -> FMPClient:
        if self._fmp is None:
            self._fmp = FMPClient(
                api_key=settings.fmp_api_key,
                redis=redis,
                plan=settings.fmp_plan,
            )
        return self._fmp

    @staticmethod
    def _is_market_day() -> bool:
        """True if today is a weekday (ignores holidays — good enough)."""
        return datetime.now(_ET).weekday() < 5

    async def fetch(self) -> int:
        if not settings.fmp_api_key:
            return 0

        # Only fetch technicals on market days — data is stale overnight/weekends
        if not self._is_market_day():
            _log("debug", "fmp_technical.skipped_weekend",
                 reason="weekend — technical indicators unchanged")
            return 0

        redis_conn = await self._get_redis()
        fmp = self._get_fmp(redis_conn)

        # Get active signal tickers from Redis (written by signal aggregator)
        active_tickers_raw = await redis_conn.smembers("active_signal_tickers")
        tickers = list(active_tickers_raw) if active_tickers_raw else []

        if not tickers:
            return 0

        updated = 0
        for ticker in tickers[:20]:  # Cap at 20 tickers to preserve free plan quota
            tech = await self._fetch_technicals(fmp, ticker)
            if tech:
                await redis_conn.setex(
                    f"fmp:technical:{ticker}",
                    86_400,  # 24 h TTL — outlasts the 12 h poll interval
                    json.dumps(tech),
                )
                updated += 1
            await asyncio.sleep(0.2)  # Rate limiting

        _log("info", "fmp_technical.updated",
             tickers=updated,
             requests_remaining=fmp.daily_requests_remaining)

        return updated

    async def _fetch_technicals(self, fmp: FMPClient, ticker: str) -> dict | None:
        # RSI (14-period daily) — requires FMP paid plan (402 on free/starter)
        # If 402 is returned, FMPClient logs fmp.http_error and returns None gracefully.
        # In that case we skip RSI — Polygon RSI in technicals.py covers the pre-trade filter.
        rsi_data = await fmp.get(
            "/stable/technical-indicators/rsi",
            symbol=ticker,
            periodLength=14,
            timeframe="1day",
        )

        # MACD — same plan requirement as RSI
        macd_data = await fmp.get(
            "/stable/technical-indicators/macd",
            symbol=ticker,
            timeframe="1day",
        )

        if not rsi_data and not macd_data:
            return None

        result: dict = {"ticker": ticker, "updated_at": datetime.now(timezone.utc).isoformat()}

        if rsi_data and isinstance(rsi_data, list) and rsi_data:
            # New endpoint returns: {"date": ..., "value": ...}
            result["rsi"] = round(float(rsi_data[0].get("value", 0)), 1)
            result["rsi_signal"] = _rsi_signal(result["rsi"])

        if macd_data and isinstance(macd_data, list) and macd_data:
            macd = macd_data[0]
            # New endpoint returns: {"date": ..., "value": ..., "signal": ..., "histogram": ...}
            result["macd"]        = round(float(macd.get("value", 0)), 4)
            result["macd_signal"] = round(float(macd.get("signal", 0)), 4)
            result["macd_hist"]   = round(float(macd.get("histogram", 0)), 4)
            result["macd_bias"]   = "bullish" if result["macd"] > result["macd_signal"] else "bearish"

        return result


class FMPSectorConnector(BaseConnector):
    """
    Polls sector performance every 60 minutes during market hours.
    Writes to Redis: fmp:sectors with TTL 1 h.
    Used by regime detection and conviction scoring.

    Market-hours guard: skips fetches outside 09:30–16:00 ET Mon–Fri so
    overnight/weekend polls (which return identical data) don't burn quota.
    Math: ~6.5 market hours × 1 call/hour ≈ 7 calls/day (down from 48).
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._fmp: FMPClient | None = None
        super().__init__()

    @property
    def source_name(self) -> str:
        return "fmp_sectors"

    @property
    def poll_interval_seconds(self) -> int:
        return 3_600  # Every 60 minutes (market hours only via _is_market_hours guard)

    def validate_config(self) -> None:
        pass

    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = await aioredis.from_url(
                settings.redis_url,
                decode_responses=True,
                max_connections=3,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        return self._redis

    def _get_fmp(self, redis) -> FMPClient:
        if self._fmp is None:
            self._fmp = FMPClient(
                api_key=settings.fmp_api_key,
                redis=redis,
                plan=settings.fmp_plan,
            )
        return self._fmp

    @staticmethod
    def _is_market_hours() -> bool:
        """True if current ET time is within regular market hours Mon–Fri."""
        now = datetime.now(_ET)
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        t = (now.hour, now.minute)
        return (9, 30) <= t < (16, 0)

    @staticmethod
    def _is_market_day() -> bool:
        """True if today is a weekday (ignores holidays — good enough)."""
        return datetime.now(_ET).weekday() < 5

    async def fetch(self) -> int:
        if not settings.fmp_api_key:
            return 0

        # Skip sector fetches outside market hours — data is static overnight
        # and on weekends. Reduces 48 calls/day → ~7 (market hours only).
        if not self._is_market_hours():
            _log("debug", "fmp_sectors.skipped_outside_hours",
                 reason="market closed — sector data unchanged")
            return 0

        redis_conn = await self._get_redis()
        fmp = self._get_fmp(redis_conn)

        # Stable endpoint requires a date parameter — use today
        from datetime import date as _date
        today_str = _date.today().isoformat()
        data = await fmp.get("/stable/sector-performance-snapshot", date=today_str)

        if not data or not isinstance(data, list):
            return 0

        # Build sector map
        # New stable response: [{sector, averageChange, exchange, date}, ...]
        # Legacy response used changesPercentage (string with % sign) — now a float
        sectors = {}
        for item in data:
            sector = item.get("sector", "")
            if not sector:
                continue
            # averageChange is a float in new endpoint; handle both formats defensively
            raw = item.get("averageChange", item.get("changesPercentage", 0))
            try:
                change_pct = float(str(raw).replace("%", ""))
            except (ValueError, TypeError):
                change_pct = 0.0
            sectors[sector] = change_pct

        # Determine overall market regime from sectors
        positive = sum(1 for v in sectors.values() if v > 0)
        negative = sum(1 for v in sectors.values() if v < 0)
        avg_change = sum(sectors.values()) / len(sectors) if sectors else 0

        if avg_change > 0.5 and positive > negative:
            regime = "risk_on"
        elif avg_change < -0.5 and negative > positive:
            regime = "risk_off"
        elif max(abs(v) for v in sectors.values()) > 2.0:
            regime = "high_vol"
        else:
            regime = "compression"

        payload = {
            "sectors": sectors,
            "regime": regime,
            "avg_change": round(avg_change, 3),
            "positive_sectors": positive,
            "negative_sectors": negative,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        await redis_conn.setex("fmp:sectors", 3_600, json.dumps(payload))  # 1 h TTL

        _log("info", "fmp_sectors.updated",
             regime=regime,
             avg_change=avg_change,
             sectors=len(sectors))

        return len(sectors)


def _rsi_signal(rsi: float) -> str:
    if rsi >= 75:  return "overbought_extreme"
    if rsi >= 65:  return "overbought"
    if rsi <= 25:  return "oversold_extreme"
    if rsi <= 35:  return "oversold"
    return "neutral"