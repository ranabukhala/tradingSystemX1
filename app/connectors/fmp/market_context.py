"""
FMP Market Context Connector.

Provides two data streams:

1. Technical Indicators (per ticker, on-demand via Redis)
   - RSI, MACD, EMA, SMA, Bollinger Bands
   - Stored in Redis keyed by ticker for AI summarizer to pull

2. Sector Performance (market-wide, every 15 min)
   - Sector returns for today
   - Used by regime detection and conviction scoring

These aren't Kafka consumers — they're background workers that
pre-populate Redis with context data that other services read.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import redis.asyncio as aioredis

from app.config import settings
from app.connectors.fmp.client import FMPClient
from app.connectors.base import BaseConnector, _log


class FMPTechnicalConnector(BaseConnector):
    """
    Polls technical indicators for all active signal tickers.
    Writes to Redis: fmp:technical:{ticker} with TTL 5min.
    AI summarizer reads this to add RSI/MACD context to prompts.
    """

    @property
    def source_name(self) -> str:
        return "fmp_technical"

    @property
    def poll_interval_seconds(self) -> int:
        return 300  # Every 5 minutes

    def validate_config(self) -> None:
        pass

    async def fetch(self) -> int:
        if not settings.fmp_api_key:
            return 0

        redis_conn = await aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
        )
        fmp = FMPClient(api_key=settings.fmp_api_key, redis=redis_conn, plan=settings.fmp_plan)

        # Get active signal tickers from Redis (written by signal aggregator)
        active_tickers_raw = await redis_conn.smembers("active_signal_tickers")
        tickers = list(active_tickers_raw) if active_tickers_raw else []

        if not tickers:
            await fmp.close()
            return 0

        updated = 0
        for ticker in tickers[:20]:  # Cap at 20 tickers to preserve free plan quota
            tech = await self._fetch_technicals(fmp, ticker)
            if tech:
                await redis_conn.setex(
                    f"fmp:technical:{ticker}",
                    300,  # 5min TTL
                    json.dumps(tech),
                )
                updated += 1
            await asyncio.sleep(0.2)  # Rate limiting

        _log("info", "fmp_technical.updated",
             tickers=updated,
             requests_remaining=fmp.daily_requests_remaining)

        await fmp.close()
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
    Polls sector performance every 15 minutes.
    Writes to Redis: fmp:sectors with TTL 15min.
    Used by regime detection and conviction scoring.
    """

    @property
    def source_name(self) -> str:
        return "fmp_sectors"

    @property
    def poll_interval_seconds(self) -> int:
        return 900  # Every 15 minutes

    def validate_config(self) -> None:
        pass

    async def fetch(self) -> int:
        if not settings.fmp_api_key:
            return 0

        redis_conn = await aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
        )
        fmp = FMPClient(api_key=settings.fmp_api_key, redis=redis_conn)

        # Stable endpoint requires a date parameter — use today
        from datetime import date as _date
        today_str = _date.today().isoformat()
        data = await fmp.get("/stable/sector-performance-snapshot", date=today_str)

        if not data or not isinstance(data, list):
            await fmp.close()
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

        await redis_conn.setex("fmp:sectors", 900, json.dumps(payload))

        _log("info", "fmp_sectors.updated",
             regime=regime,
             avg_change=avg_change,
             sectors=len(sectors))

        await fmp.close()
        return len(sectors)


def _rsi_signal(rsi: float) -> str:
    if rsi >= 75:  return "overbought_extreme"
    if rsi >= 65:  return "overbought"
    if rsi <= 25:  return "oversold_extreme"
    if rsi <= 35:  return "oversold"
    return "neutral"