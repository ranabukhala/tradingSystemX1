"""
Async Polygon REST client scoped to the Stock Context Classifier.

Provides:
  fetch_daily_bars(ticker, days)  — /v2/aggs .../range/1/day/...
  fetch_hourly_bars(ticker, days) — /v2/aggs .../range/1/hour/...

Rate limiting: asyncio.Semaphore(5) — max 5 concurrent requests.
Retry policy:  up to 3 attempts with exponential back-off on 429 / 5xx.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

POLYGON_BASE = "https://api.polygon.io"
_SEMAPHORE   = asyncio.Semaphore(5)          # global across all PolygonClient instances
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0                          # seconds; doubles each attempt


@dataclass
class OHLCV:
    """Single OHLCV bar returned by Polygon aggregates endpoint."""
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float
    ts_ms:  int      # Unix milliseconds (Polygon "t" field)


class PolygonClient:
    """
    Lightweight async Polygon REST client for bar data.

    Parameters
    ----------
    api_key : Polygon API key.  Defaults to POLYGON_API_KEY env var.
    session : Optionally inject an existing aiohttp.ClientSession.
    """

    def __init__(
        self,
        api_key: str | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._api_key = api_key or os.environ.get("POLYGON_API_KEY", "")
        if not self._api_key:
            logger.warning(
                "PolygonClient: POLYGON_API_KEY is not set — all requests will fail"
            )
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> "PolygonClient":
        if self._owns_session:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._owns_session and self._session:
            await self._session.close()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_daily_bars(self, ticker: str, days: int = 60) -> list[OHLCV]:
        """
        Return up to *days* calendar-days of daily OHLCV bars for *ticker*.

        Uses /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}.
        """
        to_dt   = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=days + 5)   # small buffer for weekends
        return await self._fetch_aggs(ticker, "day", 1, from_dt, to_dt)

    async def fetch_hourly_bars(self, ticker: str, days: int = 5) -> list[OHLCV]:
        """
        Return up to *days* calendar-days of 1-hour OHLCV bars for *ticker*.

        Uses /v2/aggs/ticker/{ticker}/range/1/hour/{from}/{to}.
        """
        to_dt   = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=days + 1)
        return await self._fetch_aggs(ticker, "hour", 1, from_dt, to_dt)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_aggs(
        self,
        ticker:     str,
        timespan:   str,
        multiplier: int,
        from_dt:    datetime,
        to_dt:      datetime,
    ) -> list[OHLCV]:
        """
        Fetch aggregate bars from Polygon with retry logic.

        Retries up to _MAX_RETRIES times on 429 and 5xx responses,
        using exponential back-off.  Returns an empty list on permanent failure.
        """
        from_str = from_dt.strftime("%Y-%m-%d")
        to_str   = to_dt.strftime("%Y-%m-%d")
        url      = (
            f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range"
            f"/{multiplier}/{timespan}/{from_str}/{to_str}"
        )
        params = {
            "apiKey":   self._api_key,
            "adjusted": "true",
            "sort":     "asc",
            "limit":    50000,
        }

        async with _SEMAPHORE:
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    bars = await self._get_json(url, params, ticker, timespan)
                    if bars is not None:
                        return bars
                except _RetryableError as exc:
                    wait = _BACKOFF_BASE * (2 ** (attempt - 1))
                    logger.warning(
                        "polygon_client.retrying",
                        extra={
                            "ticker":    ticker,
                            "timespan":  timespan,
                            "attempt":   attempt,
                            "wait_s":    wait,
                            "reason":    str(exc),
                        },
                    )
                    if attempt < _MAX_RETRIES:
                        await asyncio.sleep(wait)
                except Exception as exc:
                    logger.error(
                        "polygon_client.fetch_error",
                        extra={
                            "ticker":   ticker,
                            "endpoint": url,
                            "error":    str(exc),
                        },
                    )
                    return []

        logger.error(
            "polygon_client.max_retries_exceeded",
            extra={"ticker": ticker, "timespan": timespan, "max": _MAX_RETRIES},
        )
        return []

    async def _get_json(
        self,
        url:    str,
        params: dict,
        ticker: str,
        label:  str,
    ) -> list[OHLCV] | None:
        """
        Perform a single GET request and return parsed OHLCV list.

        Raises _RetryableError on 429 / 5xx.
        Returns None (no data) on 404 or empty results.
        """
        if self._session is None:
            raise RuntimeError("PolygonClient must be used as async context manager")

        async with self._session.get(url, params=params) as resp:
            if resp.status == 429:
                raise _RetryableError(f"Rate limited (429) for {ticker}/{label}")
            if resp.status >= 500:
                raise _RetryableError(
                    f"Server error ({resp.status}) for {ticker}/{label}"
                )
            if resp.status == 404:
                logger.warning(
                    "polygon_client.not_found",
                    extra={"ticker": ticker, "url": url},
                )
                return None
            if resp.status != 200:
                body = await resp.text()
                logger.error(
                    "polygon_client.unexpected_status",
                    extra={
                        "ticker":  ticker,
                        "status":  resp.status,
                        "body":    body[:200],
                    },
                )
                return None

            data = await resp.json()

        results = data.get("results") or []
        if not results:
            logger.debug(
                "polygon_client.no_results",
                extra={"ticker": ticker, "label": label},
            )
            return []

        bars: list[OHLCV] = []
        for r in results:
            try:
                bars.append(OHLCV(
                    open=float(r["o"]),
                    high=float(r["h"]),
                    low=float(r["l"]),
                    close=float(r["c"]),
                    volume=float(r.get("v", 0)),
                    ts_ms=int(r["t"]),
                ))
            except (KeyError, TypeError, ValueError):
                continue   # skip malformed bar, don't crash the batch

        return bars


class _RetryableError(Exception):
    """Raised internally to signal that a request should be retried."""
