"""
Background scheduler for the Stock Context Classifier.

Responsibilities
----------------
- Read the watchlist from PostgreSQL `watchlist` table (or WATCHLIST_TICKERS env var).
- Run a full context refresh at 09:00 ET every trading day.
- Run a full context refresh every 4 hours during market hours (09:30–16:00 ET).
- Run an immediate full refresh on service startup.
- Write results to Redis and to the `stock_context_log` PostgreSQL table.

Concurrency
-----------
Up to 10 tickers are processed simultaneously (asyncio.Semaphore(10)).
A full 200-ticker watchlist should complete within 60 seconds.

Error handling
--------------
Polygon failures: log ERROR, skip ticker, continue batch.
Redis failures:   log ERROR, continue (cache write more critical than DB).
DB failures:      log WARNING (DB log less critical than cache).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from . import redis_cache
from .classifier import classify_ticker
from .models import StockContext
from .polygon_client import PolygonClient

logger = logging.getLogger(__name__)

_BATCH_CONCURRENCY = 10
_DB_WRITE_TIMEOUT  = 10   # seconds


class ContextScheduler:
    """
    Wrapper around APScheduler that runs the watchlist refresh loop.

    Parameters
    ----------
    db_pool : asyncpg connection pool (passed in from main.py).
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        self._pool    = db_pool
        self._sched   = AsyncIOScheduler(timezone="America/New_York")
        self._last_refresh: datetime | None = None
        self._polygon = PolygonClient()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the scheduler and run an immediate full refresh."""
        # 09:00 ET daily (pre-market warm-up)
        self._sched.add_job(
            self._full_refresh,
            CronTrigger(hour=9, minute=0, timezone="America/New_York"),
            id="daily_0900",
            replace_existing=True,
        )
        # Every 4 hours during market hours
        self._sched.add_job(
            self._full_refresh,
            CronTrigger(hour="9-16", minute=30, timezone="America/New_York"),
            id="intraday_4h",
            replace_existing=True,
        )

        self._sched.start()
        logger.info("scheduler.started")

        # Immediate startup refresh (non-blocking — don't delay HTTP server startup)
        asyncio.create_task(self._full_refresh(), name="startup_refresh")

    async def stop(self) -> None:
        """Gracefully shut down the scheduler."""
        self._sched.shutdown(wait=False)
        logger.info("scheduler.stopped")

    @property
    def last_refresh(self) -> datetime | None:
        return self._last_refresh

    # ------------------------------------------------------------------
    # Refresh logic
    # ------------------------------------------------------------------

    async def refresh_ticker(self, ticker: str) -> StockContext | None:
        """
        Classify a single ticker and write results to Redis + DB.

        Returns the StockContext on success, or None on failure.
        """
        ticker = ticker.upper().strip()
        try:
            async with self._polygon as client:
                daily_bars  = await client.fetch_daily_bars(ticker, days=60)
                hourly_bars = await client.fetch_hourly_bars(ticker, days=5)
        except Exception as exc:
            logger.error(
                "scheduler.fetch_error",
                extra={"ticker": ticker, "error": str(exc)},
            )
            return None

        if not daily_bars:
            logger.warning(
                "scheduler.no_bars", extra={"ticker": ticker}
            )
            return None

        ctx = await classify_ticker(ticker, daily_bars, hourly_bars)

        # Write to Redis (critical path — errors logged but don't fail)
        try:
            await redis_cache.set_context(ticker, ctx)
        except Exception as exc:
            logger.error(
                "scheduler.redis_write_error",
                extra={"ticker": ticker, "error": str(exc)},
            )

        # Write to PostgreSQL (best-effort — warn but don't fail)
        await self._write_to_db(ctx)

        logger.info(
            "scheduler.ticker_refreshed",
            extra={
                "ticker":    ticker,
                "trend":     ctx.trend_regime.value,
                "vol":       ctx.volatility_regime.value,
                "clean":     ctx.cleanliness.value,
                "threshold": ctx.adjusted_threshold,
            },
        )
        return ctx

    async def _full_refresh(self) -> None:
        """
        Refresh context for every ticker in the watchlist.

        Processes tickers in concurrent batches of _BATCH_CONCURRENCY.
        Logs a summary (success / failure counts) after completion.
        """
        tickers = await self._get_watchlist()
        if not tickers:
            logger.warning("scheduler.empty_watchlist")
            return

        logger.info("scheduler.refresh_start", extra={"count": len(tickers)})
        start_ts = datetime.now(timezone.utc)

        sem       = asyncio.Semaphore(_BATCH_CONCURRENCY)
        success   = 0
        failure   = 0

        async def _bounded(t: str) -> None:
            nonlocal success, failure
            async with sem:
                result = await self.refresh_ticker(t)
                if result is not None:
                    success += 1
                else:
                    failure += 1

        await asyncio.gather(*[_bounded(t) for t in tickers])

        elapsed = (datetime.now(timezone.utc) - start_ts).total_seconds()
        self._last_refresh = datetime.now(timezone.utc)

        logger.info(
            "scheduler.refresh_complete",
            extra={
                "total":   len(tickers),
                "success": success,
                "failure": failure,
                "elapsed_s": round(elapsed, 1),
            },
        )

    # ------------------------------------------------------------------
    # Watchlist source
    # ------------------------------------------------------------------

    async def _get_watchlist(self) -> list[str]:
        """
        Return the list of tickers to classify.

        Sources (in priority order):
        1. PostgreSQL `watchlist` table (column: ticker)
        2. WATCHLIST_TICKERS environment variable (comma-separated)
        3. config/watchlist.txt file (one ticker per line)
        """
        # 1. Database watchlist table
        if self._pool:
            try:
                rows = await self._pool.fetch(
                    "SELECT ticker FROM watchlist WHERE active IS DISTINCT FROM FALSE"
                )
                tickers = [r["ticker"].upper() for r in rows if r["ticker"]]
                if tickers:
                    logger.debug(
                        "scheduler.watchlist_from_db", extra={"count": len(tickers)}
                    )
                    return tickers
            except asyncpg.UndefinedTableError:
                logger.debug("scheduler.watchlist_table_missing")
            except Exception as exc:
                logger.warning(
                    "scheduler.watchlist_db_error", extra={"error": str(exc)}
                )

        # 2. Environment variable
        env_tickers = os.environ.get("WATCHLIST_TICKERS", "")
        if env_tickers.strip():
            tickers = [t.strip().upper() for t in env_tickers.split(",") if t.strip()]
            if tickers:
                logger.debug(
                    "scheduler.watchlist_from_env", extra={"count": len(tickers)}
                )
                return tickers

        # 3. config/watchlist.txt
        watchlist_path = os.path.join(
            os.path.dirname(__file__), "..", "config", "watchlist.txt"
        )
        try:
            with open(watchlist_path) as f:
                tickers = [
                    line.strip().upper()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]
            if tickers:
                logger.debug(
                    "scheduler.watchlist_from_file", extra={"count": len(tickers)}
                )
                return tickers
        except FileNotFoundError:
            pass

        logger.error("scheduler.watchlist_unavailable")
        return []

    # ------------------------------------------------------------------
    # Database writes
    # ------------------------------------------------------------------

    async def _write_to_db(self, ctx: StockContext) -> None:
        """Insert a row into stock_context_log.  Logs WARNING on failure."""
        if not self._pool:
            return
        try:
            async with asyncio.wait_for(
                self._pool.acquire(), timeout=_DB_WRITE_TIMEOUT
            ) as conn:
                await conn.execute(
                    """
                    INSERT INTO stock_context_log (
                        ticker, evaluated_at,
                        trend_regime, volatility_regime, cleanliness,
                        adx, ma_slope_20, bb_width_trend, atr_ratio,
                        adjusted_threshold, raw_metrics
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    """,
                    ctx.ticker,
                    ctx.calculated_at,
                    ctx.trend_regime.value,
                    ctx.volatility_regime.value,
                    ctx.cleanliness.value,
                    ctx.adx,
                    ctx.ma_slope_20,
                    ctx.bb_width_trend,
                    ctx.atr_ratio,
                    ctx.adjusted_threshold,
                    json.dumps(ctx.raw_metrics),
                )
        except asyncio.TimeoutError:
            logger.warning(
                "scheduler.db_timeout", extra={"ticker": ctx.ticker}
            )
        except Exception as exc:
            logger.warning(
                "scheduler.db_write_error",
                extra={"ticker": ctx.ticker, "error": str(exc)},
            )
