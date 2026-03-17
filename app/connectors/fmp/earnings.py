"""
FMP Earnings Calendar Connector.

Polls FMP earnings calendar daily and syncs to Postgres events table.
Covers 3,000+ companies vs EarningsWhispers' 500.

Provides:
  - EPS estimates (consensus + whisper)
  - Revenue estimates
  - BMO/AMC timing (cross-validated with Finnhub)
  - Prior EPS for beat/miss context

Polls: once at startup, then every 6 hours.
After each FMP sync, a Finnhub cross-validation pass fills in any Unknown
event_time values and flags/corrects date mismatches.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, date, timezone, timedelta

import httpx
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.connectors.fmp.client import FMPClient
from app.connectors.base import BaseConnector, _log


class FMPEarningsConnector(BaseConnector):

    @property
    def source_name(self) -> str:
        return "fmp_earnings"

    @property
    def poll_interval_seconds(self) -> int:
        return 3600 * 6  # Every 6 hours

    def validate_config(self) -> None:
        if not settings.fmp_api_key:
            _log("warning", "fmp_earnings.no_key",
                 msg="FMP_API_KEY not set — earnings calendar disabled")

    async def fetch(self) -> int:
        if not settings.fmp_api_key:
            return 0

        redis_conn = await aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
        )
        fmp = FMPClient(api_key=settings.fmp_api_key, redis=redis_conn)

        # Fetch next 60 days of earnings
        today = date.today()
        end_date = today + timedelta(days=60)

        data = await fmp.get(
            "/stable/earnings-calendar",
            from_=today.isoformat(),
            to=end_date.isoformat(),
        )

        if not data or not isinstance(data, list):
            _log("warning", "fmp_earnings.no_data")
            await fmp.close()
            return 0

        # Save to Postgres
        from app.db import get_engine
        engine = get_engine()
        saved = 0

        async with AsyncSession(engine) as session:
            for item in data:
                ticker = item.get("symbol", "")
                if not ticker:
                    continue

                event_date_str = item.get("date", "")
                if not event_date_str:
                    continue

                try:
                    event_date = datetime.fromisoformat(event_date_str)
                except Exception:
                    continue

                # Determine timing
                timing = (item.get("time") or item.get("reportTime") or "").strip().lower()
                if timing in ("bmo", "before market open", "pre-market", "pre market"):
                    event_time = "BMO"
                elif timing in ("amc", "after market close", "post-market", "post market", "after market"):
                    event_time = "AMC"
                elif timing in ("dmh", "during market hours"):
                    event_time = "DMH"
                else:
                    event_time = "Unknown"
                    if timing:
                        _log("debug", "fmp_earnings.unknown_timing",
                             ticker=ticker, raw_time=timing)

                eps_estimate    = item.get("epsEstimated")
                revenue_estimate = item.get("revenueEstimated")
                eps_actual      = item.get("eps")
                revenue_actual  = item.get("revenue")

                try:
                    await session.execute(text("""
                        INSERT INTO event (
                            id, event_type, ticker, event_date, event_time,
                            consensus_eps, consensus_revenue,
                            actual_eps, actual_revenue,
                            created_at, updated_at
                        ) VALUES (
                            gen_random_uuid(), 'earnings', :ticker, :event_date, :event_time,
                            :eps_est, :rev_est,
                            :eps_actual, :rev_actual,
                            now(), now()
                        )
                        ON CONFLICT (event_type, ticker, event_date)
                        DO UPDATE SET
                            event_time       = EXCLUDED.event_time,
                            consensus_eps    = EXCLUDED.consensus_eps,
                            consensus_revenue = EXCLUDED.consensus_revenue,
                            actual_eps       = COALESCE(EXCLUDED.actual_eps, event.actual_eps),
                            actual_revenue   = COALESCE(EXCLUDED.actual_revenue, event.actual_revenue),
                            updated_at       = now()
                    """), {
                        "ticker":     ticker,
                        "event_date": event_date,
                        "event_time": event_time,
                        "eps_est":    eps_estimate,
                        "rev_est":    revenue_estimate,
                        "eps_actual": eps_actual,
                        "rev_actual": revenue_actual,
                    })
                    saved += 1
                except Exception as e:
                    _log("warning", "fmp_earnings.insert_error",
                         ticker=ticker, error=str(e))

            await session.commit()

        _log("info", "fmp_earnings.synced",
             total=len(data), saved=saved,
             from_date=today.isoformat(),
             to_date=end_date.isoformat())

        await fmp.close()

        # Cross-validate timing with Finnhub after every FMP sync
        await self._cross_validate_finnhub()

        return saved

    async def _cross_validate_finnhub(self) -> None:
        """
        Fill in missing event_time (bmo/amc) and correct date mismatches using
        Finnhub's /api/v1/calendar/earnings endpoint.

        Only processes rows with event_time = 'Unknown' in the next 14 days.
        Never overwrites existing non-Unknown timing — purely additive.
        """
        finnhub_key = os.environ.get("FINNHUB_API_KEY", "")
        if not finnhub_key:
            _log("warning", "fmp_earnings.finnhub_xval_skipped",
                 reason="No FINNHUB_API_KEY")
            return

        from app.db import get_engine
        engine = get_engine()

        # 1. Fetch all Unknown-timing events in the next 14 days
        async with AsyncSession(engine) as session:
            rows = (await session.execute(text("""
                SELECT id, ticker, event_date
                FROM event
                WHERE event_type  = 'earnings'
                  AND event_time  = 'Unknown'
                  AND event_date >= CURRENT_DATE
                  AND event_date <= CURRENT_DATE + INTERVAL '14 days'
                ORDER BY event_date
            """))).fetchall()

        if not rows:
            _log("info", "fmp_earnings.finnhub_xval_skip",
                 reason="No Unknown timing events in next 14 days")
            return

        _log("info", "fmp_earnings.finnhub_xval_start", count=len(rows))
        updated = 0
        date_mismatches = 0

        async with httpx.AsyncClient(timeout=10.0) as http:
            for row in rows:
                ticker   = row.ticker
                fmp_date = row.event_date  # datetime.date object

                # Conservative: 1 req/sec → ≤60/min, won't crowd the shared
                # Finnhub budget used by news polling containers
                await asyncio.sleep(1.0)

                try:
                    # Search a ±7-day window around the FMP date to catch shifts
                    from_date = (fmp_date - timedelta(days=7)).strftime("%Y-%m-%d")
                    to_date   = (fmp_date + timedelta(days=7)).strftime("%Y-%m-%d")

                    resp = await http.get(
                        "https://finnhub.io/api/v1/calendar/earnings",
                        params={
                            "symbol": ticker,
                            "token":  finnhub_key,
                            "from":   from_date,
                            "to":     to_date,
                        },
                    )

                    if resp.status_code == 429:
                        _log("warning", "fmp_earnings.finnhub_xval_rate_limited",
                             ticker=ticker)
                        await asyncio.sleep(30)
                        continue

                    if resp.status_code != 200:
                        continue

                    earnings = resp.json().get("earningsCalendar", [])
                    if not earnings:
                        continue

                    # Use the first (most relevant) entry
                    entry          = earnings[0]
                    finnhub_hour   = (entry.get("hour") or "").lower()   # "bmo" or "amc"
                    finnhub_date_s = entry.get("date", "")               # "YYYY-MM-DD"

                    if finnhub_hour not in ("bmo", "amc"):
                        continue  # Finnhub has no usable timing — leave as Unknown

                    fmp_date_s = fmp_date.strftime("%Y-%m-%d") if hasattr(fmp_date, "strftime") else str(fmp_date)[:10]

                    date_changed = (finnhub_date_s and finnhub_date_s != fmp_date_s)
                    if date_changed:
                        date_mismatches += 1
                        _log("warning", "fmp_earnings.date_mismatch",
                             ticker=ticker,
                             fmp_date=fmp_date_s,
                             finnhub_date=finnhub_date_s)

                    # Parse Finnhub date string → date object (asyncpg requires it)
                    finnhub_date_obj = (
                        datetime.strptime(finnhub_date_s, "%Y-%m-%d").date()
                        if finnhub_date_s else fmp_date
                    )

                    # Write back: update event_time (and event_date if it shifted)
                    async with AsyncSession(engine) as session:
                        if date_changed:
                            await session.execute(text("""
                                UPDATE event SET
                                    event_time = :hour,
                                    event_date = :new_date,
                                    updated_at = now()
                                WHERE id = :event_id
                            """), {
                                "hour":     finnhub_hour,
                                "new_date": finnhub_date_obj,
                                "event_id": str(row.id),
                            })
                        else:
                            await session.execute(text("""
                                UPDATE event SET
                                    event_time = :hour,
                                    updated_at = now()
                                WHERE id = :event_id
                            """), {
                                "hour":     finnhub_hour,
                                "event_id": str(row.id),
                            })
                        await session.commit()

                    updated += 1

                except Exception as e:
                    _log("warning", "fmp_earnings.finnhub_xval_error",
                         ticker=ticker, error=str(e))

        _log("info", "fmp_earnings.finnhub_xval_complete",
             total=len(rows),
             updated=updated,
             date_mismatches=date_mismatches)
