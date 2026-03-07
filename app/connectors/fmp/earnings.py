"""
FMP Earnings Calendar Connector.

Polls FMP earnings calendar daily and syncs to Postgres events table.
Covers 3,000+ companies vs EarningsWhispers' 500.

Provides:
  - EPS estimates (consensus + whisper)
  - Revenue estimates
  - BMO/AMC timing
  - Prior EPS for beat/miss context

Polls: once at startup, then every 6 hours.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, date, timezone, timedelta

import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
        if not os.environ.get("FMP_API_KEY"):
            _log("warning", "fmp_earnings.no_key",
                 msg="FMP_API_KEY not set — earnings calendar disabled")

    async def fetch(self) -> int:
        api_key = os.environ.get("FMP_API_KEY", "")
        if not api_key:
            return 0

        redis_conn = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        fmp = FMPClient(api_key=api_key, redis=redis_conn)

        # Fetch next 30 days of earnings
        today = date.today()
        end_date = today + timedelta(days=30)

        data = await fmp.get(
            "/stable/earning-calendar",
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
                timing = item.get("time", "")
                if timing in ("bmo", "before market open", "BMO"):
                    event_time = "BMO"
                elif timing in ("amc", "after market close", "AMC"):
                    event_time = "AMC"
                else:
                    event_time = "Unknown"

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
        return saved
