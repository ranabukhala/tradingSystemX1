"""
Earnings Calendar Connector (FMP)
─────────────────────────────────────────────
Fetches upcoming earnings events daily at 6 AM ET.
Stores in events table and emits to events.calendar Kafka topic.

API: https://financialmodelingprep.com/stable/earnings-calendar
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta

import aiohttp

from app.config import settings
from app.connectors.base import BaseConnector
from app.kafka import get_producer
from app.logging import get_logger
from app.models.events import EarningsEvent, EarningsTime

log = get_logger(__name__)

FMP_BASE_URL = "https://financialmodelingprep.com/stable"


class EarningsWhispersConnector(BaseConnector):
    """
    Daily sync of earnings calendar via FMP.
    Runs once at startup then every 24h.
    Uses upsert logic — safe to re-run.
    """

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        super().__init__()

    @property
    def source_name(self) -> str:
        return "earnings_whispers"

    @property
    def poll_interval_seconds(self) -> int:
        return 86400

    def validate_config(self) -> None:
        if not settings.fmp_api_key:
            raise ValueError("FMP_API_KEY is not set in .env")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def fetch(self) -> int:
        """Fetch next N days of earnings events."""
        session = await self._get_session()
        producer = get_producer()

        today = date.today()
        end_date = today + timedelta(days=settings.earnings_lookahead_days)

        emitted = 0

        # Fetch week by week to stay within API limits
        current = today
        while current <= end_date:
            week_end = min(current + timedelta(days=6), end_date)
            try:
                events = await self._fetch_week(session, current, week_end)
                for event in events:
                    producer.produce(
                        topic=settings.topic_events_calendar,
                        value=event.to_kafka_dict(),
                        key=f"{event.ticker}_{event.event_date}",
                    )
                    emitted += 1
                await asyncio.sleep(0.5)  # Rate limit courtesy
            except Exception as e:
                log.error("earnings.fetch_week.error", week_start=str(current), error=str(e))

            current = week_end + timedelta(days=1)

        log.info("earnings.sync.complete", events=emitted, lookahead_days=settings.earnings_lookahead_days)
        return emitted

    async def _fetch_week(
        self,
        session: aiohttp.ClientSession,
        from_date: date,
        to_date: date,
    ) -> list[EarningsEvent]:
        """Fetch earnings for a date range from FMP."""
        params = {
            "from": from_date.strftime("%Y-%m-%d"),
            "to": to_date.strftime("%Y-%m-%d"),
            "apikey": settings.fmp_api_key,
        }

        async with session.get(f"{FMP_BASE_URL}/earnings-calendar", params=params) as resp:
            if resp.status == 401:
                raise ValueError("FMP: invalid API key")
            if resp.status == 402:
                log.error("earnings.bad_response", status=resp.status, reason="FMP plan does not include earnings-calendar")
                return []
            if resp.status == 429:
                await asyncio.sleep(60)
                return []
            if resp.status != 200:
                log.error("earnings.bad_response", status=resp.status)
                return []
            data = await resp.json()

        # FMP returns a flat list
        if not isinstance(data, list):
            log.error("earnings.unexpected_format", data=str(data)[:200])
            return []

        events = []
        for item in data:
            try:
                events.append(self._parse_event(item))
            except Exception as e:
                log.error("earnings.parse_error", item=item, error=str(e))

        return events

    def _parse_event(self, item: dict) -> EarningsEvent:
        """Parse FMP earnings-calendar item into EarningsEvent."""
        date_str = item.get("date", "")
        try:
            event_date = date.fromisoformat(date_str)
        except ValueError:
            event_date = date.today()

        # FMP uses "bmo" / "amc" / "" for time
        time_str = (item.get("time") or "").lower()
        if time_str == "bmo" or "before" in time_str:
            event_time = EarningsTime.BMO
        elif time_str == "amc" or "after" in time_str:
            event_time = EarningsTime.AMC
        else:
            event_time = EarningsTime.UNKNOWN

        return EarningsEvent(
            ticker=(item.get("symbol") or "").upper().strip(),
            company_name=item.get("name", ""),
            event_date=event_date,
            event_time=event_time,
            fiscal_quarter=item.get("fiscalQuarter") or item.get("quarter"),
            fiscal_year=item.get("fiscalYear") or item.get("year"),
            whisper_eps=None,  # FMP does not provide whisper EPS
            consensus_eps=self._parse_float(item.get("epsEstimated")),
            consensus_revenue=self._parse_float(item.get("revenueEstimated")),
        )

    @staticmethod
    def _parse_float(value) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()