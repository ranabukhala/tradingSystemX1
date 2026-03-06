"""
FRED Macro Calendar Connector
─────────────────────────────────────────────
Fetches upcoming macro release dates from FRED API.
Tracks: CPI, FOMC, NFP, GDP, PCE.
Emits to events.calendar Kafka topic.

FRED API: https://fred.stlouisfed.org/docs/api/fred/
Free API key: https://fred.stlouisfed.org/docs/api/api_key.html
"""
from __future__ import annotations

from datetime import datetime, timezone, date, timedelta

import aiohttp

from app.config import settings
from app.connectors.base import BaseConnector
from app.kafka import get_producer
from app.logging import get_logger
from app.models.events import MacroEvent

log = get_logger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred"

# Key FRED series and their human-readable names
FRED_SERIES = {
    "CPIAUCSL": "CPI (Consumer Price Index)",
    "PAYEMS":   "NFP (Non-Farm Payrolls)",
    "GDP":      "GDP (Gross Domestic Product)",
    "PCE":      "PCE (Personal Consumption Expenditures)",
    "UNRATE":   "Unemployment Rate",
    "INDPRO":   "Industrial Production Index",
    "RETAILSL": "Retail Sales",
}

# FOMC meeting dates are not in FRED series — we hardcode upcoming dates
# Update this list annually
FOMC_2025_2026_DATES = [
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10",
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-16",
]


class FredCalendarConnector(BaseConnector):
    """
    Weekly refresh of macro calendar events.
    Safe to re-run — upsert by series_id + event_date.
    """

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        super().__init__()

    @property
    def source_name(self) -> str:
        return "fred_calendar"

    @property
    def poll_interval_seconds(self) -> int:
        # Run weekly
        return 7 * 86400

    def validate_config(self) -> None:
        if not settings.fred_api_key:
            raise ValueError("FRED_API_KEY is not set in .env")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20)
            )
        return self._session

    async def fetch(self) -> int:
        session = await self._get_session()
        producer = get_producer()
        emitted = 0

        # Fetch release dates for each tracked series
        for series_id, name in FRED_SERIES.items():
            try:
                events = await self._fetch_series_releases(session, series_id, name)
                for event in events:
                    producer.produce(
                        topic=settings.topic_events_calendar,
                        value=event.to_kafka_dict(),
                        key=f"macro_{series_id}_{event.event_date}",
                    )
                    emitted += 1
            except Exception as e:
                log.error("fred.series_error", series_id=series_id, error=str(e))

        # Add FOMC dates
        for fomc_date_str in FOMC_2025_2026_DATES:
            try:
                fomc_date = date.fromisoformat(fomc_date_str)
                if fomc_date >= date.today():
                    event = MacroEvent(
                        name="FOMC Rate Decision",
                        fred_series_id="FOMC",
                        event_date=fomc_date,
                        release_time_et="14:00",
                    )
                    producer.produce(
                        topic=settings.topic_events_calendar,
                        value=event.to_kafka_dict(),
                        key=f"macro_FOMC_{fomc_date_str}",
                    )
                    emitted += 1
            except Exception as e:
                log.error("fred.fomc_error", date=fomc_date_str, error=str(e))

        log.info("fred.sync.complete", events=emitted)
        return emitted

    async def _fetch_series_releases(
        self,
        session: aiohttp.ClientSession,
        series_id: str,
        name: str,
    ) -> list[MacroEvent]:
        """Fetch upcoming release dates for a FRED series."""
        # First get the release ID for this series
        params = {
            "series_id": series_id,
            "api_key": settings.fred_api_key,
            "file_type": "json",
        }

        async with session.get(f"{FRED_BASE_URL}/series/release", params=params) as resp:
            if resp.status != 200:
                log.warning("fred.series_release.bad_response", series_id=series_id, status=resp.status)
                return []
            release_data = await resp.json()

        releases = release_data.get("releases", [])
        if not releases:
            return []

        release_id = releases[0]["id"]

        # Now get upcoming release dates
        today_str = date.today().strftime("%Y-%m-%d")
        future_str = (date.today() + timedelta(days=180)).strftime("%Y-%m-%d")

        date_params = {
            "release_id": release_id,
            "api_key": settings.fred_api_key,
            "file_type": "json",
            "realtime_start": today_str,
            "realtime_end": future_str,
            "limit": 20,
            "sort_order": "asc",
        }

        async with session.get(f"{FRED_BASE_URL}/release/dates", params=date_params) as resp:
            if resp.status != 200:
                return []
            dates_data = await resp.json()

        events = []
        for release_date in dates_data.get("release_dates", []):
            date_str = release_date.get("date", "")
            if not date_str:
                continue
            try:
                event_date = date.fromisoformat(date_str)
                if event_date >= date.today():
                    events.append(MacroEvent(
                        name=name,
                        fred_series_id=series_id,
                        event_date=event_date,
                        release_time_et="08:30",  # Most macro data releases at 8:30 ET
                    ))
            except ValueError:
                continue

        return events

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
