"""
Finnhub Press Releases + SEC 8-K Connector — Earnings Fast-Path.

Polls /press-releases and /stock/filings?form=8-K for companies reporting
earnings TODAY. Press releases are the original source for earnings results —
they arrive 5-10 minutes before news articles are written about them.

Timing windows (self-managed, does not depend on market scheduler):
  - AMC tickers: poll every 60s from 3:30 PM to 6:30 PM ET
  - BMO tickers: poll every 60s from 4:00 AM to 10:00 AM ET
  - Outside these windows: skips polling to conserve API budget

Uses the shared Redis-backed Finnhub rate limiter (300 req/min budget).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone, timedelta

import httpx
import pytz
import redis.asyncio as aioredis

from app.config import settings
from app.connectors.base import BaseConnector, _log
from app.connectors.finnhub_rate_limiter import try_acquire
from app.kafka import get_producer
from app.models.news import NewsSource, RawNewsRecord

FINNHUB_BASE = "https://finnhub.io/api/v1"
EASTERN = pytz.timezone("America/New_York")

# Keywords that identify an earnings-related press release
EARNINGS_RE = re.compile(
    r"\b(earnings|results|quarter(?:ly)?|fiscal|eps|revenue|"
    r"reports?\s+(?:first|second|third|fourth|q[1-4])|"
    r"financial\s+results|net\s+income|operating\s+results|"
    r"announces?\s+(?:fourth|third|second|first)|"
    r"fourth[- ]quarter|third[- ]quarter|second[- ]quarter|first[- ]quarter)\b",
    re.I,
)

# AMC polling window: 3:30 PM – 6:30 PM ET
AMC_WINDOW = (15 * 60 + 30, 18 * 60 + 30)   # minutes since midnight ET
# BMO polling window: 4:00 AM – 10:00 AM ET
BMO_WINDOW = (4 * 60, 10 * 60)
# Earnings ticker cache refresh interval (seconds)
EARNINGS_REFRESH_S = 30 * 60   # 30 minutes


class FinnhubPressReleasesConnector(BaseConnector):
    """
    Polls Finnhub press releases and SEC 8-K filings for earnings-day tickers.
    Emits matching records to news.raw as RawNewsRecord (source=finnhub).
    """

    def __init__(self) -> None:
        self._http: httpx.AsyncClient | None = None
        self._api_key = settings.finnhub_api_key
        self._earnings_tickers: dict[str, str] = {}   # ticker → "amc"|"bmo"|"unknown"
        self._earnings_loaded_at: float = 0.0
        super().__init__()

    @property
    def source_name(self) -> str:
        return "finnhub_press_releases"

    @property
    def poll_interval_seconds(self) -> int:
        return 60   # Poll every 60 seconds

    def validate_config(self) -> None:
        if not self._api_key:
            _log("warning", "press_releases.no_api_key",
                 msg="FINNHUB_API_KEY not set — connector disabled")

    def _get_http(self) -> httpx.AsyncClient:
        if not self._http:
            self._http = httpx.AsyncClient(
                headers={"X-Finnhub-Token": self._api_key},
                timeout=12.0,
            )
        return self._http

    async def fetch(self) -> int:
        if not self._api_key:
            return 0

        redis_conn = await aioredis.from_url(
            settings.redis_url, decode_responses=True
        )
        try:
            await self._maybe_refresh_earnings(redis_conn)

            if not self._earnings_tickers:
                _log("debug", "press_releases.no_earnings_today")
                return 0

            now_et = datetime.now(pytz.utc).astimezone(EASTERN)
            now_mins = now_et.hour * 60 + now_et.minute

            http = self._get_http()
            producer = get_producer()
            today = now_et.strftime("%Y-%m-%d")
            emitted = 0

            for ticker, timing in self._earnings_tickers.items():
                t = timing.lower()

                # Gate: only poll within the relevant window
                if t == "amc":
                    in_window = AMC_WINDOW[0] <= now_mins < AMC_WINDOW[1]
                elif t == "bmo":
                    in_window = BMO_WINDOW[0] <= now_mins < BMO_WINDOW[1]
                else:
                    # Unknown timing — poll during both windows (conservative)
                    in_window = (
                        AMC_WINDOW[0] <= now_mins < AMC_WINDOW[1]
                        or BMO_WINDOW[0] <= now_mins < BMO_WINDOW[1]
                    )

                if not in_window:
                    continue

                # ── Press Releases ─────────────────────────────────────────────
                emitted += await self._fetch_press_releases(
                    ticker, today, redis_conn, http, producer
                )

                # ── SEC 8-K Filings ────────────────────────────────────────────
                emitted += await self._fetch_8k_filings(
                    ticker, today, redis_conn, http, producer
                )

                # Shared rate limit budget: 0.3s between tickers
                await asyncio.sleep(0.3)

            return emitted
        finally:
            await redis_conn.aclose()

    async def _maybe_refresh_earnings(self, redis_conn: aioredis.Redis) -> None:
        """Refresh today's earnings ticker list from Postgres every 30 minutes."""
        now = asyncio.get_event_loop().time()
        if now - self._earnings_loaded_at < EARNINGS_REFRESH_S and self._earnings_tickers:
            return

        try:
            from app.db import get_engine
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import AsyncSession

            engine = get_engine()
            async with AsyncSession(engine) as session:
                rows = (await session.execute(text("""
                    SELECT ticker, event_time
                    FROM event
                    WHERE event_type = 'earnings'
                      AND event_date::date = CURRENT_DATE
                """))).fetchall()

            self._earnings_tickers = {
                row.ticker: (row.event_time or "unknown") for row in rows
            }
            self._earnings_loaded_at = now
            _log("info", "press_releases.earnings_loaded",
                 count=len(self._earnings_tickers),
                 tickers=list(self._earnings_tickers.keys()))
        except Exception as e:
            _log("warning", "press_releases.earnings_load_error", error=str(e))

    async def _fetch_press_releases(
        self,
        ticker: str,
        today: str,
        redis_conn: aioredis.Redis,
        http: httpx.AsyncClient,
        producer,
    ) -> int:
        cache_key = f"finnhub:pr:seen:{ticker}:{today}"

        try:
            allowed, count = await try_acquire(
                redis_conn, settings.finnhub_per_minute_call_limit
            )
            if not allowed:
                _log("warning", "press_releases.rate_limited",
                     ticker=ticker, count=count)
                return 0

            resp = await http.get(
                f"{FINNHUB_BASE}/press-releases",
                params={"symbol": ticker, "from": today, "to": today},
            )
            if resp.status_code != 200:
                return 0

            articles = resp.json() if isinstance(resp.json(), list) else (
                resp.json().get("majorDevelopment", [])
            )
            if not articles:
                return 0

            # Load seen URLs from Redis
            seen_raw = await redis_conn.get(cache_key)
            seen_urls: set[str] = set(json.loads(seen_raw)) if seen_raw else set()

            emitted = 0
            new_seen = set(seen_urls)

            for item in articles:
                url = item.get("url", "") or item.get("link", "")
                headline = item.get("headline", "") or item.get("title", "")

                if not headline:
                    continue

                # Filter: only earnings-related press releases
                if not EARNINGS_RE.search(headline):
                    continue

                url_hash = hashlib.md5(url.encode()).hexdigest()[:12] if url else (
                    hashlib.md5(headline.encode()).hexdigest()[:12]
                )
                if url_hash in seen_urls:
                    continue
                new_seen.add(url_hash)

                record = self._parse_press_release(item, ticker, "press_release")
                producer.produce(
                    topic="news.raw",
                    value=record.to_kafka_dict(),
                    key=record.vendor_id,
                )
                emitted += 1
                _log("info", "press_releases.emitted",
                     ticker=ticker, headline=headline[:80])

            if new_seen != seen_urls:
                await redis_conn.setex(
                    cache_key, 3600 * 8, json.dumps(list(new_seen))
                )

            return emitted

        except Exception as e:
            _log("warning", "press_releases.fetch_error",
                 ticker=ticker, error=str(e))
            return 0

    async def _fetch_8k_filings(
        self,
        ticker: str,
        today: str,
        redis_conn: aioredis.Redis,
        http: httpx.AsyncClient,
        producer,
    ) -> int:
        cache_key = f"finnhub:8k:seen:{ticker}:{today}"

        try:
            allowed, count = await try_acquire(
                redis_conn, settings.finnhub_per_minute_call_limit
            )
            if not allowed:
                return 0

            resp = await http.get(
                f"{FINNHUB_BASE}/stock/filings",
                params={
                    "symbol": ticker,
                    "form":   "8-K",
                    "from":   today,
                    "to":     today,
                },
            )
            if resp.status_code != 200:
                return 0

            filings = resp.json() if isinstance(resp.json(), list) else (
                resp.json().get("filings", [])
            )
            if not filings:
                return 0

            seen_raw = await redis_conn.get(cache_key)
            seen_ids: set[str] = set(json.loads(seen_raw)) if seen_raw else set()

            emitted = 0
            new_seen = set(seen_ids)

            for filing in filings:
                access_num = filing.get("accessNumber", "")
                if not access_num or access_num in seen_ids:
                    continue
                new_seen.add(access_num)

                filing_url  = filing.get("filingUrl", "") or filing.get("reportUrl", "")
                accepted_at = filing.get("acceptedDate", "") or filing.get("filedDate", "")

                record = self._parse_filing(filing, ticker, accepted_at)
                producer.produce(
                    topic="news.raw",
                    value=record.to_kafka_dict(),
                    key=record.vendor_id,
                )
                emitted += 1
                _log("info", "press_releases.8k_emitted",
                     ticker=ticker, access_num=access_num)

            if new_seen != seen_ids:
                await redis_conn.setex(
                    cache_key, 3600 * 8, json.dumps(list(new_seen))
                )

            return emitted

        except Exception as e:
            _log("warning", "press_releases.8k_error",
                 ticker=ticker, error=str(e))
            return 0

    def _parse_press_release(
        self, item: dict, ticker: str, item_type: str
    ) -> RawNewsRecord:
        # Datetime: "2026-02-24 16:05:00" or Unix int
        raw_dt = item.get("datetime", "") or item.get("date", "")
        if isinstance(raw_dt, int):
            published_at = datetime.fromtimestamp(raw_dt, tz=timezone.utc)
        elif isinstance(raw_dt, str) and raw_dt:
            try:
                published_at = datetime.fromisoformat(
                    raw_dt.replace(" ", "T")
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                published_at = datetime.now(timezone.utc)
        else:
            published_at = datetime.now(timezone.utc)

        headline = (item.get("headline") or item.get("title") or "").strip()
        url      = item.get("url") or item.get("link") or ""
        snippet  = (item.get("description") or item.get("summary") or "")[:500]

        vendor_id = f"finnhub_pr_{hashlib.md5((url or headline).encode()).hexdigest()[:16]}"

        return RawNewsRecord(
            source=NewsSource.FINNHUB,
            vendor_id=vendor_id,
            published_at=published_at,
            url=url,
            title=headline,
            snippet=snippet,
            author=item.get("source", "Press Release"),
            raw_tickers=[ticker],
            raw_categories=["earnings"],
            raw_payload={
                **item,
                "_item_type": item_type,
                "_source": "finnhub_press_releases",
            },
        )

    def _parse_filing(
        self, filing: dict, ticker: str, accepted_at: str
    ) -> RawNewsRecord:
        try:
            published_at = datetime.fromisoformat(
                accepted_at.replace(" ", "T").rstrip("Z")
            ).replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            published_at = datetime.now(timezone.utc)

        access_num  = filing.get("accessNumber", "")
        filing_url  = filing.get("filingUrl") or filing.get("reportUrl") or ""
        form        = filing.get("form", "8-K")

        headline = f"{ticker} SEC {form} Filing — Earnings Report"
        snippet  = f"SEC filing {access_num}. URL: {filing_url}"[:500]
        vendor_id = f"finnhub_8k_{hashlib.md5(access_num.encode()).hexdigest()[:16]}"

        return RawNewsRecord(
            source=NewsSource.FINNHUB,
            vendor_id=vendor_id,
            published_at=published_at,
            url=filing_url,
            title=headline,
            snippet=snippet,
            author="SEC EDGAR",
            raw_tickers=[ticker],
            raw_categories=["earnings"],
            raw_payload={
                **filing,
                "_item_type": "sec_8k",
                "_source": "finnhub_press_releases",
            },
        )
