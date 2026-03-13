"""
FMP API Client — shared HTTP client for all FMP connectors.

Handles:
  - Authentication (apikey query param)
  - Redis-backed cross-container daily call budget (via fmp_rate_limiter)
  - Cache-aside (get_or_fetch): cache hit does NOT touch the daily counter
  - Intra-process HTTP rate throttle (80 ms between real calls)
  - HTTP error handling (401 / 402 / 429)

Budget enforcement:
  The old per-process _daily_count allowed each container to independently
  spend 250 calls (4 containers × 250 = 1,000 possible).  Now a single Redis
  key  fmp:api:calls:YYYY-MM-DD  is atomically INCRd for every real HTTP call
  so all containers share one counter against settings.fmp_daily_call_limit.

Usage:
    client = FMPClient(api_key="...", redis=redis_conn)
    data = await client.get("/stable/shares-float", symbol="AAPL")
    data = await client.get("/stable/earnings-calendar", from_="2026-01-01", to="2026-03-01")
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis

from app.config import settings
from app.connectors.fmp_rate_limiter import (
    build_cache_key,
    get_or_fetch,
    get_remaining,
    get_ttl_for_endpoint,
)

BASE_URL = "https://financialmodelingprep.com"
# 80 ms between real HTTP calls → at most ~750 req/min ceiling
# (well above the free-plan 300/min; keeps us from spiking)
RATE_LIMIT_DELAY = 0.08


def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


class FMPClient:

    def __init__(
        self,
        api_key: str,
        redis: aioredis.Redis | None = None,
        plan: str = "free",
    ) -> None:
        self._api_key = api_key
        self._redis = redis
        self._plan = plan
        self._daily_limit: int = settings.fmp_daily_call_limit
        self._http = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=15.0,
            headers={"User-Agent": "trading-system/1.0"},
        )
        self._last_request: float = 0.0
        # Session-level counter: counts real HTTP calls (cache misses) in THIS
        # process only.  Kept for informational Kafka metadata; cross-container
        # budget enforcement is handled by Redis (fmp_rate_limiter).
        self._session_calls: int = 0

    async def get(
        self,
        endpoint: str,
        cache: bool = True,
        **params,
    ) -> Any:
        """
        GET request with Redis cache-aside and shared daily budget.

        endpoint: e.g. "/stable/shares-float"
        params:   query parameters (symbol=AAPL, from_="2026-01-01", etc.)
                  Trailing underscores are stripped (from_ → from).

        Returns deserialized JSON or None on error / budget exhausted.
        """
        # Normalise param names: Python keyword conflict workaround (from_ → from)
        params = {k.rstrip("_"): v for k, v in params.items()}

        if cache and self._redis:
            cache_key = build_cache_key(endpoint, params)
            ttl = get_ttl_for_endpoint(endpoint)
            result = await get_or_fetch(
                self._redis,
                cache_key,
                lambda: self._raw_get(endpoint, params),
                ttl,
                self._daily_limit,
            )
            return result
        else:
            # No Redis — direct call; no shared budget tracking
            return await self._raw_get(endpoint, params)

    async def _raw_get(self, endpoint: str, params: dict) -> Any:
        """
        Raw HTTP GET — no caching, no budget check.
        Called by get_or_fetch() after a cache miss + budget approval.
        Increments _session_calls so the informational property stays accurate.
        """
        # Intra-process rate throttle
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request
        if elapsed < RATE_LIMIT_DELAY:
            await asyncio.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request = asyncio.get_event_loop().time()

        self._session_calls += 1  # count the HTTP attempt

        try:
            resp = await self._http.get(
                endpoint,
                params={"apikey": self._api_key, **params},
            )

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 401:
                _log("error", "fmp.unauthorized", endpoint=endpoint)
                return None
            elif resp.status_code == 402:
                _log("debug", "fmp.plan_required",
                     endpoint=endpoint, plan=self._plan,
                     msg="Endpoint requires a higher FMP plan")
                return None
            elif resp.status_code == 429:
                _log("warning", "fmp.rate_limited", endpoint=endpoint)
                await asyncio.sleep(60)
                return None
            else:
                _log("warning", "fmp.http_error",
                     endpoint=endpoint, status=resp.status_code)
                return None

        except Exception as e:
            _log("error", "fmp.request_error", endpoint=endpoint, error=str(e))
            return None

    # ── Informational properties ──────────────────────────────────────────────

    @property
    def daily_requests_used(self) -> int:
        """Real HTTP calls made in this process/session (excludes cache hits)."""
        return self._session_calls

    @property
    def daily_requests_remaining(self) -> int:
        """
        Estimated remaining calls based on session usage only.
        Useful for Kafka metadata; does NOT reflect other containers' usage.
        For exact cross-container count use: await client.get_exact_remaining()
        """
        return max(0, self._daily_limit - self._session_calls)

    async def get_exact_remaining(self) -> int:
        """Redis-backed exact remaining calls across all containers today."""
        if not self._redis:
            return self._daily_limit
        return await get_remaining(self._redis, self._daily_limit)

    async def close(self) -> None:
        await self._http.aclose()
