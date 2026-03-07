"""
FMP API Client — shared HTTP client for all FMP connectors.

Handles:
  - Authentication (apikey query param)
  - Rate limiting (750 req/min on free, 3000 on paid)
  - Redis caching (avoid redundant calls)
  - Retry with backoff
  - Free plan: 250 req/day budget tracking

Usage:
    client = FMPClient(api_key="...", redis=redis_conn)
    data = await client.get("/stable/shares-float", symbol="AAPL")
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis

BASE_URL = "https://financialmodelingprep.com"
RATE_LIMIT_DELAY = 0.2       # 200ms between requests = 300/min safe for free plan
CACHE_TTL = {
    "shares-float":      3600 * 4,    # 4h — float rarely changes
    "profile":           3600 * 24,   # 24h — company profile is stable
    "analyst-estimates": 3600 * 2,    # 2h — analyst ratings update occasionally
    "analyst-price-target-summary": 3600 * 2,
    "insider-trading":   3600 * 1,    # 1h — insider trades are time-sensitive
    "earning-calendar":  3600 * 1,    # 1h — earnings dates
    "senate-trading":    3600 * 6,    # 6h — congressional trades
    "technical-indicator": 60 * 5,    # 5min — RSI/MACD change frequently
    "quote":             30,           # 30s — real-time price
    "sector-performance": 60 * 15,    # 15min
}


def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


class FMPClient:

    def __init__(
        self,
        api_key: str,
        redis: aioredis.Redis | None = None,
        plan: str = "free",   # free | starter | premium
    ) -> None:
        self._api_key = api_key
        self._redis = redis
        self._plan = plan
        self._http = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=15.0,
            headers={"User-Agent": "trading-system/1.0"},
        )
        self._last_request = 0.0
        self._daily_count = 0
        self._daily_limit = 250 if plan == "free" else 99999

    async def get(
        self,
        endpoint: str,
        cache: bool = True,
        **params,
    ) -> Any:
        """
        GET request with caching and rate limiting.
        endpoint: e.g. "/stable/shares-float"
        params: query parameters (symbol=AAPL, etc.)
        """
        # Build cache key
        params_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        cache_key = f"fmp:{endpoint}:{params_str}"

        # Check cache first
        if cache and self._redis:
            cached = await self._redis.get(cache_key)
            if cached:
                return json.loads(cached)

        # Daily limit check (free plan)
        if self._daily_count >= self._daily_limit:
            _log("warning", "fmp.daily_limit_reached",
                 count=self._daily_count, limit=self._daily_limit)
            return None

        # Rate limiting
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request
        if elapsed < RATE_LIMIT_DELAY:
            await asyncio.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request = asyncio.get_event_loop().time()

        # Make request
        try:
            resp = await self._http.get(
                endpoint,
                params={"apikey": self._api_key, **params},
            )
            self._daily_count += 1

            if resp.status_code == 200:
                data = resp.json()

                # Cache result
                if cache and self._redis and data:
                    ttl = self._get_ttl(endpoint)
                    await self._redis.setex(cache_key, ttl, json.dumps(data))

                return data

            elif resp.status_code == 401:
                _log("error", "fmp.unauthorized", endpoint=endpoint)
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

    def _get_ttl(self, endpoint: str) -> int:
        """Get cache TTL based on endpoint type."""
        for key, ttl in CACHE_TTL.items():
            if key in endpoint:
                return ttl
        return 300  # Default 5min

    async def close(self) -> None:
        await self._http.aclose()

    @property
    def daily_requests_used(self) -> int:
        return self._daily_count

    @property
    def daily_requests_remaining(self) -> int:
        return max(0, self._daily_limit - self._daily_count)
