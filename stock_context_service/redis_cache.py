"""
Redis cache interface for StockContext objects.

All keys follow the pattern:  stock_context:{TICKER}
TTL:                          14 400 seconds  (4 hours)

Public API
----------
get_context(ticker)                 -> StockContext | None
set_context(ticker, context)        -> None
get_all_cached_tickers()            -> list[str]
invalidate(ticker)                  -> None
"""
from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

import redis.asyncio as aioredis

from .models import StockContext

if TYPE_CHECKING:
    pass   # keep type checker happy without circular imports

logger = logging.getLogger(__name__)

_KEY_PREFIX = "stock_context:"
_TTL        = 14_400   # 4 hours in seconds


# ── Module-level client (initialised by main.py and injected here) ───────────

_redis: aioredis.Redis | None = None


def set_redis_client(client: aioredis.Redis) -> None:
    """Inject the shared Redis connection.  Called once during startup."""
    global _redis
    _redis = client


def get_redis_client() -> aioredis.Redis:
    """Return the module-level Redis client, raising if not initialised."""
    if _redis is None:
        raise RuntimeError(
            "Redis client not initialised — call set_redis_client() first"
        )
    return _redis


# ── Cache operations ─────────────────────────────────────────────────────────

async def get_context(ticker: str) -> StockContext | None:
    """
    Read a cached StockContext for *ticker*.

    Returns None on cache miss or deserialisation error.
    """
    try:
        raw = await get_redis_client().get(_KEY_PREFIX + ticker.upper())
        if raw is None:
            return None
        data = json.loads(raw)
        return StockContext.from_dict(data)
    except Exception as exc:
        logger.warning(
            "redis_cache.get_error",
            extra={"ticker": ticker, "error": str(exc)},
        )
        return None


async def set_context(ticker: str, context: StockContext) -> None:
    """
    Serialise and store a StockContext with a 4-hour TTL.

    Logs DEBUG on success; ERROR on failure (but does not raise).
    """
    try:
        payload = json.dumps(context.to_dict())
        await get_redis_client().set(
            _KEY_PREFIX + ticker.upper(),
            payload,
            ex=_TTL,
        )
        logger.debug(
            "redis_cache.set_ok",
            extra={
                "ticker":    ticker,
                "threshold": context.adjusted_threshold,
                "trend":     context.trend_regime.value,
                "ttl":       _TTL,
            },
        )
    except Exception as exc:
        logger.error(
            "redis_cache.set_error",
            extra={"ticker": ticker, "error": str(exc)},
        )


async def get_all_cached_tickers() -> list[str]:
    """
    Return a list of all tickers that currently have a cached context.

    Uses SCAN to avoid blocking the Redis event loop.
    """
    tickers: list[str] = []
    prefix = _KEY_PREFIX
    try:
        cursor: int = 0
        client = get_redis_client()
        while True:
            cursor, keys = await client.scan(cursor, match=f"{prefix}*", count=100)
            for key in keys:
                # key may be str (decode_responses=True) or bytes
                key_str = key if isinstance(key, str) else key.decode()
                tickers.append(key_str[len(prefix):])
            if cursor == 0:
                break
    except Exception as exc:
        logger.warning(
            "redis_cache.scan_error",
            extra={"error": str(exc)},
        )
    return tickers


async def invalidate(ticker: str) -> None:
    """Delete the cached context for *ticker*."""
    try:
        await get_redis_client().delete(_KEY_PREFIX + ticker.upper())
        logger.debug("redis_cache.invalidated", extra={"ticker": ticker})
    except Exception as exc:
        logger.warning(
            "redis_cache.invalidate_error",
            extra={"ticker": ticker, "error": str(exc)},
        )
