"""
FMP API Rate Limiter — shared Redis-backed daily call counter + cache-aside.

Problem solved:
  Four separate FMP containers (fmp_enrichment, fmp_earnings, fmp_sectors,
  fmp_technical) each had an independent per-process daily_count.  With a 250
  call/day cap, each container could spend the full 250, for 1,000 total.

Solution:
  A single Redis key  fmp:api:calls:YYYY-MM-DD  shared across ALL containers.
  Every real API call atomically INCRs the key.  Cache hits skip the counter.

Public API:
    cache_key = build_cache_key(endpoint, params)
    ttl       = get_ttl_for_endpoint(endpoint)
    result    = await get_or_fetch(redis, cache_key, fetch_fn, ttl, daily_limit)

    # Diagnostics
    used      = await get_daily_count(redis)
    remaining = await get_remaining(redis, daily_limit)
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine


# ── Cache TTLs (seconds) ──────────────────────────────────────────────────────
# Rules:
#   • TTL must be ≥ poll_interval of the connector that uses the endpoint so
#     that a warm cache is always available at poll time.
#   • Slow-moving data (profiles, analyst grades) can have long TTLs.
#   • Real-time data (quotes, technicals) need short TTLs but not shorter than
#     the poll interval that refreshes them.
CACHE_TTL: dict[str, int] = {
    # ── Fundamental / slow-moving ─────────────────────────────────────────────
    "profile":                       24 * 3600,  # Company profile — stable
    "shares-float":                   4 * 3600,  # Float — rarely changes intraday
    "grades-latest":                 24 * 3600,  # Analyst grades — daily update
    "analyst-grades":                24 * 3600,  # Analyst grades (variant path)
    "price-target-summary":          24 * 3600,  # Price targets — daily
    # ── Event-driven ─────────────────────────────────────────────────────────
    "earnings-calendar":              6 * 3600,  # Earnings dates — 6 h (poll: 6 h)
    "insider-trading":                1 * 3600,  # Insider trades — hourly
    # ── Market data ──────────────────────────────────────────────────────────
    "sector-performance-snapshot":    1 * 3600,  # Sector snapshot — 1 h (poll: 30 min)
    "sector-performance":             1 * 3600,  # Sector perf — 1 h
    "quote":                          5 * 60,    # Real-time quote — 5 min
    # ── Technical indicators ─────────────────────────────────────────────────
    "technical-indicator":           15 * 60,    # RSI/MACD — 15 min
    # ── Fallback ─────────────────────────────────────────────────────────────
    "_default":                       5 * 60,    # 5 minutes
}

# ── Redis counter key ─────────────────────────────────────────────────────────
COUNTER_TTL_SECONDS = 25 * 3600  # 25 h — outlasts a calendar day with buffer


def _counter_key() -> str:
    """Daily counter key in UTC: fmp:api:calls:YYYY-MM-DD"""
    return f"fmp:api:calls:{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"


# ── Public helpers ────────────────────────────────────────────────────────────

async def get_daily_count(redis) -> int:
    """Return today's cross-container API call count from Redis."""
    val = await redis.get(_counter_key())
    return int(val) if val else 0


async def get_remaining(redis, daily_limit: int) -> int:
    """Return remaining API calls allowed today (floor 0)."""
    used = await get_daily_count(redis)
    return max(0, daily_limit - used)


async def _try_acquire(redis, daily_limit: int) -> tuple[bool, int]:
    """
    Atomically increment today's counter.
    Returns (allowed, current_count_after_attempt).

    If over the limit, the increment is rolled back and (False, count) returned.
    First call of each UTC day sets a 25 h TTL so the key auto-expires.
    """
    key = _counter_key()
    count = await redis.incr(key)
    if count == 1:
        # First call of the day — set TTL so key auto-cleans
        await redis.expire(key, COUNTER_TTL_SECONDS)
    if count > daily_limit:
        await redis.decr(key)   # rollback — don't penalise the budget
        return False, count - 1
    return True, count


def build_cache_key(endpoint: str, params: dict) -> str:
    """
    Build a deterministic Redis cache key for an FMP endpoint + params.

    Format:
        fmp:cache:{slug}:{symbol}[:{extra=val}...]   — when symbol present
        fmp:cache:{slug}:{md5[:12]}                  — hash of sorted params

    The "apikey" param is always excluded from the key.

    Examples:
        /stable/technical-indicators/rsi + {symbol=AAPL, timeframe=1day}
        → fmp:cache:rsi:AAPL:periodLength=14:timeframe=1day

        /stable/earnings-calendar + {from=2026-01-01, to=2026-03-01}
        → fmp:cache:earnings-calendar:a3f8c1de9b22
    """
    slug = endpoint.strip("/").rsplit("/", 1)[-1]
    symbol = params.get("symbol") or params.get("ticker") or ""

    if symbol:
        extras = sorted(
            (k, v) for k, v in params.items()
            if k not in ("symbol", "ticker", "apikey")
        )
        suffix = ":".join(f"{k}={v}" for k, v in extras)
        return f"fmp:cache:{slug}:{symbol}" + (f":{suffix}" if suffix else "")
    else:
        params_str = "&".join(
            f"{k}={v}" for k, v in sorted(params.items()) if k != "apikey"
        )
        digest = hashlib.md5(params_str.encode()).hexdigest()[:12]
        return f"fmp:cache:{slug}:{digest}"


def get_ttl_for_endpoint(endpoint: str) -> int:
    """Return cache TTL seconds for an endpoint path (substring match)."""
    for key, ttl in CACHE_TTL.items():
        if key in endpoint:
            return ttl
    return CACHE_TTL["_default"]


# ── Core cache-aside ──────────────────────────────────────────────────────────

def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


async def get_or_fetch(
    redis,
    cache_key: str,
    fetch_fn: Callable[[], Coroutine[Any, Any, Any]],
    ttl: int,
    daily_limit: int,
) -> Any:
    """
    Cache-aside pattern with Redis-backed daily rate limiting.

    Flow:
        1. Cache HIT  → return cached value; daily counter NOT touched.
        2. Cache MISS → atomically acquire daily budget slot.
              Over budget → log fmp.rate_limit.daily_cap_reached, return None.
              Within budget → call fetch_fn() → store result in Redis with TTL.
        3. fetch_fn returns None (API error) → result NOT cached; budget slot
           already consumed (intentional — avoids hammering a broken endpoint).

    Args:
        redis:       aioredis.Redis instance (shared across connectors).
        cache_key:   Redis key (use build_cache_key()).
        fetch_fn:    Async callable that performs the real HTTP call.
        ttl:         Cache TTL in seconds (use get_ttl_for_endpoint()).
        daily_limit: Max API calls/day shared across ALL containers.
    """
    # ── 1. Cache hit ──────────────────────────────────────────────────────────
    cached = await redis.get(cache_key)
    if cached is not None:
        _log("debug", "fmp.cache.hit", key=cache_key)
        return json.loads(cached)

    # ── 2. Cache miss — acquire budget slot ───────────────────────────────────
    _log("debug", "fmp.cache.miss", key=cache_key)
    allowed, count = await _try_acquire(redis, daily_limit)
    if not allowed:
        _log("warning", "fmp.rate_limit.daily_cap_reached",
             key=cache_key, count=count, limit=daily_limit)
        return None

    # ── 3. Real API call ──────────────────────────────────────────────────────
    result = await fetch_fn()

    # ── 4. Cache successful result ────────────────────────────────────────────
    if result is not None:
        await redis.setex(cache_key, ttl, json.dumps(result, default=str))

    return result
