"""
Finnhub API per-minute rate limiter — shared Redis-backed counter.

Problem solved:
  Finnhub free tier allows 60 req/min across ALL services sharing the same API
  key (finnhub_news, finnhub_sentiment, finnhub_fundamentals).  Without
  coordination, simultaneous bursts from multiple containers exceed the cap
  and trigger 429s.

Solution:
  A single Redis key  finnhub:api:calls:YYYY-MM-DD-HH-MM  shared across ALL
  containers.  Every real API call atomically INCRs the key.  Cache hits skip
  the counter entirely.

Public API:
    from app.connectors.finnhub_rate_limiter import try_acquire, CACHE_TTL

    # Before each Finnhub HTTP call (after confirming cache miss):
    allowed, count = await try_acquire(redis, settings.finnhub_per_minute_call_limit)
    if not allowed:
        log("warning", "finnhub.rate_limit.per_minute_cap_reached", ...)
        return None  # or break

Cache TTLs:
    CACHE_TTL["news"]         = 300   (5 min — matches news poll interval)
    CACHE_TTL["sentiment"]    = 900   (15 min)
    CACHE_TTL["insider_mspr"] = 900   (15 min)
    CACHE_TTL["earnings"]     = 86400 (24 h — slow-moving fundamental data)
    CACHE_TTL["analyst"]      = 86400 (24 h — slow-moving fundamental data)
"""
from __future__ import annotations

from datetime import datetime, timezone

# ── Cache TTLs (seconds) ──────────────────────────────────────────────────────
CACHE_TTL: dict[str, int] = {
    "news":          5 * 60,    # Company news articles — 5 min
    "sentiment":    15 * 60,    # News sentiment scores — 15 min
    "insider_mspr": 15 * 60,    # Insider MSPR — 15 min
    "earnings":     24 * 3600,  # Earnings surprises — 24 h (slow-moving)
    "analyst":      24 * 3600,  # Analyst recommendations — 24 h (slow-moving)
}

# Per-minute window TTL: 90 s outlasts the 60 s window with a 30 s buffer so
# the key is still readable just after the minute ticks over.
_RATE_LIMIT_TTL_SECONDS = 90


def per_minute_key() -> str:
    """Current UTC per-minute counter key: finnhub:api:calls:YYYY-MM-DD-HH-MM"""
    return f"finnhub:api:calls:{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M')}"


async def try_acquire(redis, per_minute_limit: int) -> tuple[bool, int]:
    """
    Atomically increment the per-minute API call counter.
    Returns (allowed, current_count_after_attempt).

    First call of each UTC minute sets a 90 s TTL so the key auto-expires.
    If over the limit, the increment is rolled back and (False, count) is
    returned so the caller can log accurately without inflating the counter.
    """
    key = per_minute_key()
    count = await redis.incr(key)
    if count == 1:
        # First call this minute — attach TTL so key auto-cleans
        await redis.expire(key, _RATE_LIMIT_TTL_SECONDS)
    if count > per_minute_limit:
        await redis.decr(key)   # rollback — don't burn a slot
        return False, count - 1
    return True, count
