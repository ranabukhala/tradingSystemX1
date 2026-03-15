"""
Time-bucket logic for semantic event clustering.

Each (ticker, catalyst_type) pair is windowed into fixed-width buckets.
Events that fall in the same bucket are candidates for semantic clustering.
Events in different buckets are treated as distinct, regardless of similarity.

Window sizes are tuned per catalyst type:
  - EARNINGS:   30 min — fast-moving, distinct beats/misses cluster tightly
  - ANALYST:    20 min — upgrades/downgrades usually re-reported for a few minutes
  - MA:         60 min — deal news slow to propagate, wide window acceptable
  - REGULATORY: 45 min — FDA/SEC actions take time to be picked up by all vendors
  - MACRO:      15 min — CPI / Fed decisions: very short window to avoid false clusters
  - OTHER:      25 min — conservative default

Redis key format:
  event:cluster:{ticker}:{catalyst}:{date_et}:{bucket_idx}:{window_min}
  e.g. event:cluster:AAPL:earnings:2024-11-01:16:30

TTL: 3× the window size in seconds (ensures expired buckets clean themselves up).
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

# Window sizes in minutes, keyed by catalyst type value string
_WINDOW_MINUTES: dict[str, int] = {
    "earnings":   30,
    "analyst":    20,
    "ma":         60,
    "regulatory": 45,
    "macro":      15,
    "filing":     25,
    "legal":      40,
    "other":      25,
}

_DEFAULT_WINDOW = 25


def _window_for(catalyst: str) -> int:
    return _WINDOW_MINUTES.get(catalyst.lower(), _DEFAULT_WINDOW)


def time_bucket(dt_utc: datetime, catalyst: str) -> tuple[str, int, int]:
    """
    Convert a UTC datetime + catalyst type to a (date_et, bucket_idx, window_min) tuple.

    date_et    — "YYYY-MM-DD" in ET timezone
    bucket_idx — floor(minute_of_day / window_min)
    window_min — window size used (for TTL calculation)
    """
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    et = dt_utc.astimezone(_ET)
    date_str = et.strftime("%Y-%m-%d")
    minute_of_day = et.hour * 60 + et.minute
    window = _window_for(catalyst)
    bucket_idx = minute_of_day // window
    return date_str, bucket_idx, window


def bucket_key(ticker: str, catalyst: str, dt_utc: datetime) -> str:
    """
    Build the Redis key for an event cluster bucket.

    Format: event:cluster:{ticker}:{catalyst}:{date_et}:{bucket_idx}:{window_min}
    """
    date_str, bucket_idx, window = time_bucket(dt_utc, catalyst)
    return f"event:cluster:{ticker.upper()}:{catalyst.lower()}:{date_str}:{bucket_idx}:{window}"


def bucket_ttl(catalyst: str) -> int:
    """
    TTL in seconds for a cluster bucket key — 3× the window size.
    Ensures old buckets expire well after all events in them have been processed.
    """
    return _window_for(catalyst) * 60 * 3
