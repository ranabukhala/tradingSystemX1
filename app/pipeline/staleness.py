"""
Signal staleness guard — blocks execution of news signals that have aged past
the maximum actionable window for their catalyst type.

A signal is "stale" when:
  now_utc − signal.news_published_at  >  max_age(catalyst_type, session, route_type)

Lifecycle of a news event through the pipeline:
  published_at ──► received_at ──► news.raw ──► ... ──► signals.actionable ──► execute
                       ^                                                ^
                  connector arrives                             execution engine sees it

Each hop adds latency. This guard prevents trading on news the market has
already fully absorbed.

Policy dimensions:
  1. Catalyst type  — earnings age out fast; analyst notes last longer
  2. Session context — "open" is hyper-reactive; premarket gives more runway
  3. Route type     — fast path has lower latency than LLM slow path

Fail-open behaviour:
  If news_published_at is missing or unparseable the guard passes the signal.
  An absent timestamp must not silently block every signal.

Config (env vars):
  STALENESS_ENABLED                     true | false (default: true)
  STALENESS_MAX_AGE_OVERRIDE_SECONDS    flat override for ALL catalysts (testing/emergency)
  STALENESS_EARNINGS_MAX_AGE            per-catalyst base age in seconds
  STALENESS_MA_MAX_AGE
  STALENESS_REGULATORY_MAX_AGE
  STALENESS_ANALYST_MAX_AGE
  STALENESS_MACRO_MAX_AGE
  STALENESS_FILING_MAX_AGE
  STALENESS_LEGAL_MAX_AGE
  STALENESS_OTHER_MAX_AGE
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone


# ── Base max ages by catalyst type (seconds) ─────────────────────────────────
# Calibrated from how quickly market participants absorb each catalyst type.
#
#   Earnings:    60–120s  — algos react within seconds; humans within 1–2 min
#   M&A:         2–5 min  — arb desks fill the spread quickly
#   Regulatory:  1–3 min  — binary FDA event, immediate gap move
#   Analyst:     5–15 min — weight of evidence, less urgent
#   Macro:       2–4 min  — data-release algos move fastest instruments first
#   Filing:      10 min   — 8-K / 10-Q; still meaningful but slower
#   Legal:       30 min   — class-action / lawsuit; lagging catalyst, already priced in
#   Other:       10 min   — default conservative window

_BUILTIN_BASE: dict[str, int] = {
    "earnings":    120,
    "ma":          300,
    "regulatory":  180,
    "analyst":     900,
    "macro":       240,
    "filing":      600,
    "legal":      1800,
    "other":       600,
}


def _load_base_max_ages() -> dict[str, int]:
    """Load base ages, honouring per-catalyst env var overrides."""
    ages = dict(_BUILTIN_BASE)
    for key in ages:
        env_key = f"STALENESS_{key.upper()}_MAX_AGE"
        val = os.environ.get(env_key)
        if val:
            try:
                ages[key] = int(val)
            except ValueError:
                pass
    return ages


_BASE_MAX_AGE: dict[str, int] = _load_base_max_ages()


# ── Session multipliers ───────────────────────────────────────────────────────
#
#   premarket  ×2.0 — overnight news; full price discovery waits for open
#   open       ×0.5 — opening 30 min ultra-reactive; stale within seconds
#   intraday   ×1.0 — baseline
#   afterhours ×1.5 — thinner book; slower price discovery post-close
#   overnight  ×3.0 — dark-hours news; market is closed, long runway to open

_SESSION_MULT: dict[str, float] = {
    "premarket":   2.0,
    "open":        0.5,
    "intraday":    1.0,
    "afterhours":  1.5,
    "overnight":   3.0,
}

# ── Route type multipliers ────────────────────────────────────────────────────
#
#   fast  ×0.8 — zero LLM cost; pipeline latency is very low; tighter window
#   slow  ×1.2 — LLM adds 2–8 s of overhead; slightly more lenient

_ROUTE_MULT: dict[str, float] = {
    "fast":  0.8,
    "slow":  1.2,
}
_ROUTE_MULT_DEFAULT = 1.0

# ── Global feature flags and overrides ───────────────────────────────────────

_ENABLED: bool = os.environ.get("STALENESS_ENABLED", "true").lower() == "true"
_FLAT_OVERRIDE: int | None = (
    int(os.environ["STALENESS_MAX_AGE_OVERRIDE_SECONDS"])
    if "STALENESS_MAX_AGE_OVERRIDE_SECONDS" in os.environ
    else None
)


# ── Public types ──────────────────────────────────────────────────────────────

@dataclass
class StalenessResult:
    """Outcome of a staleness check for a single signal."""
    is_stale:        bool
    age_seconds:     float           # How old the signal is right now
    max_age_seconds: float           # Maximum age permitted for this context
    reason:          str             # Machine-readable code (ok | signal_stale | guard_disabled | …)
    catalyst_type:   str
    session:         str
    route_type:      str | None


# ── Public API ────────────────────────────────────────────────────────────────

def compute_max_age(
    catalyst_type: str,
    session: str,
    route_type: str | None,
) -> float:
    """
    Return the maximum permitted signal age in seconds for this context.

      max_age = base_age[catalyst] × session_multiplier × route_multiplier

    The flat override (STALENESS_MAX_AGE_OVERRIDE_SECONDS) supersedes all
    per-dimension values when set.
    """
    if _FLAT_OVERRIDE is not None:
        return float(_FLAT_OVERRIDE)

    base       = _BASE_MAX_AGE.get(catalyst_type.lower(), _BUILTIN_BASE["other"])
    sess_mult  = _SESSION_MULT.get(session.lower(), 1.0)
    route_mult = _ROUTE_MULT.get(route_type or "", _ROUTE_MULT_DEFAULT)
    return float(base * sess_mult * route_mult)


def check_staleness(
    news_published_at: datetime | str | None,
    catalyst_type: str,
    session: str,
    route_type: str | None = None,
    *,
    now: datetime | None = None,
) -> StalenessResult:
    """
    Check whether a signal should be blocked due to age.

    Parameters
    ----------
    news_published_at:
        When the source article was published (UTC). Accepts a timezone-aware
        datetime or an ISO-8601 string (as written by Pydantic model_dump
        mode='json'). Naive datetimes are assumed UTC.
    catalyst_type:
        e.g. "earnings", "analyst", "ma" — matches CatalystType enum values.
    session:
        e.g. "premarket", "open", "intraday" — matches SessionContext enum values.
    route_type:
        "fast" | "slow" | None — from SummarizedRecord / TradingSignal.
    now:
        Override the current time (for testing only).

    Returns
    -------
    StalenessResult with is_stale=True if the signal is too old to act on.
    Fails open (is_stale=False) when:
      - STALENESS_ENABLED=false
      - news_published_at is None or unparseable
      - Timestamp is in the future by > 5 min (clock skew)
    """
    if not _ENABLED:
        return StalenessResult(
            is_stale=False, age_seconds=0.0, max_age_seconds=0.0,
            reason="staleness_disabled",
            catalyst_type=catalyst_type, session=session, route_type=route_type,
        )

    if news_published_at is None:
        return StalenessResult(
            is_stale=False, age_seconds=0.0, max_age_seconds=0.0,
            reason="no_published_at",
            catalyst_type=catalyst_type, session=session, route_type=route_type,
        )

    ts = _parse_dt(news_published_at)
    if ts is None:
        return StalenessResult(
            is_stale=False, age_seconds=0.0, max_age_seconds=0.0,
            reason="unparseable_timestamp",
            catalyst_type=catalyst_type, session=session, route_type=route_type,
        )

    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    age_seconds = (now_utc - ts).total_seconds()
    max_age     = compute_max_age(catalyst_type, session, route_type)

    # Future timestamp guard — clock skew or bad vendor data; fail open
    if age_seconds < -300:
        return StalenessResult(
            is_stale=False,
            age_seconds=round(age_seconds, 1),
            max_age_seconds=round(max_age, 1),
            reason="future_timestamp_clock_skew",
            catalyst_type=catalyst_type, session=session, route_type=route_type,
        )

    is_stale = age_seconds > max_age
    return StalenessResult(
        is_stale=is_stale,
        age_seconds=round(age_seconds, 1),
        max_age_seconds=round(max_age, 1),
        reason="signal_stale" if is_stale else "ok",
        catalyst_type=catalyst_type,
        session=session,
        route_type=route_type,
    )


def staleness_from_signal_dict(
    record: dict,
    *,
    now: datetime | None = None,
) -> StalenessResult:
    """
    Convenience wrapper — extracts all required fields from a TradingSignal dict
    and delegates to check_staleness().

    Useful in pretrade_filter.py and execution_engine.py where the signal
    arrives as a raw dict from Kafka.
    """
    return check_staleness(
        news_published_at=record.get("news_published_at"),
        catalyst_type=record.get("catalyst_type", "other"),
        session=record.get("session_context", "intraday"),
        route_type=record.get("route_type"),
        now=now,
    )


# ── Internal helpers ──────────────────────────────────────────────────────────

def _parse_dt(value: datetime | str) -> datetime | None:
    """Parse a datetime from either a datetime object or ISO-8601 string."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None

    return None
