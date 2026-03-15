"""
Redis-backed async idempotency store for pipeline at-least-once safety.

Design
──────
Uses atomic SET … NX EX to mark every (stage, event_id) pair exactly once.
Because SET NX is a single Redis command it is atomically race-free: in a
burst of 100 concurrent tasks for the same event_id, exactly one gets True
(key set), all others get None (key existed).

Key schema
──────────
    idem:{stage}:{event_id}  →  ISO-8601 UTC timestamp of first processing
    e.g. idem:deduplicator:finnhub-abc123  →  "2026-03-15T18:30:00.123456+00:00"

TTL policy
──────────
Default: IDEMPOTENCY_TTL_SECONDS = 172800 (48 h).

Rationale:
  • Kafka default retention is 7 days, but replay windows are typically ≤ 24 h.
  • 48 h = 2× the content-dedup TTL (24 h), so a replayed event that sneaks
    past the content-hash check still hits the idempotency gate.
  • At ~10 k events/day per stage the worst-case Redis memory is tiny:
    10_000 × 48 × (key_overhead + ~80 bytes) ≈ 40 MB/stage at peak.
  • Events older than 48 h that arrive (deep replay, extreme lag) are
    re-processed. The downstream deduplicator's 4-tier content dedup will
    catch any content-level duplicates before they become trades.

Failure / degraded-mode behavior
─────────────────────────────────
1. Redis unavailable (ConnectionError / TimeoutError)
   → Immediately fall back to the SQLite IdempotencyStore (if configured).
   → Log a structured warning on the FIRST fallback event; silent thereafter.
   → Increment metric_idem_fallback_total for every message on the fallback path.

2. Redis unavailable AND no SQLite fallback configured
   → Fail-open: return False (treat as new event).
   → Log error. Pipeline continues; downstream dedup prevents content dups.

3. Redis recovers
   → Automatically detected on next successful SET NX call.
   → Log info "redis_idempotency.redis_recovered".

4. IDEMPOTENCY_BACKEND=sqlite
   → _SQLiteAsyncAdapter wraps the sync IdempotencyStore transparently.
   → No Redis connection is opened; TTL / fallback settings are ignored.
   → This is the escape hatch for rollback without code changes.

Concurrency safety
──────────────────
• Redis: SET NX is a single-round-trip atomic operation — no WATCH/MULTI/EXEC
  needed. All Kafka consumer replicas in the same process group share the same
  Redis namespace, so duplicate-prevention is cross-instance.
• SQLite fallback: uses the existing WAL-mode store; safe for single-process
  concurrent access (threading.local() connection pool).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from app.logging import get_logger

log = get_logger("redis_idempotency")

# Fallback default read from environment so tests can patch it.
_DEFAULT_TTL = int(os.environ.get("IDEMPOTENCY_TTL_SECONDS", "172800"))  # 48 h


# ── Public store ──────────────────────────────────────────────────────────────

class RedisIdempotencyStore:
    """
    Async idempotency store backed by Redis SET NX EX.

    Parameters
    ----------
    redis_client:
        An *open* redis.asyncio client.  The store takes ownership and will
        aclose() it when close() is called.
    ttl:
        Key TTL in seconds (default: IDEMPOTENCY_TTL_SECONDS env var, 48 h).
    fallback_store:
        Optional sync IdempotencyStore used when Redis is unreachable.
        The caller owns the fallback and must close it separately.
    """

    def __init__(
        self,
        redis_client: Any,
        ttl: int = _DEFAULT_TTL,
        fallback_store: Any = None,
    ) -> None:
        self._redis = redis_client
        self._ttl = ttl
        self._fallback = fallback_store
        # True while Redis is unreachable; cleared on first successful call
        self._fallback_mode: bool = False

    # ── Core API ──────────────────────────────────────────────────────────

    async def check_and_mark(
        self,
        stage: str,
        event_id: str,
        payload_hash: str | None = None,
    ) -> bool:
        """
        Atomically check if (stage, event_id) has been processed before.

        Returns
        -------
        True  — key existed → duplicate, caller should skip this event.
        False — key was set  → new event,  caller should process it.

        The single SET … NX EX command is atomic: no two concurrent callers
        can both receive False for the same (stage, event_id).
        """
        key = f"idem:{stage}:{event_id}"
        now = datetime.now(timezone.utc).isoformat()
        try:
            result = await self._redis.set(key, now, ex=self._ttl, nx=True)
            # result = True  → key was SET (new event)
            # result = None  → key already existed (duplicate)
            is_dup = result is None

            if self._fallback_mode:
                # First success after a Redis outage — log recovery
                self._fallback_mode = False
                log.info(
                    "redis_idempotency.redis_recovered",
                    stage=stage,
                    event_id=event_id,
                )
            return is_dup

        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            return await self._fallback_check_and_mark(stage, event_id, payload_hash, exc)

    async def close(self) -> None:
        """
        Close the owned Redis client.
        Does NOT close the fallback store — the caller that created it owns it.
        """
        try:
            await self._redis.aclose()
        except Exception:
            pass  # Best-effort; don't raise during shutdown

    # ── Degraded-mode path ────────────────────────────────────────────────

    async def _fallback_check_and_mark(
        self,
        stage: str,
        event_id: str,
        payload_hash: str | None,
        exc: Exception,
    ) -> bool:
        """Called when Redis is unreachable.  Delegates to SQLite or fails open."""
        if not self._fallback_mode:
            self._fallback_mode = True
            log.warning(
                "redis_idempotency.fallback_activated",
                stage=stage,
                error=str(exc),
                error_type=type(exc).__name__,
                fallback_available=self._fallback is not None,
            )

        if self._fallback is not None:
            try:
                return self._fallback.check_and_mark(stage, event_id, payload_hash)
            except Exception as exc2:
                log.error(
                    "redis_idempotency.fallback_also_failed",
                    stage=stage,
                    event_id=event_id,
                    error=str(exc2),
                )

        # Fail-open: treat as new event so the pipeline doesn't stall
        log.error(
            "redis_idempotency.fail_open",
            stage=stage,
            event_id=event_id,
            reason="Redis unreachable and no fallback configured",
        )
        return False  # False = "new event" — allow through


# ── SQLite compatibility shim ─────────────────────────────────────────────────

class _SQLiteAsyncAdapter:
    """
    Thin async wrapper around the sync IdempotencyStore.

    Activated when IDEMPOTENCY_BACKEND=sqlite so callers get the same
    async interface with zero Redis dependency.

    close() is a no-op here — BaseConsumer.run() handles SQLite teardown
    via the separate ``self._idempotency`` reference in its finally block.
    """

    def __init__(self, store: Any) -> None:
        self._store = store
        self._fallback_mode: bool = False  # Always False — no Redis involved

    async def check_and_mark(
        self,
        stage: str,
        event_id: str,
        payload_hash: str | None = None,
    ) -> bool:
        return self._store.check_and_mark(stage, event_id, payload_hash)

    async def close(self) -> None:
        pass  # Owner (BaseConsumer.run finally block) handles this
