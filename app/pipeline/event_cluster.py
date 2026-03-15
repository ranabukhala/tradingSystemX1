"""
EventClusterStore — Redis-backed registry for semantic event clustering.

Implements the "match_or_create" pattern:
  - New events are stored as cluster representatives
  - Incoming events are checked against the bucket for SimHash similarity
  - If a match is found (and fact fingerprints agree), the new event is
    marked as non-representative (duplicate)
  - If no match, a new cluster entry is created

Redis key schema:
  event:cluster:{ticker}:{catalyst}:{date_et}:{bucket_idx}:{window}
    → Redis Hash: { simhash_hex → "event_id|fact_fp" }
    → TTL: 3× window size in seconds

  event:meta:{event_id}
    → Redis Hash: { ticker, catalyst, published_at, title_snippet }
    → TTL: 4h

  event:signal:{event_id}
    → "1"  (SET NX gate — first signal emission)
    → TTL: SIGNAL_DEDUP_TTL_SEC (default 4h)

  event:signal:{event_id}:direction
    → "long" | "short"
    → TTL: same as above (for allow_opposite policy)

  event:order:{event_id}
    → broker_order_id  (SET NX gate — first order submission)
    → TTL: ORDER_DEDUP_TTL_SEC (default 24h)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import redis.asyncio as aioredis

from app.pipeline.bucket import bucket_key, bucket_ttl
from app.pipeline.simhash import (
    DEFAULT_HAMMING_THRESHOLD,
    compute_simhash,
    hamming_distance,
    simhash_from_hex,
    simhash_to_hex,
)
from app.pipeline.fact_fingerprint import fact_fingerprint, facts_overlap_score

_SIGNAL_DEDUP_TTL = int(os.environ.get("SIGNAL_DEDUP_TTL_SEC", "14400"))   # 4h
_ORDER_DEDUP_TTL  = int(os.environ.get("ORDER_DEDUP_TTL_SEC",  "86400"))   # 24h
_META_TTL         = 14400  # 4h
_HAMMING_THRESHOLD = int(os.environ.get("SIMHASH_HAMMING_THRESHOLD", str(DEFAULT_HAMMING_THRESHOLD)))


@dataclass
class ClusterResult:
    """Result of a match_or_create call."""
    event_id: str          # canonical event ID for this cluster
    is_representative: bool
    matched_simhash: int | None = None   # The existing hash that triggered the match
    fact_overlap: float = 0.5            # 0.0 differ / 0.5 unknown / 1.0 identical


class EventClusterStore:
    """
    Manages semantic event clusters backed by Redis.

    Usage:
        store = EventClusterStore(redis_client)
        result = await store.match_or_create(
            ticker="AAPL",
            catalyst="earnings",
            published_at=dt,
            title="Apple beats EPS ...",
            facts=facts_json_obj,
            incoming_event_id="uuid-of-this-item",
        )
        if result.is_representative:
            # First to arrive — process downstream
        else:
            # Duplicate — emit to news.dropped
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def match_or_create(
        self,
        ticker: str,
        catalyst: str,
        published_at: datetime,
        title: str,
        facts: object | None,
        incoming_event_id: str,
    ) -> ClusterResult:
        """
        Check whether this event matches an existing cluster entry.

        Algorithm:
          1. Compute SimHash of title
          2. Compute fact fingerprint (if facts available)
          3. Load the bucket hash from Redis
          4. For each stored entry:
             a. Compute Hamming distance between incoming and stored SimHash
             b. If distance <= threshold: headline match candidate
             c. If fact fingerprints are available and overlap > 0 → confirmed match
             d. Return existing event_id as non-representative
          5. If no match: store incoming as new representative, return is_representative=True
        """
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)

        incoming_sh = compute_simhash(title)
        incoming_fp = fact_fingerprint(facts)
        bkey = bucket_key(ticker, catalyst, published_at)
        ttl  = bucket_ttl(catalyst)

        # Load all entries in this bucket
        bucket_data: dict[str, str] = await self._redis.hgetall(bkey)

        for stored_sh_hex, stored_meta in bucket_data.items():
            stored_sh = simhash_from_hex(stored_sh_hex)
            dist = hamming_distance(incoming_sh, stored_sh)

            if dist > _HAMMING_THRESHOLD:
                continue  # Not similar enough

            # SimHash matches — parse stored metadata
            parts = stored_meta.split("|", 1)
            stored_event_id = parts[0]
            stored_fp = parts[1] if len(parts) > 1 and parts[1] else None

            overlap = facts_overlap_score(incoming_fp, stored_fp)
            if overlap == 0.0:
                # Same headline, but confirmed different facts (e.g. different EPS)
                # Treat as a distinct event despite headline similarity
                continue

            # Match confirmed
            return ClusterResult(
                event_id=stored_event_id,
                is_representative=False,
                matched_simhash=stored_sh,
                fact_overlap=overlap,
            )

        # No match — register this event as representative in the bucket
        meta_value = f"{incoming_event_id}|{incoming_fp or ''}"
        await self._redis.hset(bkey, simhash_to_hex(incoming_sh), meta_value)
        await self._redis.expire(bkey, ttl)

        # Store event metadata for audit
        await self._redis.hset(
            f"event:meta:{incoming_event_id}",
            mapping={
                "ticker":   ticker,
                "catalyst": catalyst,
                "simhash":  simhash_to_hex(incoming_sh),
                "fact_fp":  incoming_fp or "",
            },
        )
        await self._redis.expire(f"event:meta:{incoming_event_id}", _META_TTL)

        return ClusterResult(
            event_id=incoming_event_id,
            is_representative=True,
        )

    async def mark_signal_emitted(
        self,
        event_id: str,
        direction: str,
        multi_signal_policy: str = "deny",
    ) -> bool:
        """
        Atomic gate: mark that a signal has been emitted for this event.

        Returns True if this is the first emission (proceed).
        Returns False if a signal was already emitted and policy = "deny".

        For policy = "allow_opposite": returns True only if the direction differs
        from the stored direction.
        """
        signal_key = f"event:signal:{event_id}"
        dir_key    = f"event:signal:{event_id}:direction"

        if multi_signal_policy == "allow":
            # No gate — always allow
            await self._redis.set(signal_key, "1", ex=_SIGNAL_DEDUP_TTL)
            await self._redis.set(dir_key, direction, ex=_SIGNAL_DEDUP_TTL)
            return True

        # Atomic SET NX
        result = await self._redis.set(signal_key, "1", ex=_SIGNAL_DEDUP_TTL, nx=True)
        if result:
            # First emission
            await self._redis.set(dir_key, direction, ex=_SIGNAL_DEDUP_TTL)
            return True

        if multi_signal_policy == "allow_opposite":
            stored_dir = await self._redis.get(dir_key)
            if stored_dir and stored_dir != direction:
                # Opposite direction — allow as a new signal
                return True

        return False  # "deny" — already emitted

    async def mark_order_submitted(
        self,
        event_id: str,
        broker_order_id: str,
    ) -> bool:
        """
        Atomic gate: mark that an order has been submitted for this event.

        Returns True if this is the first submission (proceed).
        Returns False if already submitted.
        """
        order_key = f"event:order:{event_id}"
        result = await self._redis.set(
            order_key, broker_order_id, ex=_ORDER_DEDUP_TTL, nx=True
        )
        return bool(result)

    async def get_event_meta(self, event_id: str) -> dict | None:
        """Retrieve stored metadata for an event (for audit/debug)."""
        data = await self._redis.hgetall(f"event:meta:{event_id}")
        return data if data else None
