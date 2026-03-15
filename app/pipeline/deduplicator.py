"""
Deduplicator Service — Stage 2 of the processing pipeline.

Consumes: news.normalized
Emits:    news.deduped
         news.dropped  (non-representative items for audit)

Dedup strategy (in order):
  1. Exact URL match         — Redis SET, TTL 24h
  2. Exact hash match        — Redis SET, TTL 24h
  3. Fuzzy title match       — Trigram similarity via Postgres pg_trgm (>= 0.75)
  4. Semantic SimHash match  — 64-bit locality-sensitive hash, Hamming distance

For duplicates:
  - Non-representative items are emitted to news.dropped for audit.
  - Representative items continue to news.deduped as before.
  - Cluster ID is shared UUID for all items in a group.

Feature flags (env vars):
  ENABLE_SEMANTIC_DEDUP=true        — activate SimHash tier (default: true)
  DEDUP_EMIT_DROPPED_TOPIC=true     — emit non-rep items to news.dropped (default: true)
  DEDUP_SQLITE_FALLBACK_READ=false  — read old SQLite idempotency on startup (default: false)
"""
from __future__ import annotations

import asyncio
import json
import os
from uuid import UUID, uuid4

import redis.asyncio as aioredis

from app.config import settings
from app.models.news import DedupedRecord, DroppedRecord, NormalizedRecord
from app.pipeline.base_consumer import BaseConsumer, _log
from app.pipeline.dedup_reason import DedupAction, DedupReason, DedupTier
from app.pipeline.event_cluster import EventClusterStore

# TTL for dedup keys in Redis
DEDUP_TTL = settings.redis_dedup_ttl_seconds  # default 86400 = 24h

# Fuzzy match threshold — trigrams below this are considered distinct
SIMILARITY_THRESHOLD = 0.75

# Feature flags
_SEMANTIC_DEDUP   = os.environ.get("ENABLE_SEMANTIC_DEDUP",     "true").lower() == "true"
_EMIT_DROPPED     = os.environ.get("DEDUP_EMIT_DROPPED_TOPIC",  "true").lower() == "true"
_DROPPED_TOPIC    = "news.dropped"


class DeduplicatorService(BaseConsumer):

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._cluster_store: EventClusterStore | None = None
        self._db_pool = None
        super().__init__()

    @property
    def service_name(self) -> str:
        return "deduplicator"

    @property
    def input_topic(self) -> str:
        return settings.topic_news_normalized

    @property
    def output_topic(self) -> str:
        return settings.topic_news_deduped

    async def on_start(self) -> None:
        self._redis = await aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=10,
        )
        if _SEMANTIC_DEDUP:
            self._cluster_store = EventClusterStore(self._redis)
        # Import here to avoid circular at module load
        from app.db import get_engine
        from sqlalchemy.ext.asyncio import AsyncSession
        from sqlalchemy.orm import sessionmaker
        engine = get_engine()
        self._Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        _log("info", "deduplicator.connections_ready",
             semantic_dedup=_SEMANTIC_DEDUP,
             emit_dropped=_EMIT_DROPPED)

    async def on_stop(self) -> None:
        if self._redis:
            await self._redis.aclose()

    async def process(self, record: dict) -> dict | None:
        try:
            norm = NormalizedRecord.from_kafka_dict(record)
        except Exception as e:
            _log("error", "deduplicator.parse_error", error=str(e))
            raise

        # ── Step 1: Exact URL dedup ──────────────────────────────
        url_key = f"dedup:url:{norm.canonical_url}"
        existing_url = await self._redis.get(url_key)

        if existing_url:
            cluster_id = UUID(existing_url)
            _log("debug", "deduplicator.exact_url_match",
                 vendor_id=norm.vendor_id, cluster_id=str(cluster_id))
            return await self._make_duplicate(
                norm, cluster_id, "exact_url", 1.0,
                dedup_tier=DedupTier.VENDOR_ID,
                dedup_reason=DedupReason.VENDOR_ID_SEEN,
            )

        # ── Step 2: Exact hash dedup ─────────────────────────────
        hash_key = f"dedup:hash:{norm.content_hash}"
        existing_hash = await self._redis.get(hash_key)

        if existing_hash:
            cluster_id = UUID(existing_hash)
            _log("debug", "deduplicator.exact_hash_match",
                 vendor_id=norm.vendor_id, cluster_id=str(cluster_id))
            # Register this URL too so future URL lookups hit cache
            await self._redis.setex(url_key, DEDUP_TTL, str(cluster_id))
            return await self._make_duplicate(
                norm, cluster_id, "exact_hash", 1.0,
                dedup_tier=DedupTier.CONTENT_HASH,
                dedup_reason=DedupReason.CONTENT_HASH_MATCH,
            )

        # ── Step 3: Fuzzy title match via Postgres pg_trgm ───────
        cluster_id, similarity = await self._fuzzy_match(norm.title, norm.published_at)

        if cluster_id and similarity and similarity >= SIMILARITY_THRESHOLD:
            _log("debug", "deduplicator.fuzzy_match",
                 vendor_id=norm.vendor_id, similarity=round(similarity, 3),
                 cluster_id=str(cluster_id))
            await self._redis.setex(url_key, DEDUP_TTL, str(cluster_id))
            await self._redis.setex(hash_key, DEDUP_TTL, str(cluster_id))
            return await self._make_duplicate(
                norm, cluster_id, "similarity", similarity,
                dedup_tier=DedupTier.SEMANTIC,
                dedup_reason=DedupReason.SIMHASH_CLUSTER,
            )

        # ── Step 4: Semantic SimHash clustering ──────────────────
        if _SEMANTIC_DEDUP and self._cluster_store and norm.raw_tickers:
            # Use the first raw ticker as the primary for bucketing.
            # catalyst_type is not yet available at this stage (resolved in enricher),
            # so we default to "other" — broad enough to cluster multi-vendor same stories.
            primary_ticker = norm.raw_tickers[0].upper()
            cluster_result = await self._cluster_store.match_or_create(
                ticker=primary_ticker,
                catalyst="other",
                published_at=norm.published_at,
                title=norm.title,
                facts=None,   # facts not available at normalization stage
                incoming_event_id=str(norm.id),
            )
            if not cluster_result.is_representative:
                semantic_cluster_id = UUID(cluster_result.event_id) if _is_valid_uuid(cluster_result.event_id) else uuid4()
                _log("info", "deduplicator.simhash_match",
                     vendor_id=norm.vendor_id,
                     matched_event_id=cluster_result.event_id,
                     fact_overlap=cluster_result.fact_overlap)
                await self._redis.setex(url_key, DEDUP_TTL, str(semantic_cluster_id))
                await self._redis.setex(hash_key, DEDUP_TTL, str(semantic_cluster_id))
                return await self._make_duplicate(
                    norm, semantic_cluster_id, "simhash", cluster_result.fact_overlap,
                    dedup_tier=DedupTier.SEMANTIC,
                    dedup_reason=(
                        DedupReason.FACT_CONFIRMED
                        if cluster_result.fact_overlap == 1.0
                        else DedupReason.SIMHASH_CLUSTER
                    ),
                )

        # ── New unique item ───────────────────────────────────────
        new_cluster_id = uuid4()
        await self._redis.setex(url_key, DEDUP_TTL, str(new_cluster_id))
        await self._redis.setex(hash_key, DEDUP_TTL, str(new_cluster_id))

        # Store title in Postgres for future fuzzy matching
        await self._store_title(norm, new_cluster_id)

        deduped = DedupedRecord(
            **norm.model_dump(),
            cluster_id=new_cluster_id,
            is_representative=True,
            dedup_method=None,
            similarity_score=None,
            dedup_tier=DedupTier.NONE,
            dedup_reason=DedupReason.NEW,
        )
        return deduped.to_kafka_dict()

    async def _make_duplicate(
        self,
        norm: NormalizedRecord,
        cluster_id: UUID,
        method: str,
        score: float,
        dedup_tier: DedupTier = DedupTier.CONTENT_HASH,
        dedup_reason: DedupReason = DedupReason.CONTENT_HASH_MATCH,
    ) -> dict:
        """Build a non-representative deduped record and optionally emit to news.dropped."""
        deduped = DedupedRecord(
            **norm.model_dump(),
            cluster_id=cluster_id,
            is_representative=False,
            dedup_method=method,
            similarity_score=score,
            dedup_tier=dedup_tier,
            dedup_reason=dedup_reason,
        )

        # Emit to news.dropped for audit trail
        if _EMIT_DROPPED:
            try:
                dropped = DroppedRecord(
                    id=norm.id,
                    source=norm.source,
                    vendor_id=norm.vendor_id,
                    title=norm.title,
                    published_at=norm.published_at,
                    cluster_id=cluster_id,
                    dedup_tier=dedup_tier,
                    dedup_reason=dedup_reason,
                    similarity_score=score,
                )
                self._producer.produce(_DROPPED_TOPIC, value=dropped.to_kafka_dict())
            except Exception as e:
                _log("warning", "deduplicator.dropped_emit_error", error=str(e))

        return deduped.to_kafka_dict()

    async def _fuzzy_match(
        self,
        title: str,
        published_at,
    ) -> tuple[UUID | None, float | None]:
        """
        Query Postgres for similar titles within a 6-hour window.
        Uses pg_trgm similarity() function.
        Returns (cluster_id, similarity_score) or (None, None).
        """
        if not self._Session:
            return None, None

        try:
            # Look back 6 hours — same story shouldn't appear much later
            from sqlalchemy import text
            from datetime import timedelta

            query = text("""
                SELECT ni.cluster_id, similarity(ni.title, :title) AS sim
                FROM news_item ni
                WHERE ni.published_at >= :cutoff
                  AND similarity(ni.title, :title) >= :threshold
                  AND ni.cluster_id IS NOT NULL
                ORDER BY sim DESC
                LIMIT 1
            """)

            cutoff = published_at - timedelta(hours=6)

            async with self._Session() as session:
                result = await session.execute(query, {
                    "title": title,
                    "cutoff": cutoff,
                    "threshold": SIMILARITY_THRESHOLD,
                })
                row = result.fetchone()

            if row:
                return UUID(str(row.cluster_id)), float(row.sim)
            return None, None

        except Exception as e:
            _log("warning", "deduplicator.fuzzy_match_error", error=str(e))
            return None, None

    async def _store_title(self, norm: NormalizedRecord, cluster_id: UUID) -> None:
        """
        Upsert into news_cluster + news_item so items are available for future
        fuzzy matches.

        IMPORTANT: news_cluster must be inserted BEFORE news_item because
        news_item.cluster_id has a FK constraint referencing news_cluster.id.
        Both writes are in a single transaction for atomicity.
        """
        if not self._Session:
            return
        try:
            from sqlalchemy import text
            async with self._Session() as session:
                # Step 1: Ensure the cluster row exists first
                await session.execute(text("""
                                           INSERT INTO news_cluster (id)
                                           VALUES (:cluster_id) ON CONFLICT (id) DO NOTHING
                                           """), {"cluster_id": str(cluster_id)})

                # Step 2: Insert the news_item referencing the cluster
                await session.execute(text("""
                                           INSERT INTO news_item (id, source, vendor_id, published_at, received_at,
                                                                  url, canonical_url, title, snippet, content_hash,
                                                                  cluster_id)
                                           VALUES (:id, :source, :vendor_id, :published_at, :received_at,
                                                   :url, :canonical_url, :title, :snippet, :content_hash,
                                                   :cluster_id) ON CONFLICT (source, vendor_id) DO NOTHING
                                           """), {
                                          "id": str(norm.id),
                                          "source": norm.source.value,
                                          "vendor_id": norm.vendor_id,
                                          "published_at": norm.published_at,
                                          "received_at": norm.received_at,
                                          "url": norm.url,
                                          "canonical_url": norm.canonical_url,
                                          "title": norm.title,
                                          "snippet": norm.snippet,
                                          "content_hash": norm.content_hash,
                                          "cluster_id": str(cluster_id),
                                      })
                await session.commit()
        except Exception as e:
            _log("warning", "deduplicator.store_title_error", error=str(e))


def _is_valid_uuid(s: str) -> bool:
    try:
        UUID(s)
        return True
    except ValueError:
        return False
