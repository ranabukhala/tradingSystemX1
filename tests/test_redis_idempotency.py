"""
Tests for Redis-backed atomic idempotency.

Coverage matrix
───────────────
TestFakeRedisAtomicity          — verifies the in-process mock's SETNX semantics
TestRedisIdempotencyStore       — unit: new / dup / stage isolation / payload_hash
TestConcurrentDuplicateHandling — 50/100-task bursts; exactly one winner
TestFallbackBehavior            — degraded mode: Redis down → SQLite → fail-open → recovery
TestTTLPolicy                   — TTL stored on first set; expired key = new event
TestVendorIdDedup               — vendor_id-keyed event_ids; content-dedup is orthogonal
TestSQLiteAsyncAdapter          — _SQLiteAsyncAdapter wraps sync store correctly
TestBaseConsumerIdempotency     — integration: run() uses async store; fallback metric fires

Docker-only packages are mocked at import time so tests run without Docker.
"""
from __future__ import annotations

import asyncio
import sys
from collections import Counter as _Counter
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

# ── Mock Docker-only packages before any pipeline imports ──────────────────────

if "confluent_kafka" not in sys.modules:
    _ck = MagicMock()
    # Give TopicPartition realistic attributes for lag tests
    class _TopicPartition:
        OFFSET_INVALID = -1001
        def __init__(self, topic="", partition=0, offset=-1001):
            self.topic = topic
            self.partition = partition
            self.offset = offset
    _ck.TopicPartition = _TopicPartition
    _ck.OFFSET_INVALID = -1001
    _ck.KafkaError = MagicMock()
    _ck.KafkaException = Exception
    sys.modules["confluent_kafka"] = _ck
    sys.modules["confluent_kafka.admin"] = MagicMock()

if "prometheus_client" not in sys.modules:
    _prom = MagicMock()
    _prom.Counter.return_value = MagicMock()
    _prom.Gauge.return_value = MagicMock()
    _prom.Histogram.return_value = MagicMock()
    _prom.start_http_server = MagicMock()
    sys.modules["prometheus_client"] = _prom

# Now safe to import pipeline code
from app.idempotency_redis import RedisIdempotencyStore, _SQLiteAsyncAdapter


# ── In-process Redis mock ───────────────────────────────────────────────────────

class FakeRedis:
    """
    Thread-and-coroutine-safe in-memory Redis mock.

    Replicates SET … NX EX semantics with an asyncio.Lock so concurrent
    coroutines cannot both "win" the SETNX for the same key.
    """

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._down = False          # Simulate Redis outage
        self._close_called = False

    async def set(self, key: str, value: str, ex: int | None = None,
                  nx: bool = False) -> bool | None:
        if self._down:
            from redis.exceptions import ConnectionError as _RCE
            raise _RCE("Simulated Redis failure")
        async with self._lock:
            if nx and key in self._store:
                return None          # Key existed — SETNX failed
            self._store[key] = value
            return True              # Key was set

    async def get(self, key: str) -> str | None:
        if self._down:
            from redis.exceptions import ConnectionError as _RCE
            raise _RCE("Simulated Redis failure")
        return self._store.get(key)

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def aclose(self) -> None:
        self._close_called = True

    # ── Test helpers ──────────────────────────────────────────────────────
    def go_down(self) -> None:
        self._down = True

    def come_back(self) -> None:
        self._down = False

    def has_key(self, key: str) -> bool:
        return key in self._store


class FakeSQLiteStore:
    """Minimal sync IdempotencyStore replacement for fallback tests."""

    def __init__(self) -> None:
        self._seen: set = set()
        self.close_called = False
        self.call_count = 0

    def check_and_mark(self, stage: str, event_id: str,
                       payload_hash: str | None = None) -> bool:
        self.call_count += 1
        key = (stage, event_id)
        if key in self._seen:
            return True
        self._seen.add(key)
        return False

    def close(self) -> None:
        self.close_called = True


# ─────────────────────────────────────────────────────────────────────────────
# 1. FakeRedis self-test — the mock must honour SETNX atomicity before we trust it
# ─────────────────────────────────────────────────────────────────────────────

class TestFakeRedisAtomicity:

    @pytest.mark.asyncio
    async def test_setnx_first_call_returns_true(self):
        r = FakeRedis()
        result = await r.set("k", "v", nx=True)
        assert result is True

    @pytest.mark.asyncio
    async def test_setnx_second_call_returns_none(self):
        r = FakeRedis()
        await r.set("k", "v", nx=True)
        result = await r.set("k", "v2", nx=True)
        assert result is None

    @pytest.mark.asyncio
    async def test_set_without_nx_overwrites(self):
        r = FakeRedis()
        await r.set("k", "first")
        await r.set("k", "second")
        assert await r.get("k") == "second"

    @pytest.mark.asyncio
    async def test_concurrent_setnx_exactly_one_winner(self):
        r = FakeRedis()
        results = await asyncio.gather(
            *[r.set("burst", "v", nx=True) for _ in range(50)]
        )
        winners = [x for x in results if x is True]
        losers  = [x for x in results if x is None]
        assert len(winners) == 1
        assert len(losers)  == 49

    @pytest.mark.asyncio
    async def test_down_flag_raises_connection_error(self):
        from redis.exceptions import ConnectionError as _RCE
        r = FakeRedis()
        r.go_down()
        with pytest.raises(_RCE):
            await r.set("k", "v")

    @pytest.mark.asyncio
    async def test_come_back_restores_normal_operation(self):
        r = FakeRedis()
        r.go_down()
        r.come_back()
        result = await r.set("k", "v", nx=True)
        assert result is True


# ─────────────────────────────────────────────────────────────────────────────
# 2. RedisIdempotencyStore — unit behaviour
# ─────────────────────────────────────────────────────────────────────────────

class TestRedisIdempotencyStore:

    def _make_store(self, **kw) -> tuple[RedisIdempotencyStore, FakeRedis]:
        r = FakeRedis()
        store = RedisIdempotencyStore(redis_client=r, ttl=3600, **kw)
        return store, r

    @pytest.mark.asyncio
    async def test_new_event_returns_false(self):
        store, _ = self._make_store()
        result = await store.check_and_mark("deduplicator", "evt-001")
        assert result is False

    @pytest.mark.asyncio
    async def test_duplicate_event_returns_true(self):
        store, _ = self._make_store()
        await store.check_and_mark("deduplicator", "evt-001")
        result = await store.check_and_mark("deduplicator", "evt-001")
        assert result is True

    @pytest.mark.asyncio
    async def test_same_event_different_stages_both_new(self):
        """Stage namespace must isolate keys — deduplicator vs normalizer."""
        store, _ = self._make_store()
        r1 = await store.check_and_mark("normalizer", "evt-001")
        r2 = await store.check_and_mark("deduplicator", "evt-001")
        assert r1 is False
        assert r2 is False

    @pytest.mark.asyncio
    async def test_different_events_both_new(self):
        store, _ = self._make_store()
        r1 = await store.check_and_mark("deduplicator", "evt-001")
        r2 = await store.check_and_mark("deduplicator", "evt-002")
        assert r1 is False
        assert r2 is False

    @pytest.mark.asyncio
    async def test_three_replays_only_first_is_new(self):
        store, _ = self._make_store()
        processed = []
        for _ in range(3):
            if not await store.check_and_mark("execution", "sig-abc"):
                processed.append("sig-abc")
        assert len(processed) == 1

    @pytest.mark.asyncio
    async def test_key_format(self):
        """Key stored in Redis must be idem:{stage}:{event_id}."""
        store, fake_r = self._make_store()
        await store.check_and_mark("signal_aggregator", "evt-xyz")
        assert fake_r.has_key("idem:signal_aggregator:evt-xyz")

    @pytest.mark.asyncio
    async def test_value_is_iso_timestamp(self):
        store, fake_r = self._make_store()
        await store.check_and_mark("normalizer", "evt-ts")
        val = await fake_r.get("idem:normalizer:evt-ts")
        assert val is not None
        # Should parse as a valid ISO-8601 datetime
        dt = datetime.fromisoformat(val)
        assert dt.tzinfo is not None  # timezone-aware

    @pytest.mark.asyncio
    async def test_fallback_mode_false_initially(self):
        store, _ = self._make_store()
        assert store._fallback_mode is False

    @pytest.mark.asyncio
    async def test_close_calls_redis_aclose(self):
        store, fake_r = self._make_store()
        await store.close()
        assert fake_r._close_called is True

    @pytest.mark.asyncio
    async def test_payload_hash_accepted_but_not_required(self):
        """payload_hash is accepted for API compatibility; Redis store ignores it."""
        store, _ = self._make_store()
        r1 = await store.check_and_mark("normalizer", "evt-ph", payload_hash="abc123")
        r2 = await store.check_and_mark("normalizer", "evt-ph", payload_hash="abc123")
        assert r1 is False
        assert r2 is True


# ─────────────────────────────────────────────────────────────────────────────
# 3. Concurrent duplicate handling — the core safety guarantee
# ─────────────────────────────────────────────────────────────────────────────

class TestConcurrentDuplicateHandling:

    @pytest.mark.asyncio
    async def test_50_concurrent_tasks_same_event_id_one_winner(self):
        """
        50 tasks race to mark the same (stage, event_id).
        Exactly one must see False (new); 49 must see True (dup).
        """
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        results = await asyncio.gather(
            *[store.check_and_mark("deduplicator", "burst-event") for _ in range(50)]
        )
        assert results.count(False) == 1, "exactly one task must win the SETNX race"
        assert results.count(True)  == 49

    @pytest.mark.asyncio
    async def test_100_concurrent_tasks_same_event_id_one_winner(self):
        """Scaled-up version to stress-test FakeRedis locking."""
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        results = await asyncio.gather(
            *[store.check_and_mark("normalizer", "burst-100") for _ in range(100)]
        )
        assert results.count(False) == 1
        assert results.count(True)  == 99

    @pytest.mark.asyncio
    async def test_100_concurrent_tasks_unique_event_ids_all_new(self):
        """Different event_ids must all succeed independently."""
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        results = await asyncio.gather(
            *[store.check_and_mark("deduplicator", f"unique-{i}") for i in range(100)]
        )
        assert all(r is False for r in results)

    @pytest.mark.asyncio
    async def test_interleaved_new_and_dup_calls_correct_counts(self):
        """
        10 distinct event_ids, each submitted 5 times concurrently.
        Each should see exactly 1 new and 4 dups.
        """
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        n_events = 10
        repeats  = 5

        coros = [
            store.check_and_mark("deduplicator", f"ev-{ev_i}")
            for ev_i in range(n_events)
            for _ in range(repeats)
        ]
        results_flat = await asyncio.gather(*coros)

        # Group results by event_id (every repeats-sized block)
        for ev_i in range(n_events):
            block = results_flat[ev_i * repeats : (ev_i + 1) * repeats]
            assert block.count(False) == 1, f"event {ev_i}: expected exactly 1 new"
            assert block.count(True)  == repeats - 1

    @pytest.mark.asyncio
    async def test_multi_stage_concurrent_same_event_id(self):
        """Same event_id across 3 stages concurrently — all should be new."""
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        stages = ["normalizer", "deduplicator", "signal_aggregator"]
        results = await asyncio.gather(
            *[store.check_and_mark(s, "shared-evt") for s in stages]
        )
        assert all(r is False for r in results), "different stages must not share keys"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Fallback / degraded-mode behaviour
# ─────────────────────────────────────────────────────────────────────────────

class TestFallbackBehavior:

    def _make_down_store(self, fallback=None):
        fake_r = FakeRedis()
        fake_r.go_down()
        store = RedisIdempotencyStore(
            redis_client=fake_r, ttl=3600, fallback_store=fallback
        )
        return store, fake_r

    @pytest.mark.asyncio
    async def test_redis_down_delegates_to_sqlite_fallback(self):
        sqlite = FakeSQLiteStore()
        store, _ = self._make_down_store(fallback=sqlite)
        result = await store.check_and_mark("deduplicator", "evt-001")
        assert result is False          # SQLite says new
        assert sqlite.call_count == 1

    @pytest.mark.asyncio
    async def test_redis_down_sqlite_fallback_detects_duplicate(self):
        sqlite = FakeSQLiteStore()
        store, _ = self._make_down_store(fallback=sqlite)
        await store.check_and_mark("deduplicator", "evt-001")
        result = await store.check_and_mark("deduplicator", "evt-001")
        assert result is True           # SQLite deduplicates in degraded mode

    @pytest.mark.asyncio
    async def test_redis_down_no_fallback_fails_open(self):
        """With no fallback, every call should return False (fail-open)."""
        store, _ = self._make_down_store(fallback=None)
        results = [await store.check_and_mark("deduplicator", "evt-fail-open")
                   for _ in range(3)]
        # All return False (new) — pipeline never stalls
        assert all(r is False for r in results)

    @pytest.mark.asyncio
    async def test_fallback_mode_set_on_first_failure(self):
        store, _ = self._make_down_store(fallback=FakeSQLiteStore())
        assert store._fallback_mode is False
        await store.check_and_mark("deduplicator", "evt-001")
        assert store._fallback_mode is True

    @pytest.mark.asyncio
    async def test_fallback_mode_clears_on_redis_recovery(self):
        sqlite = FakeSQLiteStore()
        fake_r = FakeRedis()
        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600,
                                      fallback_store=sqlite)

        # Trigger degraded mode
        fake_r.go_down()
        await store.check_and_mark("deduplicator", "evt-down")
        assert store._fallback_mode is True

        # Redis comes back
        fake_r.come_back()
        await store.check_and_mark("deduplicator", "evt-recovered")
        assert store._fallback_mode is False

    @pytest.mark.asyncio
    async def test_fallback_sqlite_also_fails_returns_false(self):
        """Both backends failing → fail-open (False = new event)."""
        class BrokenSQLite:
            def check_and_mark(self, *a, **kw):
                raise RuntimeError("SQLite also dead")

        fake_r = FakeRedis()
        fake_r.go_down()
        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600,
                                      fallback_store=BrokenSQLite())
        result = await store.check_and_mark("deduplicator", "evt")
        assert result is False  # Fail-open

    @pytest.mark.asyncio
    async def test_fallback_not_active_while_redis_healthy(self):
        sqlite = FakeSQLiteStore()
        fake_r = FakeRedis()  # healthy
        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600,
                                      fallback_store=sqlite)
        await store.check_and_mark("deduplicator", "evt-001")
        # SQLite must NOT have been called — Redis handled it
        assert sqlite.call_count == 0
        assert store._fallback_mode is False

    @pytest.mark.asyncio
    async def test_close_does_not_close_fallback_store(self):
        """Fallback store is owned by caller (BaseConsumer); store must not close it."""
        sqlite = FakeSQLiteStore()
        fake_r = FakeRedis()
        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600,
                                      fallback_store=sqlite)
        await store.close()
        assert sqlite.close_called is False  # Caller closes SQLite separately


# ─────────────────────────────────────────────────────────────────────────────
# 5. TTL policy
# ─────────────────────────────────────────────────────────────────────────────

class TestTTLPolicy:

    @pytest.mark.asyncio
    async def test_custom_ttl_passed_to_redis(self):
        """Verify the ttl constructor arg is forwarded to FakeRedis.set()."""
        calls = []

        class RecordingRedis(FakeRedis):
            async def set(self, key, value, ex=None, nx=False):
                calls.append({"key": key, "ex": ex, "nx": nx})
                return await super().set(key, value, ex=ex, nx=nx)

        store = RedisIdempotencyStore(redis_client=RecordingRedis(), ttl=7200)
        await store.check_and_mark("deduplicator", "evt-ttl")
        assert calls[0]["ex"] == 7200

    @pytest.mark.asyncio
    async def test_default_ttl_is_48h(self):
        """Default TTL must be 172800 s (48 h) when constructed without ttl arg."""
        store = RedisIdempotencyStore(redis_client=FakeRedis())
        assert store._ttl == 172800

    @pytest.mark.asyncio
    async def test_expired_key_treated_as_new_event(self):
        """
        Simulate TTL expiry by deleting the key directly.
        The store must treat the missing key as a new (unprocessed) event.
        """
        fake_r = FakeRedis()
        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600)

        # First processing
        r1 = await store.check_and_mark("deduplicator", "evt-expires")
        assert r1 is False

        # Simulate Redis key expiry
        await fake_r.delete("idem:deduplicator:evt-expires")

        # Re-process — should be treated as new
        r2 = await store.check_and_mark("deduplicator", "evt-expires")
        assert r2 is False

    @pytest.mark.asyncio
    async def test_key_persists_before_expiry(self):
        """A marked key must be detectable as a dup before TTL elapses."""
        fake_r = FakeRedis()
        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600)
        await store.check_and_mark("deduplicator", "evt-persist")
        # Key should exist in the fake store
        assert fake_r.has_key("idem:deduplicator:evt-persist")

    @pytest.mark.asyncio
    async def test_nx_always_set_on_first_call(self):
        """set(nx=True) is always used — never an overwrite on first call."""
        nx_calls = []

        class InspectingRedis(FakeRedis):
            async def set(self, key, value, ex=None, nx=False):
                nx_calls.append(nx)
                return await super().set(key, value, ex=ex, nx=nx)

        store = RedisIdempotencyStore(redis_client=InspectingRedis(), ttl=60)
        await store.check_and_mark("deduplicator", "evt-nx")
        assert all(nx_calls), "every set() call must use nx=True"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Vendor-id dedup: the base-level idempotency uses event_id, not vendor_id.
#    Vendor-id duplicate elimination is handled by the deduplicator's own
#    4-tier Redis content check (URL/hash/fuzzy/SimHash).
#    These tests verify the two layers are orthogonal.
# ─────────────────────────────────────────────────────────────────────────────

class TestVendorIdDedup:

    @pytest.mark.asyncio
    async def test_same_vendor_id_different_event_ids_both_new(self):
        """Two events from the same vendor but distinct pipeline IDs = both new."""
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        # Simulate two Kafka replays of different messages that share a vendor
        r1 = await store.check_and_mark("deduplicator", "benzinga-story-001-attempt-1")
        r2 = await store.check_and_mark("deduplicator", "benzinga-story-001-attempt-2")
        assert r1 is False
        assert r2 is False  # Different event_ids → different keys

    @pytest.mark.asyncio
    async def test_same_vendor_id_same_event_id_is_dup(self):
        """Exact replay of the same pipeline event_id must be blocked."""
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        event_id = "benzinga-story-002-v1"
        r1 = await store.check_and_mark("deduplicator", event_id)
        r2 = await store.check_and_mark("deduplicator", event_id)
        assert r1 is False
        assert r2 is True

    @pytest.mark.asyncio
    async def test_vendor_ids_from_multiple_sources_independent(self):
        """Same event_id prefix from benzinga vs polygon = same pipeline id → dup."""
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        # Both connectors happened to produce the same event_id (shouldn't happen
        # in practice, but base idempotency must still catch it)
        shared_event_id = "polygon-story-007"
        r1 = await store.check_and_mark("normalizer", shared_event_id)
        r2 = await store.check_and_mark("normalizer", shared_event_id)
        assert r1 is False
        assert r2 is True

    @pytest.mark.asyncio
    async def test_100_concurrent_same_vendor_event_exactly_one_processed(self):
        """
        Simulates 100 concurrent Kafka consumers all receiving the same vendor
        message (e.g. after a consumer-group rebalance that causes re-delivery).
        Only one must process it.
        """
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        event_id = "finnhub-rebalance-replay-event"
        results = await asyncio.gather(
            *[store.check_and_mark("deduplicator", event_id) for _ in range(100)]
        )
        assert results.count(False) == 1
        assert results.count(True)  == 99


# ─────────────────────────────────────────────────────────────────────────────
# 7. _SQLiteAsyncAdapter — backward-compat shim
# ─────────────────────────────────────────────────────────────────────────────

class TestSQLiteAsyncAdapter:

    @pytest.mark.asyncio
    async def test_delegates_to_sync_store(self):
        sqlite = FakeSQLiteStore()
        adapter = _SQLiteAsyncAdapter(sqlite)
        result = await adapter.check_and_mark("normalizer", "evt-001")
        assert result is False
        assert sqlite.call_count == 1

    @pytest.mark.asyncio
    async def test_duplicate_detected_via_sync_store(self):
        sqlite = FakeSQLiteStore()
        adapter = _SQLiteAsyncAdapter(sqlite)
        await adapter.check_and_mark("normalizer", "evt-001")
        result = await adapter.check_and_mark("normalizer", "evt-001")
        assert result is True

    @pytest.mark.asyncio
    async def test_stage_isolation(self):
        sqlite = FakeSQLiteStore()
        adapter = _SQLiteAsyncAdapter(sqlite)
        r1 = await adapter.check_and_mark("normalizer",   "evt-001")
        r2 = await adapter.check_and_mark("deduplicator", "evt-001")
        assert r1 is False
        assert r2 is False

    @pytest.mark.asyncio
    async def test_close_is_noop(self):
        """_SQLiteAsyncAdapter.close() must not close the underlying store."""
        sqlite = FakeSQLiteStore()
        adapter = _SQLiteAsyncAdapter(sqlite)
        await adapter.close()
        assert sqlite.close_called is False

    @pytest.mark.asyncio
    async def test_fallback_mode_always_false(self):
        """_SQLiteAsyncAdapter never enters fallback mode (no Redis involved)."""
        adapter = _SQLiteAsyncAdapter(FakeSQLiteStore())
        assert adapter._fallback_mode is False
        await adapter.check_and_mark("deduplicator", "evt")
        assert adapter._fallback_mode is False

    @pytest.mark.asyncio
    async def test_payload_hash_forwarded(self):
        calls = []

        class RecordingSQLite:
            def check_and_mark(self, stage, event_id, payload_hash=None):
                calls.append({"stage": stage, "event_id": event_id,
                               "payload_hash": payload_hash})
                return False

        adapter = _SQLiteAsyncAdapter(RecordingSQLite())
        await adapter.check_and_mark("normalizer", "evt-ph", payload_hash="sha256-abc")
        assert calls[0]["payload_hash"] == "sha256-abc"


# ─────────────────────────────────────────────────────────────────────────────
# 8. BaseConsumer integration — _init_redis_idempotency wiring
# ─────────────────────────────────────────────────────────────────────────────

class _DummyConsumer:
    """
    Minimal BaseConsumer subclass built via __new__ to bypass __init__
    (avoids Prometheus duplicate registration and Kafka connection setup).
    All metrics are replaced with MagicMocks.
    """

    @classmethod
    def build(cls):
        from app.pipeline.base_consumer import BaseConsumer

        class _Concrete(BaseConsumer):
            service_name = "test_consumer"
            input_topic  = "test.input"
            output_topic = "test.output"

            async def process(self, record):
                return record

        svc = object.__new__(_Concrete)
        svc._running = False
        svc._consumer = None
        svc._idempotency = None
        svc._redis_idem = None
        svc._current_lag = 0
        svc._last_lag_check = 0.0
        # Mock all metric attributes so no Prometheus registration happens
        for attr in [
            "metric_consumed", "metric_emitted", "metric_dropped",
            "metric_errors", "metric_dlq", "metric_dedup_skipped",
            "metric_process_latency", "metric_lag", "metric_up",
            "metric_idem_fallback",
        ]:
            setattr(svc, attr, MagicMock())
        return svc


class TestBaseConsumerIdempotency:

    @pytest.mark.asyncio
    async def test_redis_backend_creates_redis_store(self):
        svc = _DummyConsumer.build()
        fake_r = FakeRedis()

        with (
            patch("app.pipeline.base_consumer.settings") as mock_cfg,
            patch("redis.asyncio.from_url", return_value=fake_r),
        ):
            mock_cfg.idempotency_backend = "redis"
            mock_cfg.idempotency_ttl_seconds = 172800
            mock_cfg.idempotency_fallback_enabled = False
            mock_cfg.redis_url = "redis://localhost:6379/0"

            store = await svc._init_redis_idempotency()

        assert isinstance(store, RedisIdempotencyStore)

    @pytest.mark.asyncio
    async def test_sqlite_backend_creates_adapter(self):
        svc = _DummyConsumer.build()
        sqlite = FakeSQLiteStore()
        svc._idempotency = sqlite

        with patch("app.pipeline.base_consumer.settings") as mock_cfg:
            mock_cfg.idempotency_backend = "sqlite"
            store = await svc._init_redis_idempotency()

        assert isinstance(store, _SQLiteAsyncAdapter)

    @pytest.mark.asyncio
    async def test_fallback_enabled_shares_sqlite_store(self):
        """When fallback is enabled, the SQLite store inside the Redis store
        must be the same object as svc._idempotency."""
        svc = _DummyConsumer.build()
        sqlite = FakeSQLiteStore()
        svc._idempotency = sqlite

        fake_r = FakeRedis()

        with (
            patch("app.pipeline.base_consumer.settings") as mock_cfg,
            patch("redis.asyncio.from_url", return_value=fake_r),
        ):
            mock_cfg.idempotency_backend = "redis"
            mock_cfg.idempotency_ttl_seconds = 172800
            mock_cfg.idempotency_fallback_enabled = True
            mock_cfg.redis_url = "redis://localhost:6379/0"

            store = await svc._init_redis_idempotency()

        assert store._fallback is sqlite

    @pytest.mark.asyncio
    async def test_fallback_metric_incremented_during_redis_outage(self):
        """
        When Redis is down and fallback activates, metric_idem_redis_fallback_total
        must be incremented for each message processed via the fallback path.
        """
        svc = _DummyConsumer.build()
        fake_r = FakeRedis()
        fake_r.go_down()
        sqlite = FakeSQLiteStore()

        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600,
                                      fallback_store=sqlite)
        svc._redis_idem = store

        # Simulate what run() does for each message
        for event_id in ["evt-001", "evt-002", "evt-003"]:
            await store.check_and_mark("test_consumer", event_id)
            if store._fallback_mode:
                svc.metric_idem_fallback.labels(service="test_consumer").inc()

        # The metric should have been incremented 3 times
        assert svc.metric_idem_fallback.labels.call_count == 3

    @pytest.mark.asyncio
    async def test_fallback_metric_not_incremented_when_redis_healthy(self):
        svc = _DummyConsumer.build()
        fake_r = FakeRedis()  # Healthy

        store = RedisIdempotencyStore(redis_client=fake_r, ttl=3600)
        svc._redis_idem = store

        for event_id in ["evt-001", "evt-002"]:
            await store.check_and_mark("test_consumer", event_id)
            if store._fallback_mode:
                svc.metric_idem_fallback.labels(service="test_consumer").inc()

        svc.metric_idem_fallback.labels.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_burst_through_store_in_consumer(self):
        """
        Replicate the deduplicator container receiving 50 identical messages
        during a consumer-group rebalance.  Only one must be processed.
        """
        svc = _DummyConsumer.build()
        store = RedisIdempotencyStore(redis_client=FakeRedis(), ttl=3600)
        svc._redis_idem = store

        processed = []
        async def handle(event_id: str) -> None:
            already = await store.check_and_mark("deduplicator", event_id)
            if not already:
                processed.append(event_id)

        await asyncio.gather(*[handle("rebalance-event") for _ in range(50)])
        assert len(processed) == 1, "only one task must process the event"
