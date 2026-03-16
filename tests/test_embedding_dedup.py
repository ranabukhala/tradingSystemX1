"""
Tests for Tier 3.5 — Semantic Embedding Deduplication.

Covers:
  • embedding.py  — compute_embedding / compute_embeddings helpers
  • deduplicator.py — _embedding_match and _store_title(embedding=…) integration
  • DeduplicatorService.process() — full path through Tier 3.5

All network and DB calls are mocked; no live OpenAI API key required.
"""
from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Stub out heavy optional dependencies BEFORE importing the modules under test
# ─────────────────────────────────────────────────────────────────────────────

# Stub confluent_kafka so BaseConsumer can be imported without the C extension
# Stub sqlalchemy (not installed in local test env — only in the Docker container)
if "sqlalchemy" not in sys.modules:
    sa = types.ModuleType("sqlalchemy")
    # text() just returns a string-like sentinel so the query param can be inspected
    sa.text = lambda q: q
    sys.modules["sqlalchemy"] = sa
    # Sub-modules referenced by ORM / session imports
    for _sub in ("sqlalchemy.ext", "sqlalchemy.ext.asyncio", "sqlalchemy.orm",
                 "sqlalchemy.pool", "sqlalchemy.engine"):
        sys.modules[_sub] = types.ModuleType(_sub)
    # AsyncSession and sessionmaker stubs
    sys.modules["sqlalchemy.ext.asyncio"].AsyncSession = MagicMock()
    sys.modules["sqlalchemy.orm"].sessionmaker = MagicMock()

# Stub prometheus_client
if "prometheus_client" not in sys.modules:
    pc = types.ModuleType("prometheus_client")
    pc.Counter = MagicMock(return_value=MagicMock())
    pc.Histogram = MagicMock(return_value=MagicMock())
    pc.Gauge = MagicMock(return_value=MagicMock())
    pc.start_http_server = MagicMock()
    sys.modules["prometheus_client"] = pc

if "confluent_kafka" not in sys.modules:
    ck = types.ModuleType("confluent_kafka")
    ck.Consumer = MagicMock()
    ck.Producer = MagicMock()
    ck.KafkaException = Exception
    ck.KafkaError = Exception      # needed by base_consumer
    sys.modules["confluent_kafka"] = ck
    admin_mod = types.ModuleType("confluent_kafka.admin")
    admin_mod.AdminClient = MagicMock()
    admin_mod.NewTopic = MagicMock()
    sys.modules["confluent_kafka.admin"] = admin_mod

# Stub openai at the package level so embedding.py can be imported without it;
# tests that need openai's internals will patch at a lower level.
if "openai" not in sys.modules:
    openai_stub = types.ModuleType("openai")

    class _FakeAPIConnectionError(Exception): pass
    class _FakeRateLimitError(Exception): pass
    class _FakeAPIStatusError(Exception): pass
    class _FakeAsyncOpenAI:
        def __init__(self, **_): pass

    openai_stub.AsyncOpenAI         = _FakeAsyncOpenAI
    openai_stub.APIConnectionError  = _FakeAPIConnectionError
    openai_stub.RateLimitError      = _FakeRateLimitError
    openai_stub.APIStatusError      = _FakeAPIStatusError
    sys.modules["openai"] = openai_stub


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_fake_embedding(dim: int = 1536, seed: float = 0.1) -> list[float]:
    """Return a deterministic unit-ish vector for testing."""
    return [seed] * dim


# ─────────────────────────────────────────────────────────────────────────────
# Tests: embedding.py  — feature flag and circuit-breaker behaviour
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeEmbeddingDisabled:
    """When ENABLE_EMBEDDING_DEDUP=false, compute_embedding returns None immediately."""

    def test_returns_none_when_disabled(self, monkeypatch):
        import app.pipeline.embedding as emb_mod
        monkeypatch.setattr(emb_mod, "_ENABLED", False)
        # reset module-level client so the flag is the only thing stopping us
        monkeypatch.setattr(emb_mod, "_client", None)
        monkeypatch.setattr(emb_mod, "_client_error", None)

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            emb_mod.compute_embedding("AAPL beats earnings")
        )
        assert result is None


class TestComputeEmbeddingNoKey:
    """When OPENAI_API_KEY is absent, _get_client caches the error and returns None."""

    def test_returns_none_when_key_missing(self, monkeypatch):
        import app.pipeline.embedding as emb_mod
        monkeypatch.setattr(emb_mod, "_ENABLED", True)
        monkeypatch.setattr(emb_mod, "_client", None)
        monkeypatch.setattr(emb_mod, "_client_error", None)

        # Patch _make_client to raise (simulating missing key)
        monkeypatch.setattr(
            emb_mod,
            "_make_client",
            lambda: (_ for _ in ()).throw(ValueError("no key")),
        )

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            emb_mod.compute_embedding("Meta beats earnings")
        )
        assert result is None
        # Error should be cached so we don't hammer logs
        assert emb_mod._client_error is not None


class TestComputeEmbeddingSuccess:
    """compute_embedding returns a list of floats on success."""

    @pytest.mark.asyncio
    async def test_returns_float_list(self, monkeypatch):
        import app.pipeline.embedding as emb_mod

        fake_vec = _make_fake_embedding()

        # Build a mock AsyncOpenAI client whose embeddings.create() returns our vector
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=fake_vec)]

        mock_client = MagicMock()
        mock_client.embeddings = MagicMock()
        mock_client.embeddings.create = AsyncMock(return_value=mock_response)

        monkeypatch.setattr(emb_mod, "_ENABLED", True)
        monkeypatch.setattr(emb_mod, "_client", mock_client)
        monkeypatch.setattr(emb_mod, "_client_error", None)

        result = await emb_mod.compute_embedding("NVDA beats Q3 EPS")

        assert result is not None
        assert len(result) == 1536
        assert all(isinstance(v, float) for v in result)

    @pytest.mark.asyncio
    async def test_correct_model_and_dimensions_passed(self, monkeypatch):
        import app.pipeline.embedding as emb_mod

        call_args = {}

        async def _fake_create(**kwargs):
            call_args.update(kwargs)
            resp = MagicMock()
            resp.data = [MagicMock(embedding=_make_fake_embedding())]
            return resp

        mock_client = MagicMock()
        mock_client.embeddings = MagicMock()
        mock_client.embeddings.create = _fake_create

        monkeypatch.setattr(emb_mod, "_ENABLED", True)
        monkeypatch.setattr(emb_mod, "_client", mock_client)
        monkeypatch.setattr(emb_mod, "_client_error", None)

        await emb_mod.compute_embedding("test headline")

        assert call_args.get("model") == "text-embedding-3-small"
        assert call_args.get("dimensions") == 1536


class TestComputeEmbeddingAPIFailure:
    """Any exception from the OpenAI API returns None (circuit-breaker)."""

    @pytest.mark.asyncio
    async def test_api_error_returns_none(self, monkeypatch):
        import app.pipeline.embedding as emb_mod

        mock_client = MagicMock()
        mock_client.embeddings = MagicMock()
        mock_client.embeddings.create = AsyncMock(side_effect=RuntimeError("timeout"))

        monkeypatch.setattr(emb_mod, "_ENABLED", True)
        monkeypatch.setattr(emb_mod, "_client", mock_client)
        monkeypatch.setattr(emb_mod, "_client_error", None)

        result = await emb_mod.compute_embedding("TSLA beats estimates")
        assert result is None


class TestComputeEmbeddingsBatch:
    """compute_embeddings processes each text independently."""

    @pytest.mark.asyncio
    async def test_batch_returns_list_of_same_length(self, monkeypatch):
        import app.pipeline.embedding as emb_mod

        fake_vec = _make_fake_embedding()
        mock_response = MagicMock()
        mock_response.data = [MagicMock(embedding=fake_vec)]

        mock_client = MagicMock()
        mock_client.embeddings = MagicMock()
        mock_client.embeddings.create = AsyncMock(return_value=mock_response)

        monkeypatch.setattr(emb_mod, "_ENABLED", True)
        monkeypatch.setattr(emb_mod, "_client", mock_client)
        monkeypatch.setattr(emb_mod, "_client_error", None)

        texts = ["headline A", "headline B", "headline C"]
        results = await emb_mod.compute_embeddings(texts)

        assert len(results) == 3
        assert all(r is not None for r in results)

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty(self, monkeypatch):
        import app.pipeline.embedding as emb_mod
        results = await emb_mod.compute_embeddings([])
        assert results == []

    @pytest.mark.asyncio
    async def test_partial_failure_does_not_affect_others(self, monkeypatch):
        """One failing embedding in a batch should not prevent others."""
        import app.pipeline.embedding as emb_mod

        call_count = 0
        fake_vec = _make_fake_embedding()

        async def _flaky_create(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("flaky failure on item 2")
            resp = MagicMock()
            resp.data = [MagicMock(embedding=fake_vec)]
            return resp

        mock_client = MagicMock()
        mock_client.embeddings = MagicMock()
        mock_client.embeddings.create = _flaky_create

        monkeypatch.setattr(emb_mod, "_ENABLED", True)
        monkeypatch.setattr(emb_mod, "_client", mock_client)
        monkeypatch.setattr(emb_mod, "_client_error", None)

        results = await emb_mod.compute_embeddings(["a", "b", "c"])
        assert results[0] is not None
        assert results[1] is None    # flaky call
        assert results[2] is not None


# ─────────────────────────────────────────────────────────────────────────────
# Tests: DeduplicatorService._embedding_match
# ─────────────────────────────────────────────────────────────────────────────

class TestEmbeddingMatchMethod:
    """Unit-test _embedding_match by injecting a mock SQLAlchemy session."""

    def _make_svc(self):
        """Return a DeduplicatorService with minimal state for _embedding_match tests."""
        # We must avoid __init__ calling super().__init__() which tries to import Kafka
        from app.pipeline.deduplicator import DeduplicatorService
        svc = object.__new__(DeduplicatorService)
        svc._Session = None
        svc._redis = None
        svc._cluster_store = None
        svc._dropped_producer = None
        return svc

    @pytest.mark.asyncio
    async def test_returns_none_when_session_not_ready(self):
        svc = self._make_svc()
        # _Session is None → early return
        result = await svc._embedding_match([0.1] * 1536, "AAPL", __import__("datetime").datetime.utcnow())
        assert result == (None, None)

    @pytest.mark.asyncio
    async def test_returns_cluster_when_row_found(self, monkeypatch):
        from datetime import datetime, timezone

        from app.pipeline.deduplicator import DeduplicatorService

        svc = self._make_svc()

        # Build a fake DB row
        expected_cluster = uuid4()
        expected_sim = 0.92
        fake_row = MagicMock()
        fake_row.cluster_id = str(expected_cluster)
        fake_row.similarity = expected_sim

        # Build a mock async session context manager
        mock_result = MagicMock()
        mock_result.fetchone.return_value = fake_row

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_session_factory = MagicMock(return_value=mock_session)
        svc._Session = mock_session_factory

        cluster_id, sim = await svc._embedding_match(
            [0.1] * 1536, "META", datetime.now(timezone.utc)
        )
        assert cluster_id == expected_cluster
        assert abs(sim - expected_sim) < 1e-6

    @pytest.mark.asyncio
    async def test_returns_none_when_no_row(self):
        from datetime import datetime, timezone

        from app.pipeline.deduplicator import DeduplicatorService

        svc = self._make_svc()

        mock_result = MagicMock()
        mock_result.fetchone.return_value = None

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        svc._Session = MagicMock(return_value=mock_session)

        result = await svc._embedding_match([0.1] * 1536, "NVDA", datetime.now(timezone.utc))
        assert result == (None, None)

    @pytest.mark.asyncio
    async def test_returns_none_on_db_exception(self):
        from datetime import datetime, timezone

        from app.pipeline.deduplicator import DeduplicatorService

        svc = self._make_svc()

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=RuntimeError("DB gone"))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        svc._Session = MagicMock(return_value=mock_session)

        # Should NOT raise — graceful fallthrough
        result = await svc._embedding_match([0.1] * 1536, "TSLA", datetime.now(timezone.utc))
        assert result == (None, None)

    @pytest.mark.asyncio
    async def test_embedding_string_format(self):
        """The query receives a properly bracketed vector string."""
        from datetime import datetime, timezone

        from app.pipeline.deduplicator import DeduplicatorService

        svc = self._make_svc()
        captured = {}

        mock_result = MagicMock()
        mock_result.fetchone.return_value = None

        async def _capture_execute(query, params):
            captured.update(params)
            return mock_result

        mock_session = AsyncMock()
        mock_session.execute = _capture_execute
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        svc._Session = MagicMock(return_value=mock_session)

        vec = [0.5] * 1536
        await svc._embedding_match(vec, "AAPL", datetime.now(timezone.utc))

        emb_str = captured.get("embedding", "")
        assert emb_str.startswith("[")
        assert emb_str.endswith("]")
        # Should have 1536 comma-separated values
        parts = emb_str[1:-1].split(",")
        assert len(parts) == 1536


# ─────────────────────────────────────────────────────────────────────────────
# Tests: dedup_reason enums contain new values
# ─────────────────────────────────────────────────────────────────────────────

class TestDedupReasonEnums:
    def test_embedding_match_reason_exists(self):
        from app.pipeline.dedup_reason import DedupReason
        assert DedupReason.EMBEDDING_MATCH == "embedding_match"

    def test_embedding_tier_exists(self):
        from app.pipeline.dedup_reason import DedupTier
        assert DedupTier.EMBEDDING == "embedding"

    def test_simhash_tier_exists(self):
        from app.pipeline.dedup_reason import DedupTier
        assert DedupTier.SIMHASH == "simhash"

    def test_all_tiers_are_unique_strings(self):
        from app.pipeline.dedup_reason import DedupTier
        values = [t.value for t in DedupTier]
        assert len(values) == len(set(values))

    def test_all_reasons_are_unique_strings(self):
        from app.pipeline.dedup_reason import DedupReason
        values = [r.value for r in DedupReason]
        assert len(values) == len(set(values))


# ─────────────────────────────────────────────────────────────────────────────
# Tests: config.py — new settings present with correct defaults
# ─────────────────────────────────────────────────────────────────────────────

class TestConfigNewSettings:
    def test_openai_api_key_default_empty(self, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI", raising=False)
        # Instantiate Settings directly (bypasses lru_cache singleton) with no env file
        # so the default value is used.
        from app.config import Settings
        from pydantic_settings import SettingsConfigDict
        class _TestSettings(Settings):
            model_config = SettingsConfigDict(env_file=None, env_file_encoding="utf-8",
                                              case_sensitive=False, extra="ignore")
        s = _TestSettings()
        assert s.openai_api_key == ""

    def test_enable_embedding_dedup_default_true(self):
        from app.config import Settings
        s = Settings()
        assert s.enable_embedding_dedup is True

    def test_embedding_similarity_threshold_default(self):
        from app.config import Settings
        s = Settings()
        assert abs(s.embedding_similarity_threshold - 0.65) < 1e-9

    def test_embedding_lookback_hours_default(self):
        from app.config import Settings
        s = Settings()
        assert s.embedding_lookback_hours == 6


# ─────────────────────────────────────────────────────────────────────────────
# Tests: _store_title with and without embedding
# ─────────────────────────────────────────────────────────────────────────────

class TestStoreTitleEmbedding:
    """Verify _store_title passes the embedding literal in the INSERT when available."""

    def _make_svc(self):
        from app.pipeline.deduplicator import DeduplicatorService
        svc = object.__new__(DeduplicatorService)
        svc._Session = None
        svc._redis = None
        svc._cluster_store = None
        svc._dropped_producer = None
        return svc

    def _make_norm(self):
        """Return a minimal NormalizedRecord-like object."""
        from datetime import datetime, timezone
        from unittest.mock import MagicMock
        norm = MagicMock()
        norm.id = uuid4()
        norm.source = MagicMock(value="benzinga")
        norm.vendor_id = "test-vendor-001"
        norm.published_at = datetime.now(timezone.utc)
        norm.received_at = datetime.now(timezone.utc)
        norm.url = "https://example.com/article/1"
        norm.canonical_url = "https://example.com/article/1"
        norm.title = "AAPL beats Q4 EPS estimates by 12%"
        norm.snippet = "Apple reported..."
        norm.content_hash = "abc123"
        return norm

    @pytest.mark.asyncio
    async def test_embedding_branch_taken_when_provided(self):
        from app.pipeline.deduplicator import DeduplicatorService

        svc = self._make_svc()
        executed_sqls: list[str] = []

        mock_session = AsyncMock()

        async def _capture_execute(query, params):
            executed_sqls.append(str(query))
            return MagicMock()

        mock_session.execute = _capture_execute
        mock_session.commit = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        svc._Session = MagicMock(return_value=mock_session)

        norm = self._make_norm()
        embedding = [0.1] * 1536

        await svc._store_title(norm, uuid4(), embedding=embedding)

        # The second execute (news_item INSERT) should reference title_embedding
        item_sql = " ".join(executed_sqls[1].split())
        assert "title_embedding" in item_sql.lower()

    @pytest.mark.asyncio
    async def test_no_embedding_branch_taken_when_none(self):
        from app.pipeline.deduplicator import DeduplicatorService

        svc = self._make_svc()
        executed_sqls: list[str] = []

        mock_session = AsyncMock()

        async def _capture_execute(query, params):
            executed_sqls.append(str(query))
            return MagicMock()

        mock_session.execute = _capture_execute
        mock_session.commit = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        svc._Session = MagicMock(return_value=mock_session)

        norm = self._make_norm()

        await svc._store_title(norm, uuid4(), embedding=None)

        # The second execute (news_item INSERT) must NOT reference title_embedding
        item_sql = " ".join(executed_sqls[1].split())
        assert "title_embedding" not in item_sql.lower()

    @pytest.mark.asyncio
    async def test_store_title_no_session_is_noop(self):
        """When _Session is None the method returns silently."""
        from app.pipeline.deduplicator import DeduplicatorService

        svc = self._make_svc()
        svc._Session = None

        # Should not raise
        norm = self._make_norm()
        await svc._store_title(norm, uuid4(), embedding=None)
