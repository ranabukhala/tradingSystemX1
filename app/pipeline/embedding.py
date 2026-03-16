"""
Semantic embedding client — Tier 3.5 of the deduplication pipeline.

Wraps OpenAI's text-embedding-3-small model (1536 dims) behind:
  • async-native interface
  • 3-attempt exponential-backoff retry (tenacity)
  • circuit-breaker: any unrecoverable failure returns None so the
    caller can fall through to SimHash instead of blocking the pipeline

Cost reference:
  text-embedding-3-small = $0.020 / 1M tokens
  ~10 tokens per headline → $0.0000002 per call
  10,000 headlines/day   → ~$0.002/day

Usage::

    from app.pipeline.embedding import compute_embedding, compute_embeddings

    vec = await compute_embedding("AAPL beats Q2 EPS estimates")
    # vec is list[float] of length 1536, or None on failure

    vecs = await compute_embeddings(["headline 1", "headline 2"])
    # vecs[i] is list[float] or None for each item
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# ── Model constants ────────────────────────────────────────────────────────
_MODEL              = "text-embedding-3-small"
_DIMENSIONS         = 1536
_MAX_RETRIES        = 3
_BACKOFF_MIN_SEC    = 0.5
_BACKOFF_MAX_SEC    = 8.0

# ── Feature flag (checked once at import so tests can monkeypatch before import) ──
_ENABLED: bool = os.environ.get("ENABLE_EMBEDDING_DEDUP", "true").lower() == "true"


def _make_client():
    """Lazily create an AsyncOpenAI client using the key from the environment.

    Doing this lazily (rather than at module level) means the module can be
    imported safely even when openai is not installed or the key is absent —
    the ImportError / ValueError surfaces only when the first embedding is
    actually requested, and is caught by compute_embedding's try/except.
    """
    try:
        from openai import AsyncOpenAI  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "openai package is required for embedding dedup. "
            "Add 'openai>=1.0.0' to pyproject.toml and rebuild the container."
        ) from exc

    api_key = (
        os.environ.get("OPENAI_API_KEY")
        or os.environ.get("OPENAI")   # legacy key name used in some .env files
        or ""
    )
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY is not set. Embedding dedup will be skipped."
        )
    return AsyncOpenAI(api_key=api_key)


# Module-level client — created on first use, None when disabled or unavailable
_client = None
_client_error: str | None = None   # cached failure message so we log once


def _get_client():
    """Return (or lazily create) the shared AsyncOpenAI client.

    Returns None if the client cannot be created (missing key, missing package).
    The error is logged once and cached so subsequent calls don't spam logs.
    """
    global _client, _client_error
    if _client is not None:
        return _client
    if _client_error is not None:
        return None
    try:
        _client = _make_client()
        return _client
    except Exception as exc:
        _client_error = str(exc)
        logger.warning("embedding.client_init_failed: %s — embedding tier disabled", exc)
        return None


# ── Public API ─────────────────────────────────────────────────────────────

async def compute_embedding(text: str) -> Optional[list[float]]:
    """Return a 1536-dimensional embedding vector for *text*, or None on failure.

    Failures are logged at WARNING level and return None so the caller can
    gracefully fall through to the next dedup tier.  The pipeline is never
    blocked by OpenAI availability.

    Args:
        text: The headline (or any short text) to embed.

    Returns:
        list[float] of length 1536, or None.
    """
    if not _ENABLED:
        return None

    client = _get_client()
    if client is None:
        return None

    try:
        from openai import APIConnectionError, APIStatusError, RateLimitError  # noqa: PLC0415

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((APIConnectionError, RateLimitError)),
            stop=stop_after_attempt(_MAX_RETRIES),
            wait=wait_exponential(min=_BACKOFF_MIN_SEC, max=_BACKOFF_MAX_SEC),
            reraise=False,
        ):
            with attempt:
                response = await client.embeddings.create(
                    input=text,
                    model=_MODEL,
                    dimensions=_DIMENSIONS,
                )
                return response.data[0].embedding

    except Exception as exc:
        logger.warning(
            "embedding.compute_failed title=%r error=%s",
            text[:80],
            exc,
        )
        return None


async def compute_embeddings(texts: list[str]) -> list[Optional[list[float]]]:
    """Compute embeddings for a batch of texts concurrently.

    Each item in the returned list corresponds to the same-index input text.
    Individual failures return None without affecting other items.

    Args:
        texts: List of headlines to embed.

    Returns:
        list[Optional[list[float]]] — same length as *texts*.
    """
    if not texts:
        return []
    tasks = [compute_embedding(t) for t in texts]
    return list(await asyncio.gather(*tasks))
