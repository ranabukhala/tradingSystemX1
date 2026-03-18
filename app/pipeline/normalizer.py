"""
Normalizer Service — Stage 1 of the processing pipeline.

Consumes: news.raw
Emits:    news.normalized

Responsibilities:
  - Canonicalize URL (strip tracking params)
  - Compute content_hash (SHA-256 of title+snippet)
  - Standardize encoding/whitespace
  - Set canonical_url (same as cleaned URL for now; full redirect
    resolution would require outbound HTTP which we skip for speed)
"""
from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from app.models.news import NormalizedRecord, RawNewsRecord
from app.pipeline.base_consumer import BaseConsumer, _log

# Query parameters to strip from URLs (tracking, session, etc.)
_STRIP_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "ref", "referrer", "source", "via", "campaign", "fbclid", "gclid",
    "mc_cid", "mc_eid", "yclid", "_hsenc", "_hsmi", "mkt_tok",
}


def canonicalize_url(url: str) -> str:
    """Strip tracking params, normalize scheme, remove trailing slash."""
    try:
        parsed = urlparse(url.strip())
        params = {
            k: v for k, v in parse_qs(parsed.query).items()
            if k.lower() not in _STRIP_PARAMS
        }
        clean_query = urlencode(params, doseq=True)
        clean_path = parsed.path.rstrip("/") or "/"
        return urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            clean_path,
            parsed.params,
            clean_query,
            "",  # strip fragment
        ))
    except Exception:
        return url


def clean_text(text: str | None) -> str | None:
    """Normalize whitespace, strip HTML tags."""
    if not text:
        return text
    # Strip basic HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


class NormalizerService(BaseConsumer):

    @property
    def service_name(self) -> str:
        return "normalizer"

    @property
    def pipeline_stage(self) -> str | None:
        return "normalized"

    @property
    def input_topic(self) -> str:
        from app.config import settings
        return settings.topic_news_raw

    @property
    def output_topic(self) -> str:
        from app.config import settings
        return settings.topic_news_normalized

    async def process(self, record: dict) -> dict | None:
        # Stamp news_ingested on the input record NOW — before any processing.
        # BaseConsumer will copy this into the result dict and then add "normalized".
        # Result: both stages are set, with news_ingested < normalized.
        from app.utils.pipeline_timer import PipelineTimer
        record["stage_timestamps"] = PipelineTimer.stamp(
            record.get("stage_timestamps"), "news_ingested"
        )

        try:
            raw = RawNewsRecord.from_kafka_dict(record)
        except Exception as e:
            _log("error", "normalizer.parse_error", error=str(e))
            raise

        # Canonicalize URL
        clean_url = canonicalize_url(raw.url)

        # Clean text fields
        title = clean_text(raw.title) or raw.title
        snippet = clean_text(raw.snippet)

        # Compute content hash
        content_hash = NormalizedRecord.compute_hash(title, snippet)

        normalized = NormalizedRecord(
            id=raw.id,
            source=raw.source,
            vendor_id=raw.vendor_id,
            published_at=raw.published_at,
            received_at=raw.received_at,
            url=clean_url,
            canonical_url=clean_url,
            title=title,
            snippet=snippet,
            author=raw.author,
            content_hash=content_hash,
            raw_tickers=raw.raw_tickers,
            raw_categories=raw.raw_categories,
        )

        return normalized.to_kafka_dict()
