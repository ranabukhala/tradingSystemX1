"""
News record contract — Pydantic models for every stage of the pipeline.

Stages:
  RawNewsRecord      → emitted to news.raw by connectors
  NormalizedRecord   → emitted to news.normalized by normalizer
  DedupedRecord      → emitted to news.deduped by deduplicator
  DroppedRecord      → emitted to news.dropped (non-representative audit trail)
  EnrichedRecord     → emitted to news.enriched by entity resolver
  SummarizedRecord   → emitted to news.summarized by AI agents

route_type on SummarizedRecord:
  "fast"  — built from structured vendor facts; zero LLM cost
  "slow"  — full T1/T2 LLM path (default)
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator

if TYPE_CHECKING:
    from app.pipeline.dedup_reason import DedupReason, DedupTier


# ── Enums ─────────────────────────────────────────────────────────────────────

class NewsSource(str, Enum):
    BENZINGA = "benzinga"
    POLYGON = "polygon"
    TIINGO = "tiingo"
    DJNEWS = "djnews"
    FINNHUB = "finnhub"
    UNKNOWN = "unknown"


class CatalystType(str, Enum):
    EARNINGS = "earnings"
    ANALYST = "analyst"
    FILING = "filing"
    REGULATORY = "regulatory"
    MACRO = "macro"
    MA = "ma"                  # Mergers & Acquisitions
    LEGAL = "legal"            # Lawsuits, class actions, SEC enforcement
    OTHER = "other"


class NewsMode(str, Enum):
    STOCK_SPECIFIC = "stock_specific"
    GENERAL_MARKET = "general_market"


class SessionContext(str, Enum):
    PREMARKET = "premarket"
    OPEN = "open"
    INTRADAY = "intraday"
    AFTERHOURS = "afterhours"
    OVERNIGHT = "overnight"


class MarketCapTier(str, Enum):
    MEGA = "mega"       # > $200B
    LARGE = "large"     # $10B – $200B
    MID = "mid"         # $2B – $10B
    SMALL = "small"     # $300M – $2B
    MICRO = "micro"     # < $300M


class FloatSensitivity(str, Enum):
    HIGH = "high"       # Low float, reacts violently to news
    NORMAL = "normal"


class RegimeFlag(str, Enum):
    RISK_ON = "risk_on"
    RISK_OFF = "risk_off"
    HIGH_VOL = "high_vol"
    COMPRESSION = "compression"


# ── Stage 1: Raw record (connector output) ─────────────────────────────────────

class RawNewsRecord(BaseModel):
    """
    Emitted by connectors to news.raw.
    Minimal processing — just normalize the shape, nothing else.
    """
    id: UUID = Field(default_factory=uuid4)
    source: NewsSource
    vendor_id: str                        # Original ID from the vendor
    published_at: datetime                # UTC — connector must normalize timezone
    received_at: datetime = Field(default_factory=datetime.utcnow)
    url: str
    title: str
    snippet: str | None = None
    author: str | None = None
    raw_tickers: list[str] = Field(default_factory=list)   # Vendor-provided, unvalidated
    raw_categories: list[str] = Field(default_factory=list)
    raw_payload: dict[str, Any] = Field(default_factory=dict)  # Original vendor JSON

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "RawNewsRecord":
        return cls.model_validate(data)


# ── Stage 2: Normalized record ────────────────────────────────────────────────

class NormalizedRecord(BaseModel):
    """
    Emitted to news.normalized by the normalizer service.
    Canonical URL, content hash, standardized fields.
    """
    id: UUID
    source: NewsSource
    vendor_id: str
    published_at: datetime
    received_at: datetime
    url: str                   # Canonicalized (query params stripped)
    canonical_url: str         # Final URL after redirect resolution
    title: str
    snippet: str | None = None
    author: str | None = None
    content_hash: str          # SHA-256 of normalized title + snippet
    raw_tickers: list[str] = Field(default_factory=list)
    raw_categories: list[str] = Field(default_factory=list)

    @staticmethod
    def compute_hash(title: str, snippet: str | None) -> str:
        """Stable hash for dedup — lowercase, strip whitespace."""
        text = f"{title.lower().strip()}|{(snippet or '').lower().strip()}"
        return hashlib.sha256(text.encode()).hexdigest()

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "NormalizedRecord":
        return cls.model_validate(data)


# ── Stage 3: Deduped record ───────────────────────────────────────────────────

class DedupedRecord(NormalizedRecord):
    """
    Emitted to news.deduped by the deduplicator.
    Adds cluster membership and representative flag.
    """
    cluster_id: UUID | None = None         # Which syndication cluster this belongs to
    is_representative: bool = True         # True = chosen item for this cluster
    dedup_method: str | None = None        # "exact_url" | "exact_hash" | "similarity" | "simhash"
    similarity_score: float | None = None  # For fuzzy / SimHash matches
    dedup_tier: str | None = None          # DedupTier value: "vendor_id" | "content_hash" | "semantic" | "none"
    dedup_reason: str | None = None        # DedupReason value: granular reason code

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "DedupedRecord":
        return cls.model_validate(data)


# ── Stage 3b: Dropped record (audit trail) ────────────────────────────────────

class DroppedRecord(BaseModel):
    """
    Emitted to news.dropped for non-representative / duplicate items.
    Provides a full audit trail without polluting the main pipeline.
    """
    id: UUID
    source: NewsSource
    vendor_id: str
    title: str
    published_at: datetime
    cluster_id: UUID | None = None
    dedup_tier: str | None = None          # DedupTier value
    dedup_reason: str | None = None        # DedupReason value
    similarity_score: float | None = None
    dropped_at: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "DroppedRecord":
        return cls.model_validate(data)


# ── Stage 4: Enriched record (Entity Resolver output) ─────────────────────────

class EnrichedRecord(BaseModel):
    """
    Emitted to news.enriched by the entity resolver.
    Full news record contract with all classification fields.
    """
    # Core identity
    id: UUID
    source: NewsSource
    vendor_id: str
    published_at: datetime
    received_at: datetime
    url: str
    canonical_url: str
    title: str
    snippet: str | None = None
    author: str | None = None
    content_hash: str
    full_text_ref: str | None = None       # MinIO object key if fetched

    # Dedup
    cluster_id: UUID | None = None
    is_representative: bool = True

    # Entity resolution
    tickers: list[str] = Field(default_factory=list)         # Resolved, confidence >= 0.7
    ticker_confidence: dict[str, float] = Field(default_factory=dict)
    sectors: list[str] = Field(default_factory=list)         # GICS sectors
    themes: list[str] = Field(default_factory=list)          # e.g. "AI", "rates", "energy"

    # Classification
    catalyst_type: CatalystType = CatalystType.OTHER
    mode: NewsMode = NewsMode.GENERAL_MARKET
    session_context: SessionContext = SessionContext.INTRADAY
    market_cap_tier: MarketCapTier | None = None
    float_sensitivity: FloatSensitivity = FloatSensitivity.NORMAL
    short_interest_flag: bool = False

    # Event linkage
    earnings_proximity_h: int | None = None   # Hours to nearest earnings event (±)
    event_ids: list[UUID] = Field(default_factory=list)

    # Decay
    decay_minutes: int | None = None          # Estimated edge lifetime

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "EnrichedRecord":
        return cls.model_validate(data)


# ── Stage 5: Summarized record (AI agent output) ──────────────────────────────

class FactsJson(BaseModel):
    """Structured facts extracted by the AI agent. Fields are catalyst-type specific."""
    # Earnings
    eps_beat: bool | None = None
    eps_actual: float | None = None
    eps_estimate: float | None = None
    revenue_beat: bool | None = None
    guidance_raised: bool | None = None
    guidance_lowered: bool | None = None

    # Analyst
    rating_new: str | None = None          # e.g. "Buy", "Outperform"
    rating_prev: str | None = None
    price_target_new: float | None = None
    price_target_prev: float | None = None
    analyst_firm: str | None = None

    # M&A
    deal_price: float | None = None
    deal_premium_pct: float | None = None
    deal_type: str | None = None           # "acquisition" | "merger"

    # Regulatory
    fda_outcome: str | None = None         # "approved" | "rejected" | "delayed"
    trial_phase: str | None = None

    # Macro
    actual_value: float | None = None
    estimate_value: float | None = None
    prior_value: float | None = None

    # Headline price action (cross-validated against Polygon before use)
    headline_move_pct: float | None = None   # e.g. -25.0 for "sinks 25%", +40.0 for "surges 40%"
    # Set to True by signal_aggregator ONLY after Polygon cross-validation passes
    headline_move_validated: bool = False

    # Headline direction signals (v1.6) — lexical extraction by T1
    # Used in direction_from_facts() Phase 7 to promote headline direction to factual
    # when the headline explicitly names a direction + the move is today + no EPS facts
    price_direction: str | None = None   # "up" | "down" | null
    move_magnitude: str | None = None    # "large" | "small" | null
    move_is_today: bool | None = None    # true = intraday move; false = historical


class SummarizedRecord(EnrichedRecord):
    """
    Emitted to news.summarized. Final enriched record with AI outputs.
    """
    # AI summaries
    t1_summary: str | None = None          # Bullet facts, title+snippet only input
    t2_summary: str | None = None          # "So what for traders" — high impact only
    facts_json: FactsJson | None = None

    # Impact scores
    impact_day: float | None = None        # 0–1, intraday impact
    impact_swing: float | None = None      # 0–1, multi-day impact
    regime_flag: RegimeFlag | None = None
    source_credibility: float | None = None  # 0–1, source tier weight
    signal_bias: str | None = None           # long | short | neutral — T2 directional lean

    # New catalyst intelligence fields (v1.3)
    priced_in: str | None = None            # yes | partially | no
    priced_in_reason: str | None = None     # one-sentence explanation
    sympathy_plays: list[str] = Field(default_factory=list)  # sector sympathy tickers

    # Routing metadata (v1.4)
    route_type: str | None = None           # "fast" | "slow" — how this record was produced

    # Trust metadata (v1.6)
    # facts_validated=False means ≥1 factual field (EPS, deal_price, …) failed
    # range/type validation in llm_validation.validate_llm_output().
    # When False, direction_from_facts() will still be tried but any validated
    # field that failed is treated as absent — it does NOT flip to neutral by
    # default; only the specific invalid field is dropped.
    facts_validated: bool = True

    # Fact cross-validation metadata (v1.7)
    # Populated by signal_aggregator after cross-checking facts_json against
    # authoritative vendor data (FMP earnings calendar, analyst grades).
    # Propagated to TradingSignal and persisted to fact_validation_audit.
    validation_status: str | None = None             # confirmed|partial|mismatch|unverifiable|skipped
    validated_fields: list[str] = Field(default_factory=list)    # fields confirmed vs vendor
    mismatch_fields: list[str] = Field(default_factory=list)     # fields that diverged from vendor
    validation_confidence: float | None = None        # 0.0–1.0 fraction verified+matching

    # AI metadata
    prompt_version: str | None = None
    llm_tokens_used: int | None = None
    llm_cost_usd: float | None = None

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "SummarizedRecord":
        return cls.model_validate(data)
