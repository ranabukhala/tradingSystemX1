"""
Signal Aggregator — Stage 5 of the pipeline.

Consumes: news.summarized
Emits:    signals.actionable  (new topic)

Logic:
  - Groups summarized records by primary ticker
  - Scores conviction from impact_day + source_credibility + catalyst weight
  - Detects signal patterns: breakout / fade / event_driven / macro_shift
  - Suppresses low-conviction noise
  - Emits TradingSignal to signals.actionable topic
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, timedelta
import pytz
from typing import Any
from uuid import UUID, uuid4

import httpx
import redis.asyncio as aioredis
from pydantic import BaseModel, Field

from app.config import settings
from app.models.news import (
    CatalystType, FloatSensitivity, MarketCapTier,
    NewsMode, RegimeFlag, SessionContext, SummarizedRecord,
)
from app.pipeline.base_consumer import BaseConsumer, _log
from app.pipeline.event_cluster import EventClusterStore
from app.pipeline.llm_validation import (
    LLMValidationResult,
    validate_llm_output,
    direction_from_facts,
    apply_conviction_cap,
    apply_regime_adjustment,
    INTERPRETIVE_MAX_CONVICTION,
    validate_headline_move,
)
from app.pipeline.fact_cross_validator import (
    FactCrossValidationResult,
    FactCrossValidator,
    FactValidationStatus,
    ENABLE_FACT_VALIDATION,
)
from app.signals.conviction_features import (
    CATALYST_WEIGHT,
    SESSION_WEIGHT,
    FLOAT_WEIGHT,
    ConvictionBreakdown,
    ConvictionCalibrator,
    get_default_calibrator,
    extract_conviction_features,
    compute_conviction_breakdown,
    LOG_FEATURES,
    LOG_DROPPED,
)

# Minimum conviction to emit a signal (override via SIGNAL_CONVICTION_THRESHOLD env var)
import os
CONVICTION_THRESHOLD = float(os.environ.get("SIGNAL_CONVICTION_THRESHOLD", "0.55"))

# Signal event gate — prevent duplicate signals for the same news event
_ENABLE_SIGNAL_EVENT_GATE = os.environ.get("ENABLE_SIGNAL_EVENT_GATE", "true").lower() == "true"
_MULTI_SIGNAL_POLICY      = os.environ.get("MULTI_SIGNAL_POLICY", "deny")  # deny | allow | allow_opposite

# Macro volatility regime gate — blocks new LONG signals when VXX term structure
# shows acute backwardation (VXX 5-day spike + UVXY leverage ratio > threshold).
# Short signals are allowed because the regime CONFIRMS downside direction.
# Disable entirely via ENABLE_VOL_REGIME_GATE=false.
_ENABLE_VOL_REGIME_GATE = os.environ.get("ENABLE_VOL_REGIME_GATE", "true").lower() == "true"

ET = pytz.timezone("America/New_York")

# Time-of-day windows with conviction multiplier and label
# Dampening multipliers (< 1.0) are applied; boost multipliers (> 1.0) logged only.
TIME_WINDOWS: list[tuple[tuple[int,int], tuple[int,int], float, str, str]] = [
    # (start_hhmm, end_hhmm, multiplier, label, emoji)
    ((9, 30),  (9, 45),  0.50, "Open shakeout — high noise, fades quickly",    "🔴"),
    ((9, 45),  (11, 30), 1.10, "Prime window — best intraday trend quality",   "🟢"),
    ((11, 30), (12, 0),  1.00, "Late morning — decent, momentum slowing",      "🟡"),
    ((12, 0),  (14, 0),  0.70, "Dead zone — low volume, false breakouts",      "🔴"),
    ((14, 0),  (15, 30), 1.00, "Afternoon trend — ok if volume confirms",      "🟡"),
    ((15, 30), (16, 0),  0.80, "MOC noise — order imbalances distort moves",   "🟠"),
    ((16, 0),  (20, 0),  0.90, "After-hours — wider spreads, thin liquidity",  "🟡"),
]

class TradingSignal(BaseModel):
    """
    Actionable trading signal emitted to signals.actionable topic.
    """
    id: UUID = Field(default_factory=uuid4)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Primary ticker driving the signal
    ticker: str
    signal_type: str           # breakout | fade | event_driven | macro_shift | reversal
    direction: str             # long | short | neutral
    conviction: float          # 0.0–1.0

    # Context
    catalyst_type: str
    session_context: str
    regime_flag: str | None = None

    # Source news
    news_id: str
    news_title: str
    t1_summary: str | None = None
    t2_summary: str | None = None

    # Price action hints
    key_levels: list[float] = Field(default_factory=list)
    decay_minutes: int | None = None

    # Risk
    market_cap_tier: str | None = None
    float_sensitivity: str

    # Catalyst intelligence (v1.3)
    priced_in: str | None = None            # yes | partially | no
    priced_in_reason: str | None = None
    sympathy_plays: list[str] = Field(default_factory=list)
    is_sympathy: bool = False               # True = secondary signal from sympathy

    # Timing intelligence (dampening < 1.0 active; boosts > 1.0 logged only)
    time_window_label: str | None = None
    time_window_emoji: str | None = None
    time_window_mult: float | None = None   # Applied if < 1.0; logged-only if ≥ 1.0
    time_window_quality: str | None = None  # good / caution / poor

    # Metadata
    impact_day: float
    impact_swing: float
    source: str
    prompt_version: str | None = None

    # Staleness tracking (v1.5) — propagated from SummarizedRecord
    # Used by pretrade_filter and execution_engine to enforce the staleness guard.
    news_published_at: datetime | None = None   # When the source article was published (UTC)
    pipeline_entry_at: datetime | None = None   # When connector first received it (received_at)
    route_type: str | None = None               # "fast" | "slow" — from ai_summarizer

    # Event-level deduplication (v1.9) — propagated from SummarizedRecord.cluster_id.
    # Stable across vendors: two vendors reporting the same earnings catalyst for the
    # same ticker map to one event_id.  The signal gate and order gate both key on
    # this field so duplicate trades from the same event are blocked even when
    # news_id (vendor-specific) differs.
    # None on legacy signals (created before v1.9) — gates fall back to signal.id.
    event_id: UUID | None = None

    # Trust metadata (v1.6)
    # direction_source tracks WHAT evidence drove the final direction decision.
    # Downstream consumers (pretrade_filter, execution_engine) may use this to
    # apply additional scrutiny to interpretive-only signals.
    direction_source: str = "neutral_default"
    #   "facts"              — direction from validated factual fields (EPS, rating, etc.)
    #   "interpretive_prior" — only LLM signal_bias was available; conviction was capped
    #   "neutral_default"    — no evidence; signal is neutral and will be dropped
    interpretive_cap_applied: bool = False
    #   True when conviction was reduced to INTERPRETIVE_MAX_CONVICTION ceiling

    # Fact cross-validation metadata (v1.7)
    # Populated by FactCrossValidator; reflects cross-check against vendor data.
    validation_status: str | None = None             # confirmed|partial|mismatch|unverifiable|skipped
    validated_fields: list[str] = Field(default_factory=list)
    mismatch_fields: list[str] = Field(default_factory=list)
    validation_confidence: float | None = None        # 0.0–1.0

    # Conviction calibration metadata (v1.8)
    # Enables calibration research and replay without re-running the full pipeline.
    conviction_raw_product: float | None = None       # step_after_cross_val  (pre-TW, pre-calibration)
    conviction_pre_calibration: float | None = None   # step_after_time_window (post-TW, pre-calibration)
    correlation_risk: list[str] = Field(default_factory=list)  # e.g. ['cat_impact_day', 'float_squeeze']
    calibration_fn: str = "identity"                  # identity | sigmoid | linear

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")


def compute_conviction_with_breakdown(
    record: SummarizedRecord,
    direction_source: str,
    validation: LLMValidationResult,
    cross_val: "FactCrossValidationResult | None" = None,
    time_window_mult: float = 1.0,
    time_window_label: str | None = None,
    calibrator: "ConvictionCalibrator | None" = None,
) -> ConvictionBreakdown:
    """
    Compute conviction with a full step-by-step breakdown (v1.8).

    Extracts ConvictionFeatures from the record and runs compute_conviction_breakdown().
    The returned ConvictionBreakdown contains final_conviction and all intermediate
    steps for logging, calibration research, and replay.

    time_window_mult and time_window_label are integrated into step 7 of the
    breakdown, eliminating the two-stage threshold check in process().
    """
    features = extract_conviction_features(
        record,
        direction_source,
        validation,
        cross_val,
        time_window_mult=time_window_mult,
        time_window_label=time_window_label,
    )
    return compute_conviction_breakdown(features, calibrator)


def compute_conviction(
    record: SummarizedRecord,
    direction_source: str,
    validation: LLMValidationResult,
    cross_val: "FactCrossValidationResult | None" = None,
) -> tuple[float, bool]:
    """
    Compute conviction score 0.0–1.0 with trust-aware caps.

    Returns (conviction, interpretive_cap_applied).

    Backward-compatible wrapper around compute_conviction_with_breakdown().
    Signature is preserved exactly — existing callers and tests continue to work.
    Time window is not passed here (defaults to 1.0 = no dampening); use
    compute_conviction_with_breakdown() directly when TW integration is needed.

    Formula (v1 default — identical to pre-v1.8 behaviour):
      base     = validated impact_day
      × catalyst_weight × session_weight × float_weight × credibility_boost
      × earnings_proximity_bonus
      × priced_in multiplier (or 0.0 if blocked)
      → interpretive ceiling
      × regime_multiplier
      × cross_val_multiplier
    """
    bd = compute_conviction_with_breakdown(
        record, direction_source, validation, cross_val,
        time_window_mult=1.0,
        time_window_label=None,
        calibrator=None,   # identity — preserves pre-v1.8 output exactly
    )
    return bd.final_conviction, bd.interpretive_cap_applied


def classify_signal_type(record: SummarizedRecord, conviction: float) -> str:
    """
    Classify signal type from catalyst + impact pattern.
    """
    catalyst = record.catalyst_type

    if catalyst == CatalystType.EARNINGS:
        return "event_driven"
    if catalyst == CatalystType.MA:
        return "event_driven"
    if catalyst == CatalystType.REGULATORY:
        return "event_driven"
    if catalyst == CatalystType.MACRO:
        return "macro_shift"
    if catalyst == CatalystType.ANALYST:
        # High-conviction analyst call = breakout; low = fade
        return "breakout" if conviction >= 0.6 else "fade"

    # Generic pattern from impact scores
    if record.impact_day and record.impact_swing:
        ratio = record.impact_day / max(record.impact_swing, 0.01)
        if ratio > 2.0:
            return "fade"        # Intraday spike, likely to reverse
        if ratio < 0.5:
            return "breakout"    # Building multi-day momentum

    return "event_driven"


def classify_direction(
    record: SummarizedRecord,
    validation: LLMValidationResult,
) -> tuple[str, str]:
    """
    Determine trade direction and record where the evidence came from.

    Returns (direction, direction_source) where direction_source is one of:
      "facts"              — determined from validated structural facts
      "interpretive_prior" — only LLM signal_bias was available (conviction capped)
      "neutral_default"    — no directional evidence; signal will be dropped

    Phase 1 — Factual fields (highest trust, no conviction cap).
      Uses direction_from_facts() which reads facts_json and applies a strict
      precedence order: eps_beat > guidance > analyst > deal_price > fda_outcome.

    Phase 2 — Interpretive prior (LLM signal_bias only).
      regime_flag is INTENTIONALLY excluded from direction determination.
      It was previously able to return "long" / "short" on its own (risk_on /
      risk_off), which let a pure LLM assessment of macro conditions drive a
      trade direction with no structural backing.  It is now a conviction
      multiplier only (see apply_regime_adjustment()).

    Phase 3 — Default neutral.
      Both phases returned neutral → signal carries no directional edge.
    """
    # ── Phase 1: Factual fields ───────────────────────────────────────────
    direction = direction_from_facts(record.facts_json)
    if direction != "neutral":
        return direction, "facts"

    # ── Phase 2: Interpretive prior ───────────────────────────────────────
    # validation.cleaned_signal_bias is None when signal_bias was absent,
    # invalid, or literally "neutral" — all treated identically as no prior.
    bias = validation.cleaned_signal_bias
    if bias in ("long", "short"):
        return bias, "interpretive_prior"

    # ── Phase 3: Default neutral ──────────────────────────────────────────
    return "neutral", "neutral_default"


def get_time_window(dt_utc: datetime) -> dict:
    """
    Classify current ET time into a named trading window.
    Returns label, emoji, and multiplier for display purposes.
    """
    et_now = dt_utc.astimezone(ET)
    h, m = et_now.hour, et_now.minute
    current_mins = h * 60 + m

    for (sh, sm), (eh, em), mult, label, emoji in TIME_WINDOWS:
        start_mins = sh * 60 + sm
        end_mins   = eh * 60 + em
        if start_mins <= current_mins < end_mins:
            return {
                "window_label":  label,
                "window_emoji":  emoji,
                "multiplier":    mult,
                "time_et":       et_now.strftime("%H:%M ET"),
                "quality":       "good" if mult >= 1.0 else ("caution" if mult >= 0.8 else "poor"),
            }

    # Pre-market or overnight
    return {
        "window_label":  "Pre-market / Overnight",
        "window_emoji":  "🌙",
        "multiplier":    0.90,
        "time_et":       et_now.strftime("%H:%M ET"),
        "quality":       "caution",
    }


def build_sympathy_signal(
    primary: TradingSignal,
    sympathy_ticker: str,
    intraday_return: float | None = None,
) -> TradingSignal | None:
    """
    Build a reduced-conviction signal for a sympathy play ticker.

    Returns None when the primary signal's direction came from interpretive
    fields only.  Sympathy chains compound LLM uncertainty: if we're already
    uncertain about the primary direction (no structural facts backing it),
    generating derivative signals multiplies that uncertainty uncontrollably.

    When primary.direction_source == "facts":
      Discount is dynamic based on how much the sympathy ticker has already moved:
        flat / <1%    → × 0.72  (fresh — most upside still available)
        partial 1–3%  → × 0.55  (partially priced in)
        already ran>3%→ × 0.38  (chasing — heavy discount)
      Extra × 0.6 penalty if the ticker is already moving against expected direction.
      Additional × 0.80 base discount for all sympathy signals (v1.6).
    """
    # ── Guard: block sympathy chains from interpretive-only primaries ─────
    if primary.direction_source != "facts":
        return None

    abs_move = abs(intraday_return) if intraday_return is not None else 0.0

    if abs_move >= 3.0:
        discount = 0.38   # already ran — chasing risk
    elif abs_move >= 1.0:
        discount = 0.55   # partially moved
    else:
        discount = 0.72   # fresh — most upside available

    # Penalty when the sympathy ticker is moving the wrong way
    if intraday_return is not None:
        is_long = primary.direction == "long"
        if (is_long and intraday_return < -1.0) or (not is_long and intraday_return > 1.0):
            discount *= 0.6   # sector not following — questionable sympathy

    # Extra base discount for all sympathy signals (they inherit LLM tickers)
    discount *= 0.80

    return TradingSignal(
        ticker=sympathy_ticker,
        signal_type="sympathy",
        direction=primary.direction,
        conviction=round(primary.conviction * discount, 3),
        catalyst_type=primary.catalyst_type,
        session_context=primary.session_context,
        regime_flag=primary.regime_flag,
        news_id=primary.news_id,
        news_title=primary.news_title,
        t1_summary=primary.t1_summary,
        t2_summary=f"[Sympathy play on {primary.ticker}] {primary.t2_summary or ''}",
        key_levels=[],
        decay_minutes=primary.decay_minutes,
        market_cap_tier=None,
        float_sensitivity="normal",
        priced_in=primary.priced_in,
        priced_in_reason=primary.priced_in_reason,
        sympathy_plays=[],
        is_sympathy=True,
        impact_day=primary.impact_day * 0.6,
        impact_swing=primary.impact_swing * 0.6,
        source=primary.source,
        prompt_version=primary.prompt_version,
        # Inherit staleness timestamps from primary signal
        news_published_at=primary.news_published_at,
        pipeline_entry_at=primary.pipeline_entry_at,
        route_type=primary.route_type,
        # Sympathy signals always have interpretive direction_source
        # (they derive from LLM-suggested tickers, not structured facts)
        direction_source="interpretive_prior",
        interpretive_cap_applied=True,
    )


class SignalAggregatorService(BaseConsumer):

    @property
    def service_name(self) -> str:
        return "signal_aggregator"

    @property
    def input_topic(self) -> str:
        return settings.topic_news_summarized

    @property
    def output_topic(self) -> str:
        return "signals.actionable"

    def __init__(self) -> None:
        super().__init__()
        self._redis: aioredis.Redis | None = None
        self._cluster_store: EventClusterStore | None = None
        self._db_pool = None                              # asyncpg pool for validation + logging
        self._fact_validator: FactCrossValidator | None = None
        self._calibrator: ConvictionCalibrator | None = None
        self._http = None                                 # httpx.AsyncClient for Polygon calls
        self._polygon_key: str = os.environ.get("POLYGON_API_KEY", "")

    async def on_start(self) -> None:
        self._producer = self._make_producer()
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        self._http = httpx.AsyncClient(timeout=10.0)
        if _ENABLE_SIGNAL_EVENT_GATE:
            self._cluster_store = EventClusterStore(self._redis)

        # ── Conviction calibrator (v1.8) ───────────────────────────────────
        # Instantiated once at startup; reads CONVICTION_CALIBRATION_FN env var.
        # Default is IdentityCalibrator — no change from pre-v1.8 behaviour.
        self._calibrator = get_default_calibrator()

        # ── Fact cross-validator ───────────────────────────────────────────
        # Create a small asyncpg pool (2 connections) for earnings + analyst
        # cross-checks and audit writes.  Pool is optional; validator degrades
        # to UNVERIFIABLE when the DB is unreachable.
        if ENABLE_FACT_VALIDATION:
            try:
                import asyncpg
                dsn = os.environ.get(
                    "DATABASE_URL",
                    "postgresql://trading:trading@postgres:5432/trading_db",
                ).replace("postgresql+asyncpg://", "postgresql://")
                self._db_pool = await asyncpg.create_pool(
                    dsn,
                    min_size=1,
                    max_size=2,
                    command_timeout=5,
                )
                self._fact_validator = FactCrossValidator(db_pool=self._db_pool)
                _log("info", "signal_aggregator.fact_validator_ready")
            except Exception as exc:
                _log("warning", "signal_aggregator.fact_validator_unavailable",
                     error=str(exc))
                self._fact_validator = FactCrossValidator(db_pool=None)

    async def on_stop(self) -> None:
        if self._redis:
            await self._redis.aclose()
        if self._db_pool:
            await self._db_pool.close()
        if self._http:
            await self._http.aclose()

    async def _get_intraday_return(self, ticker: str) -> float | None:
        """
        Return the intraday % change for ticker from the Redis price cache
        written by the polygon_prices connector (key: price:{ticker}:last).
        Returns None if data is unavailable.
        """
        if not self._redis:
            return None
        try:
            raw = await self._redis.get(f"price:{ticker}:last")
            if raw:
                d = json.loads(raw)
                last = float(d.get("c") or d.get("close") or 0)
                open_ = float(d.get("o") or d.get("open") or last)
                if open_ > 0:
                    return (last - open_) / open_ * 100
        except Exception:
            pass
        return None

    async def _log_signal(self, summarized, conviction: float, passed: bool,
                          direction: str = "neutral", signal_type: str = "other") -> None:
        """Persist every evaluated signal to signal_log table."""
        try:
            import asyncpg, os
            dsn = os.environ.get("DATABASE_URL",
                "postgresql://trading:trading@postgres:5432/trading_db")
            # asyncpg requires postgresql:// not postgresql+asyncpg://
            dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
            conn = await asyncpg.connect(dsn)
            await conn.execute("""
                INSERT INTO signal_log (
                    ticker, direction, signal_type,
                    conviction, conviction_threshold, passed_gate,
                    catalyst_type, session_context,
                    market_cap_tier, float_sensitivity,
                    impact_day, impact_swing,
                    news_id, news_title, news_source,
                    t1_summary, t2_summary, prompt_version
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
                """,
                summarized.tickers[0] if summarized.tickers else "UNKNOWN",
                direction, signal_type, conviction, CONVICTION_THRESHOLD, passed,
                summarized.catalyst_type.value if summarized.catalyst_type else None,
                summarized.session_context.value if summarized.session_context else None,
                summarized.market_cap_tier.value if summarized.market_cap_tier else None,
                summarized.float_sensitivity.value if summarized.float_sensitivity else None,
                summarized.impact_day, summarized.impact_swing,
                summarized.id, summarized.title,
                summarized.source.value if summarized.source else None,
                summarized.t1_summary, summarized.t2_summary, summarized.prompt_version,
            )
            await conn.close()
        except Exception as e:
            _log("warning", "signal_aggregator.log_error", error=str(e))

    async def _write_feature_log(
        self,
        breakdown: ConvictionBreakdown,
        ticker: str,
        direction: str,
        record_id: str,
        emitted: bool,
    ) -> None:
        """
        Persist a ConvictionBreakdown to signal_feature_log.

        Called fire-and-forget via asyncio.ensure_future() — never blocks the hot path.
        Silently swallows all exceptions so a DB hiccup never affects signal flow.

        Controlled by CONVICTION_LOG_FEATURES (emitted) and
        CONVICTION_LOG_DROPPED_FEATURES (dropped) env vars.
        """
        if self._db_pool is None:
            return
        d = breakdown.as_log_dict
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO signal_feature_log (
                        record_id, ticker, direction, emitted,
                        impact_day, source_credibility, cred_boost,
                        catalyst_type, catalyst_weight, catalyst_weight_effective,
                        session_context, session_weight,
                        float_sensitivity, float_weight,
                        earnings_proximity_h, earnings_proximity_bonus,
                        earnings_proximity_bonus_effective,
                        direction_source, priced_in, regime_flag, regime_multiplier,
                        cross_val_status, cross_val_multiplier,
                        time_window_label, time_window_mult, time_window_mult_effective,
                        scoring_mode, correlation_risk,
                        step_base_product, step_after_proximity, step_after_priced_in,
                        step_after_cap, step_after_regime, step_after_cross_val,
                        step_after_time_window, step_calibrated,
                        interpretive_cap_applied, priced_in_blocked,
                        calibration_fn, final_conviction
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
                        $31,$32,$33,$34,$35,$36,$37,$38,$39,$40
                    )
                    """,
                    record_id, ticker, direction, emitted,
                    d["impact_day"], d["source_credibility"], d["cred_boost"],
                    d["catalyst_type"], d["catalyst_weight"], d["catalyst_weight_effective"],
                    d["session_context"], d["session_weight"],
                    d["float_sensitivity"], d["float_weight"],
                    d["earnings_proximity_h"], d["earnings_proximity_bonus"],
                    d["earnings_proximity_bonus_effective"],
                    d["direction_source"], d["priced_in"], d["regime_flag"],
                    d["regime_multiplier"],
                    d["cross_val_status"], d["cross_val_multiplier"],
                    d["time_window_label"], d["time_window_mult"],
                    d["time_window_mult_effective"],
                    d["scoring_mode"], d["correlation_risk"],
                    d["step_base_product"], d["step_after_proximity"],
                    d["step_after_priced_in"],
                    d["step_after_cap"], d["step_after_regime"],
                    d["step_after_cross_val"],
                    d["step_after_time_window"], d["step_calibrated"],
                    d["interpretive_cap_applied"], d["priced_in_blocked"],
                    d["calibration_fn"], d["final_conviction"],
                )
        except Exception as exc:
            _log("warning", "signal_aggregator.feature_log_error",
                 ticker=ticker, error=str(exc))

    async def process(self, record: dict) -> dict | None:
        try:
            summarized = SummarizedRecord.from_kafka_dict(record)
        except Exception as e:
            _log("error", "signal_aggregator.parse_error", error=str(e))
            raise

        # Only process stock-specific signals with tickers
        if not summarized.tickers:
            _log("debug", "signal_aggregator.skip_no_tickers",
                 vendor_id=summarized.vendor_id)
            return None

        # Skip non-representative duplicates
        if not summarized.is_representative:
            return None

        # Skip items with no LLM output (budget exhausted etc.)
        if summarized.impact_day is None:
            return None

        # Primary ticker — resolved first so the cross-validator can use it
        primary_ticker = summarized.tickers[0]

        # ── Validate all LLM outputs before any trading logic ────────────
        # validate_llm_output() checks ranges, vocabularies, and ticker formats.
        # All downstream logic uses validation.cleaned_* values, never raw fields.
        validation = validate_llm_output(summarized)
        if validation.issues:
            _log("debug", "signal_aggregator.llm_validation_issues",
                 vendor_id=summarized.vendor_id,
                 issue_count=len(validation.issues),
                 issues=[{"field": i.field, "reason": i.reason}
                         for i in validation.issues[:5]])

        # ── Cross-validate headline price action before direction classification ──
        # headline_move_pct is extracted by T1 but treated as untrusted until
        # Polygon confirms the actual price move matches the claim.  Only moves
        # >= HEADLINE_MOVE_FACTUAL_THRESHOLD (10%) are eligible for factual promotion.
        if (summarized.facts_json is not None
                and summarized.facts_json.headline_move_pct is not None
                and abs(summarized.facts_json.headline_move_pct) >= 10.0):
            validated = await validate_headline_move(
                ticker=primary_ticker,
                headline_move_pct=summarized.facts_json.headline_move_pct,
                http_client=self._http,
                api_key=self._polygon_key,
            )
            if validated:
                summarized.facts_json.headline_move_validated = True
                _log("info", "signal_aggregator.headline_move_validated",
                     ticker=primary_ticker,
                     headline_pct=summarized.facts_json.headline_move_pct)
            else:
                _log("info", "signal_aggregator.headline_move_rejected",
                     ticker=primary_ticker,
                     headline_pct=summarized.facts_json.headline_move_pct,
                     note="Polygon price does not confirm headline — falling to interpretive path")
                # headline_move_validated stays False; direction_from_facts will skip this field
        # ──────────────────────────────────────────────────────────────────────

        # Classify direction first — conviction formula depends on direction_source
        direction, direction_source = classify_direction(summarized, validation)

        # ── Fact cross-validation ─────────────────────────────────────────
        # Cross-checks LLM-extracted facts_json against authoritative vendor data
        # (Postgres event table for earnings, fmp_analyst_grades for analyst calls).
        # Returns a FactCrossValidationResult whose conviction_multiplier is applied
        # in step 6 of compute_conviction_breakdown().
        cross_val: FactCrossValidationResult | None = None
        if self._fact_validator is not None:
            cross_val = await self._fact_validator.validate(summarized, primary_ticker)
            if cross_val.mismatch_fields:
                _log("warning", "signal_aggregator.fact_validation_mismatch",
                     ticker=primary_ticker,
                     validation_status=cross_val.validation_status.value,
                     mismatch_fields=cross_val.mismatch_fields,
                     has_key_mismatch=cross_val.has_key_mismatch,
                     conviction_multiplier=cross_val.conviction_multiplier,
                     vendor_source=cross_val.vendor_source)
            elif cross_val.validation_status == FactValidationStatus.CONFIRMED:
                _log("debug", "signal_aggregator.fact_validation_confirmed",
                     ticker=primary_ticker,
                     validated_fields=cross_val.validated_fields,
                     vendor_source=cross_val.vendor_source)

        # ── Time window ───────────────────────────────────────────────────
        # Computed before conviction (v1.8) so TW is integrated into step 7 of
        # compute_conviction_breakdown(), enabling a single threshold check.
        tw = get_time_window(summarized.published_at or datetime.now(timezone.utc))
        tw_mult  = tw["multiplier"]
        tw_label = tw["window_label"]

        # ── Conviction breakdown (v1.8) ───────────────────────────────────
        # Returns a full 9-step trace.  Time window dampening is step 7;
        # calibration is step 8.  final_conviction is the value used for all
        # trading decisions.
        bd = compute_conviction_with_breakdown(
            summarized, direction_source, validation, cross_val,
            time_window_mult=tw_mult,
            time_window_label=tw_label,
            calibrator=self._calibrator,
        )
        conviction  = bd.final_conviction
        cap_applied = bd.interpretive_cap_applied

        # ── Block: interpretive direction AND fully priced in → 0.0 ──────
        # Detected in breakdown step 3; bd.priced_in_blocked is set True.
        if bd.priced_in_blocked:
            _log("info", "signal_aggregator.interpretive_priced_in_block",
                 vendor_id=summarized.vendor_id,
                 direction_source=direction_source)
            await self._log_signal(summarized, conviction, passed=False,
                                   direction=direction)
            if LOG_DROPPED and self._db_pool:
                asyncio.ensure_future(self._write_feature_log(
                    bd, primary_ticker, direction, str(summarized.id), emitted=False
                ))
            return None

        # ── Single threshold check (TW already applied in breakdown step 7) ─
        if conviction < CONVICTION_THRESHOLD:
            _log("debug", "signal_aggregator.low_conviction",
                 vendor_id=summarized.vendor_id,
                 conviction=conviction,
                 threshold=CONVICTION_THRESHOLD,
                 direction_source=direction_source,
                 time_window=tw_label)
            await self._log_signal(summarized, conviction, passed=False)
            if LOG_DROPPED and self._db_pool:
                asyncio.ensure_future(self._write_feature_log(
                    bd, primary_ticker, direction, str(summarized.id), emitted=False
                ))
            return None

        # ── Time window — observability log ──────────────────────────────
        # TW is already incorporated into conviction; this is for monitoring only.
        if tw_mult < 1.0:
            _log("info", "signal_aggregator.time_window_dampen",
                 ticker=primary_ticker,
                 window=tw_label,
                 time_et=tw["time_et"],
                 multiplier=tw_mult,
                 conviction_before=round(bd.step_after_cross_val, 3),
                 conviction_after=conviction)
        else:
            _log("info", "signal_aggregator.time_window",
                 ticker=primary_ticker,
                 time_et=tw["time_et"],
                 window=tw_label,
                 multiplier=tw_mult,
                 quality=tw["quality"])

        signal_type = classify_signal_type(summarized, conviction)

        _log("info", "signal_aggregator.signal_generated",
             ticker=primary_ticker,
             direction=direction,
             direction_source=direction_source,
             conviction=conviction,
             interpretive_cap=cap_applied,
             signal_type=signal_type,
             catalyst=summarized.catalyst_type.value,
             correlation_risk=bd.correlation_risk or None,
             calibration_fn=bd.calibration_fn,
             title=summarized.title[:80])

        signal = TradingSignal(
            ticker=primary_ticker,
            signal_type=signal_type,
            direction=direction,
            conviction=conviction,
            catalyst_type=summarized.catalyst_type.value,
            session_context=summarized.session_context.value,
            regime_flag=summarized.regime_flag.value if summarized.regime_flag else None,
            news_id=str(summarized.id),
            news_title=summarized.title,
            t1_summary=summarized.t1_summary,
            t2_summary=summarized.t2_summary,
            decay_minutes=summarized.decay_minutes,
            market_cap_tier=summarized.market_cap_tier.value if summarized.market_cap_tier else None,
            float_sensitivity=summarized.float_sensitivity.value,
            impact_day=summarized.impact_day or 0.0,
            impact_swing=summarized.impact_swing or 0.0,
            source=summarized.source.value,
            prompt_version=summarized.prompt_version,
            time_window_label=tw["window_label"],
            time_window_emoji=tw["window_emoji"],
            time_window_mult=tw_mult,
            time_window_quality=tw["quality"],
            # Staleness tracking — propagate source timestamps downstream
            news_published_at=summarized.published_at,
            pipeline_entry_at=summarized.received_at,
            route_type=getattr(summarized, "route_type", None),
            # Event-level dedup (v1.9) — stable cross-vendor event identity
            event_id=summarized.cluster_id,
            # Trust metadata (v1.6)
            direction_source=direction_source,
            interpretive_cap_applied=cap_applied,
            # Fact cross-validation metadata (v1.7)
            validation_status=(
                cross_val.validation_status.value if cross_val else None
            ),
            validated_fields=(cross_val.validated_fields if cross_val else []),
            mismatch_fields=(cross_val.mismatch_fields if cross_val else []),
            validation_confidence=(
                cross_val.validation_confidence if cross_val else None
            ),
            # Conviction calibration metadata (v1.8)
            conviction_raw_product=round(bd.step_after_cross_val, 4),
            conviction_pre_calibration=round(bd.step_after_time_window, 4),
            correlation_risk=bd.correlation_risk,
            calibration_fn=bd.calibration_fn,
        )

        await self._log_signal(summarized, conviction, passed=True,
                               direction=direction, signal_type=signal_type)

        # Feature log — fire-and-forget (never blocks the hot path)
        if LOG_FEATURES and self._db_pool:
            asyncio.ensure_future(self._write_feature_log(
                bd, primary_ticker, direction, str(summarized.id), emitted=True
            ))

        # ── Signal event gate ─────────────────────────────────────────────────
        # Prevents duplicate signals being emitted for the same underlying news event.
        # Uses Redis SETNX atomicity — first caller wins.
        if _ENABLE_SIGNAL_EVENT_GATE and self._cluster_store:
            allowed = await self._cluster_store.mark_signal_emitted(
                event_id=str(summarized.cluster_id or summarized.id),
                direction=direction,
                multi_signal_policy=_MULTI_SIGNAL_POLICY,
            )
            if not allowed:
                _log("info", "signal_aggregator.signal_gate_blocked",
                     ticker=primary_ticker,
                     event_id=str(summarized.cluster_id or summarized.id),
                     direction=direction,
                     policy=_MULTI_SIGNAL_POLICY)
                return None
        # ─────────────────────────────────────────────────────────────────────

        # ── Macro volatility regime gate ──────────────────────────────────────
        # Reads vol:regime (written by volatility_monitor_service) from Redis.
        # HIGH_VOL_BACKWARDATION = acute VXX spike + UVXY leverage > 1.7 threshold.
        # Block LONG signals — fighting the regime in a fear spike destroys alpha.
        # Allow SHORT signals — the regime confirms downside direction.
        vol_regime_name:   str  = "LOW_VOL_STABLE"
        vol_regime_detail: dict = {}
        if _ENABLE_VOL_REGIME_GATE and self._redis:
            try:
                raw_vr = await self._redis.get("vol:regime")
                if raw_vr:
                    vol_data          = json.loads(raw_vr)
                    vol_regime_name   = vol_data.get("regime", "LOW_VOL_STABLE")
                    vol_regime_detail = vol_data
            except Exception as exc:
                _log("warning", "signal_aggregator.vol_regime_fetch_error",
                     ticker=primary_ticker, error=str(exc))

        if _ENABLE_VOL_REGIME_GATE and vol_regime_name == "HIGH_VOL_BACKWARDATION" and direction == "long":
            _log("info", "signal_aggregator.vol_regime_gate_blocked",
                 ticker=primary_ticker,
                 direction=direction,
                 vol_regime=vol_regime_name,
                 vxx_5d_roc=vol_regime_detail.get("vxx_5d_roc"),
                 leverage_ratio=vol_regime_detail.get("leverage_ratio"))
            return None
        # ──────────────────────────────────────────────────────────────────────

        # Emit sympathy signals for secondary tickers.
        # Requires: (a) direction_source="facts" on primary — interpretive-only
        # primaries may not spawn sympathy chains; (b) not fully priced in;
        # (c) tickers were validated (cleaned_sympathy_plays).
        if (direction_source == "facts"
                and validation.cleaned_sympathy_plays
                and validation.cleaned_priced_in != "yes"):
            for sym_ticker in validation.cleaned_sympathy_plays:
                if sym_ticker == primary_ticker:
                    continue
                sym_return = await self._get_intraday_return(sym_ticker)
                sym_signal = build_sympathy_signal(signal, sym_ticker,
                                                   intraday_return=sym_return)
                if sym_signal is None:
                    # Blocked by trust guard (should not reach here given the outer
                    # direction_source check, but defensive)
                    continue
                if sym_signal.conviction >= CONVICTION_THRESHOLD:
                    _log("info", "signal_aggregator.sympathy_signal",
                         primary=primary_ticker,
                         sympathy=sym_ticker,
                         intraday_return=round(sym_return, 2) if sym_return is not None else None,
                         conviction=sym_signal.conviction,
                         direction_source="interpretive_prior")
                    self._producer.produce(
                        self.output_topic,
                        value=sym_signal.to_kafka_dict()
                    )

        # Attach macro vol regime so pretrade_filter / execution_engine can
        # apply conviction adjustments without re-fetching Redis.
        d = signal.to_kafka_dict()
        d["vol_regime"]        = vol_regime_name
        d["vol_regime_detail"] = vol_regime_detail
        return d
