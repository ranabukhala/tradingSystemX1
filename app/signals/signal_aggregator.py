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

import json
from datetime import datetime, timezone, timedelta
import pytz
from typing import Any
from uuid import UUID, uuid4

import redis.asyncio as aioredis
from pydantic import BaseModel, Field

from app.config import settings
from app.models.news import (
    CatalystType, FloatSensitivity, MarketCapTier,
    NewsMode, RegimeFlag, SessionContext, SummarizedRecord,
)
from app.pipeline.base_consumer import BaseConsumer, _log
from app.pipeline.event_cluster import EventClusterStore

# Minimum conviction to emit a signal (override via SIGNAL_CONVICTION_THRESHOLD env var)
import os
CONVICTION_THRESHOLD = float(os.environ.get("SIGNAL_CONVICTION_THRESHOLD", "0.55"))

# Signal event gate — prevent duplicate signals for the same news event
_ENABLE_SIGNAL_EVENT_GATE = os.environ.get("ENABLE_SIGNAL_EVENT_GATE", "true").lower() == "true"
_MULTI_SIGNAL_POLICY      = os.environ.get("MULTI_SIGNAL_POLICY", "deny")  # deny | allow | allow_opposite

# Catalyst weight multipliers
CATALYST_WEIGHT: dict[CatalystType, float] = {
    CatalystType.EARNINGS:    1.5,
    CatalystType.MA:          1.8,   # M&A = highest impact
    CatalystType.REGULATORY:  1.4,
    CatalystType.ANALYST:     0.9,
    CatalystType.FILING:      0.6,
    CatalystType.MACRO:       0.7,
    CatalystType.LEGAL:       0.3,   # Lawsuits = lagging catalyst, deeply discounted
    CatalystType.OTHER:       0.5,
}

# Session multipliers — premarket news = higher weight (less liquidity, bigger moves)
SESSION_WEIGHT: dict[SessionContext, float] = {
    SessionContext.PREMARKET:  1.4,
    SessionContext.OPEN:       1.3,
    SessionContext.INTRADAY:   1.0,
    SessionContext.AFTERHOURS: 1.1,
    SessionContext.OVERNIGHT:  0.7,
}

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

# Float sensitivity multiplier
FLOAT_WEIGHT: dict[FloatSensitivity, float] = {
    FloatSensitivity.HIGH:   1.3,
    FloatSensitivity.NORMAL: 1.0,
}


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

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")


def compute_conviction(record: SummarizedRecord) -> float:
    """
    Compute conviction score 0.0–1.0 from multiple factors.

    Formula:
      base     = impact_day (from LLM)
      × catalyst_weight
      × session_weight
      × float_weight
      × credibility_boost
    """
    base = record.impact_day or 0.0

    cat_w = CATALYST_WEIGHT.get(record.catalyst_type, 0.5)
    sess_w = SESSION_WEIGHT.get(record.session_context, 1.0)
    float_w = FLOAT_WEIGHT.get(record.float_sensitivity, 1.0)

    # Source credibility boost: high credibility = more weight
    cred = record.source_credibility or 0.6
    cred_boost = 0.8 + (cred * 0.4)   # Range: 0.8–1.2

    conviction = base * cat_w * sess_w * float_w * cred_boost

    # Earnings proximity bonus: news within 2h of earnings = very high conviction
    if record.earnings_proximity_h is not None:
        proximity_h = abs(record.earnings_proximity_h)
        if proximity_h <= 2:
            conviction *= 1.3
        elif proximity_h <= 24:
            conviction *= 1.1

    # Legal filings (class actions, lawsuits) are always lagging — auto mark priced in
    if record.catalyst_type == CatalystType.LEGAL:
        if not getattr(record, "priced_in", None):
            record.priced_in = "yes"

    # Priced-in penalty: if market already moved on this, reduce conviction
    priced_in = getattr(record, "priced_in", None)
    if priced_in == "yes":
        conviction *= 0.60      # Heavy discount — likely to fade or not extend
    elif priced_in == "partially":
        conviction *= 0.85      # Moderate discount

    return min(1.0, round(conviction, 3))


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


def classify_direction(record: SummarizedRecord) -> str:
    """
    Determine directional bias from facts and T2 analysis.
    """
    facts = record.facts_json

    # Earnings signals
    if facts:
        if facts.eps_beat is True or facts.guidance_raised is True:
            return "long"
        if facts.eps_beat is False or facts.guidance_lowered is True:
            return "short"

        # Analyst signals
        if facts.rating_new and facts.price_target_new:
            bullish_ratings = {"buy", "strong buy", "outperform", "overweight", "upgrade"}
            bearish_ratings = {"sell", "strong sell", "underperform", "underweight", "downgrade"}
            rating_lower = facts.rating_new.lower()
            if any(r in rating_lower for r in bullish_ratings):
                return "long"
            if any(r in rating_lower for r in bearish_ratings):
                return "short"

        # M&A target is almost always long
        if facts.deal_price and facts.deal_premium_pct:
            return "long"

        # FDA approval = long, rejection = short
        if facts.fda_outcome:
            if "approved" in facts.fda_outcome.lower():
                return "long"
            if any(w in facts.fda_outcome.lower() for w in ["rejected", "failed", "denied"]):
                return "short"

    # Regime-based direction for macro
    if record.regime_flag == RegimeFlag.RISK_ON:
        return "long"
    if record.regime_flag == RegimeFlag.RISK_OFF:
        return "short"

    # T2 signal_bias tiebreaker: when all fact-based checks fall through to
    # neutral, use Sonnet's holistic directional read.  Previously generated
    # on every high-impact item but never consumed — now wired in.
    if getattr(record, "signal_bias", None) in ("long", "short"):
        return record.signal_bias  # type: ignore[return-value]

    return "neutral"


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
) -> TradingSignal:
    """
    Build a reduced-conviction signal for a sympathy play ticker.

    Discount is dynamic based on how much the sympathy ticker has already moved:
      flat / <1%    → × 0.72  (fresh — most upside still available)
      partial 1–3%  → × 0.55  (partially priced in)
      already ran>3%→ × 0.38  (chasing — heavy discount)
    Extra ×0.6 penalty if the ticker is already moving against the expected direction.

    Flat 30% was the old behaviour regardless of price action.
    """
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

    async def on_start(self) -> None:
        self._producer = self._make_producer()
        self._redis = await aioredis.from_url(
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            decode_responses=True,
        )
        if _ENABLE_SIGNAL_EVENT_GATE:
            self._cluster_store = EventClusterStore(self._redis)

    async def on_stop(self) -> None:
        if self._redis:
            await self._redis.aclose()

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

        # Compute conviction
        conviction = compute_conviction(summarized)

        if conviction < CONVICTION_THRESHOLD:
            _log("debug", "signal_aggregator.low_conviction",
                 vendor_id=summarized.vendor_id,
                 conviction=conviction,
                 threshold=CONVICTION_THRESHOLD)
            await self._log_signal(summarized, conviction, passed=False)
            return None

        # Primary ticker = first resolved ticker
        primary_ticker = summarized.tickers[0]

        signal_type = classify_signal_type(summarized, conviction)
        direction = classify_direction(summarized)

        _log("info", "signal_aggregator.signal_generated",
             ticker=primary_ticker,
             direction=direction,
             conviction=conviction,
             signal_type=signal_type,
             catalyst=summarized.catalyst_type.value,
             title=summarized.title[:80])

        # Time window: apply dampening multipliers (< 1.0) now active.
        # Boost multipliers (> 1.0) not yet validated — kept inactive.
        tw = get_time_window(summarized.published_at or datetime.now(timezone.utc))
        tw_mult = tw["multiplier"]
        if tw_mult < 1.0:
            conviction_pre_tw = conviction
            conviction = round(conviction * tw_mult, 3)
            _log("info", "signal_aggregator.time_window_dampen",
                 ticker=primary_ticker,
                 window=tw["window_label"],
                 time_et=tw["time_et"],
                 multiplier=tw_mult,
                 conviction_before=conviction_pre_tw,
                 conviction_after=conviction)
            # Re-check threshold after dampening
            if conviction < CONVICTION_THRESHOLD:
                _log("info", "signal_aggregator.time_window_threshold_drop",
                     ticker=primary_ticker,
                     window=tw["window_label"],
                     conviction=conviction,
                     threshold=CONVICTION_THRESHOLD)
                await self._log_signal(summarized, conviction, passed=False)
                return None
        else:
            _log("info", "signal_aggregator.time_window",
                 ticker=primary_ticker,
                 time_et=tw["time_et"],
                 window=tw["window_label"],
                 multiplier=tw_mult,
                 quality=tw["quality"])

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
        )

        await self._log_signal(summarized, conviction, passed=True,
                               direction=direction, signal_type=signal_type)

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

        # Emit sympathy signals for secondary tickers (only if not priced in)
        if summarized.sympathy_plays and summarized.priced_in != "yes":
            for sym_ticker in summarized.sympathy_plays:
                if sym_ticker == primary_ticker:
                    continue
                sym_return = await self._get_intraday_return(sym_ticker)
                sym_signal = build_sympathy_signal(signal, sym_ticker,
                                                   intraday_return=sym_return)
                if sym_signal.conviction >= CONVICTION_THRESHOLD:
                    _log("info", "signal_aggregator.sympathy_signal",
                         primary=primary_ticker,
                         sympathy=sym_ticker,
                         intraday_return=round(sym_return, 2) if sym_return is not None else None,
                         conviction=sym_signal.conviction)
                    # Publish sympathy signal to same topic
                    self._producer.produce(
                        self.output_topic,
                        value=sym_signal.to_kafka_dict()
                    )

        return signal.to_kafka_dict()
