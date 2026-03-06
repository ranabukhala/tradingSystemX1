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
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from app.config import settings
from app.models.news import (
    CatalystType, FloatSensitivity, MarketCapTier,
    NewsMode, RegimeFlag, SessionContext, SummarizedRecord,
)
from app.pipeline.base_consumer import BaseConsumer, _log

# Minimum conviction to emit a signal (override via SIGNAL_CONVICTION_THRESHOLD env var)
import os
CONVICTION_THRESHOLD = float(os.environ.get("SIGNAL_CONVICTION_THRESHOLD", "0.55"))

# Catalyst weight multipliers
CATALYST_WEIGHT: dict[CatalystType, float] = {
    CatalystType.EARNINGS:    1.5,
    CatalystType.MA:          1.8,   # M&A = highest impact
    CatalystType.REGULATORY:  1.4,
    CatalystType.ANALYST:     0.9,
    CatalystType.FILING:      0.6,
    CatalystType.MACRO:       0.7,
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

    # Metadata
    impact_day: float
    impact_swing: float
    source: str
    prompt_version: str | None = None

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

    return "neutral"


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
        )

        return signal.to_kafka_dict()
