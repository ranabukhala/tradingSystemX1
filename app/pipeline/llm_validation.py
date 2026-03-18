"""
LLM output trust classification and validation.

Every field in SummarizedRecord that originates from an LLM call is tagged as
FACTUAL or INTERPRETIVE.  The distinction controls what may drive trade direction
and whether a conviction cap is applied.

┌─────────────────────────────────────────────────────────────────────────────┐
│  FACTUAL                                                                    │
│  Fields that derive from structured numbers in the vendor payload.          │
│  eps_beat, guidance_raised/lowered, rating_new, deal_price, fda_outcome.   │
│  These have a ground-truth source.  Validated against known ranges.        │
│  → If valid: canonical source for trade direction.  No conviction cap.     │
│  → If invalid/missing: contributes nothing to direction.                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  INTERPRETIVE                                                               │
│  Soft LLM reasoning outputs.                                                │
│  signal_bias, priced_in, regime_flag, sympathy_plays,                      │
│  impact_day, impact_swing, source_credibility.                              │
│  → When present: weak prior that shifts conviction windows.                │
│  → NEVER the sole determinant of trade direction.                          │
│  → Ambiguous (None / invalid) → neutral / absent contribution.             │
└─────────────────────────────────────────────────────────────────────────────┘

Direction decision hierarchy (enforced in signal_aggregator)
─────────────────────────────────────────────────────────────
  1. Valid factual fields           → direction_source = "facts"
                                      no conviction cap
  2. signal_bias (no valid facts)   → direction_source = "interpretive_prior"
                                      conviction ≤ INTERPRETIVE_MAX_CONVICTION
  3. Both absent / neutral          → direction = "neutral"
                                      direction_source = "neutral_default"
                                      signal dropped

Ambiguity rules
───────────────
  • facts_json absent or all-None              → no factual direction
  • eps_beat=True AND guidance_lowered=True    → eps_beat wins (stronger fact)
  • impact_day outside [0, 1]                 → clamped to boundary, issue logged
  • signal_bias not in {long,short,neutral}   → treated as absent
  • priced_in not in {yes,partially,no}        → treated as absent (no penalty)
  • regime_flag invalid                        → ignored entirely
  • sympathy ticker fails [A-Z]{1,5}           → removed; rest kept

Sympathy signal rule
────────────────────
  Sympathy chains require direction_source="facts" on the primary signal.
  An interpretive-only primary (direction_source="interpretive_prior") may NOT
  spawn sympathy trades — LLM hallucinations must not compound.

Regime flag rule (demoted in v1.6)
───────────────────────────────────
  regime_flag no longer determines direction.  It is an optional conviction
  multiplier only: risk_off → ×0.90, high_vol → ×0.85, others → ×1.00.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ── Configuration ──────────────────────────────────────────────────────────────

# Hard ceiling on conviction when direction came from LLM bias only.
# Prevents interpretive-only signals from reaching high-confidence trade levels.
INTERPRETIVE_MAX_CONVICTION: float = float(
    os.environ.get("INTERPRETIVE_MAX_CONVICTION", "0.60")
)

# Slightly elevated cap for interpretive signals when impact_day >= 0.9.
# Conservative bump (+0.05) — allows very high-impact interpretive reads to
# reach 0.65 rather than being hard-capped at 0.60.
INTERPRETIVE_MAX_CONVICTION_HIGH_IMPACT: float = float(
    os.environ.get("INTERPRETIVE_MAX_CONVICTION_HIGH_IMPACT", "0.65")
)

# Minimum absolute headline move (%) required for factual direction promotion.
# Sub-threshold moves ("dips 3%") are too small to be a strong directional signal.
HEADLINE_MOVE_FACTUAL_THRESHOLD: float = 10.0

# Regime-flag conviction multipliers (interpretive — scale only, never set direction)
REGIME_CONVICTION_ADJ: dict[str, float] = {
    "risk_on":     1.00,   # Neutral: supportive regime, no change
    "risk_off":    0.90,   # Minor headwind: reduce 10 %
    "high_vol":    0.85,   # Elevated noise: reduce 15 %
    "compression": 0.95,   # Range-bound: reduce 5 %
}


# ── Controlled vocabularies ────────────────────────────────────────────────────

_VALID_SIGNAL_BIAS  = frozenset({"long", "short", "neutral"})
_VALID_PRICED_IN    = frozenset({"yes", "partially", "no"})
_VALID_REGIME_FLAGS = frozenset({"risk_on", "risk_off", "high_vol", "compression"})
_VALID_FDA_OUTCOMES = frozenset({
    "approved", "rejected", "denied", "delayed",
    "complete_response_letter", "fast_track", "breakthrough", "failed",
})
_ANALYST_BULLISH = frozenset({
    "buy", "strong buy", "outperform", "overweight", "upgrade", "accumulate",
})
_ANALYST_BEARISH = frozenset({
    "sell", "strong sell", "underperform", "underweight", "downgrade", "reduce",
})

# Ticker sanity: 1–5 uppercase ASCII letters, nothing else
_TICKER_RE = re.compile(r"^[A-Z]{1,5}$")


# ── Data classes ───────────────────────────────────────────────────────────────

class LLMTrustClass(str, Enum):
    FACTUAL      = "factual"       # Structured vendor data — ground truth
    INTERPRETIVE = "interpretive"  # Soft LLM reasoning
    INVALID      = "invalid"       # Failed validation — treated as absent


@dataclass
class ValidationIssue:
    """One detected problem with a single LLM output field."""
    field: str
    raw_value: Any
    reason: str
    trust_class: LLMTrustClass


@dataclass
class LLMValidationResult:
    """
    Outcome of validating all LLM outputs on one SummarizedRecord.

    Consumers (signal_aggregator) use the ``cleaned_*`` fields rather than
    reading from the record directly, so invalid raw values never leak
    into trading logic.
    """
    # Factual tier
    facts_validated: bool = True      # False if ≥1 factual field failed

    # Interpretive tier — cleaned (validated) values
    cleaned_impact_day: float | None = None
    cleaned_impact_swing: float | None = None
    cleaned_source_credibility: float | None = None
    cleaned_signal_bias: str | None = None        # None → no directional prior
    cleaned_priced_in: str | None = None          # None → no penalty
    cleaned_regime_flag: str | None = None        # None → no adjustment
    cleaned_sympathy_plays: list[str] = field(default_factory=list)

    # Quality summary
    interpretive_trust_score: float = 1.0  # Degrades 0.15 per invalid field
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def has_valid_facts(self) -> bool:
        """True when at least one factual field carried a non-None cleaned value."""
        return self._has_valid_facts

    # Internal backing attribute (set by validate_llm_output)
    _has_valid_facts: bool = field(default=False, repr=False)


# ── Public API ─────────────────────────────────────────────────────────────────

def validate_llm_output(record) -> LLMValidationResult:
    """
    Validate and clean all LLM-generated fields on a SummarizedRecord.

    Returns a LLMValidationResult whose ``cleaned_*`` fields are safe to use
    directly in trading logic.  Raw fields on the record are never mutated.
    """
    result = LLMValidationResult()
    invalid_count = 0

    # ── impact_day ────────────────────────────────────────────────────────
    result.cleaned_impact_day = _validate_unit_float(
        "impact_day", record.impact_day, result
    )
    if result.cleaned_impact_day is None and record.impact_day is not None:
        invalid_count += 1

    # ── impact_swing ──────────────────────────────────────────────────────
    result.cleaned_impact_swing = _validate_unit_float(
        "impact_swing", record.impact_swing, result
    )
    if result.cleaned_impact_swing is None and record.impact_swing is not None:
        invalid_count += 1

    # ── source_credibility ────────────────────────────────────────────────
    result.cleaned_source_credibility = _validate_unit_float(
        "source_credibility", record.source_credibility, result
    )
    if result.cleaned_source_credibility is None and record.source_credibility is not None:
        invalid_count += 1

    # ── signal_bias ───────────────────────────────────────────────────────
    raw_bias = (record.signal_bias or "").strip().lower()
    if raw_bias in _VALID_SIGNAL_BIAS:
        # Treat "neutral" the same as absent — no directional prior
        result.cleaned_signal_bias = raw_bias if raw_bias != "neutral" else None
    else:
        if raw_bias:
            result.issues.append(ValidationIssue(
                field="signal_bias", raw_value=record.signal_bias,
                reason=f"not in {sorted(_VALID_SIGNAL_BIAS)}",
                trust_class=LLMTrustClass.INVALID,
            ))
            invalid_count += 1
        result.cleaned_signal_bias = None

    # ── priced_in ─────────────────────────────────────────────────────────
    raw_pi = (record.priced_in or "").strip().lower()
    if raw_pi in _VALID_PRICED_IN:
        result.cleaned_priced_in = raw_pi
    else:
        if raw_pi:
            result.issues.append(ValidationIssue(
                field="priced_in", raw_value=record.priced_in,
                reason=f"not in {sorted(_VALID_PRICED_IN)}",
                trust_class=LLMTrustClass.INVALID,
            ))
            invalid_count += 1
        result.cleaned_priced_in = None  # absent → no penalty

    # ── regime_flag ───────────────────────────────────────────────────────
    regime_raw = record.regime_flag
    raw_rf = (regime_raw.value if hasattr(regime_raw, "value") else str(regime_raw or "")).lower()
    if raw_rf in _VALID_REGIME_FLAGS:
        result.cleaned_regime_flag = raw_rf
    else:
        if raw_rf:
            result.issues.append(ValidationIssue(
                field="regime_flag", raw_value=raw_rf,
                reason=f"not in {sorted(_VALID_REGIME_FLAGS)}",
                trust_class=LLMTrustClass.INVALID,
            ))
            invalid_count += 1
        result.cleaned_regime_flag = None

    # ── sympathy_plays ────────────────────────────────────────────────────
    valid_plays: list[str] = []
    for ticker in (record.sympathy_plays or []):
        t = ticker.strip().upper()
        if _TICKER_RE.match(t):
            valid_plays.append(t)
        else:
            result.issues.append(ValidationIssue(
                field="sympathy_plays", raw_value=ticker,
                reason="not a valid ticker (must match [A-Z]{1,5})",
                trust_class=LLMTrustClass.INVALID,
            ))
            invalid_count += 1
    result.cleaned_sympathy_plays = valid_plays

    # ── facts_json (factual tier) ─────────────────────────────────────────
    has_facts = False
    if record.facts_json is not None:
        facts_ok, has_facts = _validate_facts_json(record.facts_json, result)
        if not facts_ok:
            result.facts_validated = False

    # Compute interpretive trust score
    if invalid_count > 0:
        result.interpretive_trust_score = max(0.0, 1.0 - (invalid_count * 0.15))

    # Set _has_valid_facts backing attribute
    object.__setattr__(result, "_has_valid_facts", has_facts)

    return result


def direction_from_facts(facts_json) -> str:
    """
    Derive trade direction from validated structural facts only.

    This is the ONLY path to direction_source="facts".
    Returns "long", "short", or "neutral".

    Precedence (strongest fact wins):
      1. eps_beat (earnings)
      2. guidance_raised / guidance_lowered (standalone guidance)
      3. rating_new (analyst)
      4. deal_price > 0 (M&A target)
      5. fda_outcome (regulatory)
    """
    if not facts_json:
        return "neutral"

    # Earnings: eps_beat is the strongest single fact
    if facts_json.eps_beat is True:
        return "long"
    if facts_json.eps_beat is False:
        return "short"

    # Standalone guidance (no EPS data, or EPS was None)
    guidance_up   = facts_json.guidance_raised  is True
    guidance_down = facts_json.guidance_lowered is True
    if guidance_up and not guidance_down:
        return "long"
    if guidance_down and not guidance_up:
        return "short"
    # Conflicting guidance → neutral (management sending mixed signals)

    # Analyst: rating alone is sufficient; PT alone is too ambiguous
    if facts_json.rating_new:
        rating = facts_json.rating_new.lower()
        if any(r in rating for r in _ANALYST_BULLISH):
            return "long"
        if any(r in rating for r in _ANALYST_BEARISH):
            return "short"

    # M&A: target company is always long (premium capture)
    if facts_json.deal_price and facts_json.deal_price > 0:
        return "long"

    # FDA
    if facts_json.fda_outcome:
        outcome = facts_json.fda_outcome.lower()
        if any(w in outcome for w in ("approved", "fast_track", "breakthrough")):
            return "long"
        if any(w in outcome for w in ("rejected", "denied", "failed", "complete_response")):
            return "short"

    # Headline price action (must be pre-validated against Polygon by signal_aggregator)
    # headline_move_validated is False by default; it becomes True only after the
    # Polygon cross-validation call in signal_aggregator.process().
    if (facts_json.headline_move_pct is not None
            and facts_json.headline_move_validated is True):
        if facts_json.headline_move_pct <= -HEADLINE_MOVE_FACTUAL_THRESHOLD:
            return "short"
        if facts_json.headline_move_pct >= HEADLINE_MOVE_FACTUAL_THRESHOLD:
            return "long"
        # Between -10% and +10%: too small for strong directional signal → fall through

    return "neutral"


def apply_conviction_cap(
    conviction: float,
    direction_source: str,
    impact_day: float = 0.0,
) -> tuple[float, bool]:
    """
    Apply the interpretive ceiling when direction came from LLM bias only.

    Returns (final_conviction, cap_was_applied).
    The cap is never applied to fact-driven signals — factual evidence can
    generate high-conviction trades; LLM opinion cannot.

    When impact_day >= 0.9, a slightly higher cap is used (0.65 vs 0.60)
    to avoid dropping extremely high-impact interpretive signals.
    """
    if direction_source == "interpretive_prior":
        cap = (INTERPRETIVE_MAX_CONVICTION_HIGH_IMPACT
               if impact_day >= 0.9
               else INTERPRETIVE_MAX_CONVICTION)
        if conviction > cap:
            return round(cap, 3), True
    return conviction, False


def apply_regime_adjustment(conviction: float, regime_flag: str | None) -> float:
    """
    Apply a regime-based conviction multiplier.

    regime_flag is an interpretive field — it may only scale conviction down,
    never determine direction.  A "risk_off" flag on a genuine earnings beat
    does NOT flip direction to short; it reduces confidence by 10 %.
    """
    if not regime_flag:
        return conviction
    multiplier = REGIME_CONVICTION_ADJ.get(regime_flag, 1.00)
    return round(conviction * multiplier, 3)


async def validate_headline_move(
    ticker: str,
    headline_move_pct: float,
    http_client,       # httpx.AsyncClient
    api_key: str,
    tolerance: float = 0.5,   # actual move must be ≥ tolerance × headline claim
) -> bool:
    """
    Cross-validate headline_move_pct against actual Polygon price data.

    Returns True if the actual price movement confirms the headline direction
    and magnitude (within tolerance).  Returns False if:
      - Polygon data is unavailable (fail-safe: reject factual promotion)
      - Actual move contradicts headline direction
      - Actual move is less than tolerance × headline magnitude

    tolerance=0.5 means: "sinks 25%" requires the stock to be down at least
    12.5% intraday.  Deliberately loose because headlines round ("sinks 25%"
    when actual is -22%) but tight enough to catch fabricated/historical claims.
    """
    if not api_key or not http_client:
        return False  # No API access — cannot validate, reject promotion

    try:
        resp = await http_client.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}",
            params={"apiKey": api_key},
            timeout=5.0,
        )
        if resp.status_code != 200:
            return False

        snap = resp.json().get("ticker", {})
        today_change = snap.get("todaysChangePerc", 0.0)

        if today_change is None or today_change == 0.0:
            # Try computing from prevDay vs current
            prev_close = snap.get("prevDay", {}).get("c", 0)
            current    = snap.get("day", {}).get("c", 0) or snap.get("lastTrade", {}).get("p", 0)
            if prev_close and current and prev_close > 0:
                today_change = ((current - prev_close) / prev_close) * 100
            else:
                return False  # Can't compute — reject

        # Direction must match
        if headline_move_pct > 0 and today_change <= 0:
            return False
        if headline_move_pct < 0 and today_change >= 0:
            return False

        # Magnitude check: actual move must be at least tolerance × headline claim
        min_expected = abs(headline_move_pct) * tolerance
        if abs(today_change) < min_expected:
            return False

        return True

    except Exception:
        return False  # Any error — fail safe, reject promotion


# ── Internal helpers ───────────────────────────────────────────────────────────

def _validate_unit_float(
    name: str,
    value: Any,
    result: LLMValidationResult,
) -> float | None:
    """Validate a 0–1 float.  Returns the value if valid, else None."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        result.issues.append(ValidationIssue(
            field=name, raw_value=value,
            reason="not a number",
            trust_class=LLMTrustClass.INTERPRETIVE,
        ))
        return None
    if not (0.0 <= v <= 1.0):
        result.issues.append(ValidationIssue(
            field=name, raw_value=value,
            reason=f"out of range [0, 1]: {v:.4f}",
            trust_class=LLMTrustClass.INTERPRETIVE,
        ))
        return None
    return v


def _validate_facts_json(facts, result: LLMValidationResult) -> tuple[bool, bool]:
    """
    Validate factual fields in a FactsJson object.

    Returns (all_ok: bool, has_any_meaningful_fact: bool).
    Non-blocking: unknown FDA vocabulary is noted but doesn't fail validation.
    """
    all_ok = True
    has_fact = False

    # EPS fields — plausible range check
    for fname in ("eps_actual", "eps_estimate"):
        v = getattr(facts, fname, None)
        if v is not None:
            try:
                fv = float(v)
                if not (-1_000 < fv < 1_000):
                    result.issues.append(ValidationIssue(
                        field=fname, raw_value=v,
                        reason=f"implausible EPS value: {fv}",
                        trust_class=LLMTrustClass.FACTUAL,
                    ))
                    all_ok = False
                else:
                    has_fact = True
            except (TypeError, ValueError):
                result.issues.append(ValidationIssue(
                    field=fname, raw_value=v,
                    reason="not a number", trust_class=LLMTrustClass.FACTUAL,
                ))
                all_ok = False

    # Boolean flags — must be literal bool
    for fname in ("eps_beat", "revenue_beat", "guidance_raised", "guidance_lowered"):
        v = getattr(facts, fname, None)
        if v is not None:
            if not isinstance(v, bool):
                result.issues.append(ValidationIssue(
                    field=fname, raw_value=v,
                    reason="must be bool",
                    trust_class=LLMTrustClass.FACTUAL,
                ))
                all_ok = False
            else:
                has_fact = True

    # Deal price — must be positive
    if facts.deal_price is not None:
        try:
            dp = float(facts.deal_price)
            if dp <= 0:
                result.issues.append(ValidationIssue(
                    field="deal_price", raw_value=facts.deal_price,
                    reason="deal_price must be positive",
                    trust_class=LLMTrustClass.FACTUAL,
                ))
                all_ok = False
            else:
                has_fact = True
        except (TypeError, ValueError):
            all_ok = False

    # Price target — must be positive
    if facts.price_target_new is not None:
        try:
            pt = float(facts.price_target_new)
            if pt <= 0:
                result.issues.append(ValidationIssue(
                    field="price_target_new", raw_value=facts.price_target_new,
                    reason="price_target_new must be positive",
                    trust_class=LLMTrustClass.FACTUAL,
                ))
                all_ok = False
            else:
                has_fact = True
        except (TypeError, ValueError):
            all_ok = False

    # Rating new — non-empty string is sufficient
    if facts.rating_new and isinstance(facts.rating_new, str) and facts.rating_new.strip():
        has_fact = True

    # FDA outcome — vocabulary check (non-blocking)
    if facts.fda_outcome is not None:
        if facts.fda_outcome.lower() not in _VALID_FDA_OUTCOMES:
            result.issues.append(ValidationIssue(
                field="fda_outcome", raw_value=facts.fda_outcome,
                reason=f"unknown FDA outcome — treating as neutral",
                trust_class=LLMTrustClass.FACTUAL,
            ))
            # Non-blocking: unknown outcome → neutral direction, not invalid record
        else:
            has_fact = True

    return all_ok, has_fact
