"""
Route Classifier — determines whether an enriched news record can take the
fast path (no LLM) or requires full T1/T2 processing.

Fast-path candidates (structured data already present):
  - Earnings beat / miss       — eps_actual + eps_estimate in vendor payload
  - Guidance raise / cut       — guidance_raised / guidance_lowered boolean flags
  - Analyst upgrades /         — rating_new + optional price_target_new
    downgrades / PT changes
  - Definitive M&A deal terms  — deal_price in payload
  - FDA approval / rejection   — fda_outcome field OR title pattern match
  - Trading halt / resume      — halt_type field OR title pattern match

Fast path is skipped when:
  - ENABLE_FAST_PATH=false
  - No structured data found for the catalyst type
  - Confidence < FAST_PATH_MIN_CONFIDENCE (default 0.85)

The decision is logged with route_type, reason, and confidence so operators
can tune thresholds and monitor fast-path hit rates.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

from app.models.news import CatalystType, EnrichedRecord

_FAST_PATH_ENABLED      = os.environ.get("ENABLE_FAST_PATH",         "true").lower() == "true"
_FAST_PATH_MIN_CONF     = float(os.environ.get("FAST_PATH_MIN_CONFIDENCE", "0.85"))

# Regex patterns used in title-based detection (case-insensitive).
# Use stem patterns without trailing \b so "approves", "approved", "approval"
# all match the "approv" stem.
_FDA_APPROVAL_RE  = re.compile(
    r"\bfda\b.{0,50}\b(approv\w*|clear\w*|grants?\s+approv\w*|complet\w+\s+review)",
    re.IGNORECASE,
)
_FDA_REJECTION_RE = re.compile(
    r"\bfda\b.{0,50}\b(reject\w*|den[yi]\w*|refus\w*|issu\w+\s+crl)",
    re.IGNORECASE,
)
_HALT_RE   = re.compile(r"\btrading\s+halt\w*|\bhalt\w*\s+(in\s+)?trading\b", re.IGNORECASE)
_RESUME_RE = re.compile(r"\btrading\s+resum\w*|\bresum\w*\s+(of\s+)?trading\b", re.IGNORECASE)


@dataclass
class RouteDecision:
    """Result of route classification for a single enriched record."""
    route_type:       str    # "fast" | "slow"
    reason:           str    # machine-readable reason code
    confidence:       float = 0.0
    extracted_facts:  dict  = field(default_factory=dict)

    @property
    def is_fast(self) -> bool:
        return self.route_type == "fast"


# ── Public entry point ────────────────────────────────────────────────────────

def classify_route(
    record: EnrichedRecord,
    raw_record: dict | None = None,
) -> RouteDecision:
    """
    Return a RouteDecision for the given enriched record.

    `raw_record` is the original Kafka dict, which may carry FMP-enriched fields
    (fmp_quote, fmp_analyst, etc.) and vendor raw_payload fields that are not
    present on the typed EnrichedRecord model.
    """
    if not _FAST_PATH_ENABLED:
        return RouteDecision("slow", reason="fast_path_disabled")

    raw = raw_record or {}

    # Merge raw_payload from record into the flat lookup dict so callers
    # don't have to drill into raw_payload themselves.
    payload: dict = {}
    if hasattr(record, "raw_payload") and record.raw_payload:
        payload.update(record.raw_payload)
    payload.update(raw)   # top-level raw_record fields take precedence

    _handlers = [
        _check_earnings,
        _check_guidance_standalone,
        _check_analyst,
        _check_ma,
        _check_fda,
        _check_halt,
    ]

    for handler in _handlers:
        decision = handler(record, payload)
        if decision is not None and decision.confidence >= _FAST_PATH_MIN_CONF:
            return decision

    return RouteDecision("slow", reason="insufficient_structured_data", confidence=0.0)


# ── Individual catalyst handlers ──────────────────────────────────────────────

def _get(payload: dict, *keys: str):
    """Return the first non-None value from payload for any of the given keys."""
    for k in keys:
        v = payload.get(k)
        if v is not None:
            return v
    return None


def _check_earnings(record: EnrichedRecord, payload: dict) -> RouteDecision | None:
    """Fast path for earnings with explicit EPS actuals + estimates."""
    if record.catalyst_type != CatalystType.EARNINGS:
        return None

    eps_actual   = _get(payload, "eps_actual",   "epsActual",   "eps",     "reportedEPS")
    eps_estimate = _get(payload, "eps_estimate",  "epsEstimate", "consensus", "expectedEPS")
    if eps_actual is None or eps_estimate is None:
        return None

    try:
        eps_actual   = float(eps_actual)
        eps_estimate = float(eps_estimate)
    except (TypeError, ValueError):
        return None

    eps_beat          = eps_actual >= eps_estimate
    beat_magnitude    = ((eps_actual - eps_estimate) / abs(eps_estimate) * 100
                         if eps_estimate != 0 else 0.0)
    guidance_raised   = bool(_get(payload, "guidance_raised",  "guidanceRaised"))
    guidance_lowered  = bool(_get(payload, "guidance_lowered", "guidanceLowered"))
    revenue_beat      = _get(payload, "revenue_beat", "revenueBeat")

    return RouteDecision(
        route_type="fast",
        reason="earnings_structured_eps",
        confidence=0.95,
        extracted_facts={
            "eps_actual":          round(eps_actual,   4),
            "eps_estimate":        round(eps_estimate, 4),
            "eps_beat":            eps_beat,
            "beat_magnitude_pct":  round(beat_magnitude, 2),
            "guidance_raised":     guidance_raised  or None,
            "guidance_lowered":    guidance_lowered or None,
            "revenue_beat":        bool(revenue_beat) if revenue_beat is not None else None,
        },
    )


def _check_guidance_standalone(record: EnrichedRecord, payload: dict) -> RouteDecision | None:
    """Fast path for guidance raise/cut without EPS data (e.g. mid-quarter update)."""
    # Only fire if NOT also an earnings record (earnings handler already captures guidance)
    if record.catalyst_type == CatalystType.EARNINGS:
        return None

    guidance_raised  = _get(payload, "guidance_raised",  "guidanceRaised")
    guidance_lowered = _get(payload, "guidance_lowered",  "guidanceLowered")
    if not guidance_raised and not guidance_lowered:
        return None

    return RouteDecision(
        route_type="fast",
        reason="guidance_structured",
        confidence=0.90,
        extracted_facts={
            "guidance_raised":  bool(guidance_raised),
            "guidance_lowered": bool(guidance_lowered),
            "eps_actual":       _get(payload, "eps_actual"),
            "eps_estimate":     _get(payload, "eps_estimate"),
        },
    )


def _check_analyst(record: EnrichedRecord, payload: dict) -> RouteDecision | None:
    """Fast path for analyst rating changes / price-target updates."""
    if record.catalyst_type != CatalystType.ANALYST:
        return None

    rating_new   = _get(payload, "rating_new",  "ratingNew",  "newGrade",  "toGrade",   "action")
    pt_new       = _get(payload, "price_target_new", "priceTargetNew",  "newPriceTarget",  "priceTarget")
    analyst_firm = _get(payload, "analyst_firm", "analystFirm", "firm", "analyst")
    rating_prev  = _get(payload, "rating_prev",  "ratingPrev",  "fromGrade", "prevGrade")
    pt_prev      = _get(payload, "price_target_prev", "priceTargetPrev", "prevPriceTarget")

    if rating_new is None and pt_new is None:
        return None

    # Higher confidence when both rating AND PT are present
    confidence = 0.92 if (rating_new and pt_new) else 0.85

    facts: dict = {
        "rating_new":    str(rating_new).strip()  if rating_new   else None,
        "rating_prev":   str(rating_prev).strip() if rating_prev  else None,
        "analyst_firm":  str(analyst_firm).strip() if analyst_firm else None,
    }
    for key, raw_val in (("price_target_new", pt_new), ("price_target_prev", pt_prev)):
        if raw_val is not None:
            try:
                facts[key] = float(raw_val)
            except (TypeError, ValueError):
                pass

    return RouteDecision(
        route_type="fast",
        reason="analyst_structured_rating",
        confidence=confidence,
        extracted_facts=facts,
    )


def _check_ma(record: EnrichedRecord, payload: dict) -> RouteDecision | None:
    """Fast path for M&A with definitive deal terms (deal price present)."""
    if record.catalyst_type != CatalystType.MA:
        return None

    deal_price   = _get(payload, "deal_price",   "dealPrice",   "acquisition_price", "offerPrice")
    deal_premium = _get(payload, "deal_premium_pct", "dealPremiumPct", "premium_pct", "premiumPct")
    deal_type    = _get(payload, "deal_type",    "dealType")

    if deal_price is None:
        return None

    try:
        deal_price   = float(deal_price)
        deal_premium = float(deal_premium) if deal_premium is not None else None
    except (TypeError, ValueError):
        return None

    return RouteDecision(
        route_type="fast",
        reason="ma_structured_deal_terms",
        confidence=0.95,
        extracted_facts={
            "deal_price":       deal_price,
            "deal_premium_pct": deal_premium,
            "deal_type":        str(deal_type).lower() if deal_type else "acquisition",
        },
    )


def _check_fda(record: EnrichedRecord, payload: dict) -> RouteDecision | None:
    """Fast path for FDA/regulatory outcome — structured field OR title pattern."""
    if record.catalyst_type != CatalystType.REGULATORY:
        return None

    fda_outcome  = _get(payload, "fda_outcome", "fdaOutcome", "regulatoryOutcome", "outcome")
    trial_phase  = _get(payload, "trial_phase", "trialPhase", "phase")

    if fda_outcome is None:
        # Fall back to title-pattern inference
        title = record.title or ""
        if _FDA_APPROVAL_RE.search(title):
            fda_outcome = "approved"
        elif _FDA_REJECTION_RE.search(title):
            fda_outcome = "rejected"

    if fda_outcome is None:
        return None

    return RouteDecision(
        route_type="fast",
        reason="regulatory_fda_outcome",
        confidence=0.90,
        extracted_facts={
            "fda_outcome":  str(fda_outcome).lower().strip(),
            "trial_phase":  str(trial_phase).lower() if trial_phase else None,
        },
    )


def _check_halt(record: EnrichedRecord, payload: dict) -> RouteDecision | None:
    """Fast path for trading halts / resumes — always fully deterministic."""
    halt_type   = _get(payload, "halt_type",   "haltType",   "halt_reason", "haltReason")
    halt_action = _get(payload, "halt_action", "haltAction")
    title       = record.title or ""

    if halt_action is None:
        if _RESUME_RE.search(title):
            halt_action = "resume"
        elif _HALT_RE.search(title):
            halt_action = "halt"

    if halt_type is None and halt_action is None:
        return None

    return RouteDecision(
        route_type="fast",
        reason="trading_halt_structured",
        confidence=1.0,
        extracted_facts={
            "halt_type":   str(halt_type).lower()   if halt_type   else halt_action,
            "halt_action": str(halt_action).lower() if halt_action else "halt",
        },
    )
