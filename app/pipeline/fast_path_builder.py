"""
Fast-path builder — constructs a SummarizedRecord directly from structured
vendor facts, bypassing LLM T1/T2 summarization entirely.

Called by AISummarizerService when RouteDecision.route_type == "fast".

Guarantees:
  - llm_tokens_used = 0
  - llm_cost_usd    = 0.0
  - route_type      = "fast"
  - t2_summary      = None  (requires LLM judgment)
  - priced_in       = None  (requires LLM judgment)

Impact score tables are conservative estimates calibrated from historical
catalyst moves. They are intentionally slightly lower than LLM estimates
because we lack the full qualitative context that T2 provides.
"""
from __future__ import annotations

from app.models.news import (
    CatalystType,
    EnrichedRecord,
    FactsJson,
    NewsSource,
    SummarizedRecord,
)
from app.pipeline.route_classifier import RouteDecision

# Ratings that unambiguously indicate a bullish change
_BULLISH = frozenset({
    "buy", "strong buy", "outperform", "overweight", "upgrade",
    "positive", "add", "accumulate", "top pick",
})
# Ratings that unambiguously indicate a bearish change
_BEARISH = frozenset({
    "sell", "strong sell", "underperform", "underweight", "downgrade",
    "negative", "reduce", "trim",
})

FAST_PATH_VERSION = "fast_path_v1"


# ── Public entry point ────────────────────────────────────────────────────────

def build_fast_path_summary(
    record: EnrichedRecord,
    route: RouteDecision,
) -> SummarizedRecord:
    """
    Build a SummarizedRecord from structured facts in route.extracted_facts.

    This is the fast-path equivalent of AISummarizerService._run_t1 + _run_t2.
    """
    facts      = route.extracted_facts
    catalyst   = record.catalyst_type

    direction                  = _derive_direction(facts, catalyst)
    impact_day, impact_swing   = _compute_impact(facts, catalyst, direction)
    t1_summary                 = _generate_t1_summary(facts, catalyst, record.tickers)
    facts_json                 = _build_facts_json(facts)
    source_cred                = _source_credibility(record)

    return SummarizedRecord(
        **record.model_dump(),
        t1_summary      = t1_summary,
        t2_summary      = None,          # LLM-only; not available on fast path
        facts_json      = facts_json,
        impact_day      = impact_day,
        impact_swing    = impact_swing,
        signal_bias     = direction,     # structural facts → reliable directional read
        regime_flag     = None,
        source_credibility  = source_cred,
        priced_in       = None,          # Requires LLM context
        priced_in_reason= None,
        sympathy_plays  = [],
        route_type      = "fast",
        prompt_version  = f"{FAST_PATH_VERSION}:{route.reason}",
        llm_tokens_used = 0,
        llm_cost_usd    = 0.0,
    )


# ── Direction derivation ──────────────────────────────────────────────────────

def _derive_direction(facts: dict, catalyst: CatalystType) -> str:
    """
    Derive signal_bias from structured facts alone.
    Returns "long" | "short" | "neutral".
    """
    if catalyst == CatalystType.EARNINGS:
        eps_beat = facts.get("eps_beat")
        if eps_beat is True:
            # Beat + guidance cut = "sell the news" pattern → neutral rather than long
            if facts.get("guidance_lowered"):
                return "neutral"
            return "long"
        if eps_beat is False:
            return "short"

    if catalyst == CatalystType.ANALYST:
        rating = (facts.get("rating_new") or "").lower().strip()
        if any(r in rating for r in _BULLISH):
            return "long"
        if any(r in rating for r in _BEARISH):
            return "short"
        # PT-only change without explicit rating action
        pt_new  = facts.get("price_target_new")
        pt_prev = facts.get("price_target_prev")
        if pt_new is not None and pt_prev is not None:
            try:
                return "long" if float(pt_new) > float(pt_prev) else "short"
            except (TypeError, ValueError):
                pass

    if catalyst == CatalystType.MA:
        return "long"   # Target company — almost always gap up

    if catalyst == CatalystType.REGULATORY:
        outcome = (facts.get("fda_outcome") or "").lower()
        if "approv" in outcome or "clear" in outcome:
            return "long"
        if any(w in outcome for w in ("reject", "den", "refus", "fail", "crl")):
            return "short"

    # Standalone guidance
    if facts.get("guidance_raised"):
        return "long"
    if facts.get("guidance_lowered"):
        return "short"

    # Trading halt — direction is unclear until halt lifts
    halt_action = (facts.get("halt_action") or "").lower()
    if halt_action in ("halt", "halted"):
        return "neutral"    # Could go either way when trading resumes

    return "neutral"


# ── Impact scoring ────────────────────────────────────────────────────────────

def _compute_impact(
    facts: dict,
    catalyst: CatalystType,
    direction: str,
) -> tuple[float, float]:
    """
    Return (impact_day, impact_swing) ∈ [0.0, 1.0].

    Scores are intentionally conservative relative to LLM estimates because
    we lack qualitative context (market expectations, sector conditions, etc.).
    """
    if catalyst == CatalystType.EARNINGS:
        beat_pct = abs(facts.get("beat_magnitude_pct") or 0.0)
        if beat_pct >= 15:
            base = 0.82
        elif beat_pct >= 5:
            base = 0.72
        else:
            base = 0.62
        # Guidance modifier
        if facts.get("guidance_raised"):
            base = min(1.0, base + 0.08)
        elif facts.get("guidance_lowered"):
            base = max(0.30, base - 0.12)
        impact_day   = base
        impact_swing = round(base * 0.70, 3)

    elif catalyst == CatalystType.ANALYST:
        rating  = (facts.get("rating_new") or "").lower()
        pt_new  = facts.get("price_target_new")
        pt_prev = facts.get("price_target_prev")
        pt_change_pct = 0.0
        if pt_new and pt_prev:
            try:
                pt_change_pct = abs(
                    (float(pt_new) - float(pt_prev)) / float(pt_prev) * 100
                )
            except (TypeError, ZeroDivisionError):
                pass

        has_clear_action = (
            any(r in rating for r in _BULLISH) or
            any(r in rating for r in _BEARISH)
        )
        base = 0.62 if has_clear_action else 0.52
        # PT magnitude bonus: each 5% PT move adds ~0.02 to impact
        base = min(0.80, base + pt_change_pct * 0.004)
        impact_day   = base
        impact_swing = round(base * 0.55, 3)

    elif catalyst == CatalystType.MA:
        premium = float(facts.get("deal_premium_pct") or 0.0)
        # Higher premium → higher day impact (but we cap at 0.95 — residual arb risk)
        impact_day   = min(0.95, 0.80 + premium * 0.003)
        impact_swing = 0.55   # Arb spread quickly compresses once deal is announced

    elif catalyst == CatalystType.REGULATORY:
        outcome = (facts.get("fda_outcome") or "").lower()
        if "approv" in outcome or "clear" in outcome:
            impact_day   = 0.83
            impact_swing = 0.75
        elif any(w in outcome for w in ("reject", "den", "crl")):
            impact_day   = 0.78
            impact_swing = 0.65
        else:
            impact_day   = 0.58
            impact_swing = 0.45

    else:
        # Guidance standalone / halt / other
        if direction == "neutral":
            impact_day, impact_swing = 0.40, 0.30
        else:
            impact_day, impact_swing = 0.58, 0.42

    return round(impact_day, 3), round(impact_swing, 3)


# ── T1 summary template ───────────────────────────────────────────────────────

def _generate_t1_summary(
    facts: dict,
    catalyst: CatalystType,
    tickers: list[str],
) -> str:
    """Build a human-readable one-liner from structured facts."""
    ticker = tickers[0] if tickers else "Unknown"

    if catalyst == CatalystType.EARNINGS:
        actual   = facts.get("eps_actual")
        estimate = facts.get("eps_estimate")
        eps_beat = facts.get("eps_beat")
        beat_pct = facts.get("beat_magnitude_pct", 0.0)
        verb     = "beat" if eps_beat else "missed"
        mag_str  = f" (+{beat_pct:.1f}%)" if eps_beat and beat_pct else ""
        guidance_str = ""
        if facts.get("guidance_raised"):
            guidance_str = " Guidance raised."
        elif facts.get("guidance_lowered"):
            guidance_str = " Guidance cut."
        return (
            f"{ticker} {verb} EPS estimates: ${actual} vs "
            f"${estimate} expected{mag_str}.{guidance_str}"
        )

    if catalyst == CatalystType.ANALYST:
        firm       = facts.get("analyst_firm") or "Analyst"
        rating_new = facts.get("rating_new") or ""
        pt_new     = facts.get("price_target_new")
        pt_prev    = facts.get("price_target_prev")
        pt_str = ""
        if pt_new:
            if pt_prev:
                verb  = "raises" if float(pt_new) >= float(pt_prev) else "cuts"
                pt_str = f", {verb} PT to ${pt_new} from ${pt_prev}"
            else:
                pt_str = f", PT ${pt_new}"
        return f"{firm} rates {ticker} {rating_new}{pt_str}."

    if catalyst == CatalystType.MA:
        price   = facts.get("deal_price")
        premium = facts.get("deal_premium_pct")
        prem_str = f" at {premium:.1f}% premium" if premium else ""
        return f"{ticker} acquisition announced at ${price}{prem_str}."

    if catalyst == CatalystType.REGULATORY:
        outcome = facts.get("fda_outcome") or "outcome unknown"
        return f"{ticker} FDA {outcome}."

    # Guidance standalone
    if facts.get("guidance_raised"):
        return f"{ticker} raises guidance."
    if facts.get("guidance_lowered"):
        return f"{ticker} cuts guidance."

    # Halt / resume
    action = facts.get("halt_action") or "halt"
    htype  = facts.get("halt_type") or ""
    type_str = f" ({htype})" if htype and htype != action else ""
    return f"{ticker} trading {action}{type_str}."


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_facts_json(facts: dict) -> FactsJson | None:
    valid_keys = set(FactsJson.model_fields.keys())
    filtered   = {k: v for k, v in facts.items() if k in valid_keys and v is not None}
    if not filtered:
        return None
    try:
        return FactsJson(**filtered)
    except Exception:
        return None


def _source_credibility(record: EnrichedRecord) -> float:
    """
    Fast-path records come from structured vendor data (always high credibility).
    Tier-1 wire services score 0.88; others 0.75.
    """
    tier1 = {NewsSource.BENZINGA, NewsSource.POLYGON, NewsSource.DJNEWS}
    return 0.88 if record.source in tier1 else 0.75
