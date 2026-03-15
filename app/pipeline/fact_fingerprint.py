"""
Fact fingerprinting — SHA-256 of structured catalyst facts.

Used as a secondary confirmation after SimHash headline matching.
If SimHash says "similar headline" but fact fingerprints diverge,
the events are structurally different (e.g. different EPS numbers)
and should NOT be clustered.

Usage:
    from app.models.news import FactsJson

    fp1 = fact_fingerprint(FactsJson(eps_actual=2.05, eps_estimate=1.78, eps_beat=True))
    fp2 = fact_fingerprint(FactsJson(eps_actual=2.05, eps_estimate=1.78, eps_beat=True))
    assert fp1 == fp2  # identical facts → same fingerprint

    fp3 = fact_fingerprint(FactsJson(eps_actual=1.90, eps_estimate=1.78, eps_beat=True))
    assert fp1 != fp3  # different EPS actual → different event
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# Fields considered for fingerprinting, grouped by catalyst type.
# Only these fields contribute to the fingerprint — prose summaries are excluded.
_FINGERPRINT_FIELDS: tuple[str, ...] = (
    # Earnings
    "eps_actual",
    "eps_estimate",
    "eps_beat",
    "revenue_beat",
    "guidance_raised",
    "guidance_lowered",
    # Analyst
    "rating_new",
    "rating_prev",
    "price_target_new",
    "price_target_prev",
    "analyst_firm",
    # M&A
    "deal_price",
    "deal_premium_pct",
    "deal_type",
    # Regulatory
    "fda_outcome",
    "trial_phase",
    # Macro
    "actual_value",
    "estimate_value",
    "prior_value",
)

# Round floats to this many decimal places before hashing to avoid
# fingerprint divergence from floating-point precision differences.
_FLOAT_PRECISION = 2


def _normalize_value(v: Any) -> Any:
    """Normalize a field value for stable hashing."""
    if v is None:
        return None
    if isinstance(v, float):
        return round(v, _FLOAT_PRECISION)
    if isinstance(v, str):
        return v.lower().strip()
    if isinstance(v, bool):
        return v  # Keep as bool (True/False), not 1/0
    return v


def fact_fingerprint(facts_obj: Any) -> str | None:
    """
    Compute a SHA-256 fingerprint of the structured facts in a FactsJson object.

    Returns None if facts_obj is None or has no non-null fingerprint fields.
    The fingerprint is stable: field order doesn't matter (sorted keys),
    and float precision is normalized.
    """
    if facts_obj is None:
        return None

    # Build a dict of only the fingerprint fields that have non-None values
    if hasattr(facts_obj, "model_dump"):
        raw = facts_obj.model_dump()
    elif isinstance(facts_obj, dict):
        raw = facts_obj
    else:
        return None

    normalized: dict[str, Any] = {}
    for field in _FINGERPRINT_FIELDS:
        v = raw.get(field)
        if v is not None:
            normalized[field] = _normalize_value(v)

    if not normalized:
        return None  # No structured facts — cannot fingerprint

    # Serialize with sorted keys for stability, then hash
    canonical = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]  # 64-bit prefix is sufficient


def facts_overlap_score(fp1: str | None, fp2: str | None) -> float:
    """
    Compare two fact fingerprints.

    Returns:
      1.0  — identical fingerprints (same facts)
      0.0  — fingerprints differ (different facts)
      0.5  — one or both fingerprints are None (cannot confirm or deny)
    """
    if fp1 is None or fp2 is None:
        return 0.5   # Uncertain — SimHash match alone is sufficient
    return 1.0 if fp1 == fp2 else 0.0
