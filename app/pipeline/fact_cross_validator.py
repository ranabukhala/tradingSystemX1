"""
Structured fact cross-validator (v1.7).

Compares LLM-extracted facts_json fields against authoritative vendor data
(FMP earnings calendar, FMP analyst grades) stored in Postgres.

Validation hierarchy
────────────────────
  CONFIRMED     All verifiable fields matched vendor data within tolerance.
  PARTIAL       ≥1 verifiable field confirmed; ≥1 other field is unverifiable.
  MISMATCH      ≥1 verifiable field diverged from vendor data beyond tolerance.
  UNVERIFIABLE  No vendor data available for any field (DB empty or missing).
  SKIPPED       Catalyst type has no cross-checkable facts, or fast path record.

Conviction multipliers (applied after all other adjustments in compute_conviction)
──────────────────────────────────────────────────────────────────────────────────
  CONFIRMED              × FACT_CONVICTION_CONFIRMED   (default 1.10) — small boost
  PARTIAL                × FACT_CONVICTION_PARTIAL     (default 0.85) — uncertainty
  MISMATCH (key field)   × FACT_CONVICTION_MISMATCH_KEY    (default 0.30) — direction likely wrong
  MISMATCH (non-key)     × FACT_CONVICTION_MISMATCH_NONKEY (default 0.60) — magnitude wrong
  UNVERIFIABLE           × FACT_CONVICTION_UNVERIFIABLE    (default 0.90) — can't verify
  SKIPPED                × 1.00 — no change

Key fields (direction-determining)
───────────────────────────────────
  eps_beat, guidance_raised, guidance_lowered, deal_price, fda_outcome
  A mismatch in any of these means the LLM may have gotten the DIRECTION wrong —
  the heaviest penalty applies.

Non-key fields (magnitude / context)
──────────────────────────────────────
  eps_actual, eps_estimate, revenue_beat, price_target_new, deal_premium_pct,
  analyst_firm
  These refine the signal strength, but a mismatch doesn't flip direction.

Fast path bypass
────────────────
  route_type="fast" records are built directly from structured vendor data, not
  from LLM extraction.  Cross-validation against the DB is skipped (SKIPPED
  status).  Internal consistency checks still run — they cost no DB round-trip
  and can catch bugs in the fast-path builder itself.

Internal consistency checks (always run, zero DB cost)
───────────────────────────────────────────────────────
  • eps_beat direction must be consistent with eps_actual vs eps_estimate
    if both numeric fields are present.
  • guidance_raised and guidance_lowered cannot both be True simultaneously.

Audit persistence
─────────────────
  Every MISMATCH (and optionally all statuses when FACT_AUDIT_ALL_STATUSES=true)
  is written to the fact_validation_audit table as a fire-and-forget async task.
  A write failure never blocks the signal pipeline.
"""
from __future__ import annotations

import asyncio
import json as _json
import math
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import UUID, uuid4


# ── Configuration ──────────────────────────────────────────────────────────────

# Numeric tolerances.  LLM rounds aggressively; 5 % is tight but fair for EPS.
_EPS_TOLERANCE_PCT          = float(os.environ.get("FACT_EPS_TOLERANCE_PCT",         "5.0"))
_PRICE_TARGET_TOLERANCE_PCT = float(os.environ.get("FACT_PT_TOLERANCE_PCT",          "10.0"))
_DEAL_PRICE_TOLERANCE_PCT   = float(os.environ.get("FACT_DEAL_PRICE_TOLERANCE_PCT",  "2.0"))

# How far back to look for matching vendor data
_EARNINGS_LOOKBACK_DAYS = int(os.environ.get("FACT_EARNINGS_LOOKBACK_DAYS", "3"))
_ANALYST_LOOKBACK_HOURS = int(os.environ.get("FACT_ANALYST_LOOKBACK_HOURS", "48"))

# Conviction multipliers (applied as the final trust adjustment)
CONVICTION_CONFIRMED       = float(os.environ.get("FACT_CONVICTION_CONFIRMED",       "1.10"))
CONVICTION_PARTIAL         = float(os.environ.get("FACT_CONVICTION_PARTIAL",         "0.85"))
CONVICTION_MISMATCH_KEY    = float(os.environ.get("FACT_CONVICTION_MISMATCH_KEY",    "0.30"))
CONVICTION_MISMATCH_NONKEY = float(os.environ.get("FACT_CONVICTION_MISMATCH_NONKEY", "0.60"))
CONVICTION_UNVERIFIABLE    = float(os.environ.get("FACT_CONVICTION_UNVERIFIABLE",    "0.90"))

# Write audit row for all statuses (not just MISMATCH)
_AUDIT_ALL = os.environ.get("FACT_AUDIT_ALL_STATUSES", "false").lower() == "true"

# Kill-switch: set ENABLE_FACT_VALIDATION=false to disable DB cross-checks entirely
ENABLE_FACT_VALIDATION = os.environ.get("ENABLE_FACT_VALIDATION", "true").lower() == "true"


# ── Key-field set (direction-determining) ──────────────────────────────────────

_KEY_FIELDS: frozenset[str] = frozenset({
    "eps_beat",
    "guidance_raised",
    "guidance_lowered",
    "deal_price",
    "fda_outcome",
})


# ── Enums & data classes ────────────────────────────────────────────────────────

class FactValidationStatus(str, Enum):
    CONFIRMED    = "confirmed"     # LLM and vendor agree (within tolerance)
    PARTIAL      = "partial"       # ≥1 match + ≥1 unverifiable
    MISMATCH     = "mismatch"      # ≥1 field diverged from vendor
    UNVERIFIABLE = "unverifiable"  # No vendor data available
    SKIPPED      = "skipped"       # Catalyst type / fast path — nothing to check


@dataclass
class FieldValidationResult:
    """Validation outcome for a single LLM-extracted fact field."""
    field:         str
    llm_value:     Any
    vendor_value:  Any
    status:        FactValidationStatus
    tolerance_pct: float | None = None   # Tolerance used for numeric comparisons
    deviation_pct: float | None = None   # How far off the LLM was (% deviation)
    is_key_field:  bool = False           # True → direction-determining field


@dataclass
class FactCrossValidationResult:
    """
    Cross-validation outcome for all facts_json fields on one record.

    Consumers (signal_aggregator.compute_conviction) should apply
    conviction_multiplier as the final trust-based adjustment.
    """
    validation_status:     FactValidationStatus
    validated_fields:      list[str]           # fields confirmed against vendor data
    mismatch_fields:       list[str]           # fields that diverged from vendor
    unverifiable_fields:   list[str]           # fields where no vendor data existed
    validation_confidence: float               # 0.0–1.0 fraction of verifiable that matched
    conviction_multiplier: float               # pre-computed, applied by signal_aggregator
    field_results:         list[FieldValidationResult]
    vendor_source:         str | None = None   # "fmp_earnings" | "fmp_analyst" | "internal" | None
    has_key_mismatch:      bool = False        # True when ≥1 direction field mismatched
    audit_id:              UUID | None = None  # Set after audit row is written


# ── Pre-built constant results ──────────────────────────────────────────────────

def _skipped_result() -> FactCrossValidationResult:
    return FactCrossValidationResult(
        validation_status=FactValidationStatus.SKIPPED,
        validated_fields=[], mismatch_fields=[], unverifiable_fields=[],
        validation_confidence=1.0,
        conviction_multiplier=1.0,
        field_results=[],
    )


def _unverifiable_result(
    field_names: list[str] | None = None,
) -> FactCrossValidationResult:
    return FactCrossValidationResult(
        validation_status=FactValidationStatus.UNVERIFIABLE,
        validated_fields=[],
        mismatch_fields=[],
        unverifiable_fields=field_names or [],
        validation_confidence=0.0,
        conviction_multiplier=CONVICTION_UNVERIFIABLE,
        field_results=[],
    )


# ── Validator ───────────────────────────────────────────────────────────────────

class FactCrossValidator:
    """
    Cross-validates LLM-extracted facts_json against authoritative vendor data.

    Parameters
    ----------
    db_pool : asyncpg.Pool | None
        Postgres connection pool.  When None all DB-dependent checks return
        UNVERIFIABLE — graceful degradation; never stalls the pipeline.

    Usage::

        validator = FactCrossValidator(db_pool=pool)
        result = await validator.validate(record, ticker="AAPL")
        conviction *= result.conviction_multiplier
    """

    def __init__(self, db_pool=None) -> None:
        self._pool = db_pool

    # ── Public entry point ──────────────────────────────────────────────────

    async def validate(
        self,
        record,        # SummarizedRecord
        ticker: str,
    ) -> FactCrossValidationResult:
        """
        Validate facts_json fields against vendor data.

        Fast path bypass: route_type="fast" records are built from structured
        vendor data, not from LLM extraction — DB cross-checks are skipped.
        Internal consistency checks always run (zero DB cost).
        """
        from app.models.news import CatalystType

        facts      = getattr(record, "facts_json",  None)
        catalyst   = getattr(record, "catalyst_type", None)
        route_type = getattr(record, "route_type",  None)

        # ── Always: internal consistency (self-contradicting LLM output) ────
        ic_results  = _check_internal_consistency(facts)
        ic_mismatches = [r for r in ic_results
                         if r.status == FactValidationStatus.MISMATCH]

        # Fast path: facts ARE the vendor data — skip DB cross-checks
        if route_type == "fast":
            if ic_mismatches:
                result = _build_result(ic_results, vendor_source="internal")
                asyncio.ensure_future(
                    self._write_audit(record, ticker, result)
                )
                return result
            return _skipped_result()

        if not facts:
            return _skipped_result()

        # ── Route to catalyst-specific DB validator ─────────────────────────
        try:
            if catalyst == CatalystType.EARNINGS:
                db_results  = await self._validate_earnings(facts, ticker)
                vendor_source = "fmp_earnings"
            elif catalyst == CatalystType.ANALYST:
                db_results  = await self._validate_analyst(facts, ticker)
                vendor_source = "fmp_analyst"
            else:
                # M&A, REGULATORY, MACRO, LEGAL, OTHER — no reliable vendor feed
                db_results  = []
                vendor_source = None
        except Exception as exc:
            _log().warning(
                "fact_cross_validator.db_error",
                ticker=ticker, error=str(exc),
            )
            # Degrade gracefully: return IC findings if any, else UNVERIFIABLE
            if ic_mismatches:
                return _build_result(ic_results, vendor_source="internal")
            return _unverifiable_result()

        all_results = ic_results + db_results
        result = _build_result(all_results, vendor_source=vendor_source)

        # Persist audit row for mismatches (and optionally all statuses)
        if result.mismatch_fields or _AUDIT_ALL:
            asyncio.ensure_future(
                self._write_audit(record, ticker, result)
            )

        return result

    # ── Earnings cross-check ────────────────────────────────────────────────

    async def _validate_earnings(
        self,
        facts,
        ticker: str,
    ) -> list[FieldValidationResult]:
        """
        Cross-check earnings facts_json against the event table.

        Queries the most recent earnings row with reported actuals within
        FACT_EARNINGS_LOOKBACK_DAYS days.  Returns [] when no actuals exist
        (event is pending) or pool is unavailable.
        """
        if not self._pool:
            return []

        try:
            row = await self._pool.fetchrow(
                """
                SELECT actual_eps, consensus_eps, whisper_eps,
                       actual_revenue, consensus_revenue
                FROM   event
                WHERE  event_type = 'earnings'
                  AND  ticker     = $1
                  AND  event_date >= NOW() - ($2 || ' days')::INTERVAL
                  AND  event_date <= NOW() + INTERVAL '1 day'
                  AND  actual_eps IS NOT NULL
                ORDER  BY event_date DESC
                LIMIT  1
                """,
                ticker,
                str(_EARNINGS_LOOKBACK_DAYS),
            )
        except Exception as exc:
            _log().warning(
                "fact_cross_validator.earnings_query_error",
                ticker=ticker, error=str(exc),
            )
            return []

        if row is None:
            # Actuals not yet reported; earnings event pending
            return []

        vendor_eps_actual  = row["actual_eps"]
        vendor_eps_est     = row["consensus_eps"]
        vendor_rev_actual  = row["actual_revenue"]
        vendor_rev_est     = row["consensus_revenue"]

        results: list[FieldValidationResult] = []

        # ── eps_actual ────────────────────────────────────────────────────
        if facts.eps_actual is not None and vendor_eps_actual is not None:
            ok, dev = _numeric_close(
                facts.eps_actual, vendor_eps_actual, _EPS_TOLERANCE_PCT
            )
            results.append(FieldValidationResult(
                field="eps_actual",
                llm_value=facts.eps_actual,
                vendor_value=float(vendor_eps_actual),
                status=FactValidationStatus.CONFIRMED if ok else FactValidationStatus.MISMATCH,
                tolerance_pct=_EPS_TOLERANCE_PCT,
                deviation_pct=dev,
                is_key_field=False,
            ))

        # ── eps_estimate ──────────────────────────────────────────────────
        if facts.eps_estimate is not None and vendor_eps_est is not None:
            ok, dev = _numeric_close(
                facts.eps_estimate, vendor_eps_est, _EPS_TOLERANCE_PCT
            )
            results.append(FieldValidationResult(
                field="eps_estimate",
                llm_value=facts.eps_estimate,
                vendor_value=float(vendor_eps_est),
                status=FactValidationStatus.CONFIRMED if ok else FactValidationStatus.MISMATCH,
                tolerance_pct=_EPS_TOLERANCE_PCT,
                deviation_pct=dev,
                is_key_field=False,
            ))

        # ── eps_beat — recompute from vendor actuals + estimate ───────────
        # This is the MOST important check: if the LLM got eps_beat wrong it
        # likely got the trade direction wrong.
        if (facts.eps_beat is not None
                and vendor_eps_actual is not None
                and vendor_eps_est is not None):
            vendor_beat = float(vendor_eps_actual) > float(vendor_eps_est)
            ok = (facts.eps_beat is vendor_beat)
            results.append(FieldValidationResult(
                field="eps_beat",
                llm_value=facts.eps_beat,
                vendor_value=vendor_beat,
                status=FactValidationStatus.CONFIRMED if ok else FactValidationStatus.MISMATCH,
                is_key_field=True,
            ))

        # ── revenue_beat ──────────────────────────────────────────────────
        if (facts.revenue_beat is not None
                and vendor_rev_actual is not None
                and vendor_rev_est is not None):
            vendor_rev_beat = float(vendor_rev_actual) > float(vendor_rev_est)
            ok = (facts.revenue_beat is vendor_rev_beat)
            results.append(FieldValidationResult(
                field="revenue_beat",
                llm_value=facts.revenue_beat,
                vendor_value=vendor_rev_beat,
                status=FactValidationStatus.CONFIRMED if ok else FactValidationStatus.MISMATCH,
                is_key_field=False,
            ))

        return results

    # ── Analyst cross-check ─────────────────────────────────────────────────

    async def _validate_analyst(
        self,
        facts,
        ticker: str,
    ) -> list[FieldValidationResult]:
        """
        Cross-check analyst facts against recent fmp_analyst_grades entries.

        Uses directional alignment for rating_new (exact strings differ by firm
        vocabulary), numeric tolerance for price_target_new, and token-overlap
        for analyst_firm (firm names are abbreviated inconsistently).
        """
        if not self._pool:
            return []

        try:
            row = await self._pool.fetchrow(
                """
                SELECT to_grade, price_target, analyst_firm
                FROM   fmp_analyst_grades
                WHERE  ticker     = $1
                  AND  grade_date >= NOW() - ($2 || ' hours')::INTERVAL
                ORDER  BY grade_date DESC
                LIMIT  1
                """,
                ticker,
                str(_ANALYST_LOOKBACK_HOURS),
            )
        except Exception as exc:
            # Table may not exist in all environments — log and skip
            _log().warning(
                "fact_cross_validator.analyst_query_error",
                ticker=ticker, error=str(exc),
            )
            return []

        if row is None:
            return []

        vendor_grade = (row["to_grade"]    or "").strip()
        vendor_pt    = row["price_target"]
        vendor_firm  = (row["analyst_firm"] or "").strip()

        results: list[FieldValidationResult] = []

        # ── rating_new — directional alignment ───────────────────────────
        # Exact string matching is unreliable (e.g. "Outperform" vs "OP");
        # align on bullish/bearish direction instead.  Only flag MISMATCH when
        # both sides have a clear non-neutral direction and they disagree.
        if facts.rating_new and vendor_grade:
            llm_dir    = _rating_direction(facts.rating_new)
            vendor_dir = _rating_direction(vendor_grade)
            if llm_dir != "neutral" and vendor_dir != "neutral":
                ok = (llm_dir == vendor_dir)
                results.append(FieldValidationResult(
                    field="rating_new",
                    llm_value=facts.rating_new,
                    vendor_value=vendor_grade,
                    status=(
                        FactValidationStatus.CONFIRMED
                        if ok else
                        FactValidationStatus.MISMATCH
                    ),
                    is_key_field=False,   # Analyst direction matters but less than EPS
                ))

        # ── price_target_new — numeric tolerance ─────────────────────────
        if facts.price_target_new is not None and vendor_pt is not None:
            ok, dev = _numeric_close(
                facts.price_target_new, float(vendor_pt),
                _PRICE_TARGET_TOLERANCE_PCT,
            )
            results.append(FieldValidationResult(
                field="price_target_new",
                llm_value=facts.price_target_new,
                vendor_value=float(vendor_pt),
                status=FactValidationStatus.CONFIRMED if ok else FactValidationStatus.MISMATCH,
                tolerance_pct=_PRICE_TARGET_TOLERANCE_PCT,
                deviation_pct=dev,
                is_key_field=False,
            ))

        # ── analyst_firm — soft token-overlap match ───────────────────────
        # Firm name mismatches are never MISMATCH (too noisy); PARTIAL at worst.
        if facts.analyst_firm and vendor_firm:
            llm_lower    = facts.analyst_firm.lower()
            vendor_lower = vendor_firm.lower()
            firm_ok = (
                llm_lower in vendor_lower
                or vendor_lower in llm_lower
                or _token_overlap(llm_lower, vendor_lower) >= 0.5
            )
            results.append(FieldValidationResult(
                field="analyst_firm",
                llm_value=facts.analyst_firm,
                vendor_value=vendor_firm,
                status=(
                    FactValidationStatus.CONFIRMED
                    if firm_ok else
                    FactValidationStatus.PARTIAL   # Soft mismatch — not a hard error
                ),
                is_key_field=False,
            ))

        return results

    # ── Audit write ──────────────────────────────────────────────────────────

    async def _write_audit(
        self,
        record,
        ticker: str,
        result: FactCrossValidationResult,
    ) -> None:
        """
        Persist a validation row to fact_validation_audit.

        Called via asyncio.ensure_future — never raises; a write failure is
        logged but never blocks the signal pipeline.
        """
        if not self._pool:
            return
        try:
            facts = getattr(record, "facts_json", None)
            field_detail = [
                {
                    "field":        r.field,
                    "llm":          r.llm_value,
                    "vendor":       r.vendor_value,
                    "status":       r.status.value,
                    "deviation_pct": r.deviation_pct,
                    "is_key_field": r.is_key_field,
                }
                for r in result.field_results
            ]
            cat = getattr(record, "catalyst_type", None)
            cat_str = (
                cat.value if hasattr(cat, "value") else str(cat)
            ) if cat else None

            audit_id = uuid4()
            await self._pool.execute(
                """
                INSERT INTO fact_validation_audit (
                    id, record_id, ticker, catalyst_type,
                    validation_status, validated_fields, mismatch_fields,
                    validation_confidence, conviction_multiplier,
                    llm_facts_json, field_detail_json,
                    vendor_source, has_key_mismatch
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
                )
                ON CONFLICT DO NOTHING
                """,
                audit_id,
                getattr(record, "id", None),
                ticker,
                cat_str,
                result.validation_status.value,
                result.validated_fields,
                result.mismatch_fields,
                result.validation_confidence,
                result.conviction_multiplier,
                _json.dumps(facts.model_dump() if facts else None),
                _json.dumps(field_detail),
                result.vendor_source,
                result.has_key_mismatch,
            )
            result.audit_id = audit_id

        except Exception as exc:
            _log().warning(
                "fact_cross_validator.audit_write_error",
                ticker=ticker, error=str(exc),
            )


# ── Internal helpers ────────────────────────────────────────────────────────────

def _check_internal_consistency(facts) -> list[FieldValidationResult]:
    """
    Self-consistency checks that require no DB round-trip.

    Catches cases where the LLM contradicts itself:
      • eps_beat direction inconsistent with eps_actual vs eps_estimate
      • guidance_raised AND guidance_lowered both True (logically impossible)
    """
    if facts is None:
        return []

    results: list[FieldValidationResult] = []

    # eps_beat must agree with eps_actual vs eps_estimate when both present
    if (facts.eps_beat is not None
            and facts.eps_actual is not None
            and facts.eps_estimate is not None):
        implied_beat = facts.eps_actual > facts.eps_estimate
        if facts.eps_beat != implied_beat:
            results.append(FieldValidationResult(
                field="eps_beat",
                llm_value=facts.eps_beat,
                vendor_value=(
                    f"implied={implied_beat} "
                    f"(eps_actual={facts.eps_actual} vs "
                    f"eps_estimate={facts.eps_estimate})"
                ),
                status=FactValidationStatus.MISMATCH,
                is_key_field=True,
            ))

    # guidance_raised and guidance_lowered cannot both be True
    if facts.guidance_raised is True and facts.guidance_lowered is True:
        results.append(FieldValidationResult(
            field="guidance_raised",
            llm_value=True,
            vendor_value="impossible — guidance_lowered=True simultaneously",
            status=FactValidationStatus.MISMATCH,
            is_key_field=True,
        ))
        results.append(FieldValidationResult(
            field="guidance_lowered",
            llm_value=True,
            vendor_value="impossible — guidance_raised=True simultaneously",
            status=FactValidationStatus.MISMATCH,
            is_key_field=True,
        ))

    return results


def _numeric_close(
    llm_val: float,
    vendor_val: float,
    tolerance_pct: float,
) -> tuple[bool, float | None]:
    """
    Check whether two numeric values are within tolerance_pct of each other.

    Near-zero denominator handling (EPS can legitimately be $0.01):
      When |vendor| < 0.10 we use an absolute tolerance of $0.05
      rather than a relative one (avoid division-by-near-zero amplification).

    Returns (matches: bool, deviation_pct: float | None).
    """
    try:
        llm    = float(llm_val)
        vendor = float(vendor_val)
    except (TypeError, ValueError):
        return False, None

    if math.isnan(llm) or math.isnan(vendor):
        return False, None

    abs_vendor = abs(vendor)
    if abs_vendor < 0.10:
        # Near-zero: absolute tolerance of $0.05
        diff = abs(llm - vendor)
        ok   = diff <= 0.05
        dev  = round(diff / max(abs_vendor, 0.01) * 100, 1) if not ok else 0.0
        return ok, dev

    dev = abs(llm - vendor) / abs_vendor * 100
    return dev <= tolerance_pct, round(dev, 1)


def _rating_direction(rating: str) -> str:
    """Map an analyst rating string to 'long', 'short', or 'neutral'."""
    r = rating.lower().strip()
    _BULLISH = frozenset({
        "buy", "strong buy", "outperform", "overweight",
        "upgrade", "accumulate", "add", "positive",
    })
    _BEARISH = frozenset({
        "sell", "strong sell", "underperform", "underweight",
        "downgrade", "reduce", "negative",
    })
    if any(b in r for b in _BULLISH):
        return "long"
    if any(b in r for b in _BEARISH):
        return "short"
    return "neutral"


def _token_overlap(a: str, b: str) -> float:
    """Jaccard token overlap between two strings."""
    ta = set(a.split())
    tb = set(b.split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _build_result(
    field_results: list[FieldValidationResult],
    vendor_source: str | None,
) -> FactCrossValidationResult:
    """
    Aggregate individual field results into an overall status and
    pre-compute the conviction multiplier.
    """
    if not field_results:
        return _unverifiable_result()

    confirmed    = [r for r in field_results if r.status == FactValidationStatus.CONFIRMED]
    mismatches   = [r for r in field_results if r.status == FactValidationStatus.MISMATCH]
    unverifiable = [r for r in field_results if r.status == FactValidationStatus.UNVERIFIABLE]
    partial_list = [r for r in field_results if r.status == FactValidationStatus.PARTIAL]

    key_mismatches    = [r for r in mismatches if r.is_key_field]
    nonkey_mismatches = [r for r in mismatches if not r.is_key_field]

    # Confidence = confirmed / (confirmed + mismatches)  — only verifiable fields count
    verifiable = confirmed + mismatches
    confidence = len(confirmed) / len(verifiable) if verifiable else 0.0

    validated_fields    = [r.field for r in confirmed]
    mismatch_field_names = [r.field for r in mismatches]
    unverifiable_fields = [r.field for r in unverifiable + partial_list
                          if r.status == FactValidationStatus.UNVERIFIABLE]

    # ── Overall status ────────────────────────────────────────────────────
    if mismatches:
        status = FactValidationStatus.MISMATCH
    elif not verifiable:
        # No fields could be compared against vendor data
        status = FactValidationStatus.UNVERIFIABLE
    elif unverifiable or partial_list:
        # Some fields confirmed, others couldn't be verified
        status = FactValidationStatus.PARTIAL
    else:
        status = FactValidationStatus.CONFIRMED

    # ── Conviction multiplier ─────────────────────────────────────────────
    if status == FactValidationStatus.CONFIRMED:
        mult = CONVICTION_CONFIRMED
    elif status == FactValidationStatus.PARTIAL:
        mult = CONVICTION_PARTIAL
    elif status == FactValidationStatus.MISMATCH:
        # Key mismatches (direction-determining) → heavy penalty
        mult = CONVICTION_MISMATCH_KEY if key_mismatches else CONVICTION_MISMATCH_NONKEY
    elif status == FactValidationStatus.UNVERIFIABLE:
        mult = CONVICTION_UNVERIFIABLE
    else:
        mult = 1.0

    return FactCrossValidationResult(
        validation_status=status,
        validated_fields=validated_fields,
        mismatch_fields=mismatch_field_names,
        unverifiable_fields=unverifiable_fields,
        validation_confidence=round(confidence, 3),
        conviction_multiplier=mult,
        field_results=field_results,
        vendor_source=vendor_source,
        has_key_mismatch=bool(key_mismatches),
    )


def _log():
    """Lazy import to avoid circular import at module load time."""
    from app.logging import get_logger
    return get_logger("fact_cross_validator")
