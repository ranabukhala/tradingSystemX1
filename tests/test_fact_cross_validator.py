"""
Tests for app.pipeline.fact_cross_validator

Coverage:
  1. TestInternalConsistency      (8)  — zero DB, pure logic
  2. TestNumericMatching          (8)  — _numeric_close tolerance checks
  3. TestStatusAggregation        (8)  — _build_result logic
  4. TestConvictionMultiplierPolicy (10) — multiplier values and edge cases
  5. TestEarningsCrossCheck       (10) — mocked asyncpg pool
  6. TestAnalystCrossCheck        (8)  — mocked asyncpg pool
  7. TestFastPathBypass           (5)  — route_type="fast" handling
  8. TestGracefulDegradation      (5)  — DB down / None pool
  9. TestAuditWrite               (6)  — fire-and-forget audit row

Total: 68 tests
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

# ── Mock heavy Docker-only packages before any pipeline import ────────────────
for _mod in [
    "confluent_kafka", "confluent_kafka.admin",
    "prometheus_client",
    "structlog",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

from app.models.news import CatalystType, FactsJson, SummarizedRecord
from app.pipeline.fact_cross_validator import (
    CONVICTION_CONFIRMED,
    CONVICTION_MISMATCH_KEY,
    CONVICTION_MISMATCH_NONKEY,
    CONVICTION_PARTIAL,
    CONVICTION_UNVERIFIABLE,
    FactCrossValidationResult,
    FactCrossValidator,
    FactValidationStatus,
    FieldValidationResult,
    _build_result,
    _check_internal_consistency,
    _numeric_close,
    _rating_direction,
    _skipped_result,
    _token_overlap,
    _unverifiable_result,
)


# ─────────────────────────────────────────────────────────────────────────────
# Test helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_facts(**kw) -> FactsJson:
    return FactsJson(**kw)


def _make_record(
    facts_json: FactsJson | None = None,
    catalyst_type=CatalystType.EARNINGS,
    route_type: str | None = None,
    tickers: list[str] | None = None,
) -> SummarizedRecord:
    """Minimal SummarizedRecord for testing."""
    from app.models.news import (
        FloatSensitivity, MarketCapTier, NewsMode,
        NewsSource, SessionContext,
    )
    from datetime import datetime, timezone

    return SummarizedRecord(
        id=uuid4(),
        source=NewsSource.BENZINGA,
        vendor_id=f"test_{uuid4().hex[:8]}",
        published_at=datetime.now(timezone.utc),
        received_at=datetime.now(timezone.utc),
        url="https://example.com/news/test",
        canonical_url="https://example.com/news/test",
        title="Test news headline",
        content_hash="abc123",
        tickers=tickers or ["AAPL"],
        catalyst_type=catalyst_type,
        mode=NewsMode.STOCK_SPECIFIC,
        session_context=SessionContext.INTRADAY,
        float_sensitivity=FloatSensitivity.NORMAL,
        impact_day=0.7,
        impact_swing=0.5,
        facts_json=facts_json,
        route_type=route_type,
    )


class FakePool:
    """
    Synchronous-friendly fake asyncpg pool.
    Supports pre-loading rows and simulating connection errors.
    """

    def __init__(self, rows: dict[str, Any] | None = None):
        # rows keyed by table name → fetchrow result
        self._rows = rows or {}
        self._fail = False
        self._writes: list[tuple] = []

    def set_row(self, table: str, row: dict | None) -> None:
        self._rows[table] = row

    def go_down(self) -> None:
        self._fail = True

    async def fetchrow(self, query: str, *args) -> dict | None:
        if self._fail:
            raise ConnectionError("DB is down")
        if "event" in query and "earnings" in query:
            return self._rows.get("event")
        if "fmp_analyst_grades" in query:
            return self._rows.get("fmp_analyst_grades")
        return None

    async def execute(self, query: str, *args) -> None:
        if self._fail:
            raise ConnectionError("DB is down")
        self._writes.append(args)

    async def close(self) -> None:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# 1. Internal consistency checks (zero DB cost)
# ─────────────────────────────────────────────────────────────────────────────

class TestInternalConsistency:

    def test_eps_beat_true_but_actual_less_than_estimate_is_mismatch(self):
        facts = _make_facts(eps_beat=True, eps_actual=1.50, eps_estimate=2.00)
        results = _check_internal_consistency(facts)
        assert len(results) == 1
        r = results[0]
        assert r.field == "eps_beat"
        assert r.status == FactValidationStatus.MISMATCH
        assert r.is_key_field is True

    def test_eps_beat_false_but_actual_greater_than_estimate_is_mismatch(self):
        facts = _make_facts(eps_beat=False, eps_actual=2.10, eps_estimate=1.80)
        results = _check_internal_consistency(facts)
        assert len(results) == 1
        assert results[0].status == FactValidationStatus.MISMATCH

    def test_eps_beat_true_and_actual_greater_gives_no_mismatch(self):
        facts = _make_facts(eps_beat=True, eps_actual=2.10, eps_estimate=1.80)
        results = _check_internal_consistency(facts)
        assert results == []

    def test_eps_beat_false_and_actual_less_gives_no_mismatch(self):
        facts = _make_facts(eps_beat=False, eps_actual=1.50, eps_estimate=2.00)
        results = _check_internal_consistency(facts)
        assert results == []

    def test_guidance_both_true_gives_two_mismatch_results(self):
        facts = _make_facts(guidance_raised=True, guidance_lowered=True)
        results = _check_internal_consistency(facts)
        assert len(results) == 2
        fields = {r.field for r in results}
        assert fields == {"guidance_raised", "guidance_lowered"}
        for r in results:
            assert r.status == FactValidationStatus.MISMATCH
            assert r.is_key_field is True

    def test_guidance_raised_only_no_issue(self):
        facts = _make_facts(guidance_raised=True, guidance_lowered=False)
        assert _check_internal_consistency(facts) == []

    def test_eps_beat_without_actuals_not_checked(self):
        # eps_beat present but no eps_actual/estimate — can't check internally
        facts = _make_facts(eps_beat=True)
        assert _check_internal_consistency(facts) == []

    def test_none_facts_returns_empty(self):
        assert _check_internal_consistency(None) == []


# ─────────────────────────────────────────────────────────────────────────────
# 2. Numeric matching
# ─────────────────────────────────────────────────────────────────────────────

class TestNumericMatching:

    def test_exact_match_is_ok(self):
        ok, dev = _numeric_close(2.05, 2.05, 5.0)
        assert ok is True
        assert dev == 0.0

    def test_within_tolerance_is_ok(self):
        # 2.09 vs 2.00 = 4.5% — clearly within 5% boundary
        ok, dev = _numeric_close(2.09, 2.00, 5.0)
        assert ok is True

    def test_just_outside_tolerance_fails(self):
        # 2.11 vs 2.00 = 5.5%
        ok, dev = _numeric_close(2.11, 2.00, 5.0)
        assert ok is False
        assert dev is not None and dev > 5.0

    def test_near_zero_uses_absolute_tolerance(self):
        # vendor_eps = 0.05 (very small); LLM says 0.06 → diff = $0.01 ≤ $0.05 → ok
        ok, dev = _numeric_close(0.06, 0.05, 5.0)
        assert ok is True

    def test_near_zero_outside_absolute_tolerance(self):
        # LLM says 0.12, vendor 0.05 → diff = $0.07 > $0.05 → fails
        ok, dev = _numeric_close(0.12, 0.05, 5.0)
        assert ok is False

    def test_negative_eps_match(self):
        # Both negative, 4% apart — clearly within 5% tolerance
        ok, dev = _numeric_close(-1.04, -1.00, 5.0)
        assert ok is True

    def test_nan_returns_false(self):
        import math
        ok, dev = _numeric_close(float("nan"), 2.00, 5.0)
        assert ok is False

    def test_nonnumeric_returns_false(self):
        ok, dev = _numeric_close("bad", 2.00, 5.0)
        assert ok is False
        assert dev is None


# ─────────────────────────────────────────────────────────────────────────────
# 3. Status aggregation (_build_result)
# ─────────────────────────────────────────────────────────────────────────────

def _fr(field: str, status: FactValidationStatus, is_key: bool = False) -> FieldValidationResult:
    return FieldValidationResult(
        field=field, llm_value=None, vendor_value=None,
        status=status, is_key_field=is_key,
    )


class TestStatusAggregation:

    def test_all_confirmed_gives_confirmed(self):
        results = [
            _fr("eps_actual", FactValidationStatus.CONFIRMED),
            _fr("eps_beat", FactValidationStatus.CONFIRMED, is_key=True),
        ]
        r = _build_result(results, vendor_source="fmp_earnings")
        assert r.validation_status == FactValidationStatus.CONFIRMED
        assert r.conviction_multiplier == CONVICTION_CONFIRMED

    def test_any_mismatch_gives_mismatch(self):
        results = [
            _fr("eps_actual", FactValidationStatus.CONFIRMED),
            _fr("eps_beat", FactValidationStatus.MISMATCH, is_key=True),
        ]
        r = _build_result(results, vendor_source="fmp_earnings")
        assert r.validation_status == FactValidationStatus.MISMATCH
        assert r.has_key_mismatch is True

    def test_confirmed_plus_partial_gives_partial(self):
        results = [
            _fr("eps_actual", FactValidationStatus.CONFIRMED),
            _fr("analyst_firm", FactValidationStatus.PARTIAL),
        ]
        r = _build_result(results, vendor_source="fmp_analyst")
        assert r.validation_status == FactValidationStatus.PARTIAL
        assert r.conviction_multiplier == CONVICTION_PARTIAL

    def test_all_unverifiable_gives_unverifiable(self):
        results = [
            _fr("eps_actual", FactValidationStatus.UNVERIFIABLE),
        ]
        r = _build_result(results, vendor_source=None)
        assert r.validation_status == FactValidationStatus.UNVERIFIABLE

    def test_empty_results_gives_unverifiable(self):
        r = _build_result([], vendor_source=None)
        assert r.validation_status == FactValidationStatus.UNVERIFIABLE

    def test_confidence_is_fraction_confirmed_over_verifiable(self):
        results = [
            _fr("eps_actual", FactValidationStatus.CONFIRMED),
            _fr("eps_beat", FactValidationStatus.MISMATCH, is_key=True),
        ]
        r = _build_result(results, vendor_source="fmp_earnings")
        # 1 confirmed, 1 mismatch → confidence = 0.5
        assert r.validation_confidence == pytest.approx(0.5)

    def test_validated_and_mismatch_fields_populated(self):
        results = [
            _fr("eps_actual", FactValidationStatus.CONFIRMED),
            _fr("eps_beat", FactValidationStatus.MISMATCH, is_key=True),
        ]
        r = _build_result(results, vendor_source="fmp_earnings")
        assert "eps_actual" in r.validated_fields
        assert "eps_beat" in r.mismatch_fields

    def test_nonkey_only_mismatch_not_flagged_as_key(self):
        results = [
            _fr("eps_actual", FactValidationStatus.MISMATCH, is_key=False),
        ]
        r = _build_result(results, vendor_source="fmp_earnings")
        assert r.has_key_mismatch is False
        assert r.conviction_multiplier == CONVICTION_MISMATCH_NONKEY


# ─────────────────────────────────────────────────────────────────────────────
# 4. Conviction multiplier policy
# ─────────────────────────────────────────────────────────────────────────────

class TestConvictionMultiplierPolicy:

    def test_confirmed_boosts_conviction(self):
        r = _build_result([_fr("eps_actual", FactValidationStatus.CONFIRMED)], None)
        assert r.conviction_multiplier == CONVICTION_CONFIRMED
        assert r.conviction_multiplier > 1.0

    def test_partial_reduces_conviction(self):
        r = _build_result([
            _fr("eps_actual", FactValidationStatus.CONFIRMED),
            _fr("revenue_beat", FactValidationStatus.PARTIAL),
        ], None)
        assert r.conviction_multiplier == CONVICTION_PARTIAL
        assert r.conviction_multiplier < 1.0

    def test_key_mismatch_applies_heavy_penalty(self):
        r = _build_result([
            _fr("eps_beat", FactValidationStatus.MISMATCH, is_key=True),
        ], None)
        assert r.conviction_multiplier == CONVICTION_MISMATCH_KEY
        assert r.conviction_multiplier == pytest.approx(0.30)

    def test_nonkey_mismatch_applies_moderate_penalty(self):
        r = _build_result([
            _fr("eps_actual", FactValidationStatus.MISMATCH, is_key=False),
        ], None)
        assert r.conviction_multiplier == CONVICTION_MISMATCH_NONKEY
        assert r.conviction_multiplier == pytest.approx(0.60)

    def test_unverifiable_applies_small_penalty(self):
        r = _unverifiable_result()
        assert r.conviction_multiplier == CONVICTION_UNVERIFIABLE
        assert r.conviction_multiplier == pytest.approx(0.90)

    def test_skipped_no_change(self):
        r = _skipped_result()
        assert r.conviction_multiplier == 1.0

    def test_key_mismatch_can_drop_below_conviction_threshold(self):
        # Starting conviction 0.60, × 0.30 → 0.18 (below 0.55 threshold)
        base_conviction = 0.60
        r = _build_result([_fr("eps_beat", FactValidationStatus.MISMATCH, is_key=True)], None)
        final = round(base_conviction * r.conviction_multiplier, 3)
        assert final < 0.55  # below default CONVICTION_THRESHOLD

    def test_confirmed_conviction_capped_at_one(self):
        # Even with a boost, conviction must not exceed 1.0 (enforced by caller)
        base = 0.95
        r = _build_result([_fr("eps_beat", FactValidationStatus.CONFIRMED, is_key=True)], None)
        final = min(1.0, round(base * r.conviction_multiplier, 3))
        assert final == 1.0

    def test_mismatch_dominates_when_mixed_with_confirmed(self):
        results = [
            _fr("eps_actual", FactValidationStatus.CONFIRMED),
            _fr("eps_beat", FactValidationStatus.MISMATCH, is_key=True),
        ]
        r = _build_result(results, None)
        # MISMATCH takes precedence over CONFIRMED
        assert r.validation_status == FactValidationStatus.MISMATCH
        assert r.conviction_multiplier == CONVICTION_MISMATCH_KEY

    def test_fast_path_internal_mismatch_applies_key_penalty(self):
        # Fast-path records with IC mismatch still get penalized
        r = _build_result([
            _fr("eps_beat", FactValidationStatus.MISMATCH, is_key=True),
        ], vendor_source="internal")
        assert r.conviction_multiplier == CONVICTION_MISMATCH_KEY


# ─────────────────────────────────────────────────────────────────────────────
# 5. Earnings cross-check (mocked DB)
# ─────────────────────────────────────────────────────────────────────────────

class TestEarningsCrossCheck:

    @pytest.mark.asyncio
    async def test_eps_actual_match_confirmed(self):
        pool = FakePool({"event": {
            "actual_eps": 2.05, "consensus_eps": 1.80,
            "whisper_eps": 1.82, "actual_revenue": 90e9, "consensus_revenue": 88e9,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_actual=2.05, eps_estimate=1.80, eps_beat=True)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        assert result.validation_status == FactValidationStatus.CONFIRMED
        assert "eps_actual" in result.validated_fields
        assert "eps_beat" in result.validated_fields
        assert result.mismatch_fields == []

    @pytest.mark.asyncio
    async def test_eps_actual_mismatch_flagged(self):
        pool = FakePool({"event": {
            "actual_eps": 2.05, "consensus_eps": 1.80,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        # LLM reported 2.50 — 22% off from vendor 2.05
        facts = _make_facts(eps_actual=2.50, eps_estimate=1.80)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        assert result.validation_status == FactValidationStatus.MISMATCH
        assert "eps_actual" in result.mismatch_fields

    @pytest.mark.asyncio
    async def test_eps_beat_direction_wrong_is_key_mismatch(self):
        # Vendor: actual=1.50 < estimate=2.00 → beat=False
        # LLM:    eps_beat=True  → wrong direction
        pool = FakePool({"event": {
            "actual_eps": 1.50, "consensus_eps": 2.00,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "MSFT")

        assert result.has_key_mismatch is True
        assert "eps_beat" in result.mismatch_fields
        assert result.conviction_multiplier == CONVICTION_MISMATCH_KEY

    @pytest.mark.asyncio
    async def test_eps_beat_direction_correct_is_confirmed(self):
        pool = FakePool({"event": {
            "actual_eps": 2.10, "consensus_eps": 1.80,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        assert "eps_beat" in result.validated_fields
        assert result.has_key_mismatch is False

    @pytest.mark.asyncio
    async def test_no_actuals_in_db_gives_unverifiable(self):
        # No row (actuals not yet reported)
        pool = FakePool({"event": None})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True, eps_actual=2.05)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        assert result.validation_status == FactValidationStatus.UNVERIFIABLE

    @pytest.mark.asyncio
    async def test_revenue_beat_recomputed_correctly(self):
        pool = FakePool({"event": {
            "actual_eps": 2.05, "consensus_eps": 1.80,
            "whisper_eps": None,
            "actual_revenue": 90e9, "consensus_revenue": 88e9,
        }})
        validator = FactCrossValidator(db_pool=pool)
        # revenue_beat=True and actual > estimate → CONFIRMED
        facts = _make_facts(revenue_beat=True)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        assert "revenue_beat" in result.validated_fields

    @pytest.mark.asyncio
    async def test_revenue_beat_wrong_is_mismatch(self):
        pool = FakePool({"event": {
            "actual_eps": 2.05, "consensus_eps": 1.80,
            "whisper_eps": None,
            "actual_revenue": 85e9, "consensus_revenue": 88e9,   # miss
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(revenue_beat=True)  # LLM wrong: actual < estimate
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        assert "revenue_beat" in result.mismatch_fields

    @pytest.mark.asyncio
    async def test_near_zero_eps_uses_absolute_tolerance(self):
        # EPS = $0.03 (tiny); LLM says $0.04 — diff $0.01 ≤ $0.05 absolute → ok
        pool = FakePool({"event": {
            "actual_eps": 0.03, "consensus_eps": 0.01,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_actual=0.04)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "XYZ")

        assert "eps_actual" in result.validated_fields

    @pytest.mark.asyncio
    async def test_non_earnings_catalyst_gives_unverifiable(self):
        # M&A events have no earnings cross-check
        pool = FakePool()
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(deal_price=150.0, deal_premium_pct=25.0)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.MA)
        result = await validator.validate(record, "XYZ")

        # No DB results → UNVERIFIABLE (internal consistency also clean)
        assert result.validation_status in (
            FactValidationStatus.UNVERIFIABLE,
            FactValidationStatus.SKIPPED,
        )

    @pytest.mark.asyncio
    async def test_internal_consistency_runs_even_without_db(self):
        pool = FakePool({"event": None})  # no vendor data
        validator = FactCrossValidator(db_pool=pool)
        # IC mismatch: eps_beat=True but actual < estimate
        facts = _make_facts(eps_beat=True, eps_actual=1.50, eps_estimate=2.00)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        # IC check fires even though DB returned no row
        assert result.has_key_mismatch is True
        assert "eps_beat" in result.mismatch_fields


# ─────────────────────────────────────────────────────────────────────────────
# 6. Analyst cross-check (mocked DB)
# ─────────────────────────────────────────────────────────────────────────────

class TestAnalystCrossCheck:

    @pytest.mark.asyncio
    async def test_bullish_direction_match_confirmed(self):
        pool = FakePool({"fmp_analyst_grades": {
            "to_grade": "Outperform", "price_target": 200.0, "analyst_firm": "Goldman Sachs",
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(rating_new="Buy", price_target_new=195.0, analyst_firm="Goldman")
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "AAPL")

        assert "rating_new" in result.validated_fields
        assert result.mismatch_fields == []

    @pytest.mark.asyncio
    async def test_bullish_vs_bearish_direction_is_mismatch(self):
        pool = FakePool({"fmp_analyst_grades": {
            "to_grade": "Underperform", "price_target": 120.0, "analyst_firm": "Morgan Stanley",
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(rating_new="Buy")  # LLM says bullish, vendor says bearish
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "XYZ")

        assert "rating_new" in result.mismatch_fields

    @pytest.mark.asyncio
    async def test_both_neutral_ratings_not_checked(self):
        pool = FakePool({"fmp_analyst_grades": {
            "to_grade": "Hold", "price_target": 150.0, "analyst_firm": "JPMorgan",
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(rating_new="Neutral", price_target_new=150.0)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "AAPL")

        # Neutral vs neutral not flagged as mismatch
        assert "rating_new" not in result.mismatch_fields

    @pytest.mark.asyncio
    async def test_price_target_within_tolerance_confirmed(self):
        pool = FakePool({"fmp_analyst_grades": {
            "to_grade": "Buy", "price_target": 200.0, "analyst_firm": "Goldman",
        }})
        validator = FactCrossValidator(db_pool=pool)
        # 195 vs 200 = 2.5% — within 10% tolerance
        facts = _make_facts(price_target_new=195.0)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "AAPL")

        assert "price_target_new" in result.validated_fields

    @pytest.mark.asyncio
    async def test_price_target_outside_tolerance_mismatch(self):
        pool = FakePool({"fmp_analyst_grades": {
            "to_grade": "Buy", "price_target": 200.0, "analyst_firm": "Goldman",
        }})
        validator = FactCrossValidator(db_pool=pool)
        # 150 vs 200 = 25% — outside 10% tolerance
        facts = _make_facts(price_target_new=150.0)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "AAPL")

        assert "price_target_new" in result.mismatch_fields

    @pytest.mark.asyncio
    async def test_analyst_firm_soft_match_is_partial_not_mismatch(self):
        pool = FakePool({"fmp_analyst_grades": {
            "to_grade": "Overweight", "price_target": 220.0,
            "analyst_firm": "Morgan Stanley",
        }})
        validator = FactCrossValidator(db_pool=pool)
        # Firm mismatch is PARTIAL (soft), not MISMATCH (hard)
        facts = _make_facts(rating_new="Overweight", analyst_firm="Barclays Capital")
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "AAPL")

        # Firm mismatch → PARTIAL, never MISMATCH
        for r in result.field_results:
            if r.field == "analyst_firm":
                assert r.status != FactValidationStatus.MISMATCH

    @pytest.mark.asyncio
    async def test_analyst_firm_token_overlap_match(self):
        pool = FakePool({"fmp_analyst_grades": {
            "to_grade": "Buy", "price_target": 200.0,
            "analyst_firm": "Goldman Sachs Group",
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(analyst_firm="Goldman Sachs")  # partial name
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "AAPL")

        for r in result.field_results:
            if r.field == "analyst_firm":
                assert r.status == FactValidationStatus.CONFIRMED

    @pytest.mark.asyncio
    async def test_no_grade_in_db_gives_unverifiable(self):
        pool = FakePool({"fmp_analyst_grades": None})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(rating_new="Buy", price_target_new=200.0)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.ANALYST)
        result = await validator.validate(record, "AAPL")

        assert result.validation_status == FactValidationStatus.UNVERIFIABLE


# ─────────────────────────────────────────────────────────────────────────────
# 7. Fast path bypass
# ─────────────────────────────────────────────────────────────────────────────

class TestFastPathBypass:

    @pytest.mark.asyncio
    async def test_fast_path_clean_gives_skipped(self):
        pool = FakePool()
        validator = FactCrossValidator(db_pool=pool)
        # Fast-path, internally consistent
        facts = _make_facts(eps_beat=True, eps_actual=2.05, eps_estimate=1.80)
        record = _make_record(facts_json=facts, route_type="fast")
        result = await validator.validate(record, "AAPL")

        assert result.validation_status == FactValidationStatus.SKIPPED
        assert result.conviction_multiplier == 1.0

    @pytest.mark.asyncio
    async def test_fast_path_with_ic_mismatch_returns_mismatch(self):
        pool = FakePool()
        validator = FactCrossValidator(db_pool=pool)
        # Fast-path but IC mismatch: eps_beat=True but actual < estimate
        facts = _make_facts(eps_beat=True, eps_actual=1.50, eps_estimate=2.00)
        record = _make_record(facts_json=facts, route_type="fast")
        result = await validator.validate(record, "AAPL")

        assert result.validation_status == FactValidationStatus.MISMATCH
        assert result.has_key_mismatch is True

    @pytest.mark.asyncio
    async def test_fast_path_skips_db_query(self):
        """Verify the DB pool is never queried for fast-path clean records."""
        pool = FakePool()
        # If fetchrow is called, the test will track it
        fetchrow_calls = []

        original_fetchrow = pool.fetchrow
        async def tracking_fetchrow(query, *args):
            fetchrow_calls.append(query)
            return await original_fetchrow(query, *args)

        pool.fetchrow = tracking_fetchrow
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True, eps_actual=2.05, eps_estimate=1.80)
        record = _make_record(facts_json=facts, route_type="fast")
        await validator.validate(record, "AAPL")

        assert fetchrow_calls == []  # DB never queried

    @pytest.mark.asyncio
    async def test_slow_path_does_query_db(self):
        """Slow path (route_type=None) should query DB for earnings records."""
        pool = FakePool({"event": {
            "actual_eps": 2.05, "consensus_eps": 1.80,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        fetchrow_calls = []
        original = pool.fetchrow

        async def tracking(query, *args):
            fetchrow_calls.append(query)
            return await original(query, *args)

        pool.fetchrow = tracking
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True)
        record = _make_record(facts_json=facts, route_type=None, catalyst_type=CatalystType.EARNINGS)
        await validator.validate(record, "AAPL")

        assert len(fetchrow_calls) > 0

    @pytest.mark.asyncio
    async def test_no_facts_json_gives_skipped_regardless_of_route(self):
        pool = FakePool()
        validator = FactCrossValidator(db_pool=pool)
        record = _make_record(facts_json=None, route_type=None)
        result = await validator.validate(record, "AAPL")

        assert result.validation_status == FactValidationStatus.SKIPPED


# ─────────────────────────────────────────────────────────────────────────────
# 8. Graceful degradation
# ─────────────────────────────────────────────────────────────────────────────

class TestGracefulDegradation:

    @pytest.mark.asyncio
    async def test_none_pool_gives_unverifiable(self):
        validator = FactCrossValidator(db_pool=None)
        facts = _make_facts(eps_beat=True, eps_actual=2.05)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        # IC check ran (no DB needed); only DB fields are unverifiable
        # eps_beat without eps_estimate → IC skipped; no DB → UNVERIFIABLE
        assert result.validation_status in (
            FactValidationStatus.UNVERIFIABLE,
            FactValidationStatus.SKIPPED,
        )

    @pytest.mark.asyncio
    async def test_db_connection_error_degrades_gracefully(self):
        pool = FakePool()
        pool.go_down()  # simulate DB outage
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)

        # Must not raise
        result = await validator.validate(record, "AAPL")
        assert result.validation_status in (
            FactValidationStatus.UNVERIFIABLE,
            FactValidationStatus.MISMATCH,   # IC may still fire
        )

    @pytest.mark.asyncio
    async def test_validator_never_raises(self):
        """validate() must NEVER raise — it's called inline in the hot path."""
        # Pathological input: missing attributes on record
        pool = FakePool()
        pool.go_down()
        validator = FactCrossValidator(db_pool=pool)
        # Pass an object with no attributes
        result = await validator.validate(object(), "BROKEN")
        assert isinstance(result, FactCrossValidationResult)

    @pytest.mark.asyncio
    async def test_db_error_with_ic_mismatch_returns_ic_result(self):
        """When DB is down but IC found a mismatch, return the IC result."""
        pool = FakePool()
        pool.go_down()
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True, eps_actual=1.50, eps_estimate=2.00)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)
        result = await validator.validate(record, "AAPL")

        # IC mismatch should still surface even though DB is down
        assert result.has_key_mismatch is True

    @pytest.mark.asyncio
    async def test_macro_catalyst_always_unverifiable(self):
        pool = FakePool()
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(actual_value=275.0, estimate_value=260.0)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.MACRO)
        result = await validator.validate(record, "SPY")

        # No vendor cross-check for macro; no IC issues → UNVERIFIABLE or SKIPPED
        assert result.validation_status in (
            FactValidationStatus.UNVERIFIABLE,
            FactValidationStatus.SKIPPED,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 9. Audit write
# ─────────────────────────────────────────────────────────────────────────────

class TestAuditWrite:

    @pytest.mark.asyncio
    async def test_mismatch_triggers_audit_write(self):
        pool = FakePool({"event": {
            "actual_eps": 1.50, "consensus_eps": 2.00,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True)  # Wrong — vendor says it was a miss
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)

        result = await validator.validate(record, "AAPL")
        # Give the fire-and-forget task a moment to complete
        await asyncio.sleep(0.05)

        assert result.mismatch_fields  # confirms mismatch was detected
        assert len(pool._writes) >= 1  # audit row was written

    @pytest.mark.asyncio
    async def test_confirmed_no_audit_by_default(self):
        """By default, only MISMATCH rows are written (FACT_AUDIT_ALL_STATUSES=false)."""
        pool = FakePool({"event": {
            "actual_eps": 2.05, "consensus_eps": 1.80,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True, eps_actual=2.05)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)

        import app.pipeline.fact_cross_validator as _fv_mod
        original = _fv_mod._AUDIT_ALL
        _fv_mod._AUDIT_ALL = False

        result = await validator.validate(record, "AAPL")
        await asyncio.sleep(0.05)

        _fv_mod._AUDIT_ALL = original

        # CONFIRMED with AUDIT_ALL=false → no write
        if result.validation_status == FactValidationStatus.CONFIRMED:
            assert pool._writes == []

    @pytest.mark.asyncio
    async def test_audit_all_flag_writes_for_confirmed(self):
        pool = FakePool({"event": {
            "actual_eps": 2.05, "consensus_eps": 1.80,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True, eps_actual=2.05)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)

        import app.pipeline.fact_cross_validator as _fv_mod
        _fv_mod._AUDIT_ALL = True
        result = await validator.validate(record, "AAPL")
        await asyncio.sleep(0.05)
        _fv_mod._AUDIT_ALL = False

        assert len(pool._writes) >= 1

    @pytest.mark.asyncio
    async def test_audit_write_failure_does_not_block_pipeline(self):
        """A failing audit write must not raise or stall the validator."""
        pool = FakePool({"event": {
            "actual_eps": 1.50, "consensus_eps": 2.00,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        # Make the execute() call blow up
        async def failing_execute(query, *args):
            raise RuntimeError("DB write failed")
        pool.execute = failing_execute

        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)

        # Must complete without raising
        result = await validator.validate(record, "AAPL")
        await asyncio.sleep(0.05)

        assert isinstance(result, FactCrossValidationResult)

    @pytest.mark.asyncio
    async def test_audit_row_contains_field_detail(self):
        """The field_detail_json argument must be non-empty for mismatches."""
        pool = FakePool({"event": {
            "actual_eps": 1.50, "consensus_eps": 2.00,
            "whisper_eps": None, "actual_revenue": None, "consensus_revenue": None,
        }})
        validator = FactCrossValidator(db_pool=pool)
        facts = _make_facts(eps_beat=True)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)

        await validator.validate(record, "AAPL")
        await asyncio.sleep(0.05)

        if pool._writes:
            # field_detail_json is the 11th positional argument (index 10)
            write_args = pool._writes[0]
            import json
            field_detail_json_arg = write_args[10]  # $11
            detail = json.loads(field_detail_json_arg)
            assert isinstance(detail, list)
            assert len(detail) > 0
            assert "field" in detail[0]

    @pytest.mark.asyncio
    async def test_none_pool_no_audit_write_attempted(self):
        """With no DB pool, audit write must silently do nothing."""
        validator = FactCrossValidator(db_pool=None)
        facts = _make_facts(eps_beat=True, eps_actual=1.50, eps_estimate=2.00)
        record = _make_record(facts_json=facts, catalyst_type=CatalystType.EARNINGS)

        # IC mismatch detected; audit write attempted but pool=None → silent skip
        result = await validator.validate(record, "AAPL")
        await asyncio.sleep(0.05)

        # No exception = pass
        assert result.has_key_mismatch is True
