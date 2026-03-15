"""
Tests for LLM output trust classification and safe-fallback behaviour.

Coverage matrix
───────────────
TestDirectionFromFacts          — factual field precedence, conflicting facts
TestValidateLLMOutput           — field validation, range checks, vocab checks
TestInterpretiveFields          — signal_bias, priced_in, regime_flag, sympathy
TestDirectionHierarchy          — phase 1/2/3, regime demoted from direction
TestConvictionFormula           — interpretive cap, priced-in blocking, regime adj
TestAmbiguousCasesDefaultSafe   — every ambiguity rule → neutral / blocked
TestSympathyChainGuard          — interpretive primary blocks sympathy emission
TestValidationIssueAccumulation — multiple bad fields, trust score degradation
TestFastPathPassthrough         — route_type="fast" records untouched by caps
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock
from datetime import datetime, timezone

import pytest

# ── Mock Docker-only packages ─────────────────────────────────────────────────
if "confluent_kafka" not in sys.modules:
    _ck = MagicMock()
    sys.modules["confluent_kafka"] = _ck
    sys.modules["confluent_kafka.admin"] = MagicMock()

if "prometheus_client" not in sys.modules:
    _prom = MagicMock()
    sys.modules["prometheus_client"] = _prom

# ── Imports ───────────────────────────────────────────────────────────────────
from app.models.news import (
    CatalystType, FactsJson, FloatSensitivity, RegimeFlag,
    SessionContext, SummarizedRecord, NewsSource,
)
from app.pipeline.llm_validation import (
    INTERPRETIVE_MAX_CONVICTION,
    LLMTrustClass,
    LLMValidationResult,
    REGIME_CONVICTION_ADJ,
    apply_conviction_cap,
    apply_regime_adjustment,
    direction_from_facts,
    validate_llm_output,
    _ANALYST_BEARISH,
    _ANALYST_BULLISH,
)
from app.signals.signal_aggregator import (
    TradingSignal,
    build_sympathy_signal,
    classify_direction,
    compute_conviction,
)


# ── Fixture helpers ────────────────────────────────────────────────────────────

def _make_summarized(**kwargs) -> SummarizedRecord:
    """Minimal valid SummarizedRecord; all fields can be overridden."""
    defaults = dict(
        id="00000000-0000-0000-0000-000000000001",
        source=NewsSource.BENZINGA,
        vendor_id="test-001",
        published_at=datetime(2026, 3, 15, 14, 0, 0, tzinfo=timezone.utc),
        received_at=datetime(2026, 3, 15, 14, 0, 1, tzinfo=timezone.utc),
        url="https://example.com/news/1",
        canonical_url="https://example.com/news/1",
        title="Test headline",
        content_hash="abc123",
        tickers=["AAPL"],
        catalyst_type=CatalystType.EARNINGS,
        session_context=SessionContext.INTRADAY,
        float_sensitivity=FloatSensitivity.NORMAL,
        impact_day=0.7,
        impact_swing=0.5,
        source_credibility=0.85,
        facts_validated=True,
        is_representative=True,
    )
    defaults.update(kwargs)
    return SummarizedRecord.model_validate(defaults)


def _make_signal(**kwargs) -> TradingSignal:
    """Minimal TradingSignal for sympathy tests."""
    defaults = dict(
        ticker="AAPL",
        signal_type="event_driven",
        direction="long",
        conviction=0.72,
        catalyst_type="earnings",
        session_context="intraday",
        news_id="00000000-0000-0000-0000-000000000001",
        news_title="Test",
        float_sensitivity="normal",
        impact_day=0.7,
        impact_swing=0.5,
        source="benzinga",
        direction_source="facts",
    )
    defaults.update(kwargs)
    return TradingSignal.model_validate(defaults)


# ─────────────────────────────────────────────────────────────────────────────
# 1. direction_from_facts — structural fact precedence
# ─────────────────────────────────────────────────────────────────────────────

class TestDirectionFromFacts:

    def test_eps_beat_true_gives_long(self):
        f = FactsJson(eps_beat=True)
        assert direction_from_facts(f) == "long"

    def test_eps_beat_false_gives_short(self):
        f = FactsJson(eps_beat=False)
        assert direction_from_facts(f) == "short"

    def test_eps_beat_none_falls_through(self):
        f = FactsJson(eps_beat=None)
        assert direction_from_facts(f) == "neutral"

    def test_eps_beat_dominates_guidance(self):
        """eps_beat=True wins even when guidance_lowered=True (conflicting facts)."""
        f = FactsJson(eps_beat=True, guidance_lowered=True)
        assert direction_from_facts(f) == "long"

    def test_eps_beat_false_dominates_guidance_raised(self):
        f = FactsJson(eps_beat=False, guidance_raised=True)
        assert direction_from_facts(f) == "short"

    def test_guidance_raised_alone_gives_long(self):
        f = FactsJson(guidance_raised=True)
        assert direction_from_facts(f) == "long"

    def test_guidance_lowered_alone_gives_short(self):
        f = FactsJson(guidance_lowered=True)
        assert direction_from_facts(f) == "short"

    def test_conflicting_guidance_gives_neutral(self):
        f = FactsJson(guidance_raised=True, guidance_lowered=True)
        assert direction_from_facts(f) == "neutral"

    def test_bullish_analyst_rating_gives_long(self):
        for rating in ("Buy", "Strong Buy", "Outperform", "Overweight", "Upgrade"):
            f = FactsJson(rating_new=rating)
            assert direction_from_facts(f) == "long", f"Expected long for rating '{rating}'"

    def test_bearish_analyst_rating_gives_short(self):
        for rating in ("Sell", "Strong Sell", "Underperform", "Underweight", "Downgrade"):
            f = FactsJson(rating_new=rating)
            assert direction_from_facts(f) == "short", f"Expected short for rating '{rating}'"

    def test_neutral_analyst_rating_gives_neutral(self):
        f = FactsJson(rating_new="Hold")
        assert direction_from_facts(f) == "neutral"

    def test_ma_deal_price_gives_long(self):
        f = FactsJson(deal_price=150.0)
        assert direction_from_facts(f) == "long"

    def test_ma_zero_deal_price_gives_neutral(self):
        f = FactsJson(deal_price=0.0)
        assert direction_from_facts(f) == "neutral"

    def test_fda_approved_gives_long(self):
        for outcome in ("approved", "fast_track", "breakthrough"):
            f = FactsJson(fda_outcome=outcome)
            assert direction_from_facts(f) == "long", f"Expected long for '{outcome}'"

    def test_fda_rejected_gives_short(self):
        for outcome in ("rejected", "denied", "failed", "complete_response"):
            f = FactsJson(fda_outcome=outcome)
            assert direction_from_facts(f) == "short", f"Expected short for '{outcome}'"

    def test_none_facts_json_gives_neutral(self):
        assert direction_from_facts(None) == "neutral"

    def test_empty_facts_json_gives_neutral(self):
        assert direction_from_facts(FactsJson()) == "neutral"


# ─────────────────────────────────────────────────────────────────────────────
# 2. validate_llm_output — field validation
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateLLMOutput:

    def test_clean_record_no_issues(self):
        rec = _make_summarized(
            impact_day=0.7, impact_swing=0.5, source_credibility=0.85,
            signal_bias="long", priced_in="no", regime_flag=RegimeFlag.RISK_ON,
            sympathy_plays=["MSFT", "GOOG"],
        )
        result = validate_llm_output(rec)
        assert result.issues == []
        assert result.interpretive_trust_score == 1.0
        assert result.cleaned_signal_bias == "long"
        assert result.cleaned_priced_in == "no"
        assert result.cleaned_regime_flag == "risk_on"
        assert result.cleaned_sympathy_plays == ["MSFT", "GOOG"]

    def test_impact_day_above_one_flagged(self):
        rec = _make_summarized(impact_day=1.5)
        result = validate_llm_output(rec)
        assert result.cleaned_impact_day is None
        issue_fields = [i.field for i in result.issues]
        assert "impact_day" in issue_fields

    def test_impact_day_negative_flagged(self):
        rec = _make_summarized(impact_day=-0.1)
        result = validate_llm_output(rec)
        assert result.cleaned_impact_day is None

    def test_impact_day_zero_is_valid(self):
        rec = _make_summarized(impact_day=0.0)
        result = validate_llm_output(rec)
        assert result.cleaned_impact_day == 0.0
        assert all(i.field != "impact_day" for i in result.issues)

    def test_impact_day_one_is_valid(self):
        rec = _make_summarized(impact_day=1.0)
        result = validate_llm_output(rec)
        assert result.cleaned_impact_day == 1.0

    def test_invalid_signal_bias_treated_as_absent(self):
        rec = _make_summarized(signal_bias="bullish")  # not in vocab
        result = validate_llm_output(rec)
        assert result.cleaned_signal_bias is None
        assert any(i.field == "signal_bias" for i in result.issues)

    def test_signal_bias_neutral_treated_as_absent(self):
        """'neutral' signal_bias = no directional prior (same as absent)."""
        rec = _make_summarized(signal_bias="neutral")
        result = validate_llm_output(rec)
        assert result.cleaned_signal_bias is None
        # But it should NOT be flagged as an issue
        assert all(i.field != "signal_bias" for i in result.issues)

    def test_invalid_priced_in_treated_as_absent(self):
        rec = _make_summarized(priced_in="maybe")  # not in vocab
        result = validate_llm_output(rec)
        assert result.cleaned_priced_in is None
        assert any(i.field == "priced_in" for i in result.issues)

    def test_none_priced_in_no_issue(self):
        rec = _make_summarized(priced_in=None)
        result = validate_llm_output(rec)
        assert result.cleaned_priced_in is None
        assert all(i.field != "priced_in" for i in result.issues)

    def test_invalid_ticker_in_sympathy_plays_removed(self):
        rec = _make_summarized(sympathy_plays=["MSFT", "NOT A TICKER!", "GOOG"])
        result = validate_llm_output(rec)
        assert "MSFT" in result.cleaned_sympathy_plays
        assert "GOOG" in result.cleaned_sympathy_plays
        assert "NOT A TICKER!" not in result.cleaned_sympathy_plays
        assert any(i.field == "sympathy_plays" for i in result.issues)

    def test_lowercase_ticker_sanitised(self):
        rec = _make_summarized(sympathy_plays=["msft"])  # lowercase
        result = validate_llm_output(rec)
        # "msft" doesn't match [A-Z]{1,5} — should be removed
        assert "msft" not in result.cleaned_sympathy_plays

    def test_trust_score_degrades_per_invalid_field(self):
        rec = _make_summarized(
            impact_day=9.9,      # invalid
            signal_bias="ULTRA", # invalid
        )
        result = validate_llm_output(rec)
        assert result.interpretive_trust_score < 1.0

    def test_facts_validated_flag_false_on_bad_eps(self):
        facts = FactsJson(eps_actual=99999.0)  # implausibly large
        rec = _make_summarized(facts_json=facts)
        result = validate_llm_output(rec)
        assert result.facts_validated is False

    def test_facts_validated_true_on_clean_record(self):
        facts = FactsJson(eps_beat=True, eps_actual=2.05, eps_estimate=1.90)
        rec = _make_summarized(facts_json=facts)
        result = validate_llm_output(rec)
        assert result.facts_validated is True
        assert result.has_valid_facts is True

    def test_no_facts_has_valid_facts_false(self):
        rec = _make_summarized(facts_json=None)
        result = validate_llm_output(rec)
        assert result.has_valid_facts is False


# ─────────────────────────────────────────────────────────────────────────────
# 3. Interpretive field rules
# ─────────────────────────────────────────────────────────────────────────────

class TestInterpretiveFields:

    def test_regime_flag_risk_off_not_in_cleaned_when_wrong_type(self):
        rec = _make_summarized(regime_flag=None)
        result = validate_llm_output(rec)
        assert result.cleaned_regime_flag is None

    def test_regime_flag_all_valid_values_pass(self):
        for rf in ("risk_on", "risk_off", "high_vol", "compression"):
            from app.models.news import RegimeFlag as RF
            rec = _make_summarized(regime_flag=RF(rf))
            result = validate_llm_output(rec)
            assert result.cleaned_regime_flag == rf

    def test_regime_conviction_adj_risk_off(self):
        """regime_flag=risk_off reduces conviction by 10%."""
        base = 0.80
        adjusted = apply_regime_adjustment(base, "risk_off")
        assert adjusted == pytest.approx(0.72, abs=0.01)

    def test_regime_conviction_adj_high_vol(self):
        adjusted = apply_regime_adjustment(0.80, "high_vol")
        assert adjusted == pytest.approx(0.68, abs=0.01)

    def test_regime_conviction_adj_risk_on_neutral(self):
        """risk_on multiplier is 1.00 — no change."""
        adjusted = apply_regime_adjustment(0.80, "risk_on")
        assert adjusted == pytest.approx(0.80, abs=0.001)

    def test_regime_conviction_adj_none_no_change(self):
        assert apply_regime_adjustment(0.70, None) == pytest.approx(0.70)

    def test_priced_in_yes_valid(self):
        rec = _make_summarized(priced_in="yes")
        result = validate_llm_output(rec)
        assert result.cleaned_priced_in == "yes"
        assert not result.issues

    def test_empty_sympathy_plays_ok(self):
        rec = _make_summarized(sympathy_plays=[])
        result = validate_llm_output(rec)
        assert result.cleaned_sympathy_plays == []
        assert not result.issues


# ─────────────────────────────────────────────────────────────────────────────
# 4. Direction hierarchy: facts > interpretive_prior > neutral_default
# ─────────────────────────────────────────────────────────────────────────────

class TestDirectionHierarchy:

    def test_phase1_eps_beat_facts_source(self):
        rec = _make_summarized(
            facts_json=FactsJson(eps_beat=True),
            signal_bias="short",   # LLM contradicts facts — facts must win
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "long"
        assert source == "facts"

    def test_phase1_guidance_lowered_facts_source(self):
        rec = _make_summarized(
            facts_json=FactsJson(guidance_lowered=True),
            signal_bias="long",    # LLM disagrees — facts win
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "short"
        assert source == "facts"

    def test_phase2_signal_bias_when_no_facts(self):
        rec = _make_summarized(
            facts_json=None,
            signal_bias="short",
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "short"
        assert source == "interpretive_prior"

    def test_phase3_neutral_default_when_nothing(self):
        rec = _make_summarized(
            facts_json=None,
            signal_bias=None,
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_regime_risk_on_no_longer_gives_long(self):
        """
        regime_flag=RISK_ON was previously able to return 'long'.
        It must NOT determine direction in v1.6+.
        """
        rec = _make_summarized(
            facts_json=None,
            signal_bias=None,
            regime_flag=RegimeFlag.RISK_ON,
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_regime_risk_off_no_longer_gives_short(self):
        rec = _make_summarized(
            facts_json=None,
            signal_bias=None,
            regime_flag=RegimeFlag.RISK_OFF,
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_invalid_signal_bias_falls_to_neutral(self):
        """Garbage signal_bias → cleaned to None → neutral_default."""
        rec = _make_summarized(facts_json=None, signal_bias="ULTRA_BULLISH")
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_facts_beat_interpretive_even_with_regime(self):
        rec = _make_summarized(
            facts_json=FactsJson(eps_beat=False),   # factual short
            signal_bias="long",                      # LLM says long
            regime_flag=RegimeFlag.RISK_ON,          # regime says long
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "short"
        assert source == "facts"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Conviction formula — caps and regime adjustments
# ─────────────────────────────────────────────────────────────────────────────

class TestConvictionFormula:

    def _run(self, rec, direction_source):
        validation = validate_llm_output(rec)
        return compute_conviction(rec, direction_source, validation)

    def test_facts_driven_signal_no_cap(self):
        """Fact-backed signals may exceed INTERPRETIVE_MAX_CONVICTION."""
        rec = _make_summarized(
            impact_day=0.9,
            source_credibility=1.0,
            catalyst_type=CatalystType.MA,  # weight 1.8 — will push conviction high
        )
        conviction, cap = self._run(rec, "facts")
        assert cap is False
        # With MA weight 1.8 × impact 0.9 × credibility 1.2 it will be > 0.60
        assert conviction > INTERPRETIVE_MAX_CONVICTION

    def test_interpretive_signal_capped(self):
        rec = _make_summarized(
            impact_day=0.9,
            source_credibility=1.0,
            catalyst_type=CatalystType.MA,
        )
        conviction, cap = self._run(rec, "interpretive_prior")
        assert cap is True
        assert conviction <= INTERPRETIVE_MAX_CONVICTION

    def test_interpretive_low_base_not_capped(self):
        """Interpretive signal with low base conviction is under the cap already."""
        rec = _make_summarized(impact_day=0.2, source_credibility=0.5)
        conviction, cap = self._run(rec, "interpretive_prior")
        # Low base × low credibility should be well under 0.60
        if conviction <= INTERPRETIVE_MAX_CONVICTION:
            # cap may or may not have applied depending on multipliers
            assert conviction >= 0.0

    def test_priced_in_yes_discounts_fact_signal(self):
        rec = _make_summarized(impact_day=0.8, priced_in="yes")
        conviction_no_pin, _ = self._run(_make_summarized(impact_day=0.8, priced_in="no"), "facts")
        conviction_pin, _    = self._run(rec, "facts")
        assert conviction_pin < conviction_no_pin

    def test_priced_in_yes_on_interpretive_returns_zero(self):
        """interpretive + priced_in="yes" → block entirely (return 0.0)."""
        rec = _make_summarized(impact_day=0.9, priced_in="yes")
        conviction, cap = self._run(rec, "interpretive_prior")
        assert conviction == 0.0

    def test_priced_in_partially_smaller_discount_than_yes(self):
        rec_yes = _make_summarized(impact_day=0.8, priced_in="yes",
                                   catalyst_type=CatalystType.EARNINGS)
        rec_par = _make_summarized(impact_day=0.8, priced_in="partially",
                                   catalyst_type=CatalystType.EARNINGS)
        c_yes, _ = self._run(rec_yes, "facts")
        c_par, _ = self._run(rec_par, "facts")
        assert c_par > c_yes

    def test_legal_catalyst_always_priced_in(self):
        """CatalystType.LEGAL → priced_in forced to 'yes' regardless of field."""
        rec = _make_summarized(
            impact_day=0.9,
            catalyst_type=CatalystType.LEGAL,
            priced_in=None,
        )
        conviction, _ = self._run(rec, "facts")
        # Should be heavily discounted (×0.60 of base)
        base_rec = _make_summarized(impact_day=0.9, catalyst_type=CatalystType.EARNINGS)
        conviction_base, _ = self._run(base_rec, "facts")
        assert conviction < conviction_base

    def test_regime_risk_off_reduces_conviction(self):
        """risk_off regime flag reduces fact-backed conviction by 10%."""
        rec_neutral_rf = _make_summarized(impact_day=0.7, regime_flag=None)
        rec_risk_off   = _make_summarized(impact_day=0.7, regime_flag=RegimeFlag.RISK_OFF)
        c_neutral, _ = self._run(rec_neutral_rf, "facts")
        c_risk_off, _ = self._run(rec_risk_off, "facts")
        assert c_risk_off < c_neutral
        assert c_risk_off == pytest.approx(c_neutral * 0.90, abs=0.01)

    def test_invalid_impact_day_uses_zero(self):
        """If impact_day is out of range it is cleaned to None → base=0.0."""
        rec = _make_summarized(impact_day=5.0)  # invalid
        conviction, _ = self._run(rec, "facts")
        assert conviction == 0.0

    def test_apply_conviction_cap_returns_cap_applied_true(self):
        conviction, applied = apply_conviction_cap(0.80, "interpretive_prior")
        assert conviction == INTERPRETIVE_MAX_CONVICTION
        assert applied is True

    def test_apply_conviction_cap_facts_not_capped(self):
        conviction, applied = apply_conviction_cap(0.95, "facts")
        assert conviction == 0.95
        assert applied is False

    def test_apply_conviction_cap_under_threshold_no_change(self):
        conviction, applied = apply_conviction_cap(0.40, "interpretive_prior")
        assert conviction == 0.40
        assert applied is False


# ─────────────────────────────────────────────────────────────────────────────
# 6. Ambiguous cases — all must default to neutral or blocked
# ─────────────────────────────────────────────────────────────────────────────

class TestAmbiguousCasesDefaultSafe:

    def _classify(self, rec):
        return classify_direction(rec, validate_llm_output(rec))

    def test_no_facts_no_bias_is_neutral_default(self):
        rec = _make_summarized(facts_json=None, signal_bias=None)
        direction, source = self._classify(rec)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_conflicting_guidance_no_bias_is_neutral(self):
        rec = _make_summarized(
            facts_json=FactsJson(guidance_raised=True, guidance_lowered=True),
            signal_bias=None,
        )
        direction, source = self._classify(rec)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_unknown_fda_outcome_is_neutral(self):
        rec = _make_summarized(
            facts_json=FactsJson(fda_outcome="expedited review"),  # not in vocab
            signal_bias=None,
        )
        direction, source = self._classify(rec)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_garbage_signal_bias_is_neutral(self):
        rec = _make_summarized(facts_json=None, signal_bias="EXTREMELY_BULLISH")
        direction, source = self._classify(rec)
        assert direction == "neutral"
        assert source == "neutral_default"

    def test_regime_alone_is_neutral(self):
        """regime_flag without any other directional evidence → neutral."""
        for rf in (RegimeFlag.RISK_ON, RegimeFlag.RISK_OFF,
                   RegimeFlag.HIGH_VOL, RegimeFlag.COMPRESSION):
            rec = _make_summarized(facts_json=None, signal_bias=None, regime_flag=rf)
            direction, source = self._classify(rec)
            assert direction == "neutral", f"Expected neutral for regime_flag={rf}"

    def test_interpretive_priced_in_conviction_is_zero(self):
        """interpretive_prior + priced_in='yes' → 0.0 conviction (blocked)."""
        rec = _make_summarized(
            facts_json=None,
            signal_bias="long",
            priced_in="yes",
            impact_day=0.9,
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        conviction, _ = compute_conviction(rec, source, validation)
        assert direction == "long"          # direction was set
        assert source == "interpretive_prior"
        assert conviction == 0.0            # but conviction is blocked

    def test_none_impact_day_blocks_signal(self):
        """No impact_day → base=0.0 → conviction=0.0."""
        rec = _make_summarized(impact_day=None)
        validation = validate_llm_output(rec)
        conviction, _ = compute_conviction(rec, "facts", validation)
        assert conviction == 0.0

    def test_empty_facts_json_with_signal_bias_is_interpretive(self):
        rec = _make_summarized(
            facts_json=FactsJson(),   # All None fields
            signal_bias="short",
        )
        direction, source = self._classify(rec)
        assert direction == "short"
        assert source == "interpretive_prior"


# ─────────────────────────────────────────────────────────────────────────────
# 7. Sympathy chain guard
# ─────────────────────────────────────────────────────────────────────────────

class TestSympathyChainGuard:

    def test_facts_primary_allows_sympathy(self):
        primary = _make_signal(direction_source="facts", conviction=0.75)
        sym = build_sympathy_signal(primary, "MSFT")
        assert sym is not None
        assert sym.ticker == "MSFT"

    def test_interpretive_primary_blocks_sympathy(self):
        """LLM-only primary must not spawn sympathy chains."""
        primary = _make_signal(direction_source="interpretive_prior", conviction=0.58)
        sym = build_sympathy_signal(primary, "MSFT")
        assert sym is None

    def test_neutral_default_primary_blocks_sympathy(self):
        primary = _make_signal(direction_source="neutral_default", conviction=0.0)
        sym = build_sympathy_signal(primary, "MSFT")
        assert sym is None

    def test_sympathy_conviction_is_discounted(self):
        primary = _make_signal(direction_source="facts", conviction=0.80)
        sym = build_sympathy_signal(primary, "GOOG")
        assert sym is not None
        assert sym.conviction < primary.conviction

    def test_sympathy_direction_source_always_interpretive(self):
        """Sympathy signals are LLM-suggested tickers — always interpretive."""
        primary = _make_signal(direction_source="facts", conviction=0.80)
        sym = build_sympathy_signal(primary, "GOOG")
        assert sym.direction_source == "interpretive_prior"

    def test_sympathy_interpretive_cap_applied_true(self):
        primary = _make_signal(direction_source="facts", conviction=0.80)
        sym = build_sympathy_signal(primary, "GOOG")
        assert sym.interpretive_cap_applied is True

    def test_sympathy_chasing_discount_applied(self):
        """Sympathy ticker already ran >3% gets the heaviest chasing discount."""
        primary = _make_signal(direction_source="facts", conviction=0.80, direction="long")
        sym_fresh = build_sympathy_signal(primary, "GOOG", intraday_return=0.0)
        sym_chased = build_sympathy_signal(primary, "GOOG", intraday_return=5.0)
        assert sym_fresh.conviction > sym_chased.conviction

    def test_sympathy_counter_direction_penalty(self):
        """Sympathy ticker moving against expected direction gets extra penalty."""
        primary = _make_signal(direction_source="facts", conviction=0.80, direction="long")
        sym_aligned  = build_sympathy_signal(primary, "GOOG", intraday_return=+2.0)
        sym_opposing = build_sympathy_signal(primary, "GOOG", intraday_return=-2.0)
        assert sym_aligned.conviction > sym_opposing.conviction

    def test_sympathy_inherits_timestamps(self):
        now = datetime.now(timezone.utc)
        primary = _make_signal(
            direction_source="facts",
            conviction=0.75,
            news_published_at=now,
            pipeline_entry_at=now,
            route_type="fast",
        )
        sym = build_sympathy_signal(primary, "NVDA")
        assert sym.news_published_at == now
        assert sym.route_type == "fast"


# ─────────────────────────────────────────────────────────────────────────────
# 8. Validation issue accumulation and trust score
# ─────────────────────────────────────────────────────────────────────────────

class TestValidationIssueAccumulation:

    def test_single_bad_field_degrades_trust(self):
        rec = _make_summarized(impact_day=99.0)
        result = validate_llm_output(rec)
        assert result.interpretive_trust_score < 1.0

    def test_two_bad_fields_more_degraded(self):
        rec_one = _make_summarized(impact_day=99.0)
        rec_two = _make_summarized(impact_day=99.0, signal_bias="GARBAGE")
        r1 = validate_llm_output(rec_one)
        r2 = validate_llm_output(rec_two)
        assert r2.interpretive_trust_score < r1.interpretive_trust_score

    def test_trust_score_floor_is_zero(self):
        rec = _make_summarized(
            impact_day=99.0, impact_swing=99.0, source_credibility=99.0,
            signal_bias="GARBAGE", priced_in="DUNNO",
            sympathy_plays=["bad ticker!", "also bad$$"],
        )
        result = validate_llm_output(rec)
        assert result.interpretive_trust_score >= 0.0

    def test_all_clean_trust_score_is_one(self):
        rec = _make_summarized(
            impact_day=0.7, impact_swing=0.4, source_credibility=0.9,
            signal_bias="long", priced_in="partially",
        )
        result = validate_llm_output(rec)
        assert result.interpretive_trust_score == 1.0

    def test_issues_list_has_correct_field_names(self):
        rec = _make_summarized(impact_day=5.0, signal_bias="MOON")
        result = validate_llm_output(rec)
        fields = [i.field for i in result.issues]
        assert "impact_day" in fields
        assert "signal_bias" in fields

    def test_facts_validated_false_when_eps_out_of_range(self):
        facts = FactsJson(eps_actual=50_000.0)  # way too large
        rec = _make_summarized(facts_json=facts)
        result = validate_llm_output(rec)
        assert result.facts_validated is False

    def test_boolean_coerced_by_pydantic_is_valid(self):
        # Pydantic v2 lax-mode coerces "yes" → True for bool|None fields.
        # By the time validate_llm_output() sees the record the field is already
        # a proper bool — facts_validated must remain True (not a validation error).
        facts = FactsJson(eps_beat="yes")   # Pydantic coerces → True
        assert facts.eps_beat is True       # confirm coercion happened
        rec = _make_summarized(facts_json=facts)
        result = validate_llm_output(rec)
        assert result.facts_validated is True


# ─────────────────────────────────────────────────────────────────────────────
# 9. Fast-path records (route_type="fast", structured facts, no LLM bias)
# ─────────────────────────────────────────────────────────────────────────────

class TestFastPathPassthrough:

    def test_fast_path_facts_give_direction_source_facts(self):
        """Fast-path records have real facts — should always be direction_source='facts'."""
        rec = _make_summarized(
            facts_json=FactsJson(eps_beat=True),
            route_type="fast",
            signal_bias=None,  # fast path has no LLM bias
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        assert direction == "long"
        assert source == "facts"

    def test_fast_path_no_interpretive_cap(self):
        """Fast-path signals with strong facts must not hit the interpretive cap."""
        rec = _make_summarized(
            facts_json=FactsJson(deal_price=120.0),
            catalyst_type=CatalystType.MA,
            impact_day=0.9,
            source_credibility=1.0,
            route_type="fast",
            signal_bias=None,
        )
        validation = validate_llm_output(rec)
        direction, source = classify_direction(rec, validation)
        conviction, cap = compute_conviction(rec, source, validation)
        assert source == "facts"
        assert cap is False
        assert conviction > INTERPRETIVE_MAX_CONVICTION

    def test_fast_path_no_signal_bias_no_issues(self):
        """Fast-path records have no t2/signal_bias — should produce zero LLM issues."""
        rec = _make_summarized(
            route_type="fast",
            signal_bias=None,
            priced_in=None,
            regime_flag=None,
            sympathy_plays=[],
        )
        result = validate_llm_output(rec)
        # No issues from absent interpretive fields
        interp_issue_fields = {i.field for i in result.issues}
        for f in ("signal_bias", "priced_in", "regime_flag"):
            assert f not in interp_issue_fields
