"""
Catalyst-Aware Pretrade Policy Layer (v1.9)

Applies per-catalyst-family gates, multipliers, and threshold overrides on top
of the existing 4-filter (regime / technicals / options / squeeze) result.

Design goals
------------
* Each catalyst family has a policy object read from settings at startup —
  no hardcoded branches in pretrade_filter.process().
* All values are tunable via environment variables without code changes.
* A kill-switch (ENABLE_CATALYST_POLICY=false) restores pre-v1.9 behaviour exactly.
* Sympathy trades always face the strictest path regardless of catalyst family.

Integration point
-----------------
Called from pretrade_filter.PreTradeFilterService.process() after the 4 filter
tasks complete and before the final conviction formula is computed.

    policy = get_policy(self._policy_table, catalyst_type, is_sympathy)
    result = apply_policy(
        policy, signal=record,
        tech_raw_score=tech.technical_score,
        options_delta=raw_options_delta,
        regime_scale=regime["conviction_scale"],
        mktcap_tier=record.get("market_cap_tier"),
    )
    if result.block:
        ...emit blocked...
    # use result.adjusted_options_delta, result.adjusted_regime_scale,
    #     result.multiplier, result.threshold
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.config import Settings


# ── Policy dataclass ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CatalystPolicy:
    """
    Per-catalyst-family policy parameters.

    All fields have safe defaults matching the pre-v1.9 pass-through behaviour.
    """
    # Conviction threshold to use for this catalyst (replaces global default).
    conviction_threshold: float = 0.55

    # Minimum 0-10 composite tech score.  Below this = hard block.
    min_technical_score: int = 7

    # Multiplier on options_delta before adding to the conviction formula.
    # 1.0 = unchanged.  > 1.0 = amplify smart-money signal.  < 1.0 = dampen.
    options_weight: float = 1.0

    # Multiplied onto conviction when direction_source == "interpretive_prior".
    # 1.0 = no penalty.  0.60 = aggressive discount for LLM-only direction.
    interpretive_penalty: float = 1.0

    # Additional multiplier when is_sympathy == True.
    # Compounds with interpretive_penalty when both flags are set.
    sympathy_multiplier: float = 1.0

    # Block when the technicals filter reports zero volume confirmation.
    require_volume_confirmation: bool = False

    # Block when direction_source is not "facts".  Useful for regulatory/MA
    # where LLM-inferred direction is unreliable.
    require_facts_direction: bool = False

    # Hard cap on conviction after all policy adjustments.
    max_conviction: float = 1.0


# ── PolicyResult ──────────────────────────────────────────────────────────────

@dataclass
class PolicyResult:
    """
    Output of apply_policy().  Returned to pretrade_filter.process() for use
    in the final conviction formula.
    """
    # Hard block — skip conviction formula entirely.
    block: bool = False
    block_reason: str = ""

    # Conviction multiplier (interpretive_penalty × sympathy_multiplier).
    # Applied after all 4-filter multipliers (regime / tech / options / squeeze).
    multiplier: float = 1.0

    # Policy-adjusted options_delta (raw delta × options_weight × mktcap_scale).
    adjusted_options_delta: float = 0.0

    # Policy-adjusted regime_scale (raw scale × mktcap_regime_adj).
    adjusted_regime_scale: float = 1.0

    # Conviction threshold to use for this signal (per-catalyst + sympathy bump).
    threshold: float = 0.55

    # Debug fields — propagated to outbound record for research queries.
    options_weight_effective: float = 1.0
    regime_adj_effective: float = 1.0


# ── Market-cap tier lookup tables ─────────────────────────────────────────────
# Built from settings at startup.  Keys match MarketCapTier enum values.

def _build_mktcap_options_scale(s: "Settings") -> dict[str, float]:
    return {
        "mega":  s.policy_mktcap_mega_options_scale,
        "large": s.policy_mktcap_large_options_scale,
        "mid":   s.policy_mktcap_mid_options_scale,
        "small": s.policy_mktcap_small_options_scale,
        "micro": s.policy_mktcap_micro_options_scale,
    }


def _build_mktcap_regime_adj(s: "Settings") -> dict[str, float]:
    return {
        "mega":  s.policy_mktcap_mega_regime_adj,
        "large": s.policy_mktcap_large_regime_adj,
        "mid":   s.policy_mktcap_mid_regime_adj,
        "small": s.policy_mktcap_small_regime_adj,
        "micro": s.policy_mktcap_micro_regime_adj,
    }


# ── Policy table builder ──────────────────────────────────────────────────────

def build_policy_table(settings: "Settings") -> dict[str, CatalystPolicy]:
    """
    Build the per-catalyst policy mapping from settings at service startup.

    The returned dict maps CatalystType string values → CatalystPolicy.
    An "other" fallback is always present; unknown catalyst strings map to it
    via get_policy().
    """
    s = settings
    return {
        "earnings": CatalystPolicy(
            conviction_threshold=s.policy_threshold_earnings,
            min_technical_score=s.policy_min_tech_score_earnings,
            options_weight=s.policy_options_weight_earnings,
            interpretive_penalty=s.policy_interp_penalty_earnings,
            sympathy_multiplier=s.policy_sympathy_mult_earnings,
            require_volume_confirmation=s.policy_require_volume_earnings,
        ),
        "analyst": CatalystPolicy(
            conviction_threshold=s.policy_threshold_analyst,
            min_technical_score=s.policy_min_tech_score_analyst,
            options_weight=s.policy_options_weight_analyst,
            interpretive_penalty=s.policy_interp_penalty_analyst,
            sympathy_multiplier=s.policy_sympathy_mult_analyst,
            require_volume_confirmation=s.policy_require_volume_analyst,
        ),
        "regulatory": CatalystPolicy(
            conviction_threshold=s.policy_threshold_regulatory,
            min_technical_score=s.policy_min_tech_score_regulatory,
            options_weight=s.policy_options_weight_regulatory,
            interpretive_penalty=s.policy_interp_penalty_regulatory,
            sympathy_multiplier=s.policy_sympathy_mult_regulatory,
        ),
        "filing": CatalystPolicy(
            conviction_threshold=s.policy_threshold_filing,
            min_technical_score=s.policy_min_tech_score_filing,
            options_weight=s.policy_options_weight_filing,
            interpretive_penalty=s.policy_interp_penalty_filing,
            sympathy_multiplier=s.policy_sympathy_mult_filing,
        ),
        "ma": CatalystPolicy(
            conviction_threshold=s.policy_threshold_ma,
            min_technical_score=s.policy_min_tech_score_ma,
            options_weight=s.policy_options_weight_ma,
            interpretive_penalty=s.policy_interp_penalty_ma,
            sympathy_multiplier=s.policy_sympathy_mult_ma,
            require_volume_confirmation=s.policy_require_volume_ma,
        ),
        "macro": CatalystPolicy(
            conviction_threshold=s.policy_threshold_macro,
            min_technical_score=s.policy_min_tech_score_macro,
            options_weight=s.policy_options_weight_macro,
            interpretive_penalty=s.policy_interp_penalty_macro,
            sympathy_multiplier=s.policy_sympathy_mult_macro,
        ),
        "legal": CatalystPolicy(
            conviction_threshold=s.policy_threshold_legal,
            min_technical_score=s.policy_min_tech_score_legal,
            options_weight=s.policy_options_weight_legal,
            interpretive_penalty=s.policy_interp_penalty_legal,
            sympathy_multiplier=s.policy_sympathy_mult_legal,
        ),
        "other": CatalystPolicy(
            conviction_threshold=s.policy_threshold_other,
            min_technical_score=s.policy_min_tech_score_other,
            options_weight=s.policy_options_weight_other,
            interpretive_penalty=s.policy_interp_penalty_other,
            sympathy_multiplier=s.policy_sympathy_mult_other,
            require_volume_confirmation=s.policy_require_volume_other,
        ),
    }


# ── Policy resolution ─────────────────────────────────────────────────────────

def get_policy(
    table: dict[str, CatalystPolicy],
    catalyst_type: str,
    is_sympathy: bool = False,  # noqa: ARG001 — kept for future catalyst-specific logic
) -> CatalystPolicy:
    """
    Return the CatalystPolicy for *catalyst_type*, falling back to "other".

    is_sympathy is accepted for API symmetry; the sympathy threshold bump is
    applied in apply_policy() rather than here to keep policy objects immutable.
    """
    return table.get(catalyst_type, table["other"])


# ── Core policy application ───────────────────────────────────────────────────

def apply_policy(
    policy: CatalystPolicy,
    signal: dict,
    tech_raw_score: int,
    options_delta: float,
    regime_scale: float,
    mktcap_tier: "str | None",
    sympathy_threshold_bump: float = 0.08,
    mktcap_options_scale: "dict[str, float] | None" = None,
    mktcap_regime_adj: "dict[str, float] | None" = None,
) -> PolicyResult:
    """
    Evaluate catalyst policy gates and compute adjusted multipliers.

    Parameters
    ----------
    policy                  CatalystPolicy for this catalyst family.
    signal                  Raw record dict (from pretrade_filter.process).
    tech_raw_score          0-10 composite score from technicals filter.
    options_delta           Raw conviction_delta from options filter.
    regime_scale            Raw conviction_scale from regime filter.
    mktcap_tier             "mega"|"large"|"mid"|"small"|"micro" or None.
    sympathy_threshold_bump Added to threshold when is_sympathy=True.
    mktcap_options_scale    Pre-built tier → options scale dict (from settings).
    mktcap_regime_adj       Pre-built tier → regime adj dict (from settings).

    Returns
    -------
    PolicyResult
        If .block is True, caller should emit to signals.blocked and return None.
        Otherwise, use .adjusted_options_delta, .adjusted_regime_scale,
        .multiplier, and .threshold in the conviction formula.
    """
    direction_source = signal.get("direction_source", "neutral_default")
    is_sympathy      = bool(signal.get("is_sympathy", False))

    # Default fallback scales when pre-built dicts are not supplied
    if mktcap_options_scale is None:
        mktcap_options_scale = {}
    if mktcap_regime_adj is None:
        mktcap_regime_adj = {}

    # ── Gate 1: Minimum technical score ──────────────────────────────────────
    if tech_raw_score < policy.min_technical_score:
        return PolicyResult(
            block=True,
            block_reason=(
                f"catalyst_policy: tech_score={tech_raw_score} "
                f"< min={policy.min_technical_score} "
                f"for catalyst={signal.get('catalyst_type', 'unknown')}"
            ),
        )

    # ── Gate 2: Volume confirmation ───────────────────────────────────────────
    if policy.require_volume_confirmation:
        # Technicals filter sets volume_confirmation=1 (or True) when satisfied
        tech_vol = signal.get("filter_technicals", {})
        vol_pts = 0
        if isinstance(tech_vol, dict):
            breakdown = tech_vol.get("technical_score_breakdown", {})
            vol_pts = breakdown.get("volume_confirmation", 0)
        # Also accept a direct "tech_volume_confirmed" bool from integration tests
        if not signal.get("tech_volume_confirmed") and not vol_pts:
            return PolicyResult(
                block=True,
                block_reason=(
                    f"catalyst_policy: volume_confirmation required but not met "
                    f"for catalyst={signal.get('catalyst_type', 'unknown')}"
                ),
            )

    # ── Gate 3: Facts direction required ─────────────────────────────────────
    if policy.require_facts_direction and direction_source != "facts":
        return PolicyResult(
            block=True,
            block_reason=(
                f"catalyst_policy: facts_direction required "
                f"but direction_source={direction_source}"
            ),
        )

    # ── Multiplier: interpretive penalty ──────────────────────────────────────
    multiplier = 1.0
    if direction_source == "interpretive_prior":
        multiplier *= policy.interpretive_penalty

    # ── Multiplier: sympathy ──────────────────────────────────────────────────
    if is_sympathy:
        multiplier *= policy.sympathy_multiplier

    # ── Market-cap tier: options weight ───────────────────────────────────────
    tier_key = (mktcap_tier or "").lower()
    tier_opt_scale   = mktcap_options_scale.get(tier_key, 1.0)
    effective_options_weight = policy.options_weight * tier_opt_scale
    adjusted_options_delta   = options_delta * effective_options_weight

    # ── Market-cap tier: regime scale adjustment ──────────────────────────────
    tier_regime_adj        = mktcap_regime_adj.get(tier_key, 1.0)
    adjusted_regime_scale  = regime_scale * tier_regime_adj

    # ── Threshold: per-catalyst + sympathy bump ───────────────────────────────
    threshold = policy.conviction_threshold
    if is_sympathy:
        threshold += sympathy_threshold_bump

    return PolicyResult(
        block=False,
        multiplier=multiplier,
        adjusted_options_delta=adjusted_options_delta,
        adjusted_regime_scale=adjusted_regime_scale,
        threshold=threshold,
        options_weight_effective=effective_options_weight,
        regime_adj_effective=tier_regime_adj,
    )
