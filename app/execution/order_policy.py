"""
Order Type Policy — selects MARKET vs LIMIT by catalyst urgency and ADV tier (v1.11).

The policy table maps (catalyst_type, adv_tier) → OrderTypePolicy.
Every cell is configurable via env / Settings; the defaults encode the
risk team's consensus on urgency vs. slippage trade-off:

  catalyst / ADV     | liquid   | normal     | thin
  -------------------|----------|------------|----------
  earnings           | market   | limit_ioc  | limit
  analyst            | limit    | limit      | limit
  regulatory / fda   | market   | limit_ioc  | limit_ioc
  m&a / merger       | market   | limit_ioc  | limit
  macro              | market   | market     | limit
  other (default)    | limit    | limit      | limit

ADV tiers
---------
  liquid : ADV ≥ order_adv_liquid_threshold  (default 5_000_000)
  thin   : ADV < order_adv_thin_threshold    (default 500_000)
  normal : everything in between

A None ADV (information unavailable) maps to "normal" (fail-safe).

Usage
-----
    table  = build_order_policy_table(settings)
    policy = get_order_policy(table, catalyst_type="earnings", adv=8_000_000)
    # → OrderTypePolicy(order_type="market", time_in_force="day", ...)
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.config import Settings


# ── ADV tier labels ───────────────────────────────────────────────────────────
ADV_TIER_LIQUID = "liquid"
ADV_TIER_NORMAL = "normal"
ADV_TIER_THIN   = "thin"

_ALL_TIERS = (ADV_TIER_LIQUID, ADV_TIER_NORMAL, ADV_TIER_THIN)

# ── Catalyst keys used in the policy table ───────────────────────────────────
_CATALYST_EARNINGS   = "earnings"
_CATALYST_ANALYST    = "analyst"
_CATALYST_REGULATORY = "regulatory"
_CATALYST_MA         = "m&a"
_CATALYST_MACRO      = "macro"
_CATALYST_OTHER      = "other"

# Catalyst aliases: normalise to one of the keys above
_CATALYST_ALIASES: dict[str, str] = {
    "earnings":       _CATALYST_EARNINGS,
    "analyst":        _CATALYST_ANALYST,
    "analyst_rating": _CATALYST_ANALYST,
    "upgrade":        _CATALYST_ANALYST,
    "downgrade":      _CATALYST_ANALYST,
    "regulatory":     _CATALYST_REGULATORY,
    "fda":            _CATALYST_REGULATORY,
    "fda_approval":   _CATALYST_REGULATORY,
    "m&a":            _CATALYST_MA,
    "merger":         _CATALYST_MA,
    "acquisition":    _CATALYST_MA,
    "macro":          _CATALYST_MACRO,
    "fed":            _CATALYST_MACRO,
    "other":          _CATALYST_OTHER,
}


def _log(level: str, event: str, **kw) -> None:
    import json
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


@dataclass(frozen=True)
class OrderTypePolicy:
    """
    Immutable policy cell returned by get_order_policy().

    Fields
    ------
    order_type         : "market" | "limit" | "limit_ioc"
    limit_slippage_pct : for LIMIT orders — price = mid ± this% (0 = at ask/bid)
    time_in_force      : "day" | "ioc"
    bracket_enabled    : always True — TP/SL always attached
    adv_tier           : the tier that was matched (liquid|normal|thin)
    catalyst_key       : the normalised catalyst key matched
    policy_name        : human-readable label for logging / execution_quality row
    """
    order_type: str               # "market" | "limit" | "limit_ioc"
    limit_slippage_pct: float     # 0.0 for market orders
    time_in_force: str            # "day" | "ioc"
    bracket_enabled: bool = True
    adv_tier: str = ADV_TIER_NORMAL
    catalyst_key: str = _CATALYST_OTHER
    policy_name: str = ""


def _adv_tier(adv: float | None, liquid_threshold: int, thin_threshold: int) -> str:
    """Classify ADV into liquid / normal / thin.  None → normal (fail-safe)."""
    if adv is None:
        return ADV_TIER_NORMAL
    if adv >= liquid_threshold:
        return ADV_TIER_LIQUID
    if adv < thin_threshold:
        return ADV_TIER_THIN
    return ADV_TIER_NORMAL


def _make_policy(
    order_type: str,
    time_in_force: str,
    slippage_pct: float,
    tier: str,
    catalyst: str,
) -> OrderTypePolicy:
    name = f"{catalyst}_{tier}_{order_type}"
    return OrderTypePolicy(
        order_type=order_type,
        limit_slippage_pct=slippage_pct,
        time_in_force=time_in_force,
        bracket_enabled=True,
        adv_tier=tier,
        catalyst_key=catalyst,
        policy_name=name,
    )


def build_order_policy_table(settings: "Settings") -> dict[tuple[str, str], OrderTypePolicy]:
    """
    Build the full (catalyst, adv_tier) → OrderTypePolicy mapping.

    All values are pulled from settings so every cell is env-var overridable.
    The table includes entries for all known catalyst keys × all three tiers.
    Unknown catalysts fall back to the "other" rows.
    """
    # ADV tier slippage tolerances
    slip_liquid: float = getattr(settings, "order_limit_slippage_liquid_pct", 0.05)
    slip_normal: float = getattr(settings, "order_limit_slippage_normal_pct", 0.15)
    slip_thin:   float = getattr(settings, "order_limit_slippage_thin_pct",   0.30)

    def _policy(order_type: str, tier: str, catalyst: str) -> OrderTypePolicy:
        slippage = {ADV_TIER_LIQUID: slip_liquid,
                    ADV_TIER_NORMAL: slip_normal,
                    ADV_TIER_THIN:   slip_thin}[tier]
        if order_type == "market":
            slippage = 0.0
        tif = "ioc" if order_type == "limit_ioc" else "day"
        return _make_policy(order_type, tif, slippage, tier, catalyst)

    def _get(attr: str, default: str) -> str:
        return getattr(settings, attr, default)

    table: dict[tuple[str, str], OrderTypePolicy] = {}

    # ── earnings ──────────────────────────────────────────────────────────────
    table[(_CATALYST_EARNINGS, ADV_TIER_LIQUID)] = _policy(
        _get("order_policy_earnings_liquid_type", "market"), ADV_TIER_LIQUID, _CATALYST_EARNINGS)
    table[(_CATALYST_EARNINGS, ADV_TIER_NORMAL)] = _policy(
        _get("order_policy_earnings_normal_type", "limit_ioc"), ADV_TIER_NORMAL, _CATALYST_EARNINGS)
    table[(_CATALYST_EARNINGS, ADV_TIER_THIN)] = _policy(
        _get("order_policy_earnings_thin_type", "limit"), ADV_TIER_THIN, _CATALYST_EARNINGS)

    # ── analyst ───────────────────────────────────────────────────────────────
    table[(_CATALYST_ANALYST, ADV_TIER_LIQUID)] = _policy(
        _get("order_policy_analyst_liquid_type", "limit"), ADV_TIER_LIQUID, _CATALYST_ANALYST)
    table[(_CATALYST_ANALYST, ADV_TIER_NORMAL)] = _policy(
        _get("order_policy_analyst_normal_type", "limit"), ADV_TIER_NORMAL, _CATALYST_ANALYST)
    table[(_CATALYST_ANALYST, ADV_TIER_THIN)] = _policy(
        _get("order_policy_analyst_thin_type", "limit"), ADV_TIER_THIN, _CATALYST_ANALYST)

    # ── regulatory / FDA ──────────────────────────────────────────────────────
    table[(_CATALYST_REGULATORY, ADV_TIER_LIQUID)] = _policy(
        _get("order_policy_regulatory_liquid_type", "market"), ADV_TIER_LIQUID, _CATALYST_REGULATORY)
    table[(_CATALYST_REGULATORY, ADV_TIER_NORMAL)] = _policy(
        _get("order_policy_regulatory_normal_type", "limit_ioc"), ADV_TIER_NORMAL, _CATALYST_REGULATORY)
    table[(_CATALYST_REGULATORY, ADV_TIER_THIN)] = _policy(
        _get("order_policy_regulatory_thin_type", "limit_ioc"), ADV_TIER_THIN, _CATALYST_REGULATORY)

    # ── M&A ───────────────────────────────────────────────────────────────────
    table[(_CATALYST_MA, ADV_TIER_LIQUID)] = _policy(
        _get("order_policy_ma_liquid_type", "market"), ADV_TIER_LIQUID, _CATALYST_MA)
    table[(_CATALYST_MA, ADV_TIER_NORMAL)] = _policy(
        _get("order_policy_ma_normal_type", "limit_ioc"), ADV_TIER_NORMAL, _CATALYST_MA)
    table[(_CATALYST_MA, ADV_TIER_THIN)] = _policy(
        _get("order_policy_ma_thin_type", "limit"), ADV_TIER_THIN, _CATALYST_MA)

    # ── macro ─────────────────────────────────────────────────────────────────
    table[(_CATALYST_MACRO, ADV_TIER_LIQUID)] = _policy(
        _get("order_policy_macro_liquid_type", "market"), ADV_TIER_LIQUID, _CATALYST_MACRO)
    table[(_CATALYST_MACRO, ADV_TIER_NORMAL)] = _policy(
        _get("order_policy_macro_normal_type", "market"), ADV_TIER_NORMAL, _CATALYST_MACRO)
    table[(_CATALYST_MACRO, ADV_TIER_THIN)] = _policy(
        _get("order_policy_macro_thin_type", "limit"), ADV_TIER_THIN, _CATALYST_MACRO)

    # ── other / default ───────────────────────────────────────────────────────
    for tier in _ALL_TIERS:
        table[(_CATALYST_OTHER, tier)] = _policy(
            _get("order_policy_default_type", "limit"), tier, _CATALYST_OTHER)

    return table


def get_order_policy(
    table: dict[tuple[str, str], OrderTypePolicy],
    catalyst_type: str,
    adv: float | None,
    liquid_threshold: int = 5_000_000,
    thin_threshold: int = 500_000,
) -> OrderTypePolicy:
    """
    Look up the OrderTypePolicy for a given catalyst + ADV combination.

    Unknown catalyst types fall back to "other".
    Unknown ADV falls back to "normal" tier.

    Parameters
    ----------
    table            : pre-built policy table from build_order_policy_table()
    catalyst_type    : raw catalyst string from signal (case-insensitive)
    adv              : average daily volume, or None if unavailable
    liquid_threshold : ADV threshold for "liquid" tier
    thin_threshold   : ADV threshold below which ticker is "thin"

    Returns
    -------
    OrderTypePolicy — always returns a value (never None)
    """
    # Normalise catalyst key
    catalyst_key = _CATALYST_ALIASES.get(
        (catalyst_type or "").lower().strip(),
        _CATALYST_OTHER,
    )

    # Classify ADV tier
    tier = _adv_tier(adv, liquid_threshold, thin_threshold)

    # Look up; fall back to "other" if catalyst not in table
    policy = table.get((catalyst_key, tier)) or table.get((_CATALYST_OTHER, tier))

    if policy is None:
        # Should never happen — build_order_policy_table always fills "other" rows
        _log("warning", "order_policy.missing",
             catalyst_type=catalyst_type, tier=tier,
             note="falling back to market")
        policy = OrderTypePolicy(
            order_type="market",
            limit_slippage_pct=0.0,
            time_in_force="day",
            bracket_enabled=True,
            adv_tier=tier,
            catalyst_key=catalyst_key,
            policy_name=f"fallback_{catalyst_key}_{tier}",
        )

    _log("debug", "order_policy.selected",
         catalyst_type=catalyst_type,
         catalyst_key=catalyst_key,
         adv=adv,
         tier=tier,
         order_type=policy.order_type,
         policy_name=policy.policy_name)

    return policy
