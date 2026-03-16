"""
Portfolio Risk Manager — machine-readable block reason codes (v1.10).

All codes are prefixed with "RISK:" so downstream consumers (Telegram alerts,
dashboard queries, analytics) can machine-parse block reasons without fragile
string matching.

Usage:
    from app.risk.risk_codes import KILL_SWITCH_MANUAL, MAX_POSITIONS

    record["block_code"] = MAX_POSITIONS
    _log("info", "portfolio_risk.blocked", code=MAX_POSITIONS, ...)
"""
from __future__ import annotations

# ── Kill switches ────────────────────────────────────────────────────────────
KILL_SWITCH_MANUAL   = "RISK:KILL_SWITCH_MANUAL"    # Operator-set manual halt
AUTO_HALT_DRAWDOWN   = "RISK:AUTO_HALT_DRAWDOWN"    # Equity below HWM × (1 − threshold)
AUTO_HALT_LOSS_STREAK = "RISK:AUTO_HALT_LOSS_STREAK" # N consecutive losses
AUTO_HALT_ORDER_RATE = "RISK:AUTO_HALT_ORDER_RATE"  # Hourly order rate exceeded

# ── Portfolio-level constraints ───────────────────────────────────────────────
DAILY_LOSS_HALT           = "RISK:DAILY_LOSS_HALT"           # Daily P&L loss cap hit
CONSECUTIVE_LOSS_COOLDOWN = "RISK:CONSECUTIVE_LOSS_COOLDOWN" # N losses, in cooldown window
ORDER_RATE_LIMIT          = "RISK:ORDER_RATE_LIMIT"          # Hourly order rate
DAILY_ORDER_CAP           = "RISK:DAILY_ORDER_CAP"           # Daily order count cap

# ── Per-signal / portfolio composition constraints ────────────────────────────
MAX_POSITIONS         = "RISK:MAX_POSITIONS"         # Too many open positions
TICKER_EXPOSURE       = "RISK:TICKER_EXPOSURE"       # Single-ticker USD exposure limit
SECTOR_CONCENTRATION  = "RISK:SECTOR_CONCENTRATION"  # Sector % of portfolio too high
CATALYST_CONCENTRATION = "RISK:CATALYST_CONCENTRATION" # Too many same-catalyst positions
CORRELATION_RISK      = "RISK:CORRELATION_RISK"      # Too many same (sector+direction)
EVENT_ALREADY_TRADED  = "RISK:EVENT_ALREADY_TRADED"  # News cluster already generated a trade

# ── Convenience: set of all codes for validation ─────────────────────────────
ALL_CODES: frozenset[str] = frozenset({
    KILL_SWITCH_MANUAL,
    AUTO_HALT_DRAWDOWN,
    AUTO_HALT_LOSS_STREAK,
    AUTO_HALT_ORDER_RATE,
    DAILY_LOSS_HALT,
    CONSECUTIVE_LOSS_COOLDOWN,
    ORDER_RATE_LIMIT,
    DAILY_ORDER_CAP,
    MAX_POSITIONS,
    TICKER_EXPOSURE,
    SECTOR_CONCENTRATION,
    CATALYST_CONCENTRATION,
    CORRELATION_RISK,
    EVENT_ALREADY_TRADED,
})
