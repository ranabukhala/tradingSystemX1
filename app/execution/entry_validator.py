"""
Entry Validator — pre-entry safety gate (v1.11).

Runs after quote fetch and halt check, before risk evaluation.
Three rules (all configurable, all fail-open):

  Rule 1 — Halt check: block if the exchange has halted the ticker.
  Rule 2 — Stale quote: block if the quote is older than max_quote_age_seconds.
  Rule 3 — Price drift: block if price moved more than max_price_drift_pct
            from the signal reference price (skip if no reference price).

All checks default to safe/blocking behaviour but degrade gracefully:
  - halt_check_enabled=False skips Rule 1
  - signal_price=None skips Rule 3
  - Any unexpected exception returns approved=False (conservative fail)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.config import Settings


def _log(level: str, event: str, **kw) -> None:
    import json
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


@dataclass
class EntryCheckResult:
    """Result of the EntryValidator pre-entry gate."""
    approved: bool
    reason: str = ""                   # e.g. "trading_halted" | "stale_quote_12.3s" | "price_drift_2.10pct" | "ok"
    quote_age_seconds: float = 0.0     # measured age of the quote at check time
    price_drift_pct: float = 0.0       # absolute % drift from signal ref price (0 if skipped)
    halted: bool = False               # True if halt was the blocking reason


class EntryValidator:
    """
    Stateless pre-entry gate.  Instantiate once per engine, call check() per signal.

    All configuration is read from settings at construction time so the engine
    can inject test settings without patching env vars.
    """

    def __init__(self, settings: "Settings") -> None:
        self._max_age: float = getattr(settings, "entry_max_quote_age_seconds", 10.0)
        self._max_drift: float = getattr(settings, "entry_max_price_drift_pct", 1.5)
        self._halt_enabled: bool = getattr(settings, "entry_halt_check_enabled", True)

    async def check(
        self,
        ticker: str,
        signal_price: float | None,    # price at signal creation — None → skip drift check
        current_price: float,
        quote_timestamp: datetime,     # UTC datetime captured when quote was fetched
        halted: bool,
    ) -> EntryCheckResult:
        """
        Run all three entry checks.  Returns on first failure.

        Parameters
        ----------
        ticker          : symbol (for logging only)
        signal_price    : reference price from signal generation; None = skip drift
        current_price   : price returned by the broker quote at entry time
        quote_timestamp : UTC timestamp of the broker quote
        halted          : True if broker/exchange reports the ticker is halted

        Returns
        -------
        EntryCheckResult with approved=True if all checks pass.
        """
        try:
            # ── Rule 1: Halt check ────────────────────────────────────────────
            if self._halt_enabled and halted:
                _log("warning", "entry_validator.blocked",
                     ticker=ticker, reason="trading_halted")
                return EntryCheckResult(
                    approved=False,
                    reason="trading_halted",
                    halted=True,
                )

            # ── Rule 2: Stale quote ───────────────────────────────────────────
            now_utc = datetime.now(timezone.utc)
            # Ensure quote_timestamp is offset-aware
            if quote_timestamp.tzinfo is None:
                quote_timestamp = quote_timestamp.replace(tzinfo=timezone.utc)
            age = (now_utc - quote_timestamp).total_seconds()

            if age > self._max_age:
                _log("warning", "entry_validator.blocked",
                     ticker=ticker, reason="stale_quote",
                     quote_age_seconds=round(age, 1),
                     max_age_seconds=self._max_age)
                return EntryCheckResult(
                    approved=False,
                    reason=f"stale_quote_{age:.1f}s",
                    quote_age_seconds=age,
                )

            # ── Rule 3: Price drift ───────────────────────────────────────────
            drift_pct = 0.0
            if signal_price is not None and signal_price > 0 and current_price > 0:
                drift_pct = abs(current_price - signal_price) / signal_price * 100.0
                if drift_pct > self._max_drift:
                    _log("warning", "entry_validator.blocked",
                         ticker=ticker, reason="price_drift",
                         drift_pct=round(drift_pct, 2),
                         signal_price=signal_price,
                         current_price=current_price,
                         max_drift_pct=self._max_drift)
                    return EntryCheckResult(
                        approved=False,
                        reason=f"price_drift_{drift_pct:.2f}pct",
                        quote_age_seconds=age,
                        price_drift_pct=drift_pct,
                    )

            # ── All checks passed ─────────────────────────────────────────────
            _log("debug", "entry_validator.approved",
                 ticker=ticker,
                 quote_age_seconds=round(age, 2),
                 drift_pct=round(drift_pct, 2))
            return EntryCheckResult(
                approved=True,
                reason="ok",
                quote_age_seconds=age,
                price_drift_pct=drift_pct,
            )

        except Exception as exc:  # pragma: no cover
            _log("error", "entry_validator.error", ticker=ticker, error=str(exc))
            # Fail conservatively
            return EntryCheckResult(approved=False, reason=f"validator_error: {exc}")
