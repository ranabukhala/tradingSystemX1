"""
Risk Manager — guards every order before it reaches the broker.

Rules:
  1. Max position size (% of account equity)
  2. Max daily loss — halt trading if exceeded
  3. Max open positions
  4. Sector concentration limit
  5. No duplicate positions (already long = no new long)
  6. Market hours check (configurable — allow premarket or not)
  7. Min conviction threshold
  8. Conviction-scaled position sizing

All rules are configurable via env vars.
Every blocked trade is logged with reason.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.execution.base_broker import AccountInfo, BaseBroker, OrderRequest, OrderSide, OrderType, Position, TimeInForce


def _log(level: str, event: str, **kw) -> None:
    import json
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


@dataclass
class RiskConfig:
    max_position_pct: float = 0.02        # 2% of account per trade
    max_position_usd: float = 5000.0      # Hard cap per position
    max_daily_loss_pct: float = 0.05      # 5% — halt if exceeded
    max_open_positions: int = 10
    max_sector_concentration_pct: float = 0.25  # 25% of portfolio in one sector
    min_conviction: float = 0.65
    min_price: float = 5.00          # Block penny stocks under $5
    allow_premarket: bool = True
    allow_afterhours: bool = False
    max_position_loss_pct: float = 0.05   # Per-position stop: 5%
    default_take_profit_pct: float = 0.10 # Default TP: 10%
    default_stop_loss_pct: float = 0.04   # Default SL: 4%
    use_bracket_orders: bool = True       # Attach TP/SL to every order

    @classmethod
    def from_env(cls) -> "RiskConfig":
        return cls(
            max_position_pct=float(os.environ.get("RISK_MAX_POSITION_PCT", "0.02")),
            max_position_usd=float(os.environ.get("RISK_MAX_POSITION_USD", "5000")),
            max_daily_loss_pct=float(os.environ.get("RISK_MAX_DAILY_LOSS_PCT", "0.05")),
            max_open_positions=int(os.environ.get("RISK_MAX_OPEN_POSITIONS", "10")),
            min_conviction=float(os.environ.get("RISK_MIN_CONVICTION", "0.65")),
            min_price=float(os.environ.get("RISK_MIN_PRICE", "5.00")),
            allow_premarket=os.environ.get("RISK_ALLOW_PREMARKET", "true").lower() == "true",
            allow_afterhours=os.environ.get("RISK_ALLOW_AFTERHOURS", "false").lower() == "true",
            default_take_profit_pct=float(os.environ.get("RISK_TAKE_PROFIT_PCT", "0.10")),
            default_stop_loss_pct=float(os.environ.get("RISK_STOP_LOSS_PCT", "0.04")),
            use_bracket_orders=os.environ.get("RISK_USE_BRACKET_ORDERS", "true").lower() == "true",
        )


@dataclass
class RiskDecision:
    approved: bool
    reason: str = ""
    qty: float = 0.0
    entry_price: float = 0.0
    take_profit: float | None = None
    stop_loss: float | None = None
    position_value: float = 0.0
    risk_reward: float = 0.0
    # v1.11 ATR stop fields
    stop_type: str = "fixed_pct"          # "atr" | "fixed_pct"
    atr_14: float | None = None           # ATR(14) used for stop calculation


class RiskManager:

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig.from_env()

    def compute_atr_stops(
        self,
        price: float,
        direction: str,
        atr_14: float | None,
        sl_mult: float | None = None,
        tp_mult: float | None = None,
    ) -> tuple[float | None, float | None]:
        """
        Compute (take_profit, stop_loss) using ATR-based stops.

        Returns (take_profit, stop_loss) rounded to 2 decimal places.
        Falls back to fixed-% calculation if atr_14 is None or zero.

        sl_mult defaults to config.risk_atr_sl_multiplier (env: RISK_ATR_SL_MULTIPLIER)
        tp_mult defaults to config.risk_atr_tp_multiplier (env: RISK_ATR_TP_MULTIPLIER)
        """
        _sl_mult = sl_mult if sl_mult is not None else float(
            os.environ.get("RISK_ATR_SL_MULTIPLIER", "2.0"))
        _tp_mult = tp_mult if tp_mult is not None else float(
            os.environ.get("RISK_ATR_TP_MULTIPLIER", "4.0"))

        if not atr_14 or atr_14 <= 0 or price <= 0:
            # Fall back to fixed percentage
            sl_pct = self.config.default_stop_loss_pct
            tp_pct = self.config.default_take_profit_pct
            if direction == "long":
                return (round(price * (1 + tp_pct), 2),
                        round(price * (1 - sl_pct), 2))
            else:
                return (round(price * (1 - tp_pct), 2),
                        round(price * (1 + sl_pct), 2))

        if direction == "long":
            take_profit = round(price + atr_14 * _tp_mult, 2)
            stop_loss   = round(price - atr_14 * _sl_mult, 2)
        else:
            take_profit = round(price - atr_14 * _tp_mult, 2)
            stop_loss   = round(price + atr_14 * _sl_mult, 2)

        return take_profit, stop_loss

    async def evaluate(
        self,
        ticker: str,
        direction: str,          # "long" | "short"
        conviction: float,
        current_price: float,
        account: AccountInfo,
        open_positions: list[Position],
        session_context: str = "intraday",
        decay_minutes: int | None = None,
        atr_14: float | None = None,     # v1.11: ATR(14) for volatility-aware stops
    ) -> RiskDecision:
        """
        Full risk evaluation. Returns RiskDecision with approved=True/False.
        """

        # ── 1. Conviction filter ──────────────────────────────────────────────
        if conviction < self.config.min_conviction:
            return RiskDecision(
                approved=False,
                reason=f"conviction {conviction:.2f} < min {self.config.min_conviction}"
            )

        # Block penny stocks
        if current_price < self.config.min_price:
            return RiskDecision(
                approved=False,
                reason=f"price ${current_price:.2f} below min ${self.config.min_price:.2f}"
            )

        # ── 2. Session filter ─────────────────────────────────────────────────
        if session_context == "premarket" and not self.config.allow_premarket:
            return RiskDecision(approved=False, reason="premarket trading disabled")
        if session_context == "afterhours" and not self.config.allow_afterhours:
            return RiskDecision(approved=False, reason="afterhours trading disabled")

        # ── 3. Daily loss limit ───────────────────────────────────────────────
        if account.equity > 0:
            daily_loss_pct = abs(min(account.daily_pnl, 0)) / account.equity
            if daily_loss_pct >= self.config.max_daily_loss_pct:
                return RiskDecision(
                    approved=False,
                    reason=f"daily loss limit reached: {daily_loss_pct*100:.1f}% >= {self.config.max_daily_loss_pct*100:.1f}%"
                )

        # ── 4. Max open positions ─────────────────────────────────────────────
        if len(open_positions) >= self.config.max_open_positions:
            return RiskDecision(
                approved=False,
                reason=f"max open positions reached: {len(open_positions)}/{self.config.max_open_positions}"
            )

        # ── 5. Duplicate position ─────────────────────────────────────────────
        existing = next((p for p in open_positions if p.ticker == ticker), None)
        if existing:
            existing_dir = "long" if existing.qty > 0 else "short"
            if existing_dir == direction:
                return RiskDecision(
                    approved=False,
                    reason=f"already {direction} {ticker} ({existing.qty} shares)"
                )

        # ── 6. Price sanity ───────────────────────────────────────────────────
        if current_price <= 0:
            return RiskDecision(approved=False, reason="invalid price: 0")

        # ── 7. Position sizing ────────────────────────────────────────────────
        # Base size = max_position_pct of equity
        # Scale by conviction: full size at conviction=1.0, half at conviction=0.5
        base_value = account.equity * self.config.max_position_pct
        conviction_scale = 0.5 + (conviction * 0.5)   # Range: 0.75–1.0 for 0.5–1.0 conviction
        position_value = min(
            base_value * conviction_scale,
            self.config.max_position_usd,
            account.buying_power * 0.95,   # Never use more than 95% of buying power
        )

        if position_value < current_price:
            return RiskDecision(
                approved=False,
                reason=f"position value ${position_value:.0f} < price ${current_price:.2f}"
            )

        qty = position_value / current_price
        qty = max(1, round(qty))   # Minimum 1 share, whole shares only

        # ── 8. TP/SL calculation ──────────────────────────────────────────────
        take_profit = None
        stop_loss = None
        stop_type = "fixed_pct"

        if self.config.use_bracket_orders:
            if atr_14 and atr_14 > 0:
                # v1.11: ATR-based stops
                take_profit, stop_loss = self.compute_atr_stops(
                    current_price, direction, atr_14)
                stop_type = "atr"
            else:
                # Legacy fixed-percentage stops
                tp_pct = self.config.default_take_profit_pct
                sl_pct = self.config.default_stop_loss_pct

                # Scale TP by conviction and decay: high conviction = wider TP
                if conviction >= 0.8:
                    tp_pct *= 1.5
                elif conviction >= 0.6:
                    tp_pct *= 1.2

                # Tighter stop for premarket (more volatile)
                if session_context == "premarket":
                    sl_pct *= 0.8

                if direction == "long":
                    take_profit = round(current_price * (1 + tp_pct), 2)
                    stop_loss   = round(current_price * (1 - sl_pct), 2)
                else:
                    take_profit = round(current_price * (1 - tp_pct), 2)
                    stop_loss   = round(current_price * (1 + sl_pct), 2)

        risk_reward = (
            abs(take_profit - current_price) / abs(current_price - stop_loss)
            if take_profit and stop_loss and current_price != stop_loss
            else 0.0
        )

        _log("info", "risk.approved",
             ticker=ticker,
             direction=direction,
             conviction=conviction,
             qty=qty,
             entry=current_price,
             tp=take_profit,
             sl=stop_loss,
             stop_type=stop_type,
             atr_14=atr_14,
             position_value=round(position_value, 2),
             rr=round(risk_reward, 2))

        return RiskDecision(
            approved=True,
            reason="approved",
            qty=qty,
            entry_price=current_price,
            take_profit=take_profit,
            stop_loss=stop_loss,
            position_value=round(qty * current_price, 2),
            risk_reward=round(risk_reward, 2),
            stop_type=stop_type,
            atr_14=atr_14,
        )

    def build_order(
        self,
        ticker: str,
        direction: str,
        decision: RiskDecision,
        signal_id: str = "",
        order_type_str: str = "market",   # v1.11: from OrderTypePolicy
        limit_price: float | None = None, # v1.11: pre-computed limit price
        time_in_force: str = "day",       # v1.11: from OrderTypePolicy
    ) -> OrderRequest:
        """
        Build OrderRequest from approved RiskDecision.

        v1.11: order_type_str / limit_price / time_in_force come from
        OrderTypePolicy and are passed in by the execution engine.
        """
        side = OrderSide.BUY if direction == "long" else OrderSide.SELL

        # Map policy order type string to OrderType enum
        _ot_map = {
            "market":    OrderType.MARKET,
            "limit":     OrderType.LIMIT,
            "limit_ioc": OrderType.LIMIT,  # IOC is a time_in_force, not a separate type
        }
        order_type = _ot_map.get(order_type_str, OrderType.MARKET)

        # Map time_in_force string to enum
        _tif_map = {
            "day": TimeInForce.DAY,
            "gtc": TimeInForce.GTC,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK,
        }
        tif = _tif_map.get(time_in_force, TimeInForce.DAY)
        if order_type_str == "limit_ioc":
            tif = TimeInForce.IOC

        return OrderRequest(
            ticker=ticker,
            side=side,
            qty=decision.qty,
            order_type=order_type,
            limit_price=limit_price,
            time_in_force=tif,
            take_profit_price=decision.take_profit if self.config.use_bracket_orders else None,
            stop_loss_price=decision.stop_loss if self.config.use_bracket_orders else None,
            signal_id=signal_id,
        )
