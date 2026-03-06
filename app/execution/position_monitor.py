"""
Position Monitor — watches all open positions every 30s.

Actions:
  - Detects stop-loss breach → close position
  - Detects take-profit hit → close position
  - Time stop: exit if decay window expired and still not moving
  - Trailing stop: lock in profits as position moves in our favor
  - Sends P&L alerts on close
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

from app.execution.base_broker import BaseBroker, OrderStatus, Position


def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


POLL_INTERVAL = 30   # seconds


class PositionMonitor:

    def __init__(self, broker: BaseBroker) -> None:
        self._broker = broker
        self._running = False
        self._http: httpx.AsyncClient | None = None
        self._telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._telegram_chat = os.environ.get("TELEGRAM_CHAT_ID", "")

        # In-memory tracking: ticker → metadata from execution engine
        self._position_meta: dict[str, dict] = {}

    async def start(self) -> None:
        self._running = True
        self._http = httpx.AsyncClient(timeout=10.0)
        _log("info", "position_monitor.started", broker=self._broker.name)

        while self._running:
            try:
                await self._check_all()
            except Exception as e:
                _log("error", "position_monitor.check_error", error=str(e))
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self) -> None:
        self._running = False

    def register_position(self, ticker: str, meta: dict) -> None:
        """Called by execution engine after order fills."""
        self._position_meta[ticker] = {
            **meta,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "trailing_stop": meta.get("stop_loss"),
        }
        _log("info", "position_monitor.registered", ticker=ticker)

    async def _check_all(self) -> None:
        positions = await self._broker.get_positions()
        if not positions:
            return

        _log("debug", "position_monitor.checking", count=len(positions))

        for position in positions:
            await self._check_position(position)

    async def _check_position(self, position: Position) -> None:
        ticker = position.ticker
        meta = self._position_meta.get(ticker, {})
        price = position.current_price
        direction = position.side  # "long" | "short"

        if price <= 0:
            return

        take_profit = meta.get("take_profit")
        stop_loss = meta.get("trailing_stop") or meta.get("stop_loss")
        decay_minutes = meta.get("decay_minutes")
        registered_at = meta.get("registered_at")

        # ── Stop loss check ───────────────────────────────────────────────────
        if stop_loss:
            triggered = (
                (direction == "long"  and price <= stop_loss) or
                (direction == "short" and price >= stop_loss)
            )
            if triggered:
                _log("warning", "position_monitor.stop_loss_hit",
                     ticker=ticker, price=price, stop=stop_loss,
                     pnl_pct=position.unrealized_pnl_pct)
                await self._close_and_alert(position, "stop_loss", stop_loss)
                return

        # ── Take profit check ─────────────────────────────────────────────────
        if take_profit:
            triggered = (
                (direction == "long"  and price >= take_profit) or
                (direction == "short" and price <= take_profit)
            )
            if triggered:
                _log("info", "position_monitor.take_profit_hit",
                     ticker=ticker, price=price, target=take_profit,
                     pnl_pct=position.unrealized_pnl_pct)
                await self._close_and_alert(position, "take_profit", take_profit)
                return

        # ── Trailing stop ─────────────────────────────────────────────────────
        if stop_loss and meta:
            trail_pct = 0.03   # Trail at 3% below high water mark
            current_trail = meta.get("trailing_stop", stop_loss)

            if direction == "long":
                new_trail = round(price * (1 - trail_pct), 2)
                if new_trail > current_trail:
                    meta["trailing_stop"] = new_trail
                    _log("debug", "position_monitor.trailing_stop_updated",
                         ticker=ticker, old=current_trail, new=new_trail)

            elif direction == "short":
                new_trail = round(price * (1 + trail_pct), 2)
                if new_trail < current_trail:
                    meta["trailing_stop"] = new_trail
                    _log("debug", "position_monitor.trailing_stop_updated",
                         ticker=ticker, old=current_trail, new=new_trail)

        # ── Time stop ─────────────────────────────────────────────────────────
        if decay_minutes and registered_at:
            try:
                entered = datetime.fromisoformat(registered_at)
                elapsed = (datetime.now(timezone.utc) - entered).seconds / 60
                # Exit if 2x decay window elapsed and position not profitable
                if elapsed > (decay_minutes * 2) and position.unrealized_pnl <= 0:
                    _log("info", "position_monitor.time_stop",
                         ticker=ticker,
                         elapsed_min=int(elapsed),
                         decay_min=decay_minutes,
                         pnl=position.unrealized_pnl)
                    await self._close_and_alert(position, "time_stop", price)
                    return
            except Exception:
                pass

        # ── Log current status ────────────────────────────────────────────────
        _log("debug", "position_monitor.position_ok",
             ticker=ticker,
             price=price,
             pnl=round(position.unrealized_pnl, 2),
             pnl_pct=round(position.unrealized_pnl_pct, 2),
             stop=stop_loss,
             target=take_profit)

    async def _close_and_alert(
        self, position: Position, reason: str, trigger_price: float
    ) -> None:
        ticker = position.ticker

        # Close via broker
        result = await self._broker.close_position(ticker)

        # Remove from tracking
        self._position_meta.pop(ticker, None)

        pnl = position.unrealized_pnl
        pnl_pct = position.unrealized_pnl_pct

        _log("info", "position_monitor.closed",
             ticker=ticker,
             reason=reason,
             pnl=round(pnl, 2),
             pnl_pct=round(pnl_pct, 2),
             broker_order_id=result.broker_order_id)

        # Telegram alert
        await self._send_close_alert(position, reason, pnl, pnl_pct)

    async def _send_close_alert(
        self,
        position: Position,
        reason: str,
        pnl: float,
        pnl_pct: float,
    ) -> None:
        if not self._telegram_token or not self._telegram_chat:
            return

        reason_labels = {
            "stop_loss":   "🛑 Stop Loss",
            "take_profit": "🎯 Take Profit",
            "time_stop":   "⏰ Time Stop",
            "manual":      "✋ Manual Close",
        }

        pnl_emoji = "✅" if pnl >= 0 else "❌"
        label = reason_labels.get(reason, reason)

        msg = (
            f"{pnl_emoji} *Position Closed — {position.ticker}*\n"
            f"\n"
            f"Reason:  {label}\n"
            f"P&L:     ${pnl:+.2f} ({pnl_pct:+.1f}%)\n"
            f"Qty:     {abs(int(position.qty))} shares\n"
            f"Side:    {position.side.upper()}\n"
            f"Entry:   ${position.avg_entry:.2f}\n"
            f"Exit:    ~${position.current_price:.2f}\n"
        )

        try:
            await self._http.post(
                f"https://api.telegram.org/bot{self._telegram_token}/sendMessage",
                json={
                    "chat_id": self._telegram_chat,
                    "text": msg,
                    "parse_mode": "Markdown",
                },
            )
        except Exception as e:
            _log("warning", "position_monitor.telegram_error", error=str(e))
