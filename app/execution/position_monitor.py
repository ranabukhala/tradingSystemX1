"""
Position Monitor — watches all open positions every 30s (v1.11).

Actions:
  - Detects stop-loss breach → close position
  - Detects take-profit hit → close position
  - Time stop: exit if decay window expired and still not moving
  - Trailing stop: lock in profits as position moves in our favor
  - Sends P&L alerts on close

v1.11 additions:
  - _recover_from_db()   : on startup, re-register positions from trade table
  - _persist_close()     : write exit_price/close_reason/closed_at to trade row
  - start_trade_stream() : optional WebSocket fill stream (broker must support it)
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Any, TYPE_CHECKING

import httpx

from app.config import settings
from app.execution.base_broker import BaseBroker, OrderStatus, Position


if TYPE_CHECKING:
    import asyncpg


def _log(level: str, event: str, **kw) -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


POLL_INTERVAL = 30   # seconds (overridden by settings.position_monitor_poll_interval_seconds)


class PositionMonitor:

    def __init__(self, broker: BaseBroker, db=None) -> None:
        """
        Parameters
        ----------
        broker : BaseBroker implementation
        db     : asyncpg connection pool (optional — used for DB recovery and persist_close)
        """
        self._broker = broker
        self._db = db          # asyncpg pool; None = no DB persistence
        self._running = False
        self._http: httpx.AsyncClient | None = None
        self._telegram_token = settings.telegram_bot_token
        self._telegram_chat = settings.telegram_chat_id
        self._stream_task: asyncio.Task | None = None

        # In-memory tracking: ticker → metadata from execution engine
        self._position_meta: dict[str, dict] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._http = httpx.AsyncClient(timeout=10.0)
        _log("info", "position_monitor.started", broker=self._broker.name)

        # v1.11: recover open positions from DB before first poll
        await self._recover_from_db()

        # v1.11: start trade stream if broker supports it and config enables it
        stream_enabled = getattr(settings, "position_monitor_stream_enabled", True)
        if stream_enabled and hasattr(self._broker, "start_trade_stream"):
            self._stream_task = asyncio.create_task(self._run_trade_stream())

        poll_interval = getattr(
            settings, "position_monitor_poll_interval_seconds", POLL_INTERVAL)

        while self._running:
            try:
                await self._check_all()
            except Exception as e:
                _log("error", "position_monitor.check_error", error=str(e))
            await asyncio.sleep(poll_interval)

    def stop(self) -> None:
        self._running = False
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()

    # ── Registration ──────────────────────────────────────────────────────────

    def register_position(self, ticker: str, meta: dict) -> None:
        """
        Called by execution engine after a confirmed fill.
        Overwrites any existing meta for this ticker (handles duplicate fills).
        """
        self._position_meta[ticker] = {
            **meta,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "trailing_stop": meta.get("stop_loss"),
        }
        _log("info", "position_monitor.registered", ticker=ticker)

    # ── DB recovery (v1.11) ───────────────────────────────────────────────────

    async def _recover_from_db(self) -> None:
        """
        On startup, reload open positions from the trade table so positions
        survive engine restarts.  Requires a DB pool to be provided.
        """
        if not self._db:
            return
        try:
            rows = await self._db.fetch(
                """
                SELECT ticker, entry_price, take_profit_price, stop_loss_price,
                       decay_minutes, broker_order_id
                FROM   trade
                WHERE  closed_at IS NULL
                  AND  entry_price IS NOT NULL
                """
            )
            for row in rows:
                ticker = row["ticker"]
                if ticker not in self._position_meta:
                    meta = {
                        "entry_price":     row["entry_price"],
                        "take_profit":     row["take_profit_price"],
                        "stop_loss":       row["stop_loss_price"],
                        "decay_minutes":   row["decay_minutes"],
                        "broker_order_id": row["broker_order_id"],
                    }
                    self.register_position(ticker, meta)
                    _log("info", "position_monitor.recovered", ticker=ticker)
            _log("info", "position_monitor.db_recovery_done", count=len(rows))
        except Exception as e:
            _log("warning", "position_monitor.db_recovery_error", error=str(e))
            # Non-fatal — fall through to polling without pre-loaded state

    # ── Persist close (v1.11) ─────────────────────────────────────────────────

    async def _persist_close(
        self,
        ticker: str,
        reason: str,
        exit_price: float,
        realized_pnl: float,
    ) -> None:
        """
        Write exit data to the open trade row in the database.
        UPDATE trade SET exit_price, close_reason, realized_pnl, closed_at
        WHERE ticker = X AND closed_at IS NULL
        """
        if not self._db:
            return
        try:
            await self._db.execute(
                """
                UPDATE trade
                SET    exit_price   = $1,
                       close_reason = $2,
                       realized_pnl = $3,
                       closed_at    = NOW()
                WHERE  ticker     = $4
                  AND  closed_at  IS NULL
                """,
                exit_price,
                reason,
                realized_pnl,
                ticker,
            )
            _log("info", "position_monitor.close_persisted",
                 ticker=ticker, reason=reason,
                 exit_price=exit_price, realized_pnl=round(realized_pnl, 2))
        except Exception as e:
            _log("warning", "position_monitor.persist_close_error",
                 ticker=ticker, error=str(e))

    # ── Trade stream (v1.11) ──────────────────────────────────────────────────

    async def _run_trade_stream(self) -> None:
        """
        Start the broker's trade stream as a background task.
        Falls back silently to polling-only if streaming is unsupported.
        """
        try:
            _log("info", "position_monitor.stream_starting",
                 broker=self._broker.name)
            await self._broker.start_trade_stream(self._on_trade_update)
        except NotImplementedError:
            _log("info", "position_monitor.stream_not_supported",
                 broker=self._broker.name,
                 note="falling back to polling only")
        except asyncio.CancelledError:
            _log("info", "position_monitor.stream_cancelled")
        except Exception as e:
            _log("warning", "position_monitor.stream_error",
                 broker=self._broker.name, error=str(e),
                 note="falling back to polling only")

    async def _on_trade_update(self, event: dict) -> None:
        """
        Callback invoked by the trade stream for fill / partial / cancel events.
        """
        ev_type = event.get("event", "")
        ticker  = event.get("symbol", "")
        broker_order_id = event.get("broker_order_id", "")

        if ev_type in ("fill", "partial_fill"):
            _log("info", "position_monitor.stream_fill",
                 ticker=ticker,
                 broker_order_id=broker_order_id,
                 qty_filled=event.get("qty_filled"),
                 avg_price=event.get("avg_fill_price"))

        elif ev_type == "canceled":
            # Remove meta so the cancelled order doesn't trigger spurious stops
            removed = self._position_meta.pop(ticker, None)
            if removed:
                _log("info", "position_monitor.stream_cancelled_removed",
                     ticker=ticker, broker_order_id=broker_order_id)

    # ── Polling logic ─────────────────────────────────────────────────────────

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
                if entered.tzinfo is None:
                    entered = entered.replace(tzinfo=timezone.utc)
                elapsed = (datetime.now(timezone.utc) - entered).total_seconds() / 60
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
        exit_price = trigger_price

        _log("info", "position_monitor.closed",
             ticker=ticker,
             reason=reason,
             pnl=round(pnl, 2),
             pnl_pct=round(pnl_pct, 2),
             broker_order_id=result.broker_order_id)

        # v1.11: persist close to DB
        await self._persist_close(ticker, reason, exit_price, pnl)

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
