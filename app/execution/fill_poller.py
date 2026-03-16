"""
Fill Poller — polls broker for fill status after order submission (v1.11).

Used for LIMIT and LIMIT_IOC orders where fill is not guaranteed to be
immediate. MARKET orders skip the poller (Alpaca market fills are
near-instant and the initial OrderResult already contains fill data).

Behaviour
---------
  1. Poll `broker.get_order(order_id)` every `poll_interval_seconds`.
  2. On FILLED  → return immediately.
  3. On PARTIAL fill ≥ min_partial_fill_pct → cancel remainder, return.
  4. On PARTIAL fill <  min_partial_fill_pct → continue polling.
  5. On CANCELLED / REJECTED / EXPIRED → return immediately.
  6. On timeout (max_polls exhausted)  → return last known result.

All configuration has safe defaults; `FillPollConfig.from_env()` reads
from environment variables.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.execution.base_broker import BaseBroker, OrderRequest, OrderResult

from app.execution.base_broker import OrderStatus


def _log(level: str, event: str, **kw) -> None:
    import json
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


@dataclass
class FillPollConfig:
    poll_interval_seconds: float = 2.0   # Time between polls
    max_polls: int = 15                  # 15 × 2 s = 30 s total
    min_partial_fill_pct: float = 0.80   # Accept ≥ 80% fill as "good enough"

    @classmethod
    def from_env(cls) -> "FillPollConfig":
        return cls(
            poll_interval_seconds=float(
                os.environ.get("FILL_POLL_INTERVAL_SECONDS", "2.0")),
            max_polls=int(
                os.environ.get("FILL_POLL_MAX_ATTEMPTS", "15")),
            min_partial_fill_pct=float(
                os.environ.get("FILL_MIN_PARTIAL_PCT", "0.80")),
        )


class FillPoller:
    """
    Polls broker until a LIMIT order is filled, partially filled above
    threshold, or the timeout is reached.

    Usage
    -----
        poller = FillPoller(broker, FillPollConfig.from_env())

        # LIMIT order — poll for fill
        updated = await poller.wait_for_fill(result, request)

        # MARKET order — skip (pass through unchanged)
        if request.order_type == OrderType.MARKET:
            updated = result
        else:
            updated = await poller.wait_for_fill(result, request)
    """

    def __init__(self, broker: "BaseBroker", config: FillPollConfig | None = None) -> None:
        self._broker = broker
        self._config = config or FillPollConfig.from_env()

    async def wait_for_fill(
        self,
        result: "OrderResult",
        request: "OrderRequest",
    ) -> "OrderResult":
        """
        Poll until FILLED, acceptable PARTIAL, CANCELLED/REJECTED/EXPIRED,
        or timeout.

        Parameters
        ----------
        result  : initial OrderResult from submit_order()
        request : original OrderRequest (used for qty_requested and ticker logging)

        Returns
        -------
        Updated OrderResult reflecting current fill state.
        """
        cfg = self._config
        broker_order_id = result.broker_order_id
        ticker = result.ticker or request.ticker
        qty_req = request.qty

        # If already in a terminal state, return immediately
        if result.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        ):
            return result

        last_result = result

        for poll_num in range(1, cfg.max_polls + 1):
            await asyncio.sleep(cfg.poll_interval_seconds)

            try:
                updated = await self._broker.get_order(broker_order_id)
            except Exception as exc:
                _log("warning", "fill_poller.get_order_error",
                     ticker=ticker, poll=poll_num, error=str(exc))
                continue

            last_result = updated

            # ── Terminal: filled ──────────────────────────────────────────
            if updated.status == OrderStatus.FILLED:
                _log("info", "fill_poller.filled",
                     ticker=ticker,
                     broker_order_id=broker_order_id,
                     qty_filled=updated.qty_filled,
                     avg_price=updated.avg_fill_price,
                     polls=poll_num)
                return updated

            # ── Partial fill ──────────────────────────────────────────────
            if updated.status == OrderStatus.PARTIAL and qty_req > 0:
                fill_ratio = updated.qty_filled / qty_req
                if fill_ratio >= cfg.min_partial_fill_pct:
                    # Accept partial — cancel remainder
                    _log("info", "fill_poller.partial_accepted",
                         ticker=ticker,
                         broker_order_id=broker_order_id,
                         fill_ratio=round(fill_ratio, 3),
                         qty_filled=updated.qty_filled,
                         qty_requested=qty_req,
                         polls=poll_num)
                    try:
                        await self._broker.cancel_order(broker_order_id)
                    except Exception as exc:
                        _log("warning", "fill_poller.cancel_remainder_error",
                             ticker=ticker, error=str(exc))
                    return updated
                else:
                    _log("debug", "fill_poller.partial_below_threshold",
                         ticker=ticker,
                         fill_ratio=round(fill_ratio, 3),
                         min_pct=cfg.min_partial_fill_pct,
                         poll=poll_num)

            # ── Terminal: cancelled / rejected / expired ───────────────────
            if updated.status in (
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
            ):
                _log("info", "fill_poller.terminal",
                     ticker=ticker,
                     broker_order_id=broker_order_id,
                     status=updated.status.value,
                     polls=poll_num)
                return updated

        # ── Timeout ───────────────────────────────────────────────────────
        _log("warning", "fill_poller.timeout",
             ticker=ticker,
             broker_order_id=broker_order_id,
             max_polls=cfg.max_polls,
             last_status=last_result.status.value,
             last_fill=last_result.qty_filled)
        return last_result
