"""
Execution Engine — Stage 7 of the pipeline.

Consumes: signals.actionable
Actions:  Places orders via broker (Alpaca paper/live or IBKR demo/live)
Emits:    trades.executed (for position monitor and Telegram)

Flow:
  1. Receive signal from signals.actionable
  2. Get current quote for ticker
  3. Run risk manager evaluation
  4. If approved → build order → submit to broker
  5. Save trade to Postgres
  6. Emit to trades.executed
  7. Send Telegram alert
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import redis.asyncio as aioredis

from app.execution.base_broker import BaseBroker, OrderResult, OrderStatus
from app.execution.risk_manager import RiskManager, RiskConfig
from app.pipeline.base_consumer import BaseConsumer, _log
from app.pipeline.event_cluster import EventClusterStore
from app.signals.signal_aggregator import TradingSignal

_ENABLE_ORDER_EVENT_GATE = os.environ.get("ENABLE_ORDER_EVENT_GATE", "true").lower() == "true"


def _get_broker() -> BaseBroker:
    """Factory — returns configured broker based on BROKER env var."""
    broker_name = os.environ.get("BROKER", "alpaca_paper").lower()

    if broker_name in ("alpaca_paper", "alpaca"):
        from app.execution.alpaca_broker import AlpacaBroker
        return AlpacaBroker(
            api_key=os.environ.get("ALPACA_API_KEY", ""),
            secret_key=os.environ.get("ALPACA_SECRET_KEY", ""),
            paper=True,
        )
    elif broker_name == "alpaca_live":
        from app.execution.alpaca_broker import AlpacaBroker
        return AlpacaBroker(
            api_key=os.environ.get("ALPACA_API_KEY", ""),
            secret_key=os.environ.get("ALPACA_SECRET_KEY", ""),
            paper=False,
        )
    elif broker_name in ("ibkr_paper", "ibkr", "ibkr_demo"):
        from app.execution.ibkr_broker import IBKRBroker
        return IBKRBroker(
            host=os.environ.get("IBKR_HOST", "host.docker.internal"),
            port=int(os.environ.get("IBKR_PORT", "4002")),
            client_id=int(os.environ.get("IBKR_CLIENT_ID", "1")),
            paper=True,
        )
    elif broker_name == "ibkr_live":
        from app.execution.ibkr_broker import IBKRBroker
        return IBKRBroker(
            host=os.environ.get("IBKR_HOST", "host.docker.internal"),
            port=int(os.environ.get("IBKR_PORT", "4001")),
            client_id=int(os.environ.get("IBKR_CLIENT_ID", "1")),
            paper=False,
        )
    else:
        raise ValueError(f"Unknown broker: {broker_name}")


class ExecutionEngine(BaseConsumer):

    def __init__(self) -> None:
        self._broker: BaseBroker | None = None
        self._risk = RiskManager(RiskConfig.from_env())
        self._telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._telegram_chat = os.environ.get("TELEGRAM_CHAT_ID", "")
        self._http: httpx.AsyncClient | None = None
        self._redis: aioredis.Redis | None = None
        self._cluster_store: EventClusterStore | None = None
        super().__init__()

    @property
    def service_name(self) -> str:
        return "execution_engine"

    @property
    def input_topic(self) -> str:
        # Consume from filtered signals if pre-trade filter is deployed,
        # fall back to actionable if not (controlled by env var)
        import os
        use_filter = os.environ.get("USE_PRETRADE_FILTER", "true").lower() == "true"
        return "signals.filtered" if use_filter else "signals.actionable"

    @property
    def output_topic(self) -> str:
        return "trades.executed"

    async def on_start(self) -> None:
        self._http = httpx.AsyncClient(timeout=15.0)
        self._broker = _get_broker()
        await self._broker.connect()
        if _ENABLE_ORDER_EVENT_GATE:
            self._redis = await aioredis.from_url(
                os.environ.get("REDIS_URL", "redis://redis:6379/0"),
                decode_responses=True,
            )
            self._cluster_store = EventClusterStore(self._redis)
        _log("info", "execution_engine.ready",
             broker=self._broker.name,
             paper=self._broker.paper,
             order_event_gate=_ENABLE_ORDER_EVENT_GATE)

    async def on_stop(self) -> None:
        if self._broker:
            await self._broker.disconnect()
        if self._http:
            await self._http.aclose()
        if self._redis:
            await self._redis.aclose()

    async def process(self, record: dict) -> dict | None:
        try:
            signal = TradingSignal.model_validate(record)
        except Exception as e:
            _log("error", "execution.parse_error", error=str(e))
            raise

        # Skip neutral signals
        if signal.direction == "neutral":
            return None

        ticker = signal.ticker
        direction = signal.direction

        # ── Get current price ─────────────────────────────────────────────────
        price = await self._broker.get_quote(ticker)
        if not price:
            _log("warning", "execution.no_price", ticker=ticker)
            return None

        # ── Get account + positions ───────────────────────────────────────────
        try:
            account = await self._broker.get_account()
            positions = await self._broker.get_positions()
        except Exception as e:
            _log("error", "execution.account_error", error=str(e))
            return None

        # ── Spread gate — pre-check BEFORE risk evaluation and order submission ──
        # Wide spreads mean we'd pay more in slippage than the edge is worth.
        #   > 2.0% → hard block (spread_too_wide)
        #   > 1.0% → -30% conviction
        #   > 0.5% → -15% conviction
        flow = {}
        try:
            flow = await self._broker.get_order_flow_context(ticker, price)
        except Exception as e:
            _log("warning", "execution.spread_fetch_error", ticker=ticker, error=str(e))

        conviction = signal.conviction
        spread_pct = flow.get("spread_pct")
        if spread_pct is not None:
            if spread_pct > 2.0:
                _log("info", "execution.risk_blocked",
                     ticker=ticker, direction=direction,
                     conviction=conviction,
                     reason=f"spread_too_wide ({spread_pct:.3f}%)",
                     spread_label=flow.get("spread_label", ""))
                return None
            elif spread_pct > 1.0:
                conviction = round(conviction * 0.70, 3)
                _log("info", "execution.spread_conviction_cut",
                     ticker=ticker, spread_pct=spread_pct,
                     conviction_in=signal.conviction, conviction_out=conviction,
                     cut_pct=30, spread_label=flow.get("spread_label", ""))
            elif spread_pct > 0.5:
                conviction = round(conviction * 0.85, 3)
                _log("info", "execution.spread_conviction_cut",
                     ticker=ticker, spread_pct=spread_pct,
                     conviction_in=signal.conviction, conviction_out=conviction,
                     cut_pct=15, spread_label=flow.get("spread_label", ""))

        # ── Risk evaluation ───────────────────────────────────────────────────
        decision = await self._risk.evaluate(
            ticker=ticker,
            direction=direction,
            conviction=conviction,      # spread-adjusted conviction
            current_price=price,
            account=account,
            open_positions=positions,
            session_context=signal.session_context,
            decay_minutes=signal.decay_minutes,
        )

        if not decision.approved:
            _log("info", "execution.risk_blocked",
                 ticker=ticker,
                 direction=direction,
                 conviction=signal.conviction,
                 reason=decision.reason)
            return None

        # ── Build and submit order ────────────────────────────────────────────
        order = self._risk.build_order(
            ticker=ticker,
            direction=direction,
            decision=decision,
            signal_id=str(signal.id),
        )

        # ── Order event gate — prevent double-submission for same news event ─
        # Uses cluster_id when available (set by deduplicator), falls back to signal.id.
        event_id = getattr(signal, "cluster_id", None) or str(signal.id)
        if _ENABLE_ORDER_EVENT_GATE and self._cluster_store:
            # Pre-register with a placeholder — will be updated with real order ID below
            gate_open = await self._cluster_store.mark_order_submitted(
                event_id=str(event_id),
                broker_order_id=f"pending:{signal.id}",
            )
            if not gate_open:
                _log("info", "execution.order_gate_blocked",
                     ticker=ticker,
                     direction=direction,
                     event_id=str(event_id),
                     signal_id=str(signal.id))
                return None
        # ────────────────────────────────────────────────────────────────────

        result = await self._broker.submit_order(order)

        _log("info", "execution.order_submitted",
             ticker=ticker,
             direction=direction,
             broker_order_id=result.broker_order_id,
             qty=result.qty_requested,
             status=result.status.value,
             tp=decision.take_profit,
             sl=decision.stop_loss,
             conviction=signal.conviction,
             broker=self._broker.name)

        # ── Telegram alert ────────────────────────────────────────────────────
        await self._send_trade_alert(signal, result, decision, price, flow=flow)

        # ── Persist to Postgres ───────────────────────────────────────────────
        await self._save_trade(signal, order, result, decision, price)

        # ── Emit to trades.executed ───────────────────────────────────────────
        if result.status not in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
            return {
                "id": str(uuid4()),
                "signal_id": str(signal.id),
                "broker_order_id": result.broker_order_id,
                "ticker": ticker,
                "direction": direction,
                "qty": order.qty,
                "entry_price": price,
                "take_profit": decision.take_profit,
                "stop_loss": decision.stop_loss,
                "conviction": signal.conviction,
                "broker": self._broker.name,
                "status": result.status.value,
                "catalyst_type": signal.catalyst_type,
                "signal_type": signal.signal_type,
                "t1_summary": signal.t1_summary,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        return None

    async def _send_trade_alert(
        self,
        signal: TradingSignal,
        result: OrderResult,
        decision,
        price: float,
        flow: dict | None = None,
    ) -> None:
        if not self._telegram_token or not self._telegram_chat:
            return

        # Reuse flow fetched in process() for spread gating; fetch only if not provided
        if flow is None:
            try:
                flow = await self._broker.get_order_flow_context(signal.ticker, price)
            except Exception:
                flow = {}

        if result.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
            emoji = "❌"
            header = f"Order REJECTED: {signal.ticker}"
        else:
            emoji = "📤" if signal.direction == "long" else "📥"
            header = f"{'LONG' if signal.direction == 'long' else 'SHORT'} {signal.ticker}"

        conviction_pct = int(signal.conviction * 100)
        msg_lines = [
            f"{emoji} *{header}*",
            f"",
            f"Entry:  ${price:.2f}",
        ]

        if decision.take_profit:
            msg_lines.append(f"Target: ${decision.take_profit:.2f} (+{((decision.take_profit/price)-1)*100:.1f}%)")
        if decision.stop_loss:
            msg_lines.append(f"Stop:   ${decision.stop_loss:.2f} (-{(1-(decision.stop_loss/price))*100:.1f}%)")
        if decision.risk_reward:
            msg_lines.append(f"R:R     1:{decision.risk_reward:.1f}")

        msg_lines += [
            f"",
            f"Qty:    {int(decision.qty)} shares (${decision.position_value:,.0f})",
            f"Conviction: {conviction_pct}%",
            f"",
            f"📰 _{signal.t1_summary or signal.news_title[:80]}_",
        ]

        # ── Order flow info block ─────────────────────────────────────────────
        flow_lines = []
        if flow.get("spread_pct") is not None:
            spread_pct = flow["spread_pct"]
            spread_warn = " ⚠️" if spread_pct > 0.5 else ""
            spread_label = flow.get("spread_label", "")
            # Annotate conviction impact applied upstream in process()
            if spread_pct > 1.0:
                spread_warn += " (−30% conviction)"
            elif spread_pct > 0.5:
                spread_warn += " (−15% conviction)"
            flow_lines.append(
                f"Spread: {spread_pct:.3f}% ({spread_label}){spread_warn}"
            )
        if flow.get("vwap") is not None:
            vwap_warn = " ⚠️" if "below" in (flow.get("vwap_bias") or "") and signal.direction == "long" else ""
            vwap_warn = vwap_warn or (" ⚠️" if "above" in (flow.get("vwap_bias") or "") and signal.direction == "short" else "")
            flow_lines.append(
                f"VWAP:   ${flow['vwap']:.2f} ({flow['vwap_gap_pct']:+.2f}%) — {flow['vwap_bias']}{vwap_warn}"
            )
        if flow_lines:
            msg_lines.append("")
            msg_lines.append("📊 _Order Flow_")
            msg_lines.extend(flow_lines)
        # ── End order flow block ──────────────────────────────────────────────

        # ── Timing window block (observe only) ───────────────────────────────
        tw_label   = getattr(signal, "time_window_label", None)
        tw_emoji   = getattr(signal, "time_window_emoji", "")
        tw_mult    = getattr(signal, "time_window_mult", None)
        tw_quality = getattr(signal, "time_window_quality", None)
        if tw_label:
            tw_warn = " ⚠️" if tw_quality == "poor" else ""
            mult_str = f"  (would {'+' if tw_mult >= 1.0 else ''}{(tw_mult-1)*100:.0f}% conviction)" if tw_mult else ""
            msg_lines.append("")
            msg_lines.append("🕐 _Timing (observe only)_")
            msg_lines.append(f"{tw_emoji} {tw_label}{tw_warn}")
            if mult_str:
                msg_lines.append(f"_{mult_str.strip()}_")
        # ── End timing block ──────────────────────────────────────────────────

        msg_lines += [
            f"",
            f"Broker: `{self._broker.name}`",
        ]

        if result.status == OrderStatus.REJECTED:
            msg_lines.append(f"Error: {result.error}")

        try:
            await self._http.post(
                f"https://api.telegram.org/bot{self._telegram_token}/sendMessage",
                json={
                    "chat_id": self._telegram_chat,
                    "text": "\n".join(msg_lines),
                    "parse_mode": "Markdown",
                },
            )
        except Exception as e:
            _log("warning", "execution.telegram_error", error=str(e))

    async def _save_trade(self, signal, order, result, decision, price) -> None:
        """Persist trade to Postgres trades table."""
        try:
            from app.db import get_engine
            from sqlalchemy.ext.asyncio import AsyncSession
            engine = get_engine()
            async with AsyncSession(engine) as session:
                await session.execute(text("""
                    INSERT INTO trade (
                        id, signal_id, broker_order_id, broker,
                        ticker, direction, qty, entry_price,
                        take_profit, stop_loss, conviction,
                        catalyst_type, signal_type, status,
                        t1_summary, created_at
                    ) VALUES (
                        gen_random_uuid(), :signal_id, :broker_order_id, :broker,
                        :ticker, :direction, :qty, :entry_price,
                        :take_profit, :stop_loss, :conviction,
                        :catalyst_type, :signal_type, :status,
                        :t1_summary, now()
                    )
                    ON CONFLICT DO NOTHING
                """), {
                    "signal_id": str(signal.id),
                    "broker_order_id": result.broker_order_id,
                    "broker": self._broker.name,
                    "ticker": signal.ticker,
                    "direction": signal.direction,
                    "qty": decision.qty,
                    "entry_price": price,
                    "take_profit": decision.take_profit,
                    "stop_loss": decision.stop_loss,
                    "conviction": signal.conviction,
                    "catalyst_type": signal.catalyst_type,
                    "signal_type": signal.signal_type,
                    "status": result.status.value,
                    "t1_summary": signal.t1_summary,
                })
                await session.commit()
        except Exception as e:
            _log("warning", "execution.db_save_error", error=str(e))
