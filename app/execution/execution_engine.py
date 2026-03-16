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

from app.execution.base_broker import BaseBroker, OrderResult, OrderStatus, OrderType
from app.execution.risk_manager import RiskManager, RiskConfig
from app.execution.entry_validator import EntryValidator
from app.execution.order_policy import build_order_policy_table, get_order_policy
from app.execution.fill_poller import FillPoller, FillPollConfig
from app.execution.position_monitor import PositionMonitor
from app.pipeline.base_consumer import BaseConsumer, _log
from app.pipeline.event_cluster import EventClusterStore
from app.pipeline.staleness import staleness_from_signal_dict
from app.signals.signal_aggregator import TradingSignal

_ENABLE_ORDER_EVENT_GATE = os.environ.get("ENABLE_ORDER_EVENT_GATE", "true").lower() == "true"

# Staleness guard — last line of defense after the pretrade filter.
# Catches signals that aged while sitting in signals.filtered queue.
_STALENESS_ENABLED = os.environ.get("STALENESS_ENABLED", "true").lower() == "true"

# Kafka consumer lag guard — stop placing orders when we're significantly
# behind the head of the queue.  Signals in a deep backlog are already old.
_MAX_EXECUTION_LAG = int(os.environ.get("EXECUTION_MAX_LAG_MESSAGES", "50"))


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
        # v1.11 components — initialised in on_start()
        self._entry_validator: EntryValidator | None = None
        self._fill_poller: FillPoller | None = None
        self._position_monitor: PositionMonitor | None = None
        self._order_policy_table: dict | None = None
        super().__init__()

    @property
    def service_name(self) -> str:
        return "execution_engine"

    @property
    def input_topic(self) -> str:
        # Routing controlled by two env-var flags (both default true):
        #   USE_PRETRADE_FILTER          — signals.actionable → signals.filtered
        #   USE_PORTFOLIO_RISK_MANAGER   — signals.filtered  → signals.risk_approved
        import os
        use_filter = os.environ.get("USE_PRETRADE_FILTER", "true").lower() == "true"
        use_risk   = os.environ.get("USE_PORTFOLIO_RISK_MANAGER", "true").lower() == "true"
        if use_filter and use_risk:
            return "signals.risk_approved"
        elif use_filter:
            return "signals.filtered"
        else:
            return "signals.actionable"

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

        # v1.11: initialise execution quality components
        from app.config import settings as _settings
        self._entry_validator  = EntryValidator(_settings)
        self._fill_poller      = FillPoller(self._broker, FillPollConfig.from_env())
        self._order_policy_table = build_order_policy_table(_settings)

        # v1.11: position monitor — starts as background asyncio task
        self._position_monitor = PositionMonitor(self._broker)
        asyncio.create_task(self._position_monitor.start())

        _log("info", "execution_engine.ready",
             broker=self._broker.name,
             paper=self._broker.paper,
             order_event_gate=_ENABLE_ORDER_EVENT_GATE)

    async def on_stop(self) -> None:
        if self._position_monitor:
            self._position_monitor.stop()
        if self._broker:
            await self._broker.disconnect()
        if self._http:
            await self._http.aclose()
        if self._redis:
            await self._redis.aclose()

    async def _fetch_atr(self, ticker: str) -> float | None:
        """
        Fetch ATR(14) from the stock context service.
        Returns None on any error (fail-open — caller falls back to fixed % stops).
        """
        try:
            stock_context_url = os.environ.get(
                "STOCK_CONTEXT_URL", "http://stock_context:8082")
            resp = await self._http.get(
                f"{stock_context_url}/context/{ticker}",
                timeout=3.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                atr = data.get("raw_metrics", {}).get("atr14")
                if atr and float(atr) > 0:
                    return float(atr)
        except Exception as e:
            _log("debug", "execution.atr_fetch_error", ticker=ticker, error=str(e))
        return None

    async def _save_execution_quality(
        self,
        signal: TradingSignal,
        order,
        result: OrderResult,
        entry_check,
        decision,
        atr_14: float | None,
        policy,
        spread_pct: float | None,
        submit_time: datetime,
    ) -> None:
        """
        Persist an execution_quality row (v1.11).
        Fails gracefully — never blocks the trade flow.
        """
        enabled = os.environ.get("EXECUTION_QUALITY_ENABLED", "true").lower() == "true"
        if not enabled:
            return
        try:
            from app.db import get_engine
            from sqlalchemy.ext.asyncio import AsyncSession

            # Compute slippage: (fill - ref) / ref * 100  (positive = paid more)
            entry_ref_price = getattr(signal, "entry_ref_price", None)
            avg_fill = result.avg_fill_price or 0.0
            slippage_pct: float | None = None
            if entry_ref_price and entry_ref_price > 0 and avg_fill > 0:
                slippage_pct = (avg_fill - entry_ref_price) / entry_ref_price * 100.0

            # Fill latency
            fill_latency_ms: int | None = None
            if result.filled_at and submit_time:
                delta = (result.filled_at - submit_time).total_seconds()
                fill_latency_ms = max(0, int(delta * 1000))

            adv_tier = getattr(policy, "adv_tier", None) if policy else None
            policy_name = getattr(policy, "policy_name", None) if policy else None
            order_type_str = getattr(policy, "order_type", "market") if policy else "market"

            engine = get_engine()
            async with AsyncSession(engine) as session:
                # First get the trade_id for this signal
                row = await session.execute(text(
                    "SELECT id FROM trade WHERE signal_id = :sid ORDER BY created_at DESC LIMIT 1"
                ), {"sid": str(signal.id)})
                trade_row = row.fetchone()
                trade_id = str(trade_row[0]) if trade_row else None

                await session.execute(text("""
                    INSERT INTO execution_quality (
                        trade_id, broker_order_id, ticker,
                        quote_age_seconds, price_drift_pct, spread_pct,
                        order_type, qty_requested, qty_filled,
                        avg_fill_price, entry_ref_price, slippage_pct,
                        fill_latency_ms, partial_fill,
                        atr_14, atr_stop_used, stop_loss_price, take_profit_price,
                        catalyst_type, adv_tier, policy_applied
                    ) VALUES (
                        :trade_id, :broker_order_id, :ticker,
                        :quote_age_seconds, :price_drift_pct, :spread_pct,
                        :order_type, :qty_requested, :qty_filled,
                        :avg_fill_price, :entry_ref_price, :slippage_pct,
                        :fill_latency_ms, :partial_fill,
                        :atr_14, :atr_stop_used, :stop_loss_price, :take_profit_price,
                        :catalyst_type, :adv_tier, :policy_applied
                    )
                """), {
                    "trade_id":          trade_id,
                    "broker_order_id":   result.broker_order_id,
                    "ticker":            signal.ticker,
                    "quote_age_seconds": getattr(entry_check, "quote_age_seconds", None),
                    "price_drift_pct":   getattr(entry_check, "price_drift_pct", None),
                    "spread_pct":        spread_pct,
                    "order_type":        order_type_str,
                    "qty_requested":     order.qty,
                    "qty_filled":        result.qty_filled,
                    "avg_fill_price":    avg_fill or None,
                    "entry_ref_price":   entry_ref_price,
                    "slippage_pct":      slippage_pct,
                    "fill_latency_ms":   fill_latency_ms,
                    "partial_fill":      result.status.value == "partial",
                    "atr_14":            atr_14,
                    "atr_stop_used":     decision.stop_type == "atr" if decision else False,
                    "stop_loss_price":   decision.stop_loss if decision else None,
                    "take_profit_price": decision.take_profit if decision else None,
                    "catalyst_type":     signal.catalyst_type,
                    "adv_tier":          adv_tier,
                    "policy_applied":    policy_name,
                })
                await session.commit()
        except Exception as e:
            _log("warning", "execution.quality_save_error", error=str(e))

    async def _log_lag_block(self, ticker: str, signal: TradingSignal) -> None:
        """Write a kafka_lag_log row when execution is blocked by consumer lag."""
        try:
            from app.db import get_engine
            engine = get_engine()
            async with AsyncSession(engine) as session:
                await session.execute(text("""
                    INSERT INTO kafka_lag_log
                        (service, consumer_lag, lag_threshold, blocked, ticker)
                    VALUES ('execution_engine', :lag, :threshold, TRUE, :ticker)
                """), {
                    "lag":       self._current_lag,
                    "threshold": _MAX_EXECUTION_LAG,
                    "ticker":    ticker,
                })
                await session.commit()
        except Exception as e:
            _log("warning", "execution.lag_log_error", error=str(e))

    async def _log_staleness_block(self, ticker: str, signal: TradingSignal, stale) -> None:
        """Write a staleness_log row when execution is blocked by signal age."""
        try:
            from app.db import get_engine
            engine = get_engine()
            async with AsyncSession(engine) as session:
                await session.execute(text("""
                    INSERT INTO staleness_log (
                        source, ticker, catalyst_type, session_context, route_type,
                        signal_age_s, max_age_s, reason, news_published_at
                    ) VALUES (
                        'execution_engine', :ticker, :catalyst, :session, :route,
                        :age, :max_age, :reason, :published_at
                    )
                """), {
                    "ticker":       ticker,
                    "catalyst":     stale.catalyst_type,
                    "session":      stale.session,
                    "route":        stale.route_type,
                    "age":          stale.age_seconds,
                    "max_age":      stale.max_age_seconds,
                    "reason":       stale.reason,
                    "published_at": signal.news_published_at,
                })
                await session.commit()
        except Exception as e:
            _log("warning", "execution.staleness_log_error", error=str(e))

    async def process(self, record: dict) -> dict | None:
        try:
            signal = TradingSignal.model_validate(record)
        except Exception as e:
            _log("error", "execution.parse_error", error=str(e))
            raise

        # Skip neutral signals
        if signal.direction == "neutral":
            return None

        ticker    = signal.ticker
        direction = signal.direction

        # ── Kafka consumer lag guard ───────────────────────────────────────────
        # If we are significantly behind the head of the queue the signals we
        # are about to execute are already old.  Stop trading until caught up.
        if _MAX_EXECUTION_LAG > 0 and self._lag_too_high(_MAX_EXECUTION_LAG):
            _log("info", "execution.lag_blocked",
                 ticker=ticker,
                 direction=direction,
                 consumer_lag=self._current_lag,
                 max_lag=_MAX_EXECUTION_LAG)
            await self._log_lag_block(ticker, signal)
            return None

        # ── Staleness guard ────────────────────────────────────────────────────
        # Second enforcement point (pretrade_filter is first).  Catches signals
        # that were fresh when the filter ran but aged in signals.filtered queue.
        if _STALENESS_ENABLED:
            stale = staleness_from_signal_dict(record)
            if stale.is_stale:
                _log("info", "execution.staleness_blocked",
                     ticker=ticker,
                     direction=direction,
                     catalyst_type=stale.catalyst_type,
                     session=stale.session,
                     route_type=stale.route_type,
                     age_seconds=stale.age_seconds,
                     max_age_seconds=stale.max_age_seconds,
                     reason=stale.reason)
                await self._log_staleness_block(ticker, signal, stale)
                return None
        # ─────────────────────────────────────────────────────────────────────

        # ── Step 1: Get quote WITH timestamp ─────────────────────────────────
        price, quote_timestamp = await self._broker.get_quote_with_timestamp(ticker)
        if not price:
            _log("warning", "execution.no_price", ticker=ticker)
            return None

        # ── Step 2: Halt check ────────────────────────────────────────────────
        halted = False
        try:
            halted = await self._broker.get_halted(ticker)
        except Exception as e:
            _log("debug", "execution.halt_check_error", ticker=ticker, error=str(e))

        # ── Step 3: EntryValidator gate ───────────────────────────────────────
        entry_check = None
        if self._entry_validator:
            signal_price = getattr(signal, "entry_ref_price", None)
            entry_check = await self._entry_validator.check(
                ticker=ticker,
                signal_price=signal_price,
                current_price=price,
                quote_timestamp=quote_timestamp,
                halted=halted,
            )
            if not entry_check.approved:
                _log("info", "execution.entry_blocked",
                     ticker=ticker, direction=direction,
                     reason=entry_check.reason)
                return None

        # ── Step 4: Get account + positions ───────────────────────────────────
        try:
            account = await self._broker.get_account()
            positions = await self._broker.get_positions()
        except Exception as e:
            _log("error", "execution.account_error", error=str(e))
            return None

        # ── Step 5: Spread gate — BEFORE risk evaluation ──────────────────────
        # Wide spreads → pay more slippage than edge is worth.
        #   > 2.0% → hard block
        #   > 1.0% → −30% conviction
        #   > 0.5% → −15% conviction
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

        # ── Step 6: Fetch ATR(14) from stock context service ──────────────────
        atr_14 = await self._fetch_atr(ticker)

        # ── Step 7: Risk evaluation (with ATR stops) ──────────────────────────
        decision = await self._risk.evaluate(
            ticker=ticker,
            direction=direction,
            conviction=conviction,      # spread-adjusted conviction
            current_price=price,
            account=account,
            open_positions=positions,
            session_context=signal.session_context,
            decay_minutes=signal.decay_minutes,
            atr_14=atr_14,
        )

        if not decision.approved:
            _log("info", "execution.risk_blocked",
                 ticker=ticker,
                 direction=direction,
                 conviction=signal.conviction,
                 reason=decision.reason)
            return None

        # ── Step 8: Order type policy lookup ──────────────────────────────────
        policy = None
        order_type_str = "market"
        limit_price: float | None = None
        time_in_force = "day"

        if self._order_policy_table:
            adv = flow.get("adv") if flow else None
            from app.config import settings as _settings
            policy = get_order_policy(
                self._order_policy_table,
                catalyst_type=signal.catalyst_type or "other",
                adv=adv,
                liquid_threshold=getattr(_settings, "order_adv_liquid_threshold", 5_000_000),
                thin_threshold=getattr(_settings, "order_adv_thin_threshold", 500_000),
            )
            order_type_str = policy.order_type
            time_in_force  = policy.time_in_force

            # For LIMIT orders: set limit price = current price ± slippage
            if order_type_str in ("limit", "limit_ioc") and price > 0:
                slip = policy.limit_slippage_pct / 100.0
                if direction == "long":
                    limit_price = round(price * (1 + slip), 2)
                else:
                    limit_price = round(price * (1 - slip), 2)

        # ── Step 9: Build order with policy order type ────────────────────────
        order = self._risk.build_order(
            ticker=ticker,
            direction=direction,
            decision=decision,
            signal_id=str(signal.id),
            order_type_str=order_type_str,
            limit_price=limit_price,
            time_in_force=time_in_force,
        )

        # ── Order event gate — prevent double-submission for same news event ──
        event_id = getattr(signal, "cluster_id", None) or str(signal.id)
        if _ENABLE_ORDER_EVENT_GATE and self._cluster_store:
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

        submit_time = datetime.now(timezone.utc)
        result = await self._broker.submit_order(order)

        # ── Step 10: Fill poller (LIMIT orders only) ──────────────────────────
        if (self._fill_poller
                and order.order_type != OrderType.MARKET
                and result.broker_order_id
                and result.status not in (
                    OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.EXPIRED)):
            result = await self._fill_poller.wait_for_fill(result, order)

        _log("info", "execution.order_submitted",
             ticker=ticker,
             direction=direction,
             broker_order_id=result.broker_order_id,
             qty=result.qty_requested,
             qty_filled=result.qty_filled,
             avg_fill=result.avg_fill_price,
             status=result.status.value,
             order_type=order_type_str,
             tp=decision.take_profit,
             sl=decision.stop_loss,
             stop_type=decision.stop_type,
             atr_14=atr_14,
             conviction=signal.conviction,
             broker=self._broker.name)

        # ── Step 11: Register with position monitor ───────────────────────────
        if (result.status in (OrderStatus.FILLED, OrderStatus.PARTIAL)
                and self._position_monitor):
            self._position_monitor.register_position(ticker, {
                "entry_price":     result.avg_fill_price or price,
                "take_profit":     decision.take_profit,
                "stop_loss":       decision.stop_loss,
                "decay_minutes":   signal.decay_minutes or 60,
                "broker_order_id": result.broker_order_id,
            })

        # ── Telegram alert ────────────────────────────────────────────────────
        await self._send_trade_alert(signal, result, decision, price, flow=flow)

        # ── Persist to Postgres ───────────────────────────────────────────────
        await self._save_trade(signal, order, result, decision, price)

        # ── Step 12: Persist execution quality row ────────────────────────────
        await self._save_execution_quality(
            signal=signal,
            order=order,
            result=result,
            entry_check=entry_check,
            decision=decision,
            atr_14=atr_14,
            policy=policy,
            spread_pct=spread_pct,
            submit_time=submit_time,
        )

        # ── Emit to trades.executed ───────────────────────────────────────────
        if result.status not in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
            return {
                "id": str(uuid4()),
                "signal_id": str(signal.id),
                "broker_order_id": result.broker_order_id,
                "ticker": ticker,
                "direction": direction,
                "qty": order.qty,
                "qty_filled": result.qty_filled,
                "entry_price": price,
                "avg_fill_price": result.avg_fill_price,
                "take_profit": decision.take_profit,
                "stop_loss": decision.stop_loss,
                "stop_type": decision.stop_type,
                "atr_14": atr_14,
                "order_type": order_type_str,
                "conviction": signal.conviction,
                "broker": self._broker.name,
                "status": result.status.value,
                "catalyst_type": signal.catalyst_type,
                "signal_type": signal.signal_type,
                "route_type": signal.route_type or "slow",
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

            # Compute pipeline latency: time from article publication to order submission
            pipeline_latency_ms: int | None = None
            if signal.news_published_at:
                try:
                    pub = signal.news_published_at
                    if pub.tzinfo is None:
                        from datetime import timezone as _tz
                        pub = pub.replace(tzinfo=_tz.utc)
                    latency_s = (datetime.now(timezone.utc) - pub).total_seconds()
                    pipeline_latency_ms = int(latency_s * 1000)
                except Exception:
                    pass

            async with AsyncSession(engine) as session:
                await session.execute(text("""
                    INSERT INTO trade (
                        id, signal_id, broker_order_id, broker,
                        ticker, direction, qty, entry_price,
                        take_profit, stop_loss, conviction,
                        catalyst_type, signal_type, status,
                        t1_summary, consumer_lag, pipeline_latency_ms,
                        created_at
                    ) VALUES (
                        gen_random_uuid(), :signal_id, :broker_order_id, :broker,
                        :ticker, :direction, :qty, :entry_price,
                        :take_profit, :stop_loss, :conviction,
                        :catalyst_type, :signal_type, :status,
                        :t1_summary, :consumer_lag, :pipeline_latency_ms,
                        now()
                    )
                    ON CONFLICT DO NOTHING
                """), {
                    "signal_id":           str(signal.id),
                    "broker_order_id":     result.broker_order_id,
                    "broker":              self._broker.name,
                    "ticker":              signal.ticker,
                    "direction":           signal.direction,
                    "qty":                 decision.qty,
                    "entry_price":         price,
                    "take_profit":         decision.take_profit,
                    "stop_loss":           decision.stop_loss,
                    "conviction":          signal.conviction,
                    "catalyst_type":       signal.catalyst_type,
                    "signal_type":         signal.signal_type,
                    "status":              result.status.value,
                    "t1_summary":          signal.t1_summary,
                    "consumer_lag":        self._current_lag,
                    "pipeline_latency_ms": pipeline_latency_ms,
                })
                await session.commit()
        except Exception as e:
            _log("warning", "execution.db_save_error", error=str(e))
