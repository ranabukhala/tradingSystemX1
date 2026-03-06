"""
Interactive Brokers Broker — via IB Gateway / TWS using ib_insync.

Supports IBKR demo (paper) and live accounts.

Setup:
  1. Download IB Gateway: https://www.interactivebrokers.com/en/trading/ibgateway.php
  2. Log in with your IBKR credentials (use demo account for testing)
  3. In IB Gateway settings:
     - Enable API: Configuration → Settings → API → Enable ActiveX and Socket Clients
     - Set port: 4002 (live) or 4001 (paper/demo)
     - Allow connections from: 127.0.0.1 (or Docker host IP)
  4. Add to .env:
       IBKR_HOST=host.docker.internal   # IB Gateway running on Windows host
       IBKR_PORT=4002                    # 4002=live gateway, 4001=paper gateway, 7497=TWS paper
       IBKR_CLIENT_ID=1
       IBKR_PAPER=true

Note: ib_insync runs synchronously internally but we wrap it in asyncio.
The connection requires IB Gateway or TWS to be running on the host machine.

Important IBKR demo limits:
  - Delayed data (15-20 min) unless you have market data subscriptions
  - Same order types as live
  - $1M virtual account
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from app.execution.base_broker import (
    AccountInfo, BaseBroker, OrderRequest, OrderResult,
    OrderSide, OrderStatus, OrderType, Position, TimeInForce,
)


def _log(level: str, event: str, **kw) -> None:
    import json
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


class IBKRBroker(BaseBroker):
    """
    Interactive Brokers via ib_insync.
    Requires IB Gateway or TWS running on the host.
    """

    def __init__(
        self,
        host: str = "host.docker.internal",
        port: int = 4002,
        client_id: int = 1,
        paper: bool = True,
    ) -> None:
        super().__init__(paper=paper)
        self.name = "ibkr_paper" if paper else "ibkr_live"
        self._host = host
        self._port = port
        self._client_id = client_id
        self._ib: Any = None  # ib_insync.IB instance
        self._connected = False

    async def connect(self) -> None:
        try:
            from ib_insync import IB, util
            util.patchAsyncio()

            self._ib = IB()
            await self._ib.connectAsync(
                host=self._host,
                port=self._port,
                clientId=self._client_id,
                timeout=20,
            )
            self._connected = True
            account = await self.get_account()
            _log("info", "ibkr.connected",
                 broker=self.name,
                 host=self._host,
                 port=self._port,
                 account_id=account.account_id,
                 equity=account.equity,
                 paper=self.paper)

        except ImportError:
            raise RuntimeError(
                "ib_insync not installed. Add to requirements: ib_insync>=0.9.86"
            )
        except Exception as e:
            _log("error", "ibkr.connect_failed",
                 host=self._host, port=self._port, error=str(e))
            raise

    async def disconnect(self) -> None:
        if self._ib and self._connected:
            self._ib.disconnect()
            self._connected = False
            _log("info", "ibkr.disconnected")

    async def get_account(self) -> AccountInfo:
        if not self._connected:
            raise ConnectionError("IBKR not connected")

        summary = await self._ib.accountSummaryAsync()
        values = {s.tag: s.value for s in summary}

        equity = float(values.get("NetLiquidation", 0))
        cash = float(values.get("TotalCashValue", 0))
        buying_power = float(values.get("BuyingPower", 0))
        daily_pnl = float(values.get("DailyPnL", 0))
        prev_equity = equity - daily_pnl
        daily_pnl_pct = (daily_pnl / prev_equity * 100) if prev_equity else 0

        account_id = summary[0].account if summary else ""

        return AccountInfo(
            equity=equity,
            cash=cash,
            buying_power=buying_power,
            portfolio_value=equity - cash,
            daily_pnl=daily_pnl,
            daily_pnl_pct=daily_pnl_pct,
            currency="USD",
            broker=self.name,
            account_id=account_id,
            is_paper=self.paper,
        )

    async def get_positions(self) -> list[Position]:
        if not self._connected:
            return []
        positions = await self._ib.reqPositionsAsync()
        result = []
        for p in positions:
            if p.contract.secType == "STK" and p.position != 0:
                # Get current price via contract details
                price = await self.get_quote(p.contract.symbol)
                pnl = (price - p.avgCost) * p.position
                pnl_pct = ((price / p.avgCost) - 1) * 100 if p.avgCost else 0
                result.append(Position(
                    ticker=p.contract.symbol,
                    qty=p.position,
                    avg_entry=p.avgCost,
                    current_price=price,
                    market_value=price * abs(p.position),
                    unrealized_pnl=pnl,
                    unrealized_pnl_pct=pnl_pct,
                    side="long" if p.position > 0 else "short",
                ))
        return result

    async def get_position(self, ticker: str) -> Position | None:
        positions = await self.get_positions()
        return next((p for p in positions if p.ticker == ticker), None)

    async def submit_order(self, order: OrderRequest) -> OrderResult:
        if not self._connected:
            return OrderResult(
                request_id=order.id,
                ticker=order.ticker,
                status=OrderStatus.REJECTED,
                error="IBKR not connected",
            )

        from ib_insync import Stock, MarketOrder, LimitOrder, StopOrder, StopLimitOrder, BracketOrder

        # Build contract
        contract = Stock(order.ticker, "SMART", "USD")
        await self._ib.qualifyContractsAsync(contract)

        action = "BUY" if order.side == OrderSide.BUY else "SELL"
        qty = order.qty

        _log("info", "ibkr.order_submit",
             ticker=order.ticker,
             side=action,
             qty=qty,
             type=order.order_type.value)

        try:
            # Build order
            if order.take_profit_price and order.stop_loss_price:
                # Bracket order
                bracket = self._ib.bracketOrder(
                    action=action,
                    quantity=qty,
                    limitPrice=order.limit_price or await self.get_quote(order.ticker),
                    takeProfitPrice=order.take_profit_price,
                    stopLossPrice=order.stop_loss_price,
                )
                trades = []
                for o in bracket:
                    trade = self._ib.placeOrder(contract, o)
                    trades.append(trade)
                parent_trade = trades[0]
            else:
                if order.order_type == OrderType.MARKET:
                    ib_order = MarketOrder(action, qty)
                elif order.order_type == OrderType.LIMIT:
                    ib_order = LimitOrder(action, qty, order.limit_price)
                elif order.order_type == OrderType.STOP:
                    ib_order = StopOrder(action, qty, order.stop_price)
                elif order.order_type == OrderType.STOP_LIMIT:
                    ib_order = StopLimitOrder(action, qty, order.limit_price, order.stop_price)
                else:
                    ib_order = MarketOrder(action, qty)

                parent_trade = self._ib.placeOrder(contract, ib_order)

            # Wait briefly for initial status
            await asyncio.sleep(0.5)
            self._ib.sleep(0)  # Process events

            order_id = str(parent_trade.order.orderId)
            status = parent_trade.orderStatus.status

            return OrderResult(
                broker_order_id=order_id,
                request_id=order.id,
                ticker=order.ticker,
                side=order.side,
                qty_requested=qty,
                qty_filled=parent_trade.orderStatus.filled,
                avg_fill_price=parent_trade.orderStatus.avgFillPrice or 0,
                status=self._parse_status(status),
                submitted_at=datetime.now(timezone.utc),
                raw={"status": status, "order_id": order_id},
            )

        except Exception as e:
            _log("error", "ibkr.order_error", ticker=order.ticker, error=str(e))
            return OrderResult(
                request_id=order.id,
                ticker=order.ticker,
                side=order.side,
                qty_requested=order.qty,
                status=OrderStatus.REJECTED,
                error=str(e),
            )

    async def cancel_order(self, broker_order_id: str) -> bool:
        if not self._connected:
            return False
        try:
            trades = self._ib.trades()
            for trade in trades:
                if str(trade.order.orderId) == broker_order_id:
                    self._ib.cancelOrder(trade.order)
                    return True
            return False
        except Exception:
            return False

    async def close_position(self, ticker: str) -> OrderResult:
        position = await self.get_position(ticker)
        if not position:
            return OrderResult(ticker=ticker, status=OrderStatus.REJECTED,
                               error="No position found")

        side = OrderSide.SELL if position.qty > 0 else OrderSide.BUY
        qty = abs(position.qty)

        order = OrderRequest(
            ticker=ticker,
            side=side,
            qty=qty,
            order_type=OrderType.MARKET,
            notes="close_position",
        )
        return await self.submit_order(order)

    async def get_quote(self, ticker: str) -> float:
        if not self._connected:
            return 0.0
        try:
            from ib_insync import Stock
            contract = Stock(ticker, "SMART", "USD")
            await self._ib.qualifyContractsAsync(contract)
            tickers = await self._ib.reqTickersAsync(contract)
            if tickers:
                t = tickers[0]
                # Use last trade price, fall back to midpoint
                if t.last and t.last > 0:
                    return float(t.last)
                if t.bid and t.ask and t.bid > 0:
                    return float((t.bid + t.ask) / 2)
        except Exception as e:
            _log("warning", "ibkr.quote_error", ticker=ticker, error=str(e))
        return 0.0

    async def is_market_open(self) -> bool:
        if not self._connected:
            return False
        try:
            from ib_insync import Stock
            contract = Stock("SPY", "SMART", "USD")
            details = await self._ib.reqContractDetailsAsync(contract)
            if details:
                # Check trading hours
                now_et = datetime.now(timezone.utc)
                hour = now_et.hour
                # Rough check: 9:30-16:00 ET = 13:30-20:00 UTC
                return 13 <= hour < 20
        except Exception:
            pass
        return False

    def _parse_status(self, status: str) -> OrderStatus:
        mapping = {
            "PreSubmitted": OrderStatus.SUBMITTED,
            "Submitted":    OrderStatus.SUBMITTED,
            "Filled":       OrderStatus.FILLED,
            "Cancelled":    OrderStatus.CANCELLED,
            "Inactive":     OrderStatus.CANCELLED,
            "PartiallyFilled": OrderStatus.PARTIAL,
        }
        return mapping.get(status, OrderStatus.PENDING)
