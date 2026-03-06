"""
Alpaca Broker — paper and live trading via Alpaca REST API v2.

Paper:  https://paper-api.alpaca.markets
Live:   https://api.alpaca.markets

Supports:
  - Market / limit / stop / bracket orders
  - Real-time quotes via latest trade endpoint
  - Position and account info
  - Market hours check

Setup:
  1. Create account at https://alpaca.markets
  2. Switch to Paper Trading in dashboard
  3. Generate API Key + Secret
  4. Add to .env:
       ALPACA_API_KEY=PKxxxxxxxxxxxxxxxx
       ALPACA_SECRET_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
       ALPACA_PAPER=true
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx

from app.execution.base_broker import (
    AccountInfo, BaseBroker, OrderRequest, OrderResult,
    OrderSide, OrderStatus, OrderType, Position, TimeInForce,
)

PAPER_BASE = "https://paper-api.alpaca.markets"
LIVE_BASE  = "https://api.alpaca.markets"
DATA_BASE  = "https://data.alpaca.markets"


def _log(level: str, event: str, **kw) -> None:
    import json
    from datetime import datetime, timezone
    entry = {"ts": datetime.now(timezone.utc).isoformat(),
             "level": level, "event": event, **kw}
    print(json.dumps(entry), flush=True)


class AlpacaBroker(BaseBroker):

    def __init__(self, api_key: str, secret_key: str, paper: bool = True) -> None:
        super().__init__(paper=paper)
        self.name = "alpaca_paper" if paper else "alpaca_live"
        self._api_key = api_key
        self._secret_key = secret_key
        self._base = PAPER_BASE if paper else LIVE_BASE
        self._http: httpx.AsyncClient | None = None
        self._data_http: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        headers = {
            "APCA-API-KEY-ID": self._api_key,
            "APCA-API-SECRET-KEY": self._secret_key,
            "Content-Type": "application/json",
        }
        self._http = httpx.AsyncClient(
            base_url=self._base,
            headers=headers,
            timeout=15.0,
        )
        self._data_http = httpx.AsyncClient(
            base_url=DATA_BASE,
            headers=headers,
            timeout=10.0,
        )
        # Verify connection
        account = await self.get_account()
        _log("info", "alpaca.connected",
             broker=self.name,
             account_id=account.account_id,
             equity=account.equity,
             buying_power=account.buying_power,
             paper=self.paper)

    async def disconnect(self) -> None:
        if self._http:
            await self._http.aclose()
        if self._data_http:
            await self._data_http.aclose()

    async def get_account(self) -> AccountInfo:
        resp = await self._http.get("/v2/account")
        resp.raise_for_status()
        d = resp.json()

        equity = float(d.get("equity", 0))
        prev_equity = float(d.get("last_equity", equity))
        daily_pnl = equity - prev_equity
        daily_pnl_pct = (daily_pnl / prev_equity * 100) if prev_equity else 0

        return AccountInfo(
            equity=equity,
            cash=float(d.get("cash", 0)),
            buying_power=float(d.get("buying_power", 0)),
            portfolio_value=float(d.get("portfolio_value", 0)),
            daily_pnl=daily_pnl,
            daily_pnl_pct=daily_pnl_pct,
            currency=d.get("currency", "USD"),
            broker=self.name,
            account_id=d.get("id", ""),
            is_paper=self.paper,
        )

    async def get_positions(self) -> list[Position]:
        resp = await self._http.get("/v2/positions")
        resp.raise_for_status()
        positions = []
        for p in resp.json():
            positions.append(self._parse_position(p))
        return positions

    async def get_position(self, ticker: str) -> Position | None:
        try:
            resp = await self._http.get(f"/v2/positions/{ticker}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return self._parse_position(resp.json())
        except httpx.HTTPStatusError:
            return None

    async def submit_order(self, order: OrderRequest) -> OrderResult:
        payload: dict = {
            "symbol": order.ticker,
            "qty": str(order.qty),
            "side": order.side.value,
            "type": order.order_type.value,
            "time_in_force": order.time_in_force.value,
        }

        if order.order_type == OrderType.LIMIT and order.limit_price:
            payload["limit_price"] = str(round(order.limit_price, 2))

        if order.order_type == OrderType.STOP and order.stop_price:
            payload["stop_price"] = str(round(order.stop_price, 2))

        if order.order_type == OrderType.STOP_LIMIT:
            if order.limit_price:
                payload["limit_price"] = str(round(order.limit_price, 2))
            if order.stop_price:
                payload["stop_price"] = str(round(order.stop_price, 2))

        # Bracket order (take profit + stop loss attached)
        if order.take_profit_price or order.stop_loss_price:
            payload["order_class"] = "bracket"
            if order.take_profit_price:
                payload["take_profit"] = {"limit_price": str(round(order.take_profit_price, 2))}
            if order.stop_loss_price:
                payload["stop_loss"] = {"stop_price": str(round(order.stop_loss_price, 2))}

        _log("info", "alpaca.order_submit",
             ticker=order.ticker,
             side=order.side.value,
             qty=order.qty,
             type=order.order_type.value,
             tp=order.take_profit_price,
             sl=order.stop_loss_price)

        try:
            resp = await self._http.post("/v2/orders", json=payload)
            d = resp.json()

            if resp.status_code in (200, 201):
                return OrderResult(
                    broker_order_id=d["id"],
                    request_id=order.id,
                    ticker=order.ticker,
                    side=order.side,
                    qty_requested=order.qty,
                    qty_filled=float(d.get("filled_qty", 0)),
                    avg_fill_price=float(d.get("filled_avg_price") or 0),
                    status=self._parse_status(d.get("status", "")),
                    submitted_at=datetime.now(timezone.utc),
                    raw=d,
                )
            else:
                error = d.get("message", str(d))
                _log("error", "alpaca.order_rejected",
                     ticker=order.ticker, error=error)
                return OrderResult(
                    request_id=order.id,
                    ticker=order.ticker,
                    side=order.side,
                    qty_requested=order.qty,
                    status=OrderStatus.REJECTED,
                    error=error,
                    raw=d,
                )
        except Exception as e:
            _log("error", "alpaca.order_error", ticker=order.ticker, error=str(e))
            return OrderResult(
                request_id=order.id,
                ticker=order.ticker,
                side=order.side,
                qty_requested=order.qty,
                status=OrderStatus.REJECTED,
                error=str(e),
            )

    async def cancel_order(self, broker_order_id: str) -> bool:
        try:
            resp = await self._http.delete(f"/v2/orders/{broker_order_id}")
            return resp.status_code in (200, 204)
        except Exception:
            return False

    async def close_position(self, ticker: str) -> OrderResult:
        try:
            resp = await self._http.delete(f"/v2/positions/{ticker}")
            if resp.status_code in (200, 201):
                d = resp.json()
                return OrderResult(
                    broker_order_id=d.get("id", ""),
                    ticker=ticker,
                    side=OrderSide.SELL,
                    status=OrderStatus.SUBMITTED,
                    raw=d,
                )
            return OrderResult(
                ticker=ticker,
                status=OrderStatus.REJECTED,
                error=f"HTTP {resp.status_code}",
            )
        except Exception as e:
            return OrderResult(ticker=ticker, status=OrderStatus.REJECTED, error=str(e))

    async def get_quote(self, ticker: str) -> float:
        try:
            resp = await self._data_http.get(
                f"/v2/stocks/{ticker}/trades/latest"
            )
            if resp.status_code == 200:
                return float(resp.json()["trade"]["p"])
            # Fallback to latest bar
            resp = await self._data_http.get(
                f"/v2/stocks/{ticker}/bars/latest",
                params={"feed": "iex"},
            )
            if resp.status_code == 200:
                return float(resp.json()["bar"]["c"])
        except Exception as e:
            _log("warning", "alpaca.quote_error", ticker=ticker, error=str(e))
        return 0.0

    async def is_market_open(self) -> bool:
        try:
            resp = await self._http.get("/v2/clock")
            return resp.json().get("is_open", False)
        except Exception:
            return False

    def _parse_position(self, p: dict) -> Position:
        qty = float(p.get("qty", 0))
        return Position(
            ticker=p.get("symbol", ""),
            qty=qty,
            avg_entry=float(p.get("avg_entry_price", 0)),
            current_price=float(p.get("current_price", 0)),
            market_value=float(p.get("market_value", 0)),
            unrealized_pnl=float(p.get("unrealized_pl", 0)),
            unrealized_pnl_pct=float(p.get("unrealized_plpc", 0)) * 100,
            side="long" if qty > 0 else "short",
            broker_position_id=p.get("asset_id", ""),
        )

    def _parse_status(self, status: str) -> OrderStatus:
        mapping = {
            "new":              OrderStatus.SUBMITTED,
            "partially_filled": OrderStatus.PARTIAL,
            "filled":           OrderStatus.FILLED,
            "canceled":         OrderStatus.CANCELLED,
            "cancelled":        OrderStatus.CANCELLED,
            "rejected":         OrderStatus.REJECTED,
            "expired":          OrderStatus.EXPIRED,
            "pending_new":      OrderStatus.SUBMITTED,
            "accepted":         OrderStatus.SUBMITTED,
        }
        return mapping.get(status.lower(), OrderStatus.PENDING)
