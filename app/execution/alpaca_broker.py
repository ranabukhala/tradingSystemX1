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
        price, _ = await self.get_quote_with_timestamp(ticker)
        return price

    async def get_quote_with_timestamp(self, ticker: str) -> tuple[float, datetime]:
        """
        Fetch latest trade price plus the exchange trade timestamp.
        Falls back to bar close if trade endpoint unavailable.
        Returns (price, utc_datetime) — datetime is always offset-aware.
        """
        try:
            resp = await self._data_http.get(
                f"/v2/stocks/{ticker}/trades/latest"
            )
            if resp.status_code == 200:
                d = resp.json()
                price = float(d["trade"]["p"])
                # "t" is ISO-8601 with offset, e.g. "2024-01-05T14:32:01.123Z"
                ts_raw = d["trade"].get("t", "")
                try:
                    ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                except Exception:
                    ts = datetime.now(timezone.utc)
                return price, ts
            # Fallback to latest bar
            resp = await self._data_http.get(
                f"/v2/stocks/{ticker}/bars/latest",
                params={"feed": "iex"},
            )
            if resp.status_code == 200:
                price = float(resp.json()["bar"]["c"])
                return price, datetime.now(timezone.utc)
        except Exception as e:
            _log("warning", "alpaca.quote_error", ticker=ticker, error=str(e))
        return 0.0, datetime.now(timezone.utc)

    async def get_halted(self, ticker: str) -> bool:
        """
        Check whether the exchange has halted trading for `ticker`.
        Uses GET /v2/assets/{symbol} → {"tradable": false} means halted.
        Fails open (returns False) on any error.
        """
        try:
            resp = await self._http.get(f"/v2/assets/{ticker}")
            if resp.status_code == 200:
                asset = resp.json()
                # tradable=False means the asset is currently un-tradable / halted
                return not bool(asset.get("tradable", True))
        except Exception as e:
            _log("warning", "alpaca.halt_check_error", ticker=ticker, error=str(e))
        return False  # fail-open

    async def get_order(self, broker_order_id: str) -> "OrderResult":
        """
        Fetch current order status by Alpaca order ID.
        GET /v2/orders/{order_id} → filled_qty, filled_avg_price, status.
        """
        try:
            resp = await self._http.get(f"/v2/orders/{broker_order_id}")
            if resp.status_code == 200:
                d = resp.json()
                filled_at_raw = d.get("filled_at")
                filled_at = None
                if filled_at_raw:
                    try:
                        filled_at = datetime.fromisoformat(
                            filled_at_raw.replace("Z", "+00:00")
                        )
                    except Exception:
                        pass
                return OrderResult(
                    broker_order_id=broker_order_id,
                    ticker=d.get("symbol", ""),
                    side=OrderSide.BUY if d.get("side") == "buy" else OrderSide.SELL,
                    qty_requested=float(d.get("qty", 0) or 0),
                    qty_filled=float(d.get("filled_qty", 0) or 0),
                    avg_fill_price=float(d.get("filled_avg_price") or 0),
                    status=self._parse_status(d.get("status", "")),
                    submitted_at=datetime.now(timezone.utc),
                    filled_at=filled_at,
                    raw=d,
                )
        except Exception as e:
            _log("warning", "alpaca.get_order_error",
                 broker_order_id=broker_order_id, error=str(e))
        # Return last-known PENDING on error
        return OrderResult(
            broker_order_id=broker_order_id,
            status=OrderStatus.PENDING,
            error="get_order_failed",
        )

    async def start_trade_stream(self, callback) -> None:
        """
        Start Alpaca WebSocket trade update stream via alpaca-py TradingStream.
        `callback` is called with a dict for each fill/partial/cancel event.
        Runs indefinitely — caller should wrap in asyncio.create_task().

        Requires: alpaca-py>=0.8.2  (pip install alpaca-py)
        """
        try:
            from alpaca.trading.stream import TradingStream

            stream = TradingStream(
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self.paper,
            )

            async def _handler(update) -> None:
                try:
                    event = update.event  # "fill" | "partial_fill" | "canceled" | ...
                    order = update.order
                    await callback({
                        "event":            event,
                        "symbol":           order.symbol,
                        "broker_order_id":  str(order.id),
                        "qty_filled":       float(order.filled_qty or 0),
                        "avg_fill_price":   float(order.filled_avg_price or 0),
                        "status":           order.status,
                        "timestamp":        str(update.timestamp),
                    })
                except Exception as e:
                    _log("warning", "alpaca.stream_handler_error", error=str(e))

            stream.subscribe_trade_updates(_handler)
            _log("info", "alpaca.trade_stream_starting", paper=self.paper)
            await stream._run_forever()

        except ImportError:
            _log("warning", "alpaca.trade_stream_unavailable",
                 note="alpaca-py not installed; falling back to polling")
            raise NotImplementedError("alpaca-py not installed")
        except Exception as e:
            _log("error", "alpaca.trade_stream_error", error=str(e))
            raise

    async def get_order_flow_context(self, ticker: str, current_price: float) -> dict:
        """
        Fetch bid/ask spread and compute VWAP from today's 1-min bars.
        Returns informational dict — not used for execution decisions.
        """
        result = {
            "bid": None, "ask": None,
            "spread_pct": None, "spread_label": None,
            "vwap": None, "vwap_bias": None,
            "vwap_gap_pct": None,
        }
        try:
            # Bid/ask spread from latest quote
            resp = await self._data_http.get(
                f"/v2/stocks/{ticker}/quotes/latest",
                params={"feed": "iex"},
            )
            if resp.status_code == 200:
                q = resp.json().get("quote", {})
                bid = float(q.get("bp", 0) or 0)
                ask = float(q.get("ap", 0) or 0)
                if bid > 0 and ask > 0:
                    spread = ask - bid
                    spread_pct = round((spread / ask) * 100, 3)
                    result["bid"] = round(bid, 2)
                    result["ask"] = round(ask, 2)
                    result["spread_pct"] = spread_pct
                    result["spread_label"] = (
                        "wide — caution" if spread_pct > 0.5 else
                        "moderate"       if spread_pct > 0.2 else
                        "tight — ok"
                    )

            # VWAP from today's 1-min bars (last 390 bars = full trading day)
            from datetime import date
            today = date.today().isoformat()
            resp2 = await self._data_http.get(
                f"/v2/stocks/{ticker}/bars",
                params={
                    "timeframe": "1Min",
                    "start": f"{today}T09:30:00Z",
                    "limit": 390,
                    "feed": "iex",
                    "adjustment": "raw",
                },
            )
            if resp2.status_code == 200:
                bars = resp2.json().get("bars", [])
                if bars:
                    # VWAP = sum(typical_price * volume) / sum(volume)
                    cum_tp_vol = sum(
                        ((b["h"] + b["l"] + b["c"]) / 3) * b["v"]
                        for b in bars if b.get("v", 0) > 0
                    )
                    cum_vol = sum(b["v"] for b in bars if b.get("v", 0) > 0)
                    if cum_vol > 0:
                        vwap = round(cum_tp_vol / cum_vol, 2)
                        vwap_gap = round(((current_price - vwap) / vwap) * 100, 2)
                        result["vwap"] = vwap
                        result["vwap_gap_pct"] = vwap_gap
                        result["vwap_bias"] = (
                            "bullish — above VWAP" if vwap_gap > 0.15 else
                            "bearish — below VWAP" if vwap_gap < -0.15 else
                            "neutral — at VWAP"
                        )
        except Exception as e:
            _log("warning", "alpaca.order_flow_error", ticker=ticker, error=str(e))
        return result

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
