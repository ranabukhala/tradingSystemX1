"""
Broker abstraction layer.

All brokers implement BaseBroker so execution logic is broker-agnostic.
Swap between Alpaca paper / Alpaca live / IBKR demo / IBKR live
just by changing BROKER env var.

Data models:
  OrderRequest  — what we want to do
  OrderResult   — what the broker confirmed
  Position      — current open position
  AccountInfo   — account equity, buying power, etc.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4


class OrderSide(str, Enum):
    BUY  = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET     = "market"
    LIMIT      = "limit"
    STOP       = "stop"
    STOP_LIMIT = "stop_limit"


class TimeInForce(str, Enum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"


class OrderStatus(str, Enum):
    PENDING   = "pending"
    SUBMITTED = "submitted"
    PARTIAL   = "partial"
    FILLED    = "filled"
    CANCELLED = "cancelled"
    REJECTED  = "rejected"
    EXPIRED   = "expired"


@dataclass
class OrderRequest:
    """What we want the broker to do."""
    id: UUID = field(default_factory=uuid4)
    ticker: str = ""
    side: OrderSide = OrderSide.BUY
    qty: float = 0.0
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    stop_price: float | None = None
    time_in_force: TimeInForce = TimeInForce.GTC

    # Attached orders (bracket)
    take_profit_price: float | None = None
    stop_loss_price: float | None = None

    # Extended hours
    extended_hours: bool = False   # True → submit with extended_hours=True for pre/post market

    # Metadata
    signal_id: str = ""
    strategy: str = "news_catalyst"
    notes: str = ""


@dataclass
class OrderResult:
    """What the broker confirmed."""
    broker_order_id: str = ""
    request_id: UUID = field(default_factory=uuid4)
    ticker: str = ""
    side: OrderSide = OrderSide.BUY
    qty_requested: float = 0.0
    qty_filled: float = 0.0
    avg_fill_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    filled_at: datetime | None = None
    error: str = ""
    raw: dict = field(default_factory=dict)  # Raw broker response

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def fill_value(self) -> float:
        return self.qty_filled * self.avg_fill_price


@dataclass
class Position:
    """Current open position."""
    ticker: str
    qty: float           # positive = long, negative = short
    avg_entry: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    side: str            # "long" | "short"
    opened_at: datetime | None = None
    broker_position_id: str = ""


@dataclass
class AccountInfo:
    """Account snapshot."""
    equity: float            # Total account value
    cash: float              # Available cash
    buying_power: float      # Available buying power (may include margin)
    portfolio_value: float   # Market value of all positions
    daily_pnl: float         # Today's P&L
    daily_pnl_pct: float
    currency: str = "USD"
    broker: str = ""
    account_id: str = ""
    is_paper: bool = True


class BaseBroker(ABC):
    """
    Abstract base — all brokers implement this interface.
    """

    def __init__(self, paper: bool = True) -> None:
        self.paper = paper
        self.name = "base"

    @abstractmethod
    async def connect(self) -> None:
        """Initialize connection / auth."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up connections."""
        ...

    @abstractmethod
    async def get_account(self) -> AccountInfo:
        """Get current account info."""
        ...

    @abstractmethod
    async def get_positions(self) -> list[Position]:
        """Get all open positions."""
        ...

    @abstractmethod
    async def get_position(self, ticker: str) -> Position | None:
        """Get position for a specific ticker."""
        ...

    @abstractmethod
    async def submit_order(self, order: OrderRequest) -> OrderResult:
        """Submit an order. Returns result with broker order ID."""
        ...

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> bool:
        """Cancel a pending order."""
        ...

    @abstractmethod
    async def close_position(self, ticker: str) -> OrderResult:
        """Market close entire position."""
        ...

    async def get_order_flow_context(self, ticker: str, current_price: float) -> dict:
        """
        Optional: fetch bid/ask spread and VWAP for informational display.
        Default returns empty dict — brokers override if supported.
        """
        return {}

    @abstractmethod
    async def get_quote(self, ticker: str) -> float:
        """Get latest price for a ticker."""
        ...

    @abstractmethod
    async def get_quote_with_timestamp(self, ticker: str) -> tuple[float, datetime]:
        """
        Get latest price AND the UTC timestamp of when the quote was fetched.
        Returns (price, utc_datetime).
        The timestamp must be offset-aware (timezone.utc).
        """
        ...

    @abstractmethod
    async def get_order(self, broker_order_id: str) -> "OrderResult":
        """
        Fetch the latest status of an existing order by broker order ID.
        Used by FillPoller to check fill status for limit orders.
        Returns an updated OrderResult.
        """
        ...

    async def get_halted(self, ticker: str) -> bool:
        """
        Optional: check whether the exchange has halted trading for `ticker`.
        Default returns False (fail-open — assume not halted if unsupported).
        Brokers should override this for production use.
        """
        return False

    async def start_trade_stream(self, callback) -> None:
        """
        Optional: start a WebSocket/event stream for trade updates.
        `callback` will be called with trade update dicts as they arrive.
        Default raises NotImplementedError — brokers override if supported.
        PositionMonitor checks hasattr(broker, 'start_trade_stream') before calling.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support trade streaming")

    @abstractmethod
    async def is_market_open(self) -> bool:
        """Is the market currently open?"""
        ...
