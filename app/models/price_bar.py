"""Price bar model — OHLCV + derived fields."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class PriceBar(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    ticker: str
    timestamp: datetime          # Bar start time, UTC
    timeframe: str = "1min"      # "1min" | "5min" | "1h" | "1d"

    # OHLCV
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float | None = None
    transactions: int | None = None     # Number of trades in bar

    # Derived (computed on insert)
    pct_change: float | None = None     # vs prior close
    rel_volume: float | None = None     # vs 30-day avg volume

    source: str = "polygon"
    received_at: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "PriceBar":
        return cls.model_validate(data)
