"""Calendar event models — earnings and macro events."""
from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    EARNINGS = "earnings"
    MACRO = "macro"
    SPLIT = "split"
    DIVIDEND = "dividend"
    CONFERENCE = "conference"


class EarningsTime(str, Enum):
    BMO = "BMO"        # Before Market Open
    AMC = "AMC"        # After Market Close
    UNKNOWN = "Unknown"


class EarningsEvent(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    event_type: EventType = EventType.EARNINGS
    ticker: str
    company_name: str
    event_date: date
    event_time: EarningsTime = EarningsTime.UNKNOWN
    fiscal_quarter: str | None = None     # e.g. "Q4 2024"
    fiscal_year: int | None = None

    # Estimates (pre-event)
    whisper_eps: float | None = None
    consensus_eps: float | None = None
    consensus_revenue: float | None = None

    # Actuals (post-event, null until reported)
    actual_eps: float | None = None
    actual_revenue: float | None = None
    reported_at: datetime | None = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "EarningsEvent":
        return cls.model_validate(data)


class MacroEvent(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    event_type: EventType = EventType.MACRO
    name: str                   # e.g. "CPI", "FOMC Rate Decision"
    fred_series_id: str | None = None   # e.g. "CPIAUCSL"
    event_date: date
    release_time_et: str | None = None  # e.g. "08:30"

    # Estimates
    consensus_estimate: float | None = None
    prior_value: float | None = None

    # Actuals
    actual_value: float | None = None
    revised_value: float | None = None
    reported_at: datetime | None = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_dict(cls, data: dict) -> "MacroEvent":
        return cls.model_validate(data)
