"""
SQLAlchemy ORM models — maps Python classes to Postgres tables.
All tables use UUID primary keys.
price_bar is partitioned by published_at month.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey, Integer,
    String, Text, JSON, Index, UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
from sqlalchemy.orm import relationship

from app.db import Base


class NewsItem(Base):
    __tablename__ = "news_item"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String(32), nullable=False)
    vendor_id = Column(String(128), nullable=False)
    published_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    url = Column(Text, nullable=False)
    canonical_url = Column(Text)
    title = Column(Text, nullable=False)
    snippet = Column(Text)
    author = Column(String(256))
    content_hash = Column(String(64))
    full_text_ref = Column(String(512))

    # Dedup
    cluster_id = Column(UUID(as_uuid=True), ForeignKey("news_cluster.id"), nullable=True)
    is_representative = Column(Boolean, default=True)

    # Entity resolution
    tickers = Column(ARRAY(String), default=[])
    sectors = Column(ARRAY(String), default=[])
    themes = Column(ARRAY(String), default=[])
    ticker_confidence = Column(JSONB, default={})

    # Classification
    catalyst_type = Column(String(32), default="other")
    mode = Column(String(32), default="general_market")
    session_context = Column(String(32), default="intraday")
    market_cap_tier = Column(String(16))
    float_sensitivity = Column(String(16), default="normal")
    short_interest_flag = Column(Boolean, default=False)

    # Event linkage
    earnings_proximity_h = Column(Integer)
    event_ids = Column(ARRAY(UUID(as_uuid=True)), default=[])
    decay_minutes = Column(Integer)

    # AI outputs
    t1_summary = Column(Text)
    t2_summary = Column(Text)
    facts_json = Column(JSONB)

    # Scores
    impact_day = Column(Float)
    impact_swing = Column(Float)
    regime_flag = Column(String(32))
    source_credibility = Column(Float)

    # AI metadata
    prompt_version = Column(String(32))
    llm_tokens_used = Column(Integer)
    llm_cost_usd = Column(Float)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    cluster = relationship("NewsCluster", back_populates="items")
    entities = relationship("NewsEntity", back_populates="news_item")
    summaries = relationship("Summary", back_populates="news_item")

    __table_args__ = (
        UniqueConstraint("source", "vendor_id", name="uq_news_source_vendor"),
        Index("ix_news_published_at", "published_at"),
        Index("ix_news_mode", "mode"),
        Index("ix_news_catalyst_type", "catalyst_type"),
        Index("ix_news_content_hash", "content_hash", unique=True),
        Index("ix_news_impact_day", "impact_day"),
        Index("ix_news_impact_swing", "impact_swing"),
        # GIN index for array tickers search
        Index("ix_news_tickers_gin", "tickers", postgresql_using="gin"),
    )


class NewsCluster(Base):
    __tablename__ = "news_cluster"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    representative_id = Column(UUID(as_uuid=True), ForeignKey("news_item.id"), nullable=True)
    size = Column(Integer, default=1)
    dedup_method = Column(String(32))

    items = relationship("NewsItem", back_populates="cluster", foreign_keys=[NewsItem.cluster_id])


class Entity(Base):
    __tablename__ = "entity"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticker = Column(String(16), nullable=False, unique=True)
    company_name = Column(String(256))
    sector = Column(String(64))
    industry = Column(String(64))
    market_cap_tier = Column(String(16))
    short_interest_pct = Column(Float)
    short_interest_updated_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    news_entities = relationship("NewsEntity", back_populates="entity")

    __table_args__ = (
        Index("ix_entity_ticker", "ticker"),
        Index("ix_entity_sector", "sector"),
    )


class NewsEntity(Base):
    __tablename__ = "news_entity"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    news_item_id = Column(UUID(as_uuid=True), ForeignKey("news_item.id"), nullable=False)
    entity_id = Column(UUID(as_uuid=True), ForeignKey("entity.id"), nullable=False)
    confidence = Column(Float, nullable=False)

    news_item = relationship("NewsItem", back_populates="entities")
    entity = relationship("Entity", back_populates="news_entities")

    __table_args__ = (
        UniqueConstraint("news_item_id", "entity_id", name="uq_news_entity"),
        Index("ix_news_entity_news_id", "news_item_id"),
        Index("ix_news_entity_entity_id", "entity_id"),
    )


class Summary(Base):
    __tablename__ = "summary"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    news_item_id = Column(UUID(as_uuid=True), ForeignKey("news_item.id"), nullable=False)
    tier = Column(Integer, nullable=False)       # 1 = bullet facts, 2 = "so what"
    content = Column(Text, nullable=False)
    facts_json = Column(JSONB)
    prompt_version = Column(String(32))
    model = Column(String(64))
    input_tokens = Column(Integer)
    output_tokens = Column(Integer)
    cost_usd = Column(Float)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    news_item = relationship("NewsItem", back_populates="summaries")

    __table_args__ = (
        Index("ix_summary_news_item_id", "news_item_id"),
    )


class Event(Base):
    __tablename__ = "event"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String(32), nullable=False)    # earnings | macro
    ticker = Column(String(16))                         # null for macro events
    company_name = Column(String(256))
    name = Column(String(256))                          # For macro events
    fred_series_id = Column(String(32))
    event_date = Column(DateTime(timezone=True), nullable=False)
    event_time = Column(String(16))                     # BMO | AMC | Unknown | HH:MM
    fiscal_quarter = Column(String(16))
    fiscal_year = Column(Integer)

    # Estimates
    whisper_eps = Column(Float)
    consensus_eps = Column(Float)
    consensus_revenue = Column(Float)
    consensus_estimate = Column(Float)
    prior_value = Column(Float)

    # Actuals (null until event)
    actual_eps = Column(Float)
    actual_revenue = Column(Float)
    actual_value = Column(Float)
    reported_at = Column(DateTime(timezone=True))

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_event_date", "event_date"),
        Index("ix_event_ticker", "ticker"),
        Index("ix_event_type", "event_type"),
        UniqueConstraint("event_type", "ticker", "event_date", name="uq_event_ticker_date"),
    )


class PriceBarORM(Base):
    """
    Note: In production, partition this table by month on timestamp.
    For local dev, a regular table is fine.
    Run: scripts/create_price_partitions.sql for partitioning setup.
    """
    __tablename__ = "price_bar"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticker = Column(String(16), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    timeframe = Column(String(8), default="1min")
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    vwap = Column(Float)
    transactions = Column(Integer)
    pct_change = Column(Float)
    rel_volume = Column(Float)
    source = Column(String(32), default="polygon")
    received_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("ticker", "timestamp", "timeframe", name="uq_price_bar"),
        Index("ix_price_bar_ticker_timestamp", "ticker", "timestamp"),
        Index("ix_price_bar_timestamp", "timestamp"),
    )


class SectorMetric(Base):
    __tablename__ = "sector_metric"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sector = Column(String(64), nullable=False)
    snapshot_date = Column(DateTime(timezone=True), nullable=False)
    return_1d = Column(Float)
    return_5d = Column(Float)
    return_21d = Column(Float)
    advancers = Column(Integer)
    decliners = Column(Integer)
    ad_ratio = Column(Float)
    top_ticker = Column(String(16))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("sector", "snapshot_date", name="uq_sector_metric_date"),
        Index("ix_sector_metric_date", "snapshot_date"),
    )
