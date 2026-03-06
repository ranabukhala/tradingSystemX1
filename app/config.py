"""
Centralized configuration — loads from .env via pydantic-settings.
All services import from here. Never read os.environ directly.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Environment ───────────────────────────────────────────
    environment: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"
    metrics_port: int = 8000

    # ── Postgres ──────────────────────────────────────────────
    postgres_user: str = "trading"
    postgres_password: str = "tradingpass"
    postgres_db: str = "trading_db"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    database_url: str = "postgresql+asyncpg://trading:tradingpass@localhost:5432/trading_db"

    # ── Redis ─────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"
    redis_dedup_ttl_seconds: int = 86400

    # ── Kafka / Redpanda ──────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_group_id: str = "trading-pipeline"

    # ── MinIO ─────────────────────────────────────────────────
    minio_root_user: str = "minioadmin"
    minio_root_password: str = "minioadmin123"
    minio_endpoint: str = "localhost:9000"
    minio_secure: bool = False
    minio_bucket_fulltext: str = "news-fulltext"
    minio_bucket_raw: str = "news-raw-payloads"
    minio_bucket_summaries: str = "summaries"
    minio_bucket_snapshots: str = "snapshots"

    # ── Vendor API Keys ───────────────────────────────────────
    benzinga_api_key: str = ""
    polygon_api_key: str = ""
    earnings_whispers_api_key: str = ""
    fred_api_key: str = ""
    anthropic_api_key: str = ""

    # ── Pipeline Settings ─────────────────────────────────────
    benzinga_poll_interval_seconds: int = 30
    polygon_news_poll_interval_seconds: int = 60
    earnings_sync_hour_et: int = 6
    earnings_lookahead_days: int = 30

    # Market hours (ET) as strings for simplicity
    market_premarket_start: str = "04:00"
    market_open: str = "09:30"
    market_close: str = "16:00"
    market_afterhours_end: str = "20:00"

    # ── LLM Cost Controls ─────────────────────────────────────
    llm_daily_budget_usd: float = 25.0
    llm_t2_impact_threshold: float = 0.6
    llm_fulltext_impact_threshold: float = 0.7

    # ── Kafka Topic Names (single source of truth) ────────────
    @property
    def topic_news_raw(self) -> str:
        return "news.raw"

    @property
    def topic_news_normalized(self) -> str:
        return "news.normalized"

    @property
    def topic_news_deduped(self) -> str:
        return "news.deduped"

    @property
    def topic_news_enriched(self) -> str:
        return "news.enriched"

    @property
    def topic_news_summarized(self) -> str:
        return "news.summarized"

    @property
    def topic_events_calendar(self) -> str:
        return "events.calendar"

    @property
    def topic_prices_bars(self) -> str:
        return "prices.bars"

    def validate_required_keys(self) -> list[str]:
        """Returns list of missing required API keys."""
        missing = []
        if not self.benzinga_api_key:
            missing.append("BENZINGA_API_KEY")
        if not self.polygon_api_key:
            missing.append("POLYGON_API_KEY")
        if not self.earnings_whispers_api_key:
            missing.append("EARNINGS_WHISPERS_API_KEY")
        if not self.fred_api_key:
            missing.append("FRED_API_KEY")
        return missing


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# Convenience alias
settings = get_settings()
