"""
Centralized configuration — loads from .env via pydantic-settings.
All services import from here. Never read os.environ directly.
"""
from __future__ import annotations

import sys
from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
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
    service_name: str = "trading-system"

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
    alphavantage_api_key: str = ""
    earnings_whispers_api_key: str = ""
    fred_api_key: str = ""
    anthropic_api_key: str = ""
    fmp_api_key: str = ""
    fmp_plan: str = "starter"
    finnhub_api_key: str = ""
    finnhub_input_topic: str = "news.fmp_enriched"

    # ── Broker ────────────────────────────────────────────────
    broker: str = "alpaca_paper"
    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    ibkr_host: str = "host.docker.internal"
    ibkr_port: int = 4002
    ibkr_client_id: int = 1

    # ── Trading Safety ────────────────────────────────────────
    confirm_live_trading: bool = False  # Must be True for live orders
    risk_max_position_pct: float = 0.02
    risk_max_position_usd: float = 5000.0
    risk_max_daily_loss_pct: float = 0.05
    risk_max_open_positions: int = 10
    risk_min_conviction: float = 0.40
    risk_allow_premarket: bool = True
    risk_allow_afterhours: bool = False
    risk_take_profit_pct: float = 0.10
    risk_stop_loss_pct: float = 0.04
    risk_use_bracket_orders: bool = True
    risk_max_orders_per_hour: int = 20
    risk_max_orders_per_day: int = 120  # Daily cap (separate from hourly rate limit)
    risk_consecutive_loss_halt: int = 5
    risk_daily_loss_cap_usd: float = 2000.0
    risk_cooldown_after_loss_minutes: int = 30
    risk_min_avg_volume: int = 100000
    risk_max_spread_pct: float = 0.02

    # ── Telegram ──────────────────────────────────────────────
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    admin_bot_token: str = ""
    admin_chat_id: str = ""

    # ── Pipeline Settings ─────────────────────────────────────
    benzinga_poll_interval_seconds: int = 30
    polygon_news_poll_interval_seconds: int = 60
    alphavantage_poll_interval_seconds: int = 300
    finnhub_poll_interval: int = 30
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

    # ── Signal thresholds ─────────────────────────────────────
    signal_conviction_threshold: float = 0.55
    signal_alert_threshold: float = 0.60

    # ── SQLite state DB ───────────────────────────────────────
    data_dir: str = "/data"
    sqlite_db_path: str = ""  # Computed below if empty

    @model_validator(mode="after")
    def _compute_sqlite_path(self) -> "Settings":
        if not self.sqlite_db_path:
            self.sqlite_db_path = f"{self.data_dir}/state.db"
        return self

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
    def topic_news_fmp_enriched(self) -> str:
        return "news.fmp_enriched"

    @property
    def topic_news_summarized(self) -> str:
        return "news.summarized"

    @property
    def topic_signals_actionable(self) -> str:
        return "signals.actionable"

    @property
    def topic_trades_executed(self) -> str:
        return "trades.executed"

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
        return missing

    def is_live_broker(self) -> bool:
        return self.broker.endswith("_live")

    def require_live_trading_confirmation(self) -> None:
        """Fail fast if live broker selected without confirmation flag."""
        if self.is_live_broker() and not self.confirm_live_trading:
            print(
                "FATAL: Live broker selected (BROKER={}) but CONFIRM_LIVE_TRADING "
                "is not set to true. Set CONFIRM_LIVE_TRADING=true to proceed "
                "with live trading. This is a safety guard to prevent accidental "
                "live order submission.".format(self.broker),
                file=sys.stderr,
            )
            sys.exit(1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# Convenience alias
settings = get_settings()
