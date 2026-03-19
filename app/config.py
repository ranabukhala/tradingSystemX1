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
    fmp_daily_call_limit: int = 200  # Shared across all FMP containers; 200 leaves 50 as buffer on free/starter tier
    finnhub_api_key: str = ""
    finnhub_input_topic: str = "news.fmp_enriched"
    finlight_api_key: str = ""
    # OpenAI — used for embedding dedup (Tier 3.5).
    # Pydantic-settings will also check OPENAI env var as a fallback alias.
    openai_api_key: str = ""

    # ── Embedding Dedup (Tier 3.5) ────────────────────────────
    # Kill-switch: set False to skip embedding lookup entirely (falls through to SimHash).
    enable_embedding_dedup: bool = True
    # Cosine similarity threshold — headlines with cosine_similarity >= this are duplicates.
    # Empirically calibrated using text-embedding-3-small:
    #   Same-event, cross-vendor rewrites:  0.67 – 0.88  (should be caught)
    #   Different events, same company:     0.38 – 0.56  (should NOT be caught)
    #   Threshold at 0.65 sits in the 0.09-wide gap with comfortable margins either side.
    embedding_similarity_threshold: float = 0.65
    # Look-back window for embedding match queries (hours).
    embedding_lookback_hours: int = 6

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
    finnhub_poll_interval: int = 300              # 5 min — news doesn't change per-minute
    finnhub_per_minute_call_limit: int = 48      # 60 req/min hard cap − 12 buffer for rolling-window overlap
    earnings_sync_hour_et: int = 6
    earnings_lookahead_days: int = 30

    # Market hours (ET) as strings for simplicity
    market_premarket_start: str = "04:00"
    market_open: str = "09:30"
    market_close: str = "16:00"
    market_afterhours_end: str = "20:00"

    # ── LLM Cost Controls ─────────────────────────────────────
    llm_daily_budget_usd: float = 15.0
    llm_monthly_budget_usd: float = 500.0  # Monthly LLM budget cap (all providers)
    llm_t2_impact_threshold: float = 0.6
    llm_fulltext_impact_threshold: float = 0.7

    # ── Signal thresholds ─────────────────────────────────────
    signal_conviction_threshold: float = 0.55
    signal_alert_threshold: float = 0.60

    # ── Fact cross-validation (v1.7) ──────────────────────────
    # Kill-switch: set to false to disable DB cross-checks entirely
    enable_fact_validation: bool = True
    # Numeric tolerance for EPS fields (LLM rounds to 2 dp; 5 % is fair)
    fact_eps_tolerance_pct: float = 5.0
    # Price-target tolerance (analyst PTs vary by ±5–8 % in vendor feeds)
    fact_pt_tolerance_pct: float = 10.0
    # Deal price tolerance (M&A premiums are exact; 2 % is generous)
    fact_deal_price_tolerance_pct: float = 2.0
    # How far back to look for matching vendor earnings actuals
    fact_earnings_lookback_days: int = 3
    # How far back to look for matching analyst grade entries
    fact_analyst_lookback_hours: int = 48
    # Conviction multipliers applied after all other adjustments
    fact_conviction_confirmed: float = 1.10       # LLM was accurate — small boost
    fact_conviction_partial: float = 0.85         # Mixed: some unverifiable
    fact_conviction_mismatch_key: float = 0.30    # Direction field wrong — heavy penalty
    fact_conviction_mismatch_nonkey: float = 0.60 # Magnitude field wrong — moderate
    fact_conviction_unverifiable: float = 0.90    # No vendor data — small uncertainty discount
    # Write audit row for all validation statuses (not just MISMATCH)
    fact_audit_all_statuses: bool = False

    # ── Conviction scoring v1.8 ───────────────────────────────
    # Scoring formula version.
    #   v1  Identical to pre-v1.8 multiplicative formula (default, no behaviour change).
    #   v2  Enables optional correlation-risk mitigations (requires conviction_correlation_guard=true).
    conviction_scoring_mode: str = "v1"

    # When true AND conviction_scoring_mode=v2, apply correlation mitigations:
    #   • Catalyst-weight compression toward 1.0 (reduces cat_w × impact_day compound)
    #   • Earnings combined-factor cap (bounds cat_w_eff × proximity_bonus)
    conviction_correlation_guard: bool = False

    # Calibration function applied after the raw multiplicative formula.
    #   identity  No change — preserves pre-v1.8 output exactly (default)
    #   sigmoid   σ-curve centred at conviction_sigmoid_center
    #   linear    Per-catalyst slope × raw + bias (set via CONVICTION_LINEAR_PARAMS JSON)
    conviction_calibration_fn: str = "identity"

    # v2 correlation mitigations — tuneable without code changes
    # Compression factor: cat_w_eff = 1.0 + (cat_w - 1.0) × factor
    # 0.5 → range [0.3, 1.8] compresses to [0.65, 1.40]
    conviction_catalyst_compression: float = 0.5
    # Cap on combined earnings amplification: cat_w_eff × proximity_bonus ≤ cap
    conviction_earnings_cap: float = 1.80

    # Sigmoid calibration parameters
    conviction_sigmoid_center:    float = 0.55
    conviction_sigmoid_steepness: float = 8.0

    # Correlation-risk detection thresholds
    corr_cat_weight_threshold: float = 1.4
    corr_impact_day_threshold: float = 0.75

    # Feature logging — write ConvictionBreakdown to signal_feature_log
    #   true   Log every emitted signal (recommended for calibration research)
    #   false  Disable logging (reduces DB writes in high-volume environments)
    conviction_log_features: bool = True
    # Also log dropped signals (signals that did not cross the conviction threshold).
    # High volume — enable sparingly (e.g., during calibration research only).
    conviction_log_dropped_features: bool = False

    # ── Catalyst-aware pretrade policies (v1.9) ───────────────────────────
    # Kill-switch: set False to disable policy layer entirely (pure 4-filter mode).
    enable_catalyst_policy: bool = True

    # Sympathy trade threshold bump (added on top of per-catalyst threshold).
    policy_sympathy_threshold_bump: float = 0.08

    # Per-catalyst conviction thresholds (override via env: POLICY_THRESHOLD_EARNINGS=0.52)
    policy_threshold_earnings:   float = 0.55
    policy_threshold_analyst:    float = 0.60
    policy_threshold_regulatory: float = 0.58
    policy_threshold_filing:     float = 0.58
    policy_threshold_ma:         float = 0.50
    policy_threshold_macro:      float = 0.60
    policy_threshold_legal:      float = 0.62
    policy_threshold_other:      float = 0.60

    # Per-catalyst interpretive (LLM-only direction) conviction penalty multipliers
    policy_interp_penalty_earnings:   float = 0.70
    policy_interp_penalty_analyst:    float = 0.60
    policy_interp_penalty_regulatory: float = 0.75
    policy_interp_penalty_filing:     float = 0.75
    policy_interp_penalty_ma:         float = 0.65
    policy_interp_penalty_macro:      float = 0.80
    policy_interp_penalty_legal:      float = 0.65
    policy_interp_penalty_other:      float = 0.80

    # Per-catalyst options flow weight multipliers
    policy_options_weight_earnings:   float = 1.30
    policy_options_weight_analyst:    float = 0.80
    policy_options_weight_regulatory: float = 1.10
    policy_options_weight_filing:     float = 1.10
    policy_options_weight_ma:         float = 1.20
    policy_options_weight_macro:      float = 1.10
    policy_options_weight_legal:      float = 0.90
    policy_options_weight_other:      float = 1.00

    # Market-cap tier options weight scalers
    policy_mktcap_mega_options_scale:  float = 0.90
    policy_mktcap_large_options_scale: float = 0.90
    policy_mktcap_mid_options_scale:   float = 1.00
    policy_mktcap_small_options_scale: float = 1.20
    policy_mktcap_micro_options_scale: float = 1.20

    # Market-cap tier regime scale adjustments (multiplied on top of regime_scale)
    policy_mktcap_mega_regime_adj:  float = 1.10
    policy_mktcap_large_regime_adj: float = 1.10
    policy_mktcap_mid_regime_adj:   float = 1.00
    policy_mktcap_small_regime_adj: float = 0.85
    policy_mktcap_micro_regime_adj: float = 0.85

    # Minimum 0-10 composite tech score per catalyst family (below = block)
    policy_min_tech_score_earnings:   int = 7
    policy_min_tech_score_analyst:    int = 8
    policy_min_tech_score_regulatory: int = 7
    policy_min_tech_score_filing:     int = 7
    policy_min_tech_score_ma:         int = 6
    policy_min_tech_score_macro:      int = 7
    policy_min_tech_score_legal:      int = 7
    policy_min_tech_score_other:      int = 7

    # Per-catalyst sympathy conviction multipliers
    policy_sympathy_mult_earnings:   float = 0.80
    policy_sympathy_mult_analyst:    float = 0.75
    policy_sympathy_mult_regulatory: float = 0.85
    policy_sympathy_mult_filing:     float = 0.85
    policy_sympathy_mult_ma:         float = 0.90
    policy_sympathy_mult_macro:      float = 0.80
    policy_sympathy_mult_legal:      float = 0.80
    policy_sympathy_mult_other:      float = 0.80

    # Volume confirmation required (blocks signals without volume > 1.5× avg)
    policy_require_volume_earnings: bool = True
    policy_require_volume_analyst:  bool = False
    policy_require_volume_ma:       bool = True
    policy_require_volume_other:    bool = False

    # ── Portfolio Risk Manager (v1.10) ────────────────────────────────────
    # Kill-switch: set False to bypass portfolio risk entirely
    # (signals.filtered flows directly to execution engine).
    enable_portfolio_risk_manager: bool = True

    # Manual kill switch Redis key (set "1" to halt, delete to resume)
    risk_kill_switch_redis_key: str = "risk:kill_switch"

    # Auto-halt thresholds
    risk_halt_drawdown_pct: float = 0.08          # Halt if equity drops 8% below HWM
    risk_halt_consecutive_losses: int = 5          # Halt after N consecutive losses

    # Portfolio-level exposure caps
    risk_max_portfolio_heat_pct: float = 0.20     # Max 20% of equity in open risk
    risk_max_ticker_exposure_usd: float = 10000.0 # Max USD in any single ticker
    risk_max_ticker_exposure_pct: float = 0.05    # Max 5% of equity in any single ticker

    # Sector concentration — aligns with existing risk_manager stub
    risk_max_sector_pct: float = 0.25

    # Catalyst-family max concurrent open positions
    risk_max_catalyst_earnings_positions:   int = 3
    risk_max_catalyst_analyst_positions:    int = 4
    risk_max_catalyst_regulatory_positions: int = 2
    risk_max_catalyst_ma_positions:         int = 2
    risk_max_catalyst_macro_positions:      int = 3
    risk_max_catalyst_default_positions:    int = 5

    # Correlation proxy — block if ≥ N open positions share (sector, direction)
    risk_max_correlated_positions: int = 3

    # Per-event (news cluster) dedup
    risk_max_trades_per_cluster: int = 1
    risk_cluster_ttl_seconds: int = 3600          # Track clusters for 1 hour

    # Position / account cache TTL inside the risk service
    risk_position_cache_ttl_seconds: int = 30

    # ── Execution Quality (v1.11) ──────────────────────────────────────────────
    # EntryValidator — pre-entry safety checks
    entry_max_quote_age_seconds: float = 10.0        # Reject if quote older than this
    entry_max_price_drift_pct: float = 1.5           # Reject if price moved >1.5% since signal
    entry_halt_check_enabled: bool = True            # Set False to skip halt detection

    # OrderTypePolicy — ADV tier thresholds
    order_adv_liquid_threshold: int = 5_000_000      # ADV ≥ this → "liquid" tier
    order_adv_thin_threshold:   int = 500_000        # ADV < this → "thin" tier

    # Per-catalyst, per-ADV-tier order type  (market | limit | limit_ioc)
    order_policy_earnings_liquid_type:     str = "market"
    order_policy_earnings_normal_type:     str = "limit_ioc"
    order_policy_earnings_thin_type:       str = "limit"
    order_policy_analyst_liquid_type:      str = "limit"
    order_policy_analyst_normal_type:      str = "limit"
    order_policy_analyst_thin_type:        str = "limit"
    order_policy_ma_liquid_type:           str = "market"
    order_policy_ma_normal_type:           str = "limit_ioc"
    order_policy_ma_thin_type:             str = "limit"
    order_policy_regulatory_liquid_type:   str = "market"
    order_policy_regulatory_normal_type:   str = "limit_ioc"
    order_policy_regulatory_thin_type:     str = "limit_ioc"
    order_policy_macro_liquid_type:        str = "market"
    order_policy_macro_normal_type:        str = "market"
    order_policy_macro_thin_type:          str = "limit"
    order_policy_default_type:             str = "limit"

    # Limit price slippage tolerance — how far above ask (long) / below bid (short)
    order_limit_slippage_liquid_pct: float = 0.05    # 0.05% for liquid tickers
    order_limit_slippage_normal_pct: float = 0.15    # 0.15% for normal
    order_limit_slippage_thin_pct:   float = 0.30    # 0.30% for thin

    # FillPoller — partial fill handling
    fill_poll_interval_seconds: float = 2.0          # Poll every N seconds
    fill_poll_max_attempts: int = 15                 # Give up after 15 polls (30 s)
    fill_min_partial_pct: float = 0.80               # Accept ≥80% partial fill

    # ATR-based stops
    risk_atr_sl_multiplier: float = 2.0              # SL = entry ± ATR × multiplier
    risk_atr_tp_multiplier: float = 4.0              # TP = entry ± ATR × multiplier (2:1 R:R)

    # Stock context service URL (shared with pretrade filter)
    stock_context_url: str = "http://stock_context:8082"

    # Position monitor
    position_monitor_stream_enabled: bool = True
    position_monitor_poll_interval_seconds: int = 30

    # Execution quality row persistence
    execution_quality_enabled: bool = True

    # ── Idempotency backend ───────────────────────────────────
    # "redis"  — async SET NX EX (atomic, cross-instance, default)
    # "sqlite" — sync WAL-mode SQLite (single-container fallback / rollback path)
    idempotency_backend: str = "redis"
    # TTL for pipeline event keys in Redis.  48 h = 2× content-dedup TTL (24 h).
    # Events older than this that arrive via deep replay will be re-processed;
    # the downstream deduplicator's 4-tier content dedup prevents content dups.
    idempotency_ttl_seconds: int = 172800
    # When true, an in-process SQLite store acts as a hot-spare when Redis
    # is temporarily unreachable.  Set false to disable the fallback entirely
    # (fail-open behaviour is preserved — the pipeline never stalls).
    idempotency_fallback_enabled: bool = True

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
    def topic_signals_risk_approved(self) -> str:
        return "signals.risk_approved"

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
