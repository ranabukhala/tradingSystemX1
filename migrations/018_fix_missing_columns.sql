-- Migration 018: Fix missing columns and widen narrow VARCHAR fields
-- Idempotent — safe to run multiple times
-- Generated: 2026-03-18
-- Fixes: catalyst_policy_applied missing, technical_score missing,
--        VARCHAR(16) overflow on signal_log
-- Also ensures all tables/columns from migrations 009–017 are present.

BEGIN;

-- ── 1. Add confirmed-missing columns to signal_log ────────────────────────────

-- From migration 008_technical_score
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_log' AND column_name = 'technical_score'
    ) THEN
        ALTER TABLE signal_log ADD COLUMN technical_score double precision;
        RAISE NOTICE 'Added signal_log.technical_score';
    ELSE
        RAISE NOTICE 'signal_log.technical_score already exists — skipping';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_log' AND column_name = 'technical_score_breakdown'
    ) THEN
        ALTER TABLE signal_log ADD COLUMN technical_score_breakdown jsonb;
        RAISE NOTICE 'Added signal_log.technical_score_breakdown';
    ELSE
        RAISE NOTICE 'signal_log.technical_score_breakdown already exists — skipping';
    END IF;
END $$;

-- From migration 014_signal_log_catalyst_policy
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_log' AND column_name = 'catalyst_policy_applied'
    ) THEN
        ALTER TABLE signal_log ADD COLUMN catalyst_policy_applied boolean DEFAULT NULL;
        RAISE NOTICE 'Added signal_log.catalyst_policy_applied';
    ELSE
        RAISE NOTICE 'signal_log.catalyst_policy_applied already exists — skipping';
    END IF;
END $$;


-- ── 2. Widen VARCHAR(16) columns at overflow risk ─────────────────────────────
-- ALTER COLUMN TYPE is idempotent-safe: widening VARCHAR never loses data.

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_log'
          AND column_name = 'market_cap_tier'
          AND character_maximum_length <= 16
    ) THEN
        ALTER TABLE signal_log ALTER COLUMN market_cap_tier TYPE VARCHAR(64);
        RAISE NOTICE 'Widened signal_log.market_cap_tier to VARCHAR(64)';
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_log'
          AND column_name = 'float_sensitivity'
          AND character_maximum_length <= 16
    ) THEN
        ALTER TABLE signal_log ALTER COLUMN float_sensitivity TYPE VARCHAR(64);
        RAISE NOTICE 'Widened signal_log.float_sensitivity to VARCHAR(64)';
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_log'
          AND column_name = 'filter_flow_bias'
          AND character_maximum_length <= 16
    ) THEN
        -- filter_funnel view references filter_flow_bias; drop it first, recreate below.
        DROP VIEW IF EXISTS filter_funnel;
        ALTER TABLE signal_log ALTER COLUMN filter_flow_bias TYPE VARCHAR(64);
        RAISE NOTICE 'Widened signal_log.filter_flow_bias to VARCHAR(64)';
    END IF;
END $$;

-- Recreate filter_funnel (idempotent — OR REPLACE is safe even if it still exists)
CREATE OR REPLACE VIEW filter_funnel AS
SELECT
    date_trunc('hour', created_at)                                              AS hour,
    count(*)                                                                    AS total,
    sum(passed_gate::int)                                                       AS passed_conviction,
    sum((NOT COALESCE(filter_blocked, false))::int)                             AS passed_filter,
    sum(COALESCE(filter_blocked, false)::int)                                   AS blocked_by_filter,
    sum(((filter_regime)::text = 'risk_off')::int)                              AS risk_off_count,
    round(avg(filter_tech_score)::numeric, 3)                                   AS avg_tech_score,
    sum(((filter_flow_bias)::text = 'bearish' AND (direction)::text = 'long')::int)
                                                                                AS options_warned_longs,
    sum((COALESCE(filter_squeeze_score, 0.0) > 0.5)::int)                      AS squeeze_candidates
FROM signal_log
GROUP BY date_trunc('hour', created_at)
ORDER BY date_trunc('hour', created_at) DESC;

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'signal_log'
          AND column_name = 'prompt_version'
          AND character_maximum_length <= 16
    ) THEN
        ALTER TABLE signal_log ALTER COLUMN prompt_version TYPE VARCHAR(64);
        RAISE NOTICE 'Widened signal_log.prompt_version to VARCHAR(64)';
    END IF;
END $$;


-- ── 3. Ensure tables and columns from migrations 009–017 are present ──────────

-- ── 009: stock_context_log + watchlist + signal_log context columns ───────────

CREATE TABLE IF NOT EXISTS stock_context_log (
    id                 SERIAL PRIMARY KEY,
    ticker             VARCHAR(10)  NOT NULL,
    evaluated_at       TIMESTAMPTZ  NOT NULL,
    trend_regime       VARCHAR(20),
    volatility_regime  VARCHAR(20),
    cleanliness        VARCHAR(20),
    adx                NUMERIC(6,2),
    ma_slope_20        NUMERIC(8,4),
    bb_width_trend     VARCHAR(20),
    atr_ratio          NUMERIC(6,3),
    adjusted_threshold INTEGER,
    raw_metrics        JSONB,
    created_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_context_ticker_time
    ON stock_context_log (ticker, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_stock_context_trend
    ON stock_context_log (trend_regime, evaluated_at DESC);

-- signal_log context columns (ADD COLUMN IF NOT EXISTS is natively idempotent)
ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS trend_regime        VARCHAR(20),
    ADD COLUMN IF NOT EXISTS volatility_regime   VARCHAR(20),
    ADD COLUMN IF NOT EXISTS cleanliness         VARCHAR(20),
    ADD COLUMN IF NOT EXISTS adjusted_threshold  INTEGER;

CREATE INDEX IF NOT EXISTS idx_signal_log_trend_regime
    ON signal_log (trend_regime)
    WHERE trend_regime IS NOT NULL;

CREATE TABLE IF NOT EXISTS watchlist (
    id         SERIAL PRIMARY KEY,
    ticker     VARCHAR(10) NOT NULL UNIQUE,
    active     BOOLEAN DEFAULT TRUE,
    added_at   TIMESTAMPTZ DEFAULT NOW(),
    notes      TEXT
);

CREATE INDEX IF NOT EXISTS idx_watchlist_active
    ON watchlist (active)
    WHERE active = TRUE;

CREATE OR REPLACE VIEW stock_context_latest AS
SELECT DISTINCT ON (ticker)
    ticker, evaluated_at, trend_regime, volatility_regime,
    cleanliness, adx, ma_slope_20, bb_width_trend, atr_ratio, adjusted_threshold
FROM stock_context_log
ORDER BY ticker, evaluated_at DESC;

CREATE OR REPLACE VIEW signal_performance_by_context AS
SELECT
    sl.trend_regime, sl.volatility_regime, sl.cleanliness, sl.adjusted_threshold,
    COUNT(*)                                                   AS total_signals,
    SUM(COALESCE(filter_blocked, FALSE)::int)                  AS blocked,
    ROUND(AVG(conviction)::numeric, 3)                         AS avg_conviction,
    ROUND(AVG(technical_score)::numeric, 1)                    AS avg_tech_score,
    ROUND(100.0 * SUM(outcome_correct::int)
          / NULLIF(COUNT(outcome_correct), 0), 1)              AS accuracy_pct
FROM signal_log sl
WHERE sl.trend_regime IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY total_signals DESC;


-- ── 010: dropped_events + signal_log/trade dedup columns ─────────────────────

CREATE TABLE IF NOT EXISTS dropped_events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source           TEXT        NOT NULL,
    vendor_id        TEXT        NOT NULL,
    title            TEXT        NOT NULL,
    published_at     TIMESTAMPTZ NOT NULL,
    cluster_id       UUID,
    dedup_tier       TEXT,
    dedup_reason     TEXT,
    similarity_score NUMERIC(5,4),
    dropped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dropped_events_cluster
    ON dropped_events (cluster_id, dropped_at DESC);

CREATE INDEX IF NOT EXISTS idx_dropped_events_source_vendor
    ON dropped_events (source, vendor_id);

CREATE INDEX IF NOT EXISTS idx_dropped_events_tier_reason
    ON dropped_events (dedup_tier, dedup_reason, dropped_at DESC);

ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS cluster_id          UUID,
    ADD COLUMN IF NOT EXISTS event_gate_blocked  BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS dedup_tier          TEXT,
    ADD COLUMN IF NOT EXISTS dedup_reason        TEXT;

ALTER TABLE trade
    ADD COLUMN IF NOT EXISTS cluster_id          UUID,
    ADD COLUMN IF NOT EXISTS order_gate_blocked  BOOLEAN DEFAULT FALSE;

CREATE OR REPLACE VIEW event_cluster_summary AS
SELECT
    cluster_id,
    COUNT(*)                                    AS total_dropped,
    COUNT(DISTINCT source)                      AS vendor_count,
    MIN(dropped_at)                             AS first_seen,
    MAX(dropped_at)                             AS last_seen,
    EXTRACT(EPOCH FROM (MAX(dropped_at) - MIN(dropped_at))) / 60
                                                AS spread_minutes,
    MODE() WITHIN GROUP (ORDER BY dedup_tier)   AS primary_tier,
    MODE() WITHIN GROUP (ORDER BY dedup_reason) AS primary_reason
FROM dropped_events
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;

CREATE OR REPLACE VIEW event_signal_duplicates AS
SELECT sl.ticker, sl.cluster_id, sl.direction, sl.conviction,
       sl.catalyst_type, sl.created_at, sl.event_gate_blocked, sl.dedup_reason
FROM signal_log sl
WHERE sl.event_gate_blocked = TRUE
ORDER BY sl.created_at DESC;

CREATE OR REPLACE VIEW event_order_duplicates AS
SELECT t.ticker, t.cluster_id, t.direction, t.conviction,
       t.catalyst_type, t.created_at, t.order_gate_blocked
FROM trade t
WHERE t.order_gate_blocked = TRUE
ORDER BY t.created_at DESC;


-- ── 011: staleness_log + kafka_lag_log + signal_log/trade staleness columns ───

ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS signal_age_seconds    NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS staleness_blocked     BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS staleness_reason      TEXT,
    ADD COLUMN IF NOT EXISTS max_age_seconds       NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS news_published_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pipeline_latency_ms   INTEGER;

CREATE INDEX IF NOT EXISTS idx_signal_log_staleness
    ON signal_log (staleness_blocked, catalyst_type, created_at DESC)
    WHERE staleness_blocked = TRUE;

CREATE INDEX IF NOT EXISTS idx_signal_log_latency
    ON signal_log (catalyst_type, pipeline_latency_ms)
    WHERE pipeline_latency_ms IS NOT NULL;

ALTER TABLE trade
    ADD COLUMN IF NOT EXISTS staleness_blocked     BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS lag_blocked           BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS consumer_lag          INTEGER,
    ADD COLUMN IF NOT EXISTS signal_age_seconds    NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS pipeline_latency_ms   INTEGER;

CREATE INDEX IF NOT EXISTS idx_trade_staleness
    ON trade (staleness_blocked, lag_blocked, created_at DESC)
    WHERE staleness_blocked = TRUE OR lag_blocked = TRUE;

CREATE TABLE IF NOT EXISTS staleness_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT        NOT NULL,
    ticker          TEXT        NOT NULL,
    catalyst_type   TEXT,
    session_context TEXT,
    route_type      TEXT,
    signal_age_s    NUMERIC(10,2) NOT NULL,
    max_age_s       NUMERIC(10,2) NOT NULL,
    reason          TEXT        NOT NULL,
    news_published_at TIMESTAMPTZ,
    blocked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_staleness_log_ticker_time
    ON staleness_log (ticker, blocked_at DESC);

CREATE INDEX IF NOT EXISTS idx_staleness_log_catalyst
    ON staleness_log (catalyst_type, reason, blocked_at DESC);

CREATE TABLE IF NOT EXISTS kafka_lag_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service         TEXT        NOT NULL,
    consumer_lag    INTEGER     NOT NULL,
    lag_threshold   INTEGER     NOT NULL,
    blocked         BOOLEAN     NOT NULL DEFAULT FALSE,
    ticker          TEXT,
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kafka_lag_service_time
    ON kafka_lag_log (service, logged_at DESC);

CREATE OR REPLACE VIEW stale_signal_summary AS
SELECT
    catalyst_type, session_context, staleness_reason,
    COUNT(*)                                                AS total_blocked,
    ROUND(AVG(signal_age_seconds)::numeric, 1)              AS avg_age_seconds,
    ROUND(MAX(signal_age_seconds)::numeric, 1)              AS max_age_seconds,
    ROUND(AVG(max_age_seconds)::numeric, 1)                 AS avg_limit_seconds,
    MIN(created_at)                                         AS first_seen,
    MAX(created_at)                                         AS last_seen
FROM signal_log
WHERE staleness_blocked = TRUE
GROUP BY catalyst_type, session_context, staleness_reason
ORDER BY total_blocked DESC;

CREATE OR REPLACE VIEW pipeline_latency_percentiles AS
SELECT
    catalyst_type,
    COUNT(*)                                                       AS sample_count,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p50_ms,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p90_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p99_ms,
    MAX(pipeline_latency_ms)                                       AS max_ms
FROM signal_log
WHERE pipeline_latency_ms IS NOT NULL
GROUP BY catalyst_type
ORDER BY p95_ms DESC NULLS LAST;

CREATE OR REPLACE VIEW kafka_lag_trend AS
SELECT
    service,
    DATE_TRUNC('minute', logged_at)             AS minute,
    ROUND(AVG(consumer_lag)::numeric, 0)        AS avg_lag,
    MAX(consumer_lag)                            AS peak_lag,
    SUM(CASE WHEN blocked THEN 1 ELSE 0 END)    AS executions_blocked
FROM kafka_lag_log
GROUP BY service, DATE_TRUNC('minute', logged_at)
ORDER BY minute DESC;


-- ── 012: fact_validation_audit + fmp_analyst_grades ──────────────────────────

CREATE TABLE IF NOT EXISTS fact_validation_audit (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    record_id             UUID,
    ticker                TEXT        NOT NULL,
    catalyst_type         TEXT,
    validation_status     TEXT        NOT NULL,
    validated_fields      TEXT[]      DEFAULT '{}',
    mismatch_fields       TEXT[]      DEFAULT '{}',
    validation_confidence FLOAT,
    conviction_multiplier FLOAT,
    llm_facts_json        JSONB,
    field_detail_json     JSONB,
    vendor_source         TEXT,
    has_key_mismatch      BOOLEAN     DEFAULT FALSE,
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_fva_ticker_created
    ON fact_validation_audit (ticker, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_fva_status
    ON fact_validation_audit (validation_status, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_fva_record_id
    ON fact_validation_audit (record_id);

CREATE INDEX IF NOT EXISTS ix_fva_key_mismatch
    ON fact_validation_audit (has_key_mismatch, created_at DESC)
    WHERE has_key_mismatch = TRUE;

CREATE TABLE IF NOT EXISTS fmp_analyst_grades (
    id           BIGSERIAL    PRIMARY KEY,
    ticker       TEXT         NOT NULL,
    analyst_firm TEXT,
    from_grade   TEXT,
    to_grade     TEXT         NOT NULL,
    action       TEXT,
    price_target FLOAT,
    grade_date   TIMESTAMPTZ  NOT NULL,
    created_at   TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_fmp_grade_ticker_firm_date
        UNIQUE (ticker, analyst_firm, grade_date)
);

CREATE INDEX IF NOT EXISTS ix_fmp_ag_ticker_date
    ON fmp_analyst_grades (ticker, grade_date DESC);


-- ── 013: signal_feature_log ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS signal_feature_log (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    record_id   UUID,
    ticker      TEXT        NOT NULL,
    direction   TEXT,
    emitted     BOOLEAN     DEFAULT TRUE,

    impact_day                          FLOAT,
    source_credibility                  FLOAT,
    cred_boost                          FLOAT,
    catalyst_type                       TEXT,
    catalyst_weight                     FLOAT,
    catalyst_weight_effective           FLOAT,
    session_context                     TEXT,
    session_weight                      FLOAT,
    float_sensitivity                   TEXT,
    float_weight                        FLOAT,
    earnings_proximity_h                INT,
    earnings_proximity_bonus            FLOAT,
    earnings_proximity_bonus_effective  FLOAT,
    direction_source                    TEXT,
    priced_in                           TEXT,
    regime_flag                         TEXT,
    regime_multiplier                   FLOAT,
    cross_val_status                    TEXT,
    cross_val_multiplier                FLOAT,
    time_window_label                   TEXT,
    time_window_mult                    FLOAT,
    time_window_mult_effective          FLOAT,
    scoring_mode                        TEXT,
    correlation_risk    TEXT[]      DEFAULT '{}',

    step_base_product       FLOAT,
    step_after_proximity    FLOAT,
    step_after_priced_in    FLOAT,
    step_after_cap          FLOAT,
    step_after_regime       FLOAT,
    step_after_cross_val    FLOAT,
    step_after_time_window  FLOAT,
    step_calibrated         FLOAT,

    interpretive_cap_applied    BOOLEAN DEFAULT FALSE,
    priced_in_blocked           BOOLEAN DEFAULT FALSE,
    calibration_fn  TEXT,
    final_conviction    FLOAT       NOT NULL,

    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_sfl_ticker_created
    ON signal_feature_log (ticker, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_sfl_catalyst_created
    ON signal_feature_log (catalyst_type, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_sfl_emitted_created
    ON signal_feature_log (emitted, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_sfl_direction_source
    ON signal_feature_log (direction_source, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_sfl_scoring_mode
    ON signal_feature_log (scoring_mode, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_sfl_record_id
    ON signal_feature_log (record_id);

CREATE INDEX IF NOT EXISTS ix_sfl_correlation_risk_gin
    ON signal_feature_log USING GIN (correlation_risk);

CREATE INDEX IF NOT EXISTS ix_sfl_cap_applied
    ON signal_feature_log (interpretive_cap_applied, created_at DESC)
    WHERE interpretive_cap_applied = TRUE;


-- ── 014: remaining catalyst policy columns (catalyst_policy_applied already ───
--         handled in section 1 above)                                          ──

ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS policy_threshold_used    FLOAT    DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_block_reason      TEXT     DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_multiplier        FLOAT    DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_options_weight    FLOAT    DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_regime_adj        FLOAT    DEFAULT NULL;

CREATE INDEX IF NOT EXISTS ix_sl_policy_block_reason
    ON signal_log (policy_block_reason, created_at DESC)
    WHERE policy_block_reason IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_sl_catalyst_policy_applied
    ON signal_log (catalyst_type, catalyst_policy_applied, created_at DESC);


-- ── 015: portfolio_risk_log ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS portfolio_risk_log (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    news_id         UUID,
    ticker          TEXT        NOT NULL,
    direction       TEXT,
    catalyst_type   TEXT,
    cluster_id      UUID,
    block_code      TEXT        NOT NULL,
    block_reason    TEXT        NOT NULL,
    conviction      FLOAT,
    open_positions  INT,
    daily_pnl_pct   FLOAT,
    sector_pct      FLOAT,
    catalyst_count  INT,
    hourly_orders   INT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_prl_ticker_created
    ON portfolio_risk_log (ticker, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_prl_block_code
    ON portfolio_risk_log (block_code, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_prl_cluster_id
    ON portfolio_risk_log (cluster_id)
    WHERE cluster_id IS NOT NULL;


-- ── 016: execution_quality + trade fill columns ───────────────────────────────

ALTER TABLE trade
    ADD COLUMN IF NOT EXISTS order_type       VARCHAR(16),
    ADD COLUMN IF NOT EXISTS fill_qty         FLOAT,
    ADD COLUMN IF NOT EXISTS avg_fill_price   FLOAT,
    ADD COLUMN IF NOT EXISTS slippage_pct     FLOAT,
    ADD COLUMN IF NOT EXISTS partial_fill     BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS atr_14           FLOAT,
    ADD COLUMN IF NOT EXISTS atr_stop_used    BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS execution_quality (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id         UUID        REFERENCES trade(id) ON DELETE CASCADE,
    broker_order_id  VARCHAR(128),
    ticker           VARCHAR(16)  NOT NULL,

    quote_age_seconds   FLOAT,
    price_drift_pct     FLOAT,
    spread_pct          FLOAT,

    order_type          VARCHAR(16),
    qty_requested       FLOAT,
    qty_filled          FLOAT,
    avg_fill_price      FLOAT,
    entry_ref_price     FLOAT,
    slippage_pct        FLOAT,
    fill_latency_ms     INT,
    partial_fill        BOOLEAN DEFAULT FALSE,

    atr_14              FLOAT,
    atr_stop_used       BOOLEAN DEFAULT FALSE,
    stop_loss_price     FLOAT,
    take_profit_price   FLOAT,

    catalyst_type       VARCHAR(32),
    adv_tier            VARCHAR(16),
    policy_applied      VARCHAR(64),

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_eq_trade_id   ON execution_quality (trade_id);
CREATE INDEX IF NOT EXISTS ix_eq_ticker     ON execution_quality (ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_eq_slippage   ON execution_quality (slippage_pct)
    WHERE slippage_pct IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_eq_order_type ON execution_quality (order_type, ticker);


-- ── 017: news_item embedding column (pgvector) ────────────────────────────────

CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE news_item
    ADD COLUMN IF NOT EXISTS title_embedding vector(1536);

-- HNSW index — only created if pgvector supports it (v0.5+)
-- Wrapped in DO block so a missing extension doesn't abort the whole migration.
DO $$ BEGIN
    CREATE INDEX IF NOT EXISTS ix_news_title_embedding_hnsw
        ON news_item
        USING hnsw (title_embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64);
    RAISE NOTICE 'Created ix_news_title_embedding_hnsw (pgvector HNSW)';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ix_news_title_embedding_hnsw skipped: %', SQLERRM;
END $$;


-- ── 4. Migration tracking ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR(64) PRIMARY KEY,
    applied_at  TIMESTAMP WITH TIME ZONE DEFAULT now(),
    description TEXT
);

INSERT INTO schema_migrations (version, description) VALUES
    ('018_fix_missing_columns',
     'Add technical_score, technical_score_breakdown, catalyst_policy_applied; '
     'widen VARCHAR(16) fields; ensure all tables/columns from 009–017 present')
ON CONFLICT (version) DO NOTHING;

COMMIT;
