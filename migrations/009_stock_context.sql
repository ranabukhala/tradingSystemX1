-- Migration 009: Stock Context Classifier tables and signal_log enrichment
-- Adds stock_context_log table and extends signal_log with regime/cleanliness
-- columns populated by the stock_context_service.
--
-- NOTE: 008_technical_score.sql already exists — this file is 009.
--
-- Run after 008_technical_score.sql:
--   docker cp migrations/009_stock_context.sql trading_postgres:/tmp/009_stock_context.sql
--   docker exec trading_postgres psql -U trading -d trading_db -f /tmp/009_stock_context.sql

-- ── Stock context history log ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_context_log (
    id                 SERIAL PRIMARY KEY,
    ticker             VARCHAR(10)  NOT NULL,
    evaluated_at       TIMESTAMPTZ  NOT NULL,
    trend_regime       VARCHAR(20),              -- TRENDING_UP | TRENDING_DOWN | RANGING
    volatility_regime  VARCHAR(20),              -- EXPANDING | CONTRACTING | NORMAL
    cleanliness        VARCHAR(20),              -- CLEAN | CHOPPY | INSUFFICIENT_DATA
    adx                NUMERIC(6,2),
    ma_slope_20        NUMERIC(8,4),
    bb_width_trend     VARCHAR(20),              -- EXPANDING | CONTRACTING | STABLE
    atr_ratio          NUMERIC(6,3),
    adjusted_threshold INTEGER,                  -- 7 | 8 | 9
    raw_metrics        JSONB,
    created_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_context_ticker_time
    ON stock_context_log (ticker, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_stock_context_trend
    ON stock_context_log (trend_regime, evaluated_at DESC);

-- ── Extend signal_log with context columns ────────────────────────────────────
ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS trend_regime        VARCHAR(20),
    ADD COLUMN IF NOT EXISTS volatility_regime   VARCHAR(20),
    ADD COLUMN IF NOT EXISTS cleanliness         VARCHAR(20),
    ADD COLUMN IF NOT EXISTS adjusted_threshold  INTEGER;

CREATE INDEX IF NOT EXISTS idx_signal_log_trend_regime
    ON signal_log (trend_regime)
    WHERE trend_regime IS NOT NULL;

-- ── Watchlist table (if not already created by another migration) ─────────────
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

-- ── Analytical views ─────────────────────────────────────────────────────────

-- Latest context per ticker
CREATE OR REPLACE VIEW stock_context_latest AS
SELECT DISTINCT ON (ticker)
    ticker,
    evaluated_at,
    trend_regime,
    volatility_regime,
    cleanliness,
    adx,
    ma_slope_20,
    bb_width_trend,
    atr_ratio,
    adjusted_threshold
FROM stock_context_log
ORDER BY ticker, evaluated_at DESC;

-- Signal performance by context
CREATE OR REPLACE VIEW signal_performance_by_context AS
SELECT
    sl.trend_regime,
    sl.volatility_regime,
    sl.cleanliness,
    sl.adjusted_threshold,
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

\echo 'Migration 009 complete: stock_context_log created, signal_log extended.'
