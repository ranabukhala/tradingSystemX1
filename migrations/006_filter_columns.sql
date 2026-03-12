-- Phase 6: Pre-trade filter columns
-- Adds filter result tracking to signal_log table.
-- Run after 005_signal_log.sql.
--
-- Run:
--   docker cp migrations/006_filter_columns.sql trading_postgres:/tmp/006_filter_columns.sql
--   docker exec trading_postgres psql -U trading -d trading_db -f /tmp/006_filter_columns.sql

ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS conviction_original  FLOAT,
    ADD COLUMN IF NOT EXISTS filter_blocked       BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS filter_block_reason  TEXT,
    ADD COLUMN IF NOT EXISTS filter_regime        VARCHAR(32),
    ADD COLUMN IF NOT EXISTS filter_tech_score    FLOAT,
    ADD COLUMN IF NOT EXISTS filter_flow_bias     VARCHAR(16),
    ADD COLUMN IF NOT EXISTS filter_squeeze_score FLOAT;

-- View: filter funnel analytics
-- Shows how many signals each filter is blocking/adjusting
CREATE OR REPLACE VIEW filter_funnel AS
SELECT
    date_trunc('hour', created_at) AS hour,
    COUNT(*)                                                  AS total,
    SUM(passed_gate::int)                                     AS passed_conviction,
    SUM((NOT COALESCE(filter_blocked, FALSE))::int)           AS passed_filter,
    SUM(COALESCE(filter_blocked, FALSE)::int)                 AS blocked_by_filter,
    SUM((filter_regime = 'risk_off')::int)                    AS risk_off_count,
    ROUND(AVG(filter_tech_score)::numeric, 3)                 AS avg_tech_score,
    SUM((filter_flow_bias = 'bearish' AND direction = 'long')::int) AS options_warned_longs,
    SUM((COALESCE(filter_squeeze_score, 0) > 0.5)::int)       AS squeeze_candidates
FROM signal_log
GROUP BY 1
ORDER BY 1 DESC;

-- View: squeeze candidates today
CREATE OR REPLACE VIEW squeeze_candidates_today AS
SELECT
    created_at AT TIME ZONE 'America/New_York' AS time_et,
    ticker,
    direction,
    round(conviction::numeric, 3)         AS conviction,
    round(filter_squeeze_score::numeric, 2) AS squeeze_score,
    signal_type,
    left(news_title, 80)                  AS headline
FROM signal_log
WHERE created_at >= now() - interval '24 hours'
  AND filter_squeeze_score > 0.5
ORDER BY filter_squeeze_score DESC;

\echo 'Filter columns and views added to signal_log.'
