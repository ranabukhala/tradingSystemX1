-- Migration 008: Technical Confluence Score Columns
-- Adds the new 0-10 composite technical score and its per-component breakdown
-- to signal_log. The legacy filter_tech_score FLOAT column (added in 006)
-- continues to store the old -1.0/+1.0 score for backward compatibility.
--
-- Run after 007_core_tables.sql.
--
-- Run:
--   docker cp migrations/008_technical_score.sql trading_postgres:/tmp/008_technical_score.sql
--   docker exec trading_postgres psql -U trading -d trading_db -f /tmp/008_technical_score.sql

ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS technical_score           INTEGER,
    ADD COLUMN IF NOT EXISTS technical_score_breakdown JSONB;

-- Index on technical_score for percentile / histogram queries
CREATE INDEX IF NOT EXISTS idx_signal_log_technical_score
    ON signal_log (technical_score)
    WHERE technical_score IS NOT NULL;

-- View: technical scoring distribution
-- Shows score distribution by direction and whether trades were blocked
CREATE OR REPLACE VIEW technical_score_distribution AS
SELECT
    date_trunc('day', created_at AT TIME ZONE 'America/New_York') AS day_et,
    direction,
    technical_score,
    COUNT(*)                                             AS signals,
    SUM(COALESCE(filter_blocked, FALSE)::int)           AS blocked,
    ROUND(AVG(conviction)::numeric, 3)                  AS avg_conviction,
    ROUND(AVG(filter_tech_score)::numeric, 3)           AS avg_legacy_score
FROM signal_log
WHERE technical_score IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- View: technical score component analysis
-- Unpacks JSONB breakdown to show which components pass/fail most often
CREATE OR REPLACE VIEW technical_score_components AS
SELECT
    date_trunc('day', created_at AT TIME ZONE 'America/New_York') AS day_et,
    direction,
    COUNT(*)                                                          AS signals,
    ROUND(AVG(technical_score)::numeric, 1)                           AS avg_score,
    ROUND(AVG((technical_score_breakdown->'volume'->>'pts')::int)::numeric, 2)           AS avg_vol_pts,
    ROUND(AVG((technical_score_breakdown->'multi_timeframe'->>'pts')::int)::numeric, 2)  AS avg_mtf_pts,
    ROUND(AVG((technical_score_breakdown->'key_levels'->>'pts')::int)::numeric, 2)       AS avg_levels_pts,
    ROUND(AVG((technical_score_breakdown->'vwap'->>'pts')::int)::numeric, 2)             AS avg_vwap_pts,
    ROUND(AVG((technical_score_breakdown->'atr'->>'pts')::int)::numeric, 2)              AS avg_atr_pts,
    ROUND(AVG((technical_score_breakdown->'rsi_divergence'->>'pts')::int)::numeric, 2)   AS avg_rsi_div_pts,
    ROUND(AVG((technical_score_breakdown->'relative_strength'->>'pts')::int)::numeric, 2) AS avg_rs_pts
FROM signal_log
WHERE technical_score IS NOT NULL
  AND technical_score_breakdown IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

\echo 'Migration 008 complete: technical_score and technical_score_breakdown columns added.'
