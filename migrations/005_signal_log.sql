-- Phase 5: Signal logging
-- Captures EVERY signal evaluated — whether it passed the conviction gate or not.
-- This lets you review signal quality, tune thresholds, and build a performance record.
--
-- Run:
--   docker cp migrations/005_signal_log.sql trading_postgres:/tmp/005_signal_log.sql
--   docker exec trading_postgres psql -U trading -d trading_db -f /tmp/005_signal_log.sql

CREATE TABLE IF NOT EXISTS signal_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Ticker & direction
    ticker              VARCHAR(16) NOT NULL,
    direction           VARCHAR(8),                     -- long | short | neutral
    signal_type         VARCHAR(32),                    -- breakout | fade | event_driven | macro_shift

    -- Conviction breakdown
    conviction          FLOAT NOT NULL,
    conviction_threshold FLOAT NOT NULL,
    passed_gate         BOOLEAN NOT NULL,               -- did it cross SIGNAL_CONVICTION_THRESHOLD?
    was_alerted         BOOLEAN DEFAULT FALSE,          -- did it trigger a Telegram alert?
    was_executed        BOOLEAN DEFAULT FALSE,          -- did it trigger a broker order?

    -- Context
    catalyst_type       VARCHAR(32),
    session_context     VARCHAR(32),
    market_cap_tier     VARCHAR(16),
    float_sensitivity   VARCHAR(16),
    impact_day          FLOAT,
    impact_swing        FLOAT,

    -- Source
    news_id             UUID,
    news_title          TEXT,
    news_source         VARCHAR(32),
    news_published_at   TIMESTAMPTZ,

    -- AI analysis
    t1_summary          TEXT,
    t2_summary          TEXT,
    prompt_version      VARCHAR(16),

    -- Outcome tracking (filled in later by a separate job or manually)
    -- How did the price actually move after this signal?
    outcome_price_1h    FLOAT,       -- % price change 1h after signal
    outcome_price_4h    FLOAT,       -- % price change 4h after signal
    outcome_price_1d    FLOAT,       -- % price change EOD
    outcome_correct     BOOLEAN,     -- did direction match actual move?
    outcome_filled_at   TIMESTAMPTZ
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS ix_signal_log_ticker     ON signal_log (ticker);
CREATE INDEX IF NOT EXISTS ix_signal_log_created_at ON signal_log (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_signal_log_passed     ON signal_log (passed_gate, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_signal_log_direction  ON signal_log (direction, catalyst_type);

-- View: today's signals with pass/fail status
CREATE OR REPLACE VIEW signal_log_today AS
SELECT
    created_at AT TIME ZONE 'America/New_York' AS time_et,
    ticker,
    direction,
    round(conviction::numeric, 3)   AS conviction,
    passed_gate,
    was_alerted,
    was_executed,
    catalyst_type,
    session_context,
    left(news_title, 80)            AS headline
FROM signal_log
WHERE created_at >= now() - interval '24 hours'
ORDER BY created_at DESC;

-- View: signal performance summary (once outcome columns are filled)
CREATE OR REPLACE VIEW signal_performance AS
SELECT
    ticker,
    direction,
    catalyst_type,
    COUNT(*)                                        AS total_signals,
    SUM(passed_gate::int)                           AS passed,
    SUM(was_executed::int)                          AS executed,
    ROUND(AVG(conviction)::numeric, 3)              AS avg_conviction,
    SUM(outcome_correct::int)                       AS correct,
    ROUND(
        100.0 * SUM(outcome_correct::int) / NULLIF(COUNT(outcome_correct), 0), 1
    )                                               AS accuracy_pct,
    ROUND(AVG(outcome_price_1d)::numeric, 2)        AS avg_1d_move_pct
FROM signal_log
WHERE outcome_correct IS NOT NULL
GROUP BY ticker, direction, catalyst_type
ORDER BY total_signals DESC;

\echo 'signal_log table, views, and indexes created.'
