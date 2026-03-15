-- ─────────────────────────────────────────────────────────────────────────────
-- Migration 011: Signal Staleness and Pipeline Latency Tracking
--
-- Adds:
--   1. Staleness columns on signal_log  — tracks age at each pipeline stage
--   2. Staleness + lag columns on trade — execution-time diagnostics
--   3. Pipeline latency columns         — end-to-end measurement
--   4. Views: stale_signal_summary, pipeline_latency_percentiles
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Extend signal_log with staleness fields ────────────────────────────────

ALTER TABLE signal_log
    -- Age of the source article (now - published_at) when this signal was evaluated
    ADD COLUMN IF NOT EXISTS signal_age_seconds    NUMERIC(10,2),

    -- Was this signal blocked by the staleness guard?
    ADD COLUMN IF NOT EXISTS staleness_blocked     BOOLEAN DEFAULT FALSE,

    -- Machine-readable reason code from staleness check
    -- e.g. "signal_stale", "no_published_at", "ok"
    ADD COLUMN IF NOT EXISTS staleness_reason      TEXT,

    -- Maximum age permitted for this catalyst/session/route combination
    ADD COLUMN IF NOT EXISTS max_age_seconds       NUMERIC(10,2),

    -- Original article publication time (propagated from SummarizedRecord)
    ADD COLUMN IF NOT EXISTS news_published_at     TIMESTAMPTZ,

    -- End-to-end latency: signal.created_at - news.received_at (ms)
    ADD COLUMN IF NOT EXISTS pipeline_latency_ms   INTEGER;

-- Index for staleness analysis queries
CREATE INDEX IF NOT EXISTS idx_signal_log_staleness
    ON signal_log (staleness_blocked, catalyst_type, created_at DESC)
    WHERE staleness_blocked = TRUE;

-- Index for latency percentile computation
CREATE INDEX IF NOT EXISTS idx_signal_log_latency
    ON signal_log (catalyst_type, pipeline_latency_ms)
    WHERE pipeline_latency_ms IS NOT NULL;

-- ── 2. Extend trade table with staleness and lag fields ───────────────────────

ALTER TABLE trade
    -- Was execution blocked by the staleness guard?
    ADD COLUMN IF NOT EXISTS staleness_blocked     BOOLEAN DEFAULT FALSE,

    -- Was execution blocked by Kafka consumer lag?
    ADD COLUMN IF NOT EXISTS lag_blocked           BOOLEAN DEFAULT FALSE,

    -- Kafka consumer lag at execution time (messages behind head)
    ADD COLUMN IF NOT EXISTS consumer_lag          INTEGER,

    -- Age of the signal at execution time (seconds)
    ADD COLUMN IF NOT EXISTS signal_age_seconds    NUMERIC(10,2),

    -- End-to-end latency: trade.created_at - news.published_at (ms)
    ADD COLUMN IF NOT EXISTS pipeline_latency_ms   INTEGER;

-- Index for stale execution analysis
CREATE INDEX IF NOT EXISTS idx_trade_staleness
    ON trade (staleness_blocked, lag_blocked, created_at DESC)
    WHERE staleness_blocked = TRUE OR lag_blocked = TRUE;

-- ── 3. Staleness audit log (immutable record of every block decision) ─────────

CREATE TABLE IF NOT EXISTS staleness_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT        NOT NULL,   -- "pretrade_filter" | "execution_engine"
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

-- ── 4. Kafka lag audit log ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS kafka_lag_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service         TEXT        NOT NULL,
    consumer_lag    INTEGER     NOT NULL,
    lag_threshold   INTEGER     NOT NULL,
    blocked         BOOLEAN     NOT NULL DEFAULT FALSE,
    ticker          TEXT,                           -- NULL for periodic checks
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kafka_lag_service_time
    ON kafka_lag_log (service, logged_at DESC);

-- ── 5. Views ──────────────────────────────────────────────────────────────────

-- Stale signal summary: how many signals are being wasted by staleness
CREATE OR REPLACE VIEW stale_signal_summary AS
SELECT
    catalyst_type,
    session_context,
    staleness_reason,
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

-- Pipeline latency percentiles: tracks end-to-end processing time by catalyst
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

-- Kafka lag history: shows lag trends over time by service
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
