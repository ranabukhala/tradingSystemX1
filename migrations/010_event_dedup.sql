-- ─────────────────────────────────────────────────────────────────────────────
-- Migration 010: Event-Level Deduplication
--
-- Adds:
--   1. dropped_events table  — audit trail for non-representative items
--   2. Indexes on dropped_events
--   3. New columns on signal_log and trade for event dedup tracking
--   4. Views: event_cluster_summary, event_trade_duplicates
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Dropped events audit table ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dropped_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT        NOT NULL,
    vendor_id       TEXT        NOT NULL,
    title           TEXT        NOT NULL,
    published_at    TIMESTAMPTZ NOT NULL,
    cluster_id      UUID,                           -- matching representative cluster
    dedup_tier      TEXT,                           -- vendor_id | content_hash | semantic
    dedup_reason    TEXT,                           -- granular reason code
    similarity_score NUMERIC(5,4),                  -- 0–1 for fuzzy/simhash matches
    dropped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Composite index for cluster analysis
CREATE INDEX IF NOT EXISTS idx_dropped_events_cluster
    ON dropped_events (cluster_id, dropped_at DESC);

-- Index for per-vendor duplicate rate reporting
CREATE INDEX IF NOT EXISTS idx_dropped_events_source_vendor
    ON dropped_events (source, vendor_id);

-- Index for dedup tier breakdowns in dashboards
CREATE INDEX IF NOT EXISTS idx_dropped_events_tier_reason
    ON dropped_events (dedup_tier, dedup_reason, dropped_at DESC);

-- ── 2. Extend signal_log with dedup fields ────────────────────────────────────

ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS cluster_id     UUID,
    ADD COLUMN IF NOT EXISTS event_gate_blocked  BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS dedup_tier     TEXT,
    ADD COLUMN IF NOT EXISTS dedup_reason   TEXT;

-- ── 3. Extend trade table with dedup / event gate fields ─────────────────────

ALTER TABLE trade
    ADD COLUMN IF NOT EXISTS cluster_id          UUID,
    ADD COLUMN IF NOT EXISTS order_gate_blocked  BOOLEAN DEFAULT FALSE;

-- ── 4. Views ──────────────────────────────────────────────────────────────────

-- Cluster drop rate summary: how many items per cluster, how many dropped
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

-- Shows signals that were blocked by the event gate
CREATE OR REPLACE VIEW event_signal_duplicates AS
SELECT
    sl.ticker,
    sl.cluster_id,
    sl.direction,
    sl.conviction,
    sl.catalyst_type,
    sl.created_at,
    sl.event_gate_blocked,
    sl.dedup_reason
FROM signal_log sl
WHERE sl.event_gate_blocked = TRUE
ORDER BY sl.created_at DESC;

-- Shows orders blocked by order gate (double-submission prevention)
CREATE OR REPLACE VIEW event_order_duplicates AS
SELECT
    t.ticker,
    t.cluster_id,
    t.direction,
    t.conviction,
    t.catalyst_type,
    t.created_at,
    t.order_gate_blocked
FROM trade t
WHERE t.order_gate_blocked = TRUE
ORDER BY t.created_at DESC;
