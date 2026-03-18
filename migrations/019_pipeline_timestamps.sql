-- Migration 019: Pipeline latency instrumentation
-- Adds per-stage timestamps and end-to-end latency columns to the trade table.
-- stage_timestamps JSONB stores an ISO-8601 timestamp for each pipeline stage
-- so we can pinpoint exactly where time is spent on every signal.
--
-- Stage order:
--   news_ingested → normalized → entity_resolved → ai_summarized →
--   signal_aggregated → pretrade_filtered → execution_submitted → execution_filled

-- ── trade table ──────────────────────────────────────────────────────────────
ALTER TABLE trade
    ADD COLUMN IF NOT EXISTS stage_timestamps      JSONB  DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS end_to_end_latency_ms FLOAT;

-- Fast lookup for latency analysis and alerting
CREATE INDEX IF NOT EXISTS idx_trade_e2e_latency
    ON trade(end_to_end_latency_ms)
    WHERE end_to_end_latency_ms IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trade_created_latency
    ON trade(created_at, end_to_end_latency_ms);

-- GIN index for querying by stage name (e.g. WHERE stage_timestamps ? 'ai_summarized')
CREATE INDEX IF NOT EXISTS idx_trade_stage_timestamps_gin
    ON trade USING GIN (stage_timestamps);
