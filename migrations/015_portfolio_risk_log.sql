-- Migration 015: Portfolio Risk Manager audit log
-- Created for Task 9 (v1.10) — portfolio risk manager and kill switches

CREATE TABLE IF NOT EXISTS portfolio_risk_log (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    news_id         UUID,
    ticker          TEXT        NOT NULL,
    direction       TEXT,
    catalyst_type   TEXT,
    cluster_id      UUID,
    block_code      TEXT        NOT NULL,   -- machine-readable RISK:* code
    block_reason    TEXT        NOT NULL,   -- human-readable description
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
