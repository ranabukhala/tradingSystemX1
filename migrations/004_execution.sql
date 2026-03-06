-- Phase 4: Execution tables
-- Run once: docker exec -i trading_postgres psql -U trading -d trading_db < migrations/004_execution.sql

CREATE TABLE IF NOT EXISTS trade (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id           UUID,
    broker_order_id     VARCHAR(128),
    broker              VARCHAR(32) NOT NULL,
    ticker              VARCHAR(16) NOT NULL,
    direction           VARCHAR(8) NOT NULL,   -- long | short
    qty                 FLOAT NOT NULL,
    entry_price         FLOAT,
    exit_price          FLOAT,
    take_profit         FLOAT,
    stop_loss           FLOAT,
    conviction          FLOAT,
    catalyst_type       VARCHAR(32),
    signal_type         VARCHAR(32),
    status              VARCHAR(32) DEFAULT 'submitted',  -- submitted|filled|cancelled|rejected
    close_reason        VARCHAR(32),           -- stop_loss|take_profit|time_stop|manual
    realized_pnl        FLOAT,
    realized_pnl_pct    FLOAT,
    t1_summary          TEXT,
    opened_at           TIMESTAMPTZ DEFAULT now(),
    closed_at           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_trade_ticker      ON trade (ticker);
CREATE INDEX IF NOT EXISTS ix_trade_status      ON trade (status);
CREATE INDEX IF NOT EXISTS ix_trade_opened_at   ON trade (opened_at);
CREATE INDEX IF NOT EXISTS ix_trade_signal_id   ON trade (signal_id);
CREATE INDEX IF NOT EXISTS ix_trade_broker      ON trade (broker);

-- Daily P&L view
CREATE OR REPLACE VIEW daily_pnl AS
SELECT
    DATE(opened_at AT TIME ZONE 'UTC') AS trade_date,
    broker,
    COUNT(*)                            AS total_trades,
    COUNT(*) FILTER (WHERE realized_pnl > 0) AS winners,
    COUNT(*) FILTER (WHERE realized_pnl < 0) AS losers,
    ROUND(SUM(realized_pnl)::numeric, 2)      AS total_pnl,
    ROUND(AVG(realized_pnl)::numeric, 2)      AS avg_pnl,
    ROUND(AVG(realized_pnl_pct)::numeric, 2)  AS avg_pnl_pct,
    ROUND(
        (COUNT(*) FILTER (WHERE realized_pnl > 0)::float /
         NULLIF(COUNT(*), 0) * 100)::numeric, 1
    ) AS win_rate_pct
FROM trade
WHERE closed_at IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC;

COMMENT ON TABLE trade IS 'All executed trades with entry/exit and P&L';
COMMENT ON VIEW daily_pnl IS 'Daily trading performance summary';
