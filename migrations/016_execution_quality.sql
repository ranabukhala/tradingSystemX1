-- Migration 016: Execution quality metrics
-- Created for Task 10 (v1.11) — execution and position-monitor upgrade

-- Extend the trade table with fill-quality columns
ALTER TABLE trade
    ADD COLUMN IF NOT EXISTS order_type       VARCHAR(16),        -- market|limit|limit_ioc
    ADD COLUMN IF NOT EXISTS fill_qty         FLOAT,              -- actual filled qty
    ADD COLUMN IF NOT EXISTS avg_fill_price   FLOAT,              -- broker avg fill price
    ADD COLUMN IF NOT EXISTS slippage_pct     FLOAT,              -- (avg_fill - entry_ref) / entry_ref * 100
    ADD COLUMN IF NOT EXISTS partial_fill     BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS atr_14           FLOAT,              -- ATR(14) at entry time
    ADD COLUMN IF NOT EXISTS atr_stop_used    BOOLEAN DEFAULT FALSE; -- True if ATR stop was used

-- Separate 1:1 execution quality table (detailed research metrics)
CREATE TABLE IF NOT EXISTS execution_quality (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id         UUID        REFERENCES trade(id) ON DELETE CASCADE,
    broker_order_id  VARCHAR(128),
    ticker           VARCHAR(16)  NOT NULL,

    -- Quote quality at entry decision time
    quote_age_seconds   FLOAT,           -- age of quote when entry check ran
    price_drift_pct     FLOAT,           -- drift from signal ref price to execution price
    spread_pct          FLOAT,           -- bid-ask spread % at entry

    -- Fill quality
    order_type          VARCHAR(16),     -- which order type was selected
    qty_requested       FLOAT,
    qty_filled          FLOAT,
    avg_fill_price      FLOAT,
    entry_ref_price     FLOAT,           -- price at time of signal generation
    slippage_pct        FLOAT,           -- positive = paid more than expected (long)
    fill_latency_ms     INT,             -- ms from submit_order() to fill confirmation
    partial_fill        BOOLEAN DEFAULT FALSE,

    -- ATR / stop context
    atr_14              FLOAT,
    atr_stop_used       BOOLEAN DEFAULT FALSE,
    stop_loss_price     FLOAT,
    take_profit_price   FLOAT,

    -- OrderTypePolicy debug
    catalyst_type       VARCHAR(32),
    adv_tier            VARCHAR(16),     -- liquid | normal | thin
    policy_applied      VARCHAR(64),     -- e.g. "earnings_liquid_market"

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_eq_trade_id  ON execution_quality (trade_id);
CREATE INDEX IF NOT EXISTS ix_eq_ticker    ON execution_quality (ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_eq_slippage  ON execution_quality (slippage_pct)
    WHERE slippage_pct IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_eq_order_type ON execution_quality (order_type, ticker);

COMMENT ON TABLE execution_quality IS
    'Per-trade execution quality metrics: fill quality, slippage, ATR context. 1:1 with trade table.';
