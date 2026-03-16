-- Migration 014: Catalyst-aware pretrade policy columns in signal_log
-- Adds 6 columns persisted by pretrade_filter._update_signal_log() (v1.9).
--
-- All columns are nullable — existing rows are unaffected.
-- The policy layer is optional (kill-switch: ENABLE_CATALYST_POLICY=false);
-- when disabled these columns are written as NULL.
--
-- Research queries:
--
--   -- Policy impact by catalyst family (last 7 days)
--   SELECT catalyst_type,
--          AVG(policy_multiplier)      AS avg_policy_mult,
--          AVG(policy_threshold_used)  AS avg_threshold,
--          COUNT(*)                    AS signals,
--          SUM(CASE WHEN filter_blocked THEN 1 ELSE 0 END) AS blocked
--   FROM   signal_log
--   WHERE  created_at >= NOW() - INTERVAL '7 days'
--     AND  catalyst_policy_applied = TRUE
--   GROUP  BY catalyst_type ORDER BY signals DESC;
--
--   -- Signals blocked by policy (not regime/tech hard blocks)
--   SELECT ticker, catalyst_type, policy_block_reason, conviction, created_at
--   FROM   signal_log
--   WHERE  policy_block_reason IS NOT NULL
--   ORDER  BY created_at DESC LIMIT 50;
--
--   -- Options weight adjustment vs raw flow delta
--   SELECT catalyst_type,
--          AVG(policy_options_weight) AS avg_opt_weight,
--          AVG(filter_flow_bias::TEXT) AS avg_bias
--   FROM   signal_log
--   WHERE  created_at >= NOW() - INTERVAL '7 days'
--   GROUP  BY catalyst_type;

ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS catalyst_policy_applied  BOOLEAN  DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_threshold_used    FLOAT    DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_block_reason      TEXT     DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_multiplier        FLOAT    DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_options_weight    FLOAT    DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS policy_regime_adj        FLOAT    DEFAULT NULL;

-- Index for policy block analysis (sparse — most rows will have NULL)
CREATE INDEX IF NOT EXISTS ix_sl_policy_block_reason
    ON signal_log (policy_block_reason, created_at DESC)
    WHERE policy_block_reason IS NOT NULL;

-- Index for per-catalyst policy research
CREATE INDEX IF NOT EXISTS ix_sl_catalyst_policy_applied
    ON signal_log (catalyst_type, catalyst_policy_applied, created_at DESC);

SELECT 'Migration 014 complete: signal_log catalyst_policy columns added' AS status;
