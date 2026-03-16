-- Migration 013: Conviction feature log table
-- Stores the full ConvictionBreakdown for every emitted (and optionally dropped)
-- signal, enabling calibration, A/B testing, and root-cause analysis.
--
-- Populated by signal_aggregator._write_feature_log() (fire-and-forget) when
-- CONVICTION_LOG_FEATURES=true.  Dropped signals are also written when
-- CONVICTION_LOG_DROPPED_FEATURES=true.
--
-- Research queries:
--
--   -- Per-catalyst conviction distribution (last 7 days)
--   SELECT catalyst_type, AVG(final_conviction), STDDEV(final_conviction), COUNT(*)
--   FROM   signal_feature_log
--   WHERE  created_at >= NOW() - INTERVAL '7 days'
--   GROUP  BY catalyst_type ORDER BY AVG DESC;
--
--   -- Signals with correlation risk flags (last 24 h)
--   SELECT ticker, catalyst_type, final_conviction, correlation_risk
--   FROM   signal_feature_log
--   WHERE  created_at >= NOW() - INTERVAL '24 hours'
--     AND  correlation_risk && ARRAY['cat_impact_day']::TEXT[];
--
--   -- v1 vs v2 scoring mode comparison for AAPL
--   SELECT scoring_mode,
--          AVG(step_base_product) AS avg_base,
--          AVG(final_conviction)  AS avg_final
--   FROM   signal_feature_log
--   WHERE  ticker = 'AAPL'
--   GROUP  BY scoring_mode;
--
--   -- Signals that hit the interpretive ceiling or were priced-in blocked
--   SELECT ticker, direction_source, step_after_priced_in, final_conviction
--   FROM   signal_feature_log
--   WHERE  interpretive_cap_applied OR priced_in_blocked
--   ORDER  BY created_at DESC LIMIT 50;
--
--   -- Calibration dataset: raw step vs actual outcome (join with trade results)
--   SELECT sfl.ticker, sfl.step_after_cross_val AS raw_score,
--          sfl.final_conviction, sfl.calibration_fn
--   FROM   signal_feature_log sfl
--   WHERE  sfl.emitted = TRUE AND sfl.created_at >= NOW() - INTERVAL '30 days';

CREATE TABLE IF NOT EXISTS signal_feature_log (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- ── Record linkage ───────────────────────────────────────────────────
    record_id   UUID,                       -- SummarizedRecord.id
    ticker      TEXT        NOT NULL,
    direction   TEXT,                       -- BUY | SELL | NEUTRAL
    emitted     BOOLEAN     DEFAULT TRUE,   -- FALSE = dropped below threshold

    -- ── Raw conviction inputs (ConvictionFeatures) ───────────────────────
    impact_day                          FLOAT,
    source_credibility                  FLOAT,
    cred_boost                          FLOAT,

    catalyst_type                       TEXT,
    catalyst_weight                     FLOAT,
    catalyst_weight_effective           FLOAT,

    session_context                     TEXT,
    session_weight                      FLOAT,

    float_sensitivity                   TEXT,
    float_weight                        FLOAT,

    earnings_proximity_h                INT,        -- NULL when no earnings context
    earnings_proximity_bonus            FLOAT,
    earnings_proximity_bonus_effective  FLOAT,

    direction_source                    TEXT,       -- facts | interpretive_prior | neutral_default
    priced_in                           TEXT,       -- yes | partially | NULL
    regime_flag                         TEXT,       -- risk_off | high_vol | compression | risk_on | NULL
    regime_multiplier                   FLOAT,

    cross_val_status                    TEXT,       -- confirmed | partial | mismatch | unverifiable | NULL
    cross_val_multiplier                FLOAT,

    time_window_label                   TEXT,       -- NULL outside time-restricted windows
    time_window_mult                    FLOAT,      -- raw mult from get_time_window()
    time_window_mult_effective          FLOAT,      -- 1.0 when ≥ 1.0 (boosts not applied)

    scoring_mode                        TEXT,       -- v1 | v2

    -- ── Correlation risk labels (TEXT[] for GIN containment queries) ─────
    correlation_risk    TEXT[]      DEFAULT '{}',   -- [cat_impact_day, earnings_proximity, float_squeeze]

    -- ── Conviction step trace (ConvictionBreakdown) ──────────────────────
    step_base_product       FLOAT,   -- impact_day × cat_w_eff × sess_w × float_w × cred_boost
    step_after_proximity    FLOAT,   -- × earnings_proximity_bonus_effective
    step_after_priced_in    FLOAT,   -- × priced_in mult (or 0.0 if blocked)
    step_after_cap          FLOAT,   -- after interpretive ceiling
    step_after_regime       FLOAT,   -- × regime_multiplier
    step_after_cross_val    FLOAT,   -- × cross_val_multiplier  (≈ "raw score" pre-TW)
    step_after_time_window  FLOAT,   -- × time_window_mult_effective (dampening only)
    step_calibrated         FLOAT,   -- output of calibration function

    -- ── Gates ────────────────────────────────────────────────────────────
    interpretive_cap_applied    BOOLEAN DEFAULT FALSE,
    priced_in_blocked           BOOLEAN DEFAULT FALSE,

    -- ── Calibration ──────────────────────────────────────────────────────
    calibration_fn  TEXT,           -- identity | sigmoid | linear

    -- ── Final conviction ─────────────────────────────────────────────────
    final_conviction    FLOAT       NOT NULL,

    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ──────────────────────────────────────────────────────────────────

-- Most common research query: per-ticker time series
CREATE INDEX IF NOT EXISTS ix_sfl_ticker_created
    ON signal_feature_log (ticker, created_at DESC);

-- Per-catalyst analysis (distribution, calibration)
CREATE INDEX IF NOT EXISTS ix_sfl_catalyst_created
    ON signal_feature_log (catalyst_type, created_at DESC);

-- Emitted vs dropped split (high cardinality: most rows will be emitted=true)
CREATE INDEX IF NOT EXISTS ix_sfl_emitted_created
    ON signal_feature_log (emitted, created_at DESC);

-- Direction source split (facts vs interpretive performance comparison)
CREATE INDEX IF NOT EXISTS ix_sfl_direction_source
    ON signal_feature_log (direction_source, created_at DESC);

-- Scoring mode comparison (v1 vs v2 rollout)
CREATE INDEX IF NOT EXISTS ix_sfl_scoring_mode
    ON signal_feature_log (scoring_mode, created_at DESC);

-- Join back to pipeline records
CREATE INDEX IF NOT EXISTS ix_sfl_record_id
    ON signal_feature_log (record_id);

-- GIN index for correlation_risk array containment queries
-- e.g. WHERE correlation_risk && ARRAY['float_squeeze']
CREATE INDEX IF NOT EXISTS ix_sfl_correlation_risk_gin
    ON signal_feature_log USING GIN (correlation_risk);

-- Fast path to signals that hit the interpretive cap
CREATE INDEX IF NOT EXISTS ix_sfl_cap_applied
    ON signal_feature_log (interpretive_cap_applied, created_at DESC)
    WHERE interpretive_cap_applied = TRUE;

SELECT 'Migration 013 complete: signal_feature_log created' AS status;
