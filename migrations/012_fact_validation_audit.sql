-- Migration 012: Structured fact cross-validation audit table
-- Stores per-record fact cross-validation diagnostics for research and audit.
--
-- Every MISMATCH between LLM-extracted facts and authoritative vendor data is
-- written here (fire-and-forget) by the signal_aggregator.  CONFIRMED / PARTIAL /
-- UNVERIFIABLE rows are written when FACT_AUDIT_ALL_STATUSES=true.
--
-- Research queries:
--   SELECT ticker, mismatch_fields, field_detail_json
--   FROM   fact_validation_audit
--   WHERE  has_key_mismatch = TRUE
--   ORDER  BY created_at DESC LIMIT 50;
--
--   SELECT vendor_source,
--          COUNT(*) FILTER (WHERE validation_status='mismatch') AS mismatches,
--          COUNT(*) FILTER (WHERE validation_status='confirmed') AS confirmed,
--          AVG(conviction_multiplier) AS avg_multiplier
--   FROM   fact_validation_audit
--   WHERE  created_at >= NOW() - INTERVAL '7 days'
--   GROUP  BY vendor_source;

CREATE TABLE IF NOT EXISTS fact_validation_audit (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Source record
    record_id             UUID,                                -- SummarizedRecord.id
    ticker                TEXT        NOT NULL,
    catalyst_type         TEXT,                                -- earnings | analyst | …

    -- Validation outcome
    validation_status     TEXT        NOT NULL,                -- confirmed | partial | mismatch | unverifiable | skipped
    validated_fields      TEXT[]      DEFAULT '{}',            -- fields that matched vendor
    mismatch_fields       TEXT[]      DEFAULT '{}',            -- fields that diverged
    validation_confidence FLOAT,                               -- 0.0–1.0 fraction verified+matching
    conviction_multiplier FLOAT,                               -- applied to signal conviction

    -- Detailed diagnostics (JSONB for flexible querying)
    llm_facts_json        JSONB,                               -- raw LLM facts_json snapshot
    field_detail_json     JSONB,                               -- [{field, llm, vendor, status, deviation_pct, is_key_field}]
    vendor_source         TEXT,                                -- fmp_earnings | fmp_analyst | internal | null

    -- Fast-path flag
    has_key_mismatch      BOOLEAN     DEFAULT FALSE,           -- TRUE = direction-determining field was wrong

    created_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Lookup by ticker (most common research query)
CREATE INDEX IF NOT EXISTS ix_fva_ticker_created
    ON fact_validation_audit (ticker, created_at DESC);

-- Filter to mismatches only
CREATE INDEX IF NOT EXISTS ix_fva_status
    ON fact_validation_audit (validation_status, created_at DESC);

-- Join back to pipeline records
CREATE INDEX IF NOT EXISTS ix_fva_record_id
    ON fact_validation_audit (record_id);

-- Fast path to key mismatches (direction-level errors)
CREATE INDEX IF NOT EXISTS ix_fva_key_mismatch
    ON fact_validation_audit (has_key_mismatch, created_at DESC)
    WHERE has_key_mismatch = TRUE;

-- ── fmp_analyst_grades table ─────────────────────────────────────────────────
-- Stores the most recent analyst rating actions from FMP /stable/grades-latest.
-- Populated by the FMP enrichment connector; used by FactCrossValidator to
-- cross-check analyst facts extracted by the LLM.

CREATE TABLE IF NOT EXISTS fmp_analyst_grades (
    id           BIGSERIAL    PRIMARY KEY,
    ticker       TEXT         NOT NULL,
    analyst_firm TEXT,
    from_grade   TEXT,
    to_grade     TEXT         NOT NULL,
    action       TEXT,                    -- upgrade | downgrade | initiate | maintain
    price_target FLOAT,                   -- price target at time of grade (may be NULL)
    grade_date   TIMESTAMPTZ  NOT NULL,
    created_at   TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_fmp_grade_ticker_firm_date
        UNIQUE (ticker, analyst_firm, grade_date)
);

CREATE INDEX IF NOT EXISTS ix_fmp_ag_ticker_date
    ON fmp_analyst_grades (ticker, grade_date DESC);

SELECT 'Migration 012 complete: fact_validation_audit, fmp_analyst_grades created' AS status;
