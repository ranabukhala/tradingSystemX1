-- Migration 007: Core tables missing from initial schema
-- Creates: news_cluster, news_item, news_entity, summary, event
-- These are referenced by deduplicator and entity_resolver but were never created.

-- ── news_cluster ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS news_cluster (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── news_item ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS news_item (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source              VARCHAR(32)  NOT NULL,
    vendor_id           VARCHAR(128) NOT NULL,
    published_at        TIMESTAMPTZ  NOT NULL,
    received_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    url                 TEXT         NOT NULL,
    canonical_url       TEXT,
    title               TEXT         NOT NULL,
    snippet             TEXT,
    author              VARCHAR(256),
    content_hash        VARCHAR(64),
    full_text_ref       VARCHAR(512),

    -- Dedup
    cluster_id          UUID REFERENCES news_cluster(id),
    is_representative   BOOLEAN DEFAULT TRUE,

    -- Entity resolution
    tickers             TEXT[]   DEFAULT '{}',
    sectors             TEXT[]   DEFAULT '{}',
    themes              TEXT[]   DEFAULT '{}',
    ticker_confidence   JSONB    DEFAULT '{}',

    -- Classification
    catalyst_type       VARCHAR(32)  DEFAULT 'other',
    mode                VARCHAR(32)  DEFAULT 'general_market',
    session_context     VARCHAR(32)  DEFAULT 'intraday',
    market_cap_tier     VARCHAR(16),
    float_sensitivity   VARCHAR(16)  DEFAULT 'normal',
    short_interest_flag BOOLEAN      DEFAULT FALSE,

    -- Event linkage
    earnings_proximity_h INTEGER,
    decay_minutes        INTEGER,

    -- AI outputs
    t1_summary          TEXT,
    t2_summary          TEXT,
    facts_json          JSONB,

    -- Scores
    impact_day          FLOAT,
    impact_swing        FLOAT,
    regime_flag         VARCHAR(32),
    source_credibility  FLOAT,

    -- AI metadata
    prompt_version      VARCHAR(32),
    llm_tokens_used     INTEGER,
    llm_cost_usd        FLOAT,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_news_source_vendor UNIQUE (source, vendor_id)
);

CREATE INDEX IF NOT EXISTS ix_news_published_at    ON news_item (published_at);
CREATE INDEX IF NOT EXISTS ix_news_mode            ON news_item (mode);
CREATE INDEX IF NOT EXISTS ix_news_catalyst_type   ON news_item (catalyst_type);
CREATE UNIQUE INDEX IF NOT EXISTS ix_news_content_hash ON news_item (content_hash);
CREATE INDEX IF NOT EXISTS ix_news_impact_day      ON news_item (impact_day);
CREATE INDEX IF NOT EXISTS ix_news_impact_swing    ON news_item (impact_swing);
CREATE INDEX IF NOT EXISTS ix_news_tickers_gin     ON news_item USING gin (tickers);

-- ── news_entity ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS news_entity (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    news_item_id UUID NOT NULL REFERENCES news_item(id),
    entity_id    VARCHAR(32) NOT NULL,
    entity_type  VARCHAR(32),
    confidence   FLOAT,
    created_at   TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_news_entity UNIQUE (news_item_id, entity_id)
);

CREATE INDEX IF NOT EXISTS ix_news_entity_news_id ON news_entity (news_item_id);

-- ── summary ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS summary (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    news_item_id    UUID NOT NULL REFERENCES news_item(id),
    model           VARCHAR(64),
    prompt_version  VARCHAR(32),
    summary_text    TEXT,
    input_tokens    INTEGER,
    output_tokens   INTEGER,
    cost_usd        FLOAT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_summary_news_item_id ON summary (news_item_id);

-- ── event ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS event (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type          VARCHAR(32)  NOT NULL,   -- earnings | macro
    ticker              VARCHAR(16),
    company_name        VARCHAR(256),
    name                VARCHAR(256),
    fred_series_id      VARCHAR(32),
    event_date          TIMESTAMPTZ  NOT NULL,
    event_time          VARCHAR(16),             -- BMO | AMC | Unknown | HH:MM
    fiscal_quarter      VARCHAR(16),
    fiscal_year         INTEGER,

    -- Estimates
    whisper_eps         FLOAT,
    consensus_eps       FLOAT,
    consensus_revenue   FLOAT,
    consensus_estimate  FLOAT,
    prior_value         FLOAT,

    -- Actuals (null until event occurs)
    actual_eps          FLOAT,
    actual_revenue      FLOAT,
    actual_value        FLOAT,
    reported_at         TIMESTAMPTZ,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_event_ticker_date UNIQUE (event_type, ticker, event_date)
);

CREATE INDEX IF NOT EXISTS ix_event_date   ON event (event_date);
CREATE INDEX IF NOT EXISTS ix_event_ticker ON event (ticker);
CREATE INDEX IF NOT EXISTS ix_event_type   ON event (event_type);

SELECT 'Migration 007 complete: news_cluster, news_item, news_entity, summary, event created' AS status;
