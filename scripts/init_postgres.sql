-- ─────────────────────────────────────────────
--  Postgres initialization script
--  Runs once on first container start
-- ─────────────────────────────────────────────

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable pg_trgm for fuzzy text search (used by deduplicator)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Enable btree_gin for composite GIN indexes
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Set timezone
SET timezone = 'UTC';

-- Confirm
SELECT 'Postgres initialized with pgvector, uuid-ossp, pg_trgm, btree_gin' AS status;
