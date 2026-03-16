-- Migration 017: Semantic embedding deduplication (Tier 3.5)
-- Adds title_embedding vector column to news_item and a cosine-similarity HNSW index.
-- Requires pgvector 0.5+ (already installed as extension 'vector' v0.8.2).

-- Enable the extension (idempotent — OK if already enabled).
CREATE EXTENSION IF NOT EXISTS vector;

-- Add the embedding column.
-- 1536 dimensions = text-embedding-3-small output size.
-- Nullable so existing rows and items that fail the OpenAI call are unaffected.
ALTER TABLE news_item
    ADD COLUMN IF NOT EXISTS title_embedding vector(1536);

-- HNSW index for approximate nearest-neighbour cosine search.
-- m=16 / ef_construction=64 are pgvector defaults — good balance of accuracy vs build time.
-- The GIN index on tickers (ix_news_tickers_gin) handles the ticker pre-filter;
-- this index only needs to serve the vector distance computation.
CREATE INDEX IF NOT EXISTS ix_news_title_embedding_hnsw
    ON news_item
    USING hnsw (title_embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

COMMENT ON COLUMN news_item.title_embedding IS
    'OpenAI text-embedding-3-small (1536-dim) vector of the article title. '
    'NULL for items processed before migration 017 or when OpenAI call failed.';
