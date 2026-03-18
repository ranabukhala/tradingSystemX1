-- Pipeline Latency Diagnostic Queries
-- =====================================
-- These queries operate on the `trade` table after migration 019 has run.
-- stage_timestamps JSONB contains one ISO-8601 timestamp per pipeline stage.
-- end_to_end_latency_ms is the pre-computed first→last stage duration in ms.
--
-- Stage order:
--   news_ingested → normalized → entity_resolved → ai_summarized →
--   signal_aggregated → pretrade_filtered → execution_submitted → execution_filled


-- ── 1. Summary statistics for the last 24 hours ──────────────────────────────
SELECT
    COUNT(*)                                                                 AS signal_count,
    ROUND(AVG(end_to_end_latency_ms))                                       AS avg_ms,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p50_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p95_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p99_ms,
    ROUND(MAX(end_to_end_latency_ms))                                       AS max_ms
FROM trade
WHERE created_at > NOW() - INTERVAL '24 hours'
  AND end_to_end_latency_ms IS NOT NULL;


-- ── 2. Slowest 20 signals in the last 24 hours ───────────────────────────────
SELECT
    id,
    ticker,
    catalyst_type,
    direction,
    ROUND(end_to_end_latency_ms)   AS e2e_ms,
    ROUND(end_to_end_latency_ms / 1000.0, 1) AS e2e_s,
    stage_timestamps,
    created_at
FROM trade
WHERE created_at > NOW() - INTERVAL '24 hours'
  AND end_to_end_latency_ms IS NOT NULL
ORDER BY end_to_end_latency_ms DESC
LIMIT 20;


-- ── 3. Hourly latency trend over the last 7 days ─────────────────────────────
SELECT
    DATE_TRUNC('hour', created_at)                                            AS hour,
    COUNT(*)                                                                   AS count,
    ROUND(AVG(end_to_end_latency_ms))                                         AS avg_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p95_ms
FROM trade
WHERE created_at > NOW() - INTERVAL '7 days'
  AND end_to_end_latency_ms IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- ── 4. Signals that hit the 10-second slow-pipeline warning threshold ─────────
SELECT
    ticker,
    catalyst_type,
    ROUND(end_to_end_latency_ms / 1000.0, 1)  AS e2e_s,
    stage_timestamps,
    created_at
FROM trade
WHERE created_at > NOW() - INTERVAL '24 hours'
  AND end_to_end_latency_ms > 10000
ORDER BY end_to_end_latency_ms DESC;


-- ── 5. Per-stage extraction (run in Python, not raw SQL) ─────────────────────
-- SQL cannot easily compute per-stage durations from the JSONB because the
-- timestamps are ISO strings that need datetime arithmetic.
-- Use PipelineTimer.stage_durations_ms() in Python instead:
--
--   import asyncpg, asyncio, json
--   from app.utils.pipeline_timer import PipelineTimer
--
--   async def main():
--       conn = await asyncpg.connect(DATABASE_URL)
--       rows = await conn.fetch(
--           "SELECT id, stage_timestamps FROM trade "
--           "WHERE created_at > NOW() - INTERVAL '24 hours' "
--           "  AND stage_timestamps != '{}'::jsonb"
--       )
--       for row in rows:
--           ts = json.loads(row["stage_timestamps"])
--           durations = PipelineTimer.stage_durations_ms(ts)
--           print(row["id"], durations)
--   asyncio.run(main())


-- ── 6. Latency by catalyst type ──────────────────────────────────────────────
SELECT
    catalyst_type,
    COUNT(*)                                                                   AS count,
    ROUND(AVG(end_to_end_latency_ms))                                         AS avg_ms,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p50_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p95_ms
FROM trade
WHERE created_at > NOW() - INTERVAL '7 days'
  AND end_to_end_latency_ms IS NOT NULL
GROUP BY catalyst_type
ORDER BY avg_ms DESC;


-- ── 7. Latency by route type (fast path vs slow path) ────────────────────────
SELECT
    route_type,
    COUNT(*)                                                                   AS count,
    ROUND(AVG(end_to_end_latency_ms))                                         AS avg_ms,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p50_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p95_ms
FROM trade
WHERE created_at > NOW() - INTERVAL '7 days'
  AND end_to_end_latency_ms IS NOT NULL
GROUP BY route_type
ORDER BY avg_ms DESC;


-- ── 8. Signals where ai_summarized stage exists (slow path only) ─────────────
SELECT
    COUNT(*)                                                                   AS count,
    ROUND(AVG(end_to_end_latency_ms))                                         AS avg_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY end_to_end_latency_ms)) AS p95_ms
FROM trade
WHERE created_at > NOW() - INTERVAL '24 hours'
  AND stage_timestamps ? 'ai_summarized';


-- ── 9. Quick verification after running migration + restarting services ───────
-- Expected: rows should appear within a few minutes of the first signal
SELECT
    id,
    ticker,
    ROUND(end_to_end_latency_ms) AS e2e_ms,
    stage_timestamps,
    created_at
FROM trade
ORDER BY created_at DESC
LIMIT 5;
