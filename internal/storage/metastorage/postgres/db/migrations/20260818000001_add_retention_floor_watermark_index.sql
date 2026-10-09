-- +goose NO TRANSACTION

-- +goose Up
-- Supports the retention floor watermark: the lowest height that still holds an
-- undeleted single-block object for a storage generation. The cron probes from
-- that height instead of from the operator's approved_start_height, which keeps
-- the probe's height range narrow as the chain grows (INF-1330).
--
-- Why the range has to stay narrow: the due-cohort query joins its due_keys CTE
-- back to block_consolidation_shadow to expand each cohort, and that join is
-- bounded only by the height range. Measured on solana-mainnet prod, upper
-- bound pinned at the consolidation cursor:
--
--     range width   plan
--     ------------  ----------------------------------------------
--       549,000     Parallel Index Scan (idx_..._tag_height)
--     1,400,000     Parallel Index Scan
--     1,600,000     Parallel Seq Scan     <- planner abandons the index
--   439,880,000     Parallel Seq Scan, aborted past 150s
--
-- The flip happens where the two plans are near-equal in cost, so there is no
-- warning shoulder. With a fixed floor the range grows at the chain's block
-- rate (~216k/day on Solana) and crosses that threshold on its own; a watermark
-- floor holds it at the retention delay expressed in blocks (~650k at 72h).
--
-- The partial predicate keeps only rows that still have work, so entries leave
-- the index as retention deletes them and MIN(height) is answered from the
-- front of the index rather than by walking already-deleted history. That is
-- what stops the watermark lookup itself from degrading over time: bounded by
-- approved_start_height but without this index, the same lookup has to skip
-- every row retention has already finished with.
--
-- single_block_storage_generation IS NOT NULL is part of the predicate so
-- legacy-generation rows stay out. They are never retired individually — the v1
-- bucket is dropped wholesale — and including them would hold ~21M permanently
-- undeleted entries in the index forever. The watermark query repeats that
-- clause verbatim so the planner matches the partial predicate syntactically
-- rather than having to infer it from the equality test.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_retention_watermark
    ON block_consolidation_shadow (
        tag,
        single_block_storage_generation,
        height
    )
    WHERE single_block_object_deleted_at IS NULL
        AND single_block_object_key_main IS NOT NULL
        AND single_block_object_key_main <> ''
        AND single_block_storage_generation IS NOT NULL;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_retention_watermark;
