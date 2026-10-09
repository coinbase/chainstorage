-- +goose NO TRANSACTION

-- +goose Up
-- Drops idx_block_consolidation_shadow_retention_due, superseded by
-- idx_block_consolidation_shadow_retention_due_generation in 20260817000001.
--
-- That migration deliberately kept this one, on the reasoning that it still
-- served probes which do not constrain storage generation, and said dropping it
-- "belongs in a follow-up once production plans confirm the new one is chosen."
-- This is that follow-up. Measured on solana-mainnet prod:
--
--     index                                   size     scans   tuples read
--     --------------------------------------  -------  ------  -----------
--     ..._retention_due             (this)     1732 MB       0            0
--     ..._retention_due_generation             1389 MB     120    2,838,841
--
-- pg_stat_database.stats_reset is NULL on this database, so those counters are
-- lifetime rather than a recent window: nothing has ever used this index. The
-- reading was also taken after the watermark index was dropped in
-- 20260818000002, so it reflects the planner's current choices rather than a
-- state where some other index was crowding it out.
--
-- Both indexes carry the same partial predicate; the survivor simply leads with
-- (tag, single_block_storage_generation, ...) instead of (tag, ...), which is
-- what makes the retention probe able to seek straight to the active write
-- generation rather than walking superseded ones.
--
-- Worth noting this is not only 1.7 GB of disk. Every insert into
-- block_consolidation_shadow has been maintaining it, and historical backfill
-- writes those rows in bulk — so this also removes write amplification from the
-- hottest path on the table.
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_retention_due;

-- +goose Down
-- Definition preserved verbatim from 20260723000001 so the rollback is exact.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_retention_due
    ON block_consolidation_shadow (
        tag,
        single_block_delete_after,
        height
    )
    WHERE validated_at IS NOT NULL
        AND single_block_delete_after IS NOT NULL
        AND single_block_object_deleted_at IS NULL
        AND single_block_object_key_main IS NOT NULL
        AND single_block_object_key_main <> ''
        AND consolidated_object_key_main IS NOT NULL
        AND consolidated_object_key_main <> '';
