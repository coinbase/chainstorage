-- +goose NO TRANSACTION

-- +goose Up
-- Drops idx_block_consolidation_shadow_retention_watermark, added one migration
-- earlier in 20260818000001. It was a net loss in production and the measurement
-- that proves it could only be taken with the index in place.
--
-- What it was for: MIN(height) over rows that still hold an undeleted
-- single-block object, so the retention cron can advance its probe floor. That
-- part worked exactly as designed — on solana-mainnet prod the lookup went from
-- 44.5s unbounded to 0.058ms, and it removed the sequential-scan cliff from the
-- due-cohort probe at every range width tested.
--
-- Why it still had to go: the planner also picked it up for the *due-cohort
-- probe*, and there it is much worse in wall clock despite a lower estimated
-- cost. Measured on prod, same query, same floor, bare execution (no EXPLAIN
-- instrumentation), three runs each:
--
--     with the index      36.8 / 37.1 / 36.8 s   cost   161,219
--     without the index   14.4 / 14.6 / 16.3 s   cost 1,695,921
--
-- The cause is a row misestimate — rows=1 estimated against 138,379 actual —
-- which makes a serial nested-loop plan look cheap and displaces the parallel
-- index scan on idx_block_consolidation_shadow_tag_height. Statistics tuning
-- would not have rescued it: with nested loops disabled the planner fell back to
-- sequential scans of canonical_blocks (183M rows) and took 91.6s, so the
-- nested-loop plan was already the best available *while this index existed*.
-- Its mere presence is what leads the planner astray.
--
-- The retention floor watermark does not need it. The watermark query carries
-- its own `height >= approved_start_height` bound, which is both semantically
-- right (retention may not delete below the approval floor) and enough to keep
-- the lookup cheap: 97ms on prod through idx_block_consolidation_shadow_tag_height,
-- against a probe that costs seconds. That bound is why this is a clean
-- subtraction rather than a redesign.
--
-- If the sequential-scan cliff is worth removing later, it needs an index shape
-- the due-cohort probe cannot use, or a restructured cohort-expansion join —
-- designed with both queries measured together rather than one in isolation.
-- Do not simply re-add this index.
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_retention_watermark;

-- +goose Down
-- Recreating it restores the regression above. The definition is kept here only
-- so the migration is reversible; see 20260818000001 for the original rationale.
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
