-- +goose NO TRANSACTION

-- +goose Up
-- The due-cohort probe filters storage generation as well as tag, due time and
-- height, but idx_block_consolidation_shadow_retention_due leads with
-- (tag, single_block_delete_after, height) only. Every legacy-generation row
-- therefore has to be examined and discarded on each probe. On solana-mainnet
-- prod that is ~20.9M rows against ~768k v2 rows, which pushed the hourly
-- retention probe past its 60s statement timeout and stalled retention
-- entirely (INF-1330).
--
-- Leading with the generation lets the planner seek straight to the rows of the
-- active write generation, so probe cost tracks live work rather than the
-- accumulated history of superseded generations.
--
-- This index is only reachable because the queries now match generation with
-- plain equality (or IS NULL for the legacy generation). The previous
-- `IS NOT DISTINCT FROM NULLIF($n, '')` form cannot use a btree index at all,
-- so this migration is inert without that code change.
--
-- The predicate is kept identical to the existing due index so both describe
-- the same row set. The older index is deliberately left in place: it still
-- serves probes that do not constrain generation, and dropping it belongs in a
-- follow-up once production plans confirm the new one is chosen.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_retention_due_generation
    ON block_consolidation_shadow (
        tag,
        single_block_storage_generation,
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

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_retention_due_generation;
