-- +goose NO TRANSACTION

-- +goose Up
-- The retirement selector starts from delete-after time rather than
-- height because repaired historical CSCBs become eligible out of order. Keep
-- the full CSCB object key out of this sorted B-tree; the existing hash index
-- serves the exact-key join without repeating large keys here.
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

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_retention_due;
