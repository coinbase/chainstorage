-- +goose NO TRANSACTION

-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_promotable
    ON block_consolidation_shadow (tag, height, block_metadata_id)
    WHERE validated_at IS NOT NULL
        AND consolidated_object_key_main IS NOT NULL
        AND consolidated_object_key_main <> ''
        AND object_format = 1
        AND byte_offset >= 0
        AND byte_length > 0
        AND uncompressed_length IS NOT NULL
        AND uncompressed_length > 0;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_promotable;
