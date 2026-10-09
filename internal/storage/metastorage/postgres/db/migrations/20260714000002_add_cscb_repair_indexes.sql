-- +goose NO TRANSACTION

-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_metadata_cscb_repair_candidate
    ON block_metadata (tag, height DESC, object_key_main, id)
    WHERE object_format = 1
        AND object_key_main IS NOT NULL
        AND object_key_main <> '';

-- CSCB repair only performs exact object-key reference checks. B-trees over
-- the full keys and row IDs consume unnecessary space and the block_metadata
-- index exceeds Aurora local scratch storage on large chains such as Solana.
-- Hash indexes preserve the required equality lookups without storing or
-- sorting the full keys.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_metadata_object_key_reference
    ON block_metadata USING HASH (object_key_main)
    WHERE object_key_main IS NOT NULL
        AND object_key_main <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_object_key_reference
    ON block_consolidation_shadow USING HASH (consolidated_object_key_main)
    WHERE consolidated_object_key_main IS NOT NULL
        AND consolidated_object_key_main <> '';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_consolidation_shadow_object_key_reference;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_object_key_reference;
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_cscb_repair_candidate;
