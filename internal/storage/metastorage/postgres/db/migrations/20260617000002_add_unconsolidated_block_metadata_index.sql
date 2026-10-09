-- +goose NO TRANSACTION

-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_metadata_unconsolidated
    ON block_metadata (tag, height)
    WHERE byte_length IS NULL AND skipped = false;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_block_metadata_unconsolidated;
