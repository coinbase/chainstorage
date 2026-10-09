-- +goose Up
-- Add timestamp index to block_metadata table for efficient time-based queries
CREATE INDEX idx_block_metadata_timestamp ON block_metadata(timestamp);

-- +goose Down
-- Remove the timestamp index
DROP INDEX IF EXISTS idx_block_metadata_timestamp;