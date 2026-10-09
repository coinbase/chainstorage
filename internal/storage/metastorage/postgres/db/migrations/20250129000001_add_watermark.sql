-- +goose Up
-- Add is_watermark column to canonical_blocks table
-- This column controls visibility of blocks to the streamer
-- Blocks are written with is_watermark=FALSE initially, then set to TRUE after validation
ALTER TABLE canonical_blocks ADD COLUMN is_watermark BOOLEAN NOT NULL DEFAULT FALSE;

-- Create partial index for efficient watermark queries
-- This index only includes watermarked blocks, keeping it small
CREATE INDEX idx_canonical_watermark ON canonical_blocks (tag, height DESC) WHERE is_watermark = TRUE;

-- Set watermark on the current highest block for each tag to prevent GetLatestBlock failures
-- This ensures continuity during migration by marking existing latest blocks as validated
-- Use CTE for better performance on large tables (avoids correlated subquery)
WITH max_heights AS (
    SELECT tag, MAX(height) as max_height
    FROM canonical_blocks
    GROUP BY tag
)
UPDATE canonical_blocks
SET is_watermark = TRUE
FROM max_heights
WHERE canonical_blocks.tag = max_heights.tag
  AND canonical_blocks.height = max_heights.max_height;

-- +goose Down
-- Drop the partial index first
DROP INDEX IF EXISTS idx_canonical_watermark;

-- Drop the is_watermark column
ALTER TABLE canonical_blocks DROP COLUMN IF EXISTS is_watermark;