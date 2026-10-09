-- +goose Up
-- Consolidated block objects need longer object keys and per-block byte metadata.
ALTER TABLE block_metadata
    ALTER COLUMN object_key_main TYPE TEXT,
    ADD COLUMN byte_offset BIGINT,
    ADD COLUMN byte_length BIGINT,
    ADD COLUMN uncompressed_length BIGINT,
    ADD COLUMN object_format SMALLINT NOT NULL DEFAULT 0;

-- Shadow placements are written after the primary legacy block commit and validation.
CREATE TABLE block_consolidation_shadow (
    block_metadata_id BIGINT PRIMARY KEY REFERENCES block_metadata (id) ON DELETE RESTRICT,
    tag INT NOT NULL,
    height BIGINT NOT NULL,
    hash VARCHAR(66),
    legacy_object_key_main TEXT NOT NULL,
    consolidated_object_key_main TEXT NOT NULL,
    object_format SMALLINT NOT NULL DEFAULT 1,
    byte_offset BIGINT NOT NULL,
    byte_length BIGINT NOT NULL,
    uncompressed_length BIGINT,
    format_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    validated_at TIMESTAMP
);

CREATE INDEX idx_block_consolidation_shadow_tag_height
    ON block_consolidation_shadow (tag, height);

-- +goose Down
DROP INDEX IF EXISTS idx_block_consolidation_shadow_tag_height;
DROP TABLE IF EXISTS block_consolidation_shadow;

ALTER TABLE block_metadata
    DROP COLUMN IF EXISTS uncompressed_length,
    DROP COLUMN IF EXISTS byte_length,
    DROP COLUMN IF EXISTS byte_offset,
    DROP COLUMN IF EXISTS object_format;
