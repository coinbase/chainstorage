-- +goose Up
-- Create block_metadata table (append-only storage for every block ever observed)
CREATE TABLE block_metadata (
    id BIGSERIAL PRIMARY KEY, -- for canonical_blocks and event fk reference
    height BIGINT NOT NULL,
    tag INT NOT NULL,
    hash VARCHAR(66), -- can hold a "0x"+64-hex string
    parent_hash VARCHAR(66),
    parent_height BIGINT NOT NULL DEFAULT 0,
    object_key_main VARCHAR(255),
    timestamp BIGINT NOT NULL, -- Unix timestamp in seconds
    skipped BOOLEAN NOT NULL DEFAULT FALSE
);

-- Enforce uniqueness rules based on block processing status:
-- 1. For normal blocks (skipped = FALSE), (tag, hash) must be unique, even if hash is NULL.
-- 2. For skipped blocks (skipped = TRUE), hash is always NULL, so enforce uniqueness on (tag, height) instead.
-- These partial unique indexes ensure correct behavior for both cases without violating constraints on NULLs.
CREATE UNIQUE INDEX unique_tag_hash_regular ON block_metadata(tag, hash) 
WHERE hash IS NOT NULL AND NOT skipped;
CREATE UNIQUE INDEX unique_tag_height_skipped ON block_metadata(tag, height) 
WHERE skipped = true;

-- Create canonical_blocks table (track the "winning" block at each height)
CREATE TABLE canonical_blocks (
    height BIGINT NOT NULL,
    block_metadata_id BIGINT NOT NULL,
    tag INT NOT NULL,
    -- Constraints
    PRIMARY KEY (height, tag),
    UNIQUE (
        height,
        tag,
        block_metadata_id
    ), -- Prevent same block from being canonical multiple times
    FOREIGN KEY (block_metadata_id) REFERENCES block_metadata (id) ON DELETE RESTRICT
);

-- Supports: JOINs between canonical_blocks and block_metadata tables
CREATE INDEX idx_canonical_block_metadata_fk ON canonical_blocks (block_metadata_id);

-- Create block_events table (append-only stream of all blockchain state changes)
CREATE TYPE event_type_enum AS ENUM ('BLOCK_ADDED', 'BLOCK_REMOVED', 'UNKNOWN');

CREATE TABLE block_events (
    event_tag INT NOT NULL DEFAULT 0, -- version
    event_sequence BIGINT NOT NULL, -- monotonically-increasing per tag
    event_type event_type_enum NOT NULL,
    block_metadata_id BIGINT NOT NULL, -- fk referencing block_metadata 
    height BIGINT NOT NULL,
    hash VARCHAR(66),
    -- Constraints
    PRIMARY KEY (event_tag, event_sequence),
    FOREIGN KEY (block_metadata_id) REFERENCES block_metadata (id) ON DELETE RESTRICT
);

-- Supports: GetEventsByBlockHeight(), GetFirstEventIdByBlockHeight()
CREATE INDEX idx_events_height_tag ON block_events (height, event_tag);
-- Supports: JOINs to get full block details from events
CREATE INDEX idx_events_block_meta ON block_events (block_metadata_id);

-- +goose Down
-- Drop tables in reverse order due to foreign key constraints

DROP TABLE IF EXISTS block_events;

DROP TABLE IF EXISTS canonical_blocks;

DROP TABLE IF EXISTS block_metadata;

-- Drop custom types
DROP TYPE IF EXISTS event_type_enum; 