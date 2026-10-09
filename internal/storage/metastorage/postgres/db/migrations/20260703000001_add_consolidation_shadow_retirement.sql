-- +goose Up
ALTER TABLE block_consolidation_shadow
    ADD COLUMN IF NOT EXISTS legacy_object_retired_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS legacy_object_retire_after TIMESTAMP;

-- +goose Down
ALTER TABLE block_consolidation_shadow
    DROP COLUMN IF EXISTS legacy_object_retired_at,
    DROP COLUMN IF EXISTS legacy_object_retire_after;
