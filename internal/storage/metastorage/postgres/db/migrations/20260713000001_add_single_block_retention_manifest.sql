-- +goose Up
-- The three compatibility columns were deployed before the single-block
-- terminology was standardized. Keep schema reads and writes synchronized
-- during rolling deployments. This compatibility does not authorize retirement
-- execution until every single-block writer runs the guarded uploader.
ALTER TABLE block_consolidation_shadow
    ALTER COLUMN legacy_object_key_main DROP NOT NULL,
    ADD COLUMN single_block_object_key_main TEXT,
    ADD COLUMN single_block_retention_started_at TIMESTAMP,
    ADD COLUMN single_block_delete_after TIMESTAMP,
    ADD COLUMN single_block_object_deleted_at TIMESTAMPTZ;

UPDATE block_consolidation_shadow
SET single_block_object_key_main = legacy_object_key_main,
    single_block_retention_started_at = legacy_object_retired_at,
    single_block_delete_after = legacy_object_retire_after;

-- +goose StatementBegin
CREATE FUNCTION sync_block_consolidation_shadow_single_block_columns()
RETURNS TRIGGER AS $$
DECLARE
    compatibility_key_changed BOOLEAN;
    canonical_key_changed BOOLEAN;
    compatibility_started_at_changed BOOLEAN;
    canonical_started_at_changed BOOLEAN;
    compatibility_delete_after_changed BOOLEAN;
    canonical_delete_after_changed BOOLEAN;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.single_block_object_key_main IS NULL THEN
            NEW.single_block_object_key_main := NEW.legacy_object_key_main;
        ELSIF NEW.legacy_object_key_main IS NULL THEN
            NEW.legacy_object_key_main := NEW.single_block_object_key_main;
        ELSIF NEW.single_block_object_key_main IS DISTINCT FROM NEW.legacy_object_key_main THEN
            RAISE EXCEPTION 'single-block object key compatibility columns disagree for block_metadata id %', NEW.block_metadata_id;
        END IF;

        IF NEW.single_block_retention_started_at IS NULL THEN
            NEW.single_block_retention_started_at := NEW.legacy_object_retired_at;
        ELSIF NEW.legacy_object_retired_at IS NULL THEN
            NEW.legacy_object_retired_at := NEW.single_block_retention_started_at;
        ELSIF NEW.single_block_retention_started_at IS DISTINCT FROM NEW.legacy_object_retired_at THEN
            RAISE EXCEPTION 'single-block retention start compatibility columns disagree for block_metadata id %', NEW.block_metadata_id;
        END IF;

        IF NEW.single_block_delete_after IS NULL THEN
            NEW.single_block_delete_after := NEW.legacy_object_retire_after;
        ELSIF NEW.legacy_object_retire_after IS NULL THEN
            NEW.legacy_object_retire_after := NEW.single_block_delete_after;
        ELSIF NEW.single_block_delete_after IS DISTINCT FROM NEW.legacy_object_retire_after THEN
            RAISE EXCEPTION 'single-block delete-after compatibility columns disagree for block_metadata id %', NEW.block_metadata_id;
        END IF;
    ELSE
        compatibility_key_changed := NEW.legacy_object_key_main IS DISTINCT FROM OLD.legacy_object_key_main;
        canonical_key_changed := NEW.single_block_object_key_main IS DISTINCT FROM OLD.single_block_object_key_main;
        IF compatibility_key_changed AND canonical_key_changed
            AND NEW.legacy_object_key_main IS DISTINCT FROM NEW.single_block_object_key_main THEN
            RAISE EXCEPTION 'single-block object key compatibility columns changed to different values for block_metadata id %', NEW.block_metadata_id;
        ELSIF compatibility_key_changed THEN
            NEW.single_block_object_key_main := NEW.legacy_object_key_main;
        ELSIF canonical_key_changed THEN
            NEW.legacy_object_key_main := NEW.single_block_object_key_main;
        ELSIF NEW.legacy_object_key_main IS DISTINCT FROM NEW.single_block_object_key_main THEN
            RAISE EXCEPTION 'single-block object key compatibility columns disagree for block_metadata id %', NEW.block_metadata_id;
        END IF;

        compatibility_started_at_changed := NEW.legacy_object_retired_at IS DISTINCT FROM OLD.legacy_object_retired_at;
        canonical_started_at_changed := NEW.single_block_retention_started_at IS DISTINCT FROM OLD.single_block_retention_started_at;
        IF compatibility_started_at_changed AND canonical_started_at_changed
            AND NEW.legacy_object_retired_at IS DISTINCT FROM NEW.single_block_retention_started_at THEN
            RAISE EXCEPTION 'single-block retention start compatibility columns changed to different values for block_metadata id %', NEW.block_metadata_id;
        ELSIF compatibility_started_at_changed THEN
            NEW.single_block_retention_started_at := NEW.legacy_object_retired_at;
        ELSIF canonical_started_at_changed THEN
            NEW.legacy_object_retired_at := NEW.single_block_retention_started_at;
        ELSIF NEW.legacy_object_retired_at IS DISTINCT FROM NEW.single_block_retention_started_at THEN
            RAISE EXCEPTION 'single-block retention start compatibility columns disagree for block_metadata id %', NEW.block_metadata_id;
        END IF;

        compatibility_delete_after_changed := NEW.legacy_object_retire_after IS DISTINCT FROM OLD.legacy_object_retire_after;
        canonical_delete_after_changed := NEW.single_block_delete_after IS DISTINCT FROM OLD.single_block_delete_after;
        IF compatibility_delete_after_changed AND canonical_delete_after_changed
            AND NEW.legacy_object_retire_after IS DISTINCT FROM NEW.single_block_delete_after THEN
            RAISE EXCEPTION 'single-block delete-after compatibility columns changed to different values for block_metadata id %', NEW.block_metadata_id;
        ELSIF compatibility_delete_after_changed THEN
            NEW.single_block_delete_after := NEW.legacy_object_retire_after;
        ELSIF canonical_delete_after_changed THEN
            NEW.legacy_object_retire_after := NEW.single_block_delete_after;
        ELSIF NEW.legacy_object_retire_after IS DISTINCT FROM NEW.single_block_delete_after THEN
            RAISE EXCEPTION 'single-block delete-after compatibility columns disagree for block_metadata id %', NEW.block_metadata_id;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_consolidation_shadow_single_block_compatibility_trigger
BEFORE INSERT OR UPDATE OF
    legacy_object_key_main,
    legacy_object_retired_at,
    legacy_object_retire_after,
    single_block_object_key_main,
    single_block_retention_started_at,
    single_block_delete_after
ON block_consolidation_shadow
FOR EACH ROW
EXECUTE FUNCTION sync_block_consolidation_shadow_single_block_columns();

ALTER TABLE block_consolidation_shadow
    ADD CONSTRAINT block_consolidation_shadow_single_block_retention_state CHECK (
        legacy_object_key_main IS NOT DISTINCT FROM single_block_object_key_main
        AND legacy_object_retired_at IS NOT DISTINCT FROM single_block_retention_started_at
        AND legacy_object_retire_after IS NOT DISTINCT FROM single_block_delete_after
        AND (
            (single_block_object_key_main IS NOT NULL AND single_block_object_deleted_at IS NULL)
            OR
            (single_block_object_key_main IS NULL AND single_block_object_deleted_at IS NOT NULL)
        )
    ) NOT VALID;

CREATE TABLE block_single_block_retention (
    block_metadata_id BIGINT PRIMARY KEY REFERENCES block_consolidation_shadow (block_metadata_id) ON DELETE RESTRICT,
    tag INTEGER NOT NULL,
    height BIGINT NOT NULL CHECK (height >= 0),
    hash VARCHAR(66),
    state TEXT NOT NULL CHECK (state IN ('eligible', 'deleting', 'deleted_verified')),
    bucket TEXT NOT NULL CHECK (bucket <> ''),
    single_block_object_key_main TEXT,
    single_block_object_key_sha256 CHAR(64) NOT NULL CHECK (single_block_object_key_sha256 ~ '^[0-9a-f]{64}$'),
    single_block_object_version_ids TEXT[] NOT NULL CHECK (cardinality(single_block_object_version_ids) = 1 AND single_block_object_version_ids[1] <> ''),
    single_block_object_etag TEXT NOT NULL,
    single_block_object_bytes BIGINT NOT NULL CHECK (single_block_object_bytes > 0),
    consolidated_object_key_main TEXT NOT NULL CHECK (consolidated_object_key_main <> ''),
    consolidated_object_version_id TEXT NOT NULL CHECK (consolidated_object_version_id <> ''),
    consolidated_object_etag TEXT NOT NULL CHECK (consolidated_object_etag <> ''),
    consolidated_byte_offset BIGINT NOT NULL CHECK (consolidated_byte_offset >= 0),
    consolidated_byte_length BIGINT NOT NULL CHECK (consolidated_byte_length > 0),
    consolidated_uncompressed_length BIGINT NOT NULL CHECK (consolidated_uncompressed_length > 0),
    payload_sha256 CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    outcome TEXT NOT NULL DEFAULT '',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    prepared_at TIMESTAMPTZ NOT NULL,
    delete_started_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (state = 'eligible' AND single_block_object_key_main IS NOT NULL AND single_block_object_key_main <> '' AND single_block_object_etag <> '' AND delete_started_at IS NULL AND deleted_at IS NULL)
        OR
        (state = 'deleting' AND single_block_object_key_main IS NOT NULL AND single_block_object_key_main <> '' AND single_block_object_etag <> '' AND delete_started_at IS NOT NULL AND last_attempt_at IS NOT NULL AND attempt_count > 0 AND deleted_at IS NULL)
        OR
        (state = 'deleted_verified' AND single_block_object_key_main IS NULL AND single_block_object_etag = '' AND outcome <> '' AND delete_started_at IS NOT NULL AND last_attempt_at IS NOT NULL AND attempt_count > 0 AND deleted_at IS NOT NULL)
    )
);

CREATE INDEX idx_block_single_block_retention_pending
    ON block_single_block_retention (tag, height, block_metadata_id)
    WHERE state IN ('eligible', 'deleting');

-- +goose Down
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM block_single_block_retention) THEN
        RAISE EXCEPTION 'cannot roll back single-block retention migration while retirement manifests exist';
    END IF;
    IF EXISTS (SELECT 1 FROM block_consolidation_shadow WHERE single_block_object_key_main IS NULL) THEN
        RAISE EXCEPTION 'cannot roll back single-block retention migration after a verified deletion';
    END IF;
END $$;

DROP INDEX IF EXISTS idx_block_single_block_retention_pending;
DROP TABLE IF EXISTS block_single_block_retention;

ALTER TABLE block_consolidation_shadow
    DROP CONSTRAINT IF EXISTS block_consolidation_shadow_single_block_retention_state;

DROP TRIGGER IF EXISTS block_consolidation_shadow_single_block_compatibility_trigger ON block_consolidation_shadow;
DROP FUNCTION IF EXISTS sync_block_consolidation_shadow_single_block_columns();

ALTER TABLE block_consolidation_shadow
    ALTER COLUMN legacy_object_key_main SET NOT NULL,
    DROP COLUMN IF EXISTS single_block_object_deleted_at,
    DROP COLUMN IF EXISTS single_block_delete_after,
    DROP COLUMN IF EXISTS single_block_retention_started_at,
    DROP COLUMN IF EXISTS single_block_object_key_main;
