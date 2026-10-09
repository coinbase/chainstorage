-- +goose Up
-- This migration hardens the preceding retirement manifest migration.
-- Fail closed if that draft schema was ever used; its in-flight rows cannot be
-- assigned trustworthy claims or verification timestamps retroactively.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM block_single_block_retention) THEN
        RAISE EXCEPTION 'cannot harden single-block retirement schema while retirement manifests exist';
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE block_metadata
    ADD COLUMN single_block_retention_fenced_at TIMESTAMPTZ;

ALTER TABLE block_metadata
    ADD CONSTRAINT block_metadata_single_block_retirement_fence CHECK (
        single_block_retention_fenced_at IS NULL
        OR (
            object_key_main IS NOT NULL
            AND object_key_main <> ''
            AND object_format = 1
            AND byte_offset IS NOT NULL
            AND byte_offset >= 0
            AND byte_length IS NOT NULL
            AND byte_length > 0
            AND uncompressed_length IS NOT NULL
            AND uncompressed_length > 0
        )
    ) NOT VALID;

-- +goose StatementBegin
CREATE FUNCTION enforce_block_metadata_single_block_retirement_fence()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.single_block_retention_fenced_at IS NOT NULL
        AND (
            NEW.single_block_retention_fenced_at IS DISTINCT FROM OLD.single_block_retention_fenced_at
            OR NEW.object_key_main IS DISTINCT FROM OLD.object_key_main
            OR NEW.object_format IS DISTINCT FROM OLD.object_format
            OR NEW.byte_offset IS DISTINCT FROM OLD.byte_offset
            OR NEW.byte_length IS DISTINCT FROM OLD.byte_length
            OR NEW.uncompressed_length IS DISTINCT FROM OLD.uncompressed_length
        ) THEN
        RAISE EXCEPTION 'cannot change CSCB placement after single-block object retirement is fenced for block_metadata id %', OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_metadata_single_block_retirement_fence_trigger
BEFORE UPDATE OF single_block_retention_fenced_at, object_key_main, object_format, byte_offset, byte_length, uncompressed_length ON block_metadata
FOR EACH ROW
WHEN (OLD.single_block_retention_fenced_at IS NOT NULL)
EXECUTE FUNCTION enforce_block_metadata_single_block_retirement_fence();

-- +goose StatementBegin
CREATE FUNCTION enforce_block_consolidation_shadow_single_block_retirement()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.single_block_object_deleted_at IS NOT NULL
        AND (
            NEW.single_block_object_key_main IS DISTINCT FROM OLD.single_block_object_key_main
            OR NEW.single_block_object_deleted_at IS DISTINCT FROM OLD.single_block_object_deleted_at
        ) THEN
        RAISE EXCEPTION 'cannot restore or rewrite deleted single-block object metadata for block_metadata id %', OLD.block_metadata_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_consolidation_shadow_single_block_retirement_trigger
BEFORE UPDATE OF single_block_object_key_main, single_block_object_deleted_at ON block_consolidation_shadow
FOR EACH ROW
WHEN (OLD.single_block_object_deleted_at IS NOT NULL)
EXECUTE FUNCTION enforce_block_consolidation_shadow_single_block_retirement();

ALTER TABLE block_single_block_retention
    ADD COLUMN claim_token TEXT,
    ADD COLUMN claim_expires_at TIMESTAMPTZ,
    ADD COLUMN verified_at TIMESTAMPTZ;

ALTER TABLE block_single_block_retention
    DROP CONSTRAINT block_single_block_retention_state_check,
    DROP CONSTRAINT block_single_block_retention_check;

ALTER TABLE block_single_block_retention
    ADD CONSTRAINT block_single_block_retention_state_check CHECK (
        state IN ('eligible', 'deleting', 'deleted_pending_verification', 'deleted_verified')
    ),
    ADD CONSTRAINT block_single_block_retention_immutable_version_ids_check CHECK (
        cardinality(single_block_object_version_ids) = 1
        AND single_block_object_version_ids[1] IS NOT NULL
        AND BTRIM(single_block_object_version_ids[1]) <> ''
        AND LOWER(BTRIM(single_block_object_version_ids[1])) <> 'null'
        AND consolidated_object_version_id IS NOT NULL
        AND BTRIM(consolidated_object_version_id) <> ''
        AND LOWER(BTRIM(consolidated_object_version_id)) <> 'null'
    ),
    ADD CONSTRAINT block_single_block_retention_lifecycle_check CHECK (
        (
            state = 'eligible'
            AND single_block_object_key_main IS NOT NULL
            AND single_block_object_key_main <> ''
            AND single_block_object_etag <> ''
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
            AND delete_started_at IS NULL
            AND deleted_at IS NULL
            AND verified_at IS NULL
        )
        OR (
            state = 'deleting'
            AND single_block_object_key_main IS NOT NULL
            AND single_block_object_key_main <> ''
            AND single_block_object_etag <> ''
            AND claim_token IS NOT NULL
            AND claim_token <> ''
            AND claim_expires_at IS NOT NULL
            AND delete_started_at IS NOT NULL
            AND delete_started_at >= prepared_at
            AND last_attempt_at IS NOT NULL
            AND last_attempt_at >= delete_started_at
            AND attempt_count > 0
            AND deleted_at IS NULL
            AND verified_at IS NULL
        )
        OR (
            state = 'deleted_pending_verification'
            AND single_block_object_key_main IS NULL
            AND single_block_object_etag = ''
            AND outcome <> ''
            AND claim_token IS NOT NULL
            AND claim_token <> ''
            AND claim_expires_at IS NOT NULL
            AND delete_started_at IS NOT NULL
            AND last_attempt_at IS NOT NULL
            AND attempt_count > 0
            AND deleted_at IS NOT NULL
            AND deleted_at >= delete_started_at
            AND last_attempt_at >= deleted_at
            AND verified_at IS NULL
        )
        OR (
            state = 'deleted_verified'
            AND single_block_object_key_main IS NULL
            AND single_block_object_etag = ''
            AND outcome <> ''
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
            AND delete_started_at IS NOT NULL
            AND last_attempt_at IS NOT NULL
            AND attempt_count > 0
            AND deleted_at IS NOT NULL
            AND deleted_at >= delete_started_at
            AND verified_at IS NOT NULL
            AND verified_at >= deleted_at
            AND last_attempt_at >= verified_at
        )
    );

CREATE TABLE cscb_retirement_safety_observation (
    bucket TEXT NOT NULL CHECK (bucket <> ''),
    consolidated_object_key_main TEXT NOT NULL CHECK (consolidated_object_key_main <> ''),
    configuration_sha256 CHAR(64) NOT NULL CHECK (configuration_sha256 ~ '^[0-9a-f]{64}$'),
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_observed_at TIMESTAMPTZ NOT NULL CHECK (last_observed_at >= first_observed_at),
    PRIMARY KEY (bucket, consolidated_object_key_main)
);

-- +goose StatementBegin
CREATE FUNCTION enforce_single_block_object_retirement_insert()
RETURNS TRIGGER AS $$
DECLARE
    metadata_matches BOOLEAN;
BEGIN
    -- Let an idempotent INSERT ... ON CONFLICT reach its conflict handler. The
    -- existing row remains protected by the update and delete triggers.
    IF EXISTS (
        SELECT 1
        FROM block_single_block_retention
        WHERE block_metadata_id = NEW.block_metadata_id
    ) THEN
        RETURN NEW;
    END IF;

    IF NEW.state <> 'eligible'
        OR NEW.outcome <> ''
        OR NEW.attempt_count <> 0
        OR NEW.claim_token IS NOT NULL
        OR NEW.claim_expires_at IS NOT NULL
        OR NEW.delete_started_at IS NOT NULL
        OR NEW.last_attempt_at IS NOT NULL
        OR NEW.deleted_at IS NOT NULL
        OR NEW.verified_at IS NOT NULL THEN
        RAISE EXCEPTION 'single-block retirement manifest must be inserted in eligible state for block_metadata id %', NEW.block_metadata_id;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM canonical_blocks cb
        JOIN block_metadata bm
            ON bm.id = cb.block_metadata_id
            AND bm.tag = cb.tag
            AND bm.height = cb.height
        JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
        WHERE bm.id = NEW.block_metadata_id
            AND bm.tag = NEW.tag
            AND bm.height = NEW.height
            AND bm.hash IS NOT DISTINCT FROM NEW.hash
            AND NOT bm.skipped
            AND bm.object_key_main = NEW.consolidated_object_key_main
            AND bm.object_format = 1
            AND bm.byte_offset = NEW.consolidated_byte_offset
            AND bm.byte_length = NEW.consolidated_byte_length
            AND bm.uncompressed_length = NEW.consolidated_uncompressed_length
            AND bm.single_block_retention_fenced_at IS NOT NULL
            AND shadow.tag = NEW.tag
            AND shadow.height = NEW.height
            AND shadow.hash IS NOT DISTINCT FROM NEW.hash
            AND shadow.single_block_object_key_main = NEW.single_block_object_key_main
            AND shadow.consolidated_object_key_main = NEW.consolidated_object_key_main
            AND shadow.object_format = 1
            AND shadow.byte_offset = NEW.consolidated_byte_offset
            AND shadow.byte_length = NEW.consolidated_byte_length
            AND shadow.uncompressed_length = NEW.consolidated_uncompressed_length
            AND shadow.validated_at IS NOT NULL
            AND shadow.single_block_retention_started_at IS NOT NULL
            AND shadow.single_block_delete_after IS NOT NULL
            AND shadow.single_block_delete_after <= CURRENT_TIMESTAMP
            AND shadow.single_block_object_deleted_at IS NULL
    ) INTO metadata_matches;
    IF NOT metadata_matches THEN
        RAISE EXCEPTION 'single-block retirement manifest does not match fenced canonical CSCB metadata for block_metadata id %', NEW.block_metadata_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_single_block_retention_insert_trigger
BEFORE INSERT ON block_single_block_retention
FOR EACH ROW
EXECUTE FUNCTION enforce_single_block_object_retirement_insert();

-- +goose StatementBegin
CREATE FUNCTION enforce_single_block_object_retirement_transition()
RETURNS TRIGGER AS $$
DECLARE
    shadow_single_block_key TEXT;
    shadow_deleted_at TIMESTAMPTZ;
BEGIN
    IF NEW.block_metadata_id IS DISTINCT FROM OLD.block_metadata_id
        OR NEW.tag IS DISTINCT FROM OLD.tag
        OR NEW.height IS DISTINCT FROM OLD.height
        OR NEW.hash IS DISTINCT FROM OLD.hash
        OR NEW.bucket IS DISTINCT FROM OLD.bucket
        OR NEW.single_block_object_key_sha256 IS DISTINCT FROM OLD.single_block_object_key_sha256
        OR NEW.single_block_object_version_ids IS DISTINCT FROM OLD.single_block_object_version_ids
        OR NEW.single_block_object_bytes IS DISTINCT FROM OLD.single_block_object_bytes
        OR NEW.consolidated_object_key_main IS DISTINCT FROM OLD.consolidated_object_key_main
        OR NEW.consolidated_object_version_id IS DISTINCT FROM OLD.consolidated_object_version_id
        OR NEW.consolidated_object_etag IS DISTINCT FROM OLD.consolidated_object_etag
        OR NEW.consolidated_byte_offset IS DISTINCT FROM OLD.consolidated_byte_offset
        OR NEW.consolidated_byte_length IS DISTINCT FROM OLD.consolidated_byte_length
        OR NEW.consolidated_uncompressed_length IS DISTINCT FROM OLD.consolidated_uncompressed_length
        OR NEW.payload_sha256 IS DISTINCT FROM OLD.payload_sha256
        OR NEW.prepared_at IS DISTINCT FROM OLD.prepared_at THEN
        RAISE EXCEPTION 'cannot change pinned retirement manifest fields for block_metadata id %', OLD.block_metadata_id;
    END IF;

    IF NOT (
        (OLD.state = 'eligible' AND NEW.state IN ('eligible', 'deleting'))
        OR (OLD.state = 'deleting' AND NEW.state IN ('deleting', 'deleted_pending_verification'))
        OR (OLD.state = 'deleted_pending_verification' AND NEW.state IN ('deleted_pending_verification', 'deleted_verified'))
        OR (OLD.state = 'deleted_verified' AND NEW.state = 'deleted_verified')
    ) THEN
        RAISE EXCEPTION 'invalid single-block retirement transition from % to % for block_metadata id %', OLD.state, NEW.state, OLD.block_metadata_id;
    END IF;

    IF OLD.state IN ('eligible', 'deleting')
        AND NEW.state IN ('eligible', 'deleting')
        AND (
            NEW.single_block_object_key_main IS DISTINCT FROM OLD.single_block_object_key_main
            OR NEW.single_block_object_etag IS DISTINCT FROM OLD.single_block_object_etag
        ) THEN
        RAISE EXCEPTION 'cannot change single-block object identity before verified deletion for block_metadata id %', OLD.block_metadata_id;
    END IF;

    IF OLD.state IN ('deleting', 'deleted_pending_verification')
        AND NEW.state = OLD.state
        AND NEW.claim_token IS DISTINCT FROM OLD.claim_token
        AND OLD.claim_expires_at > CURRENT_TIMESTAMP THEN
        RAISE EXCEPTION 'cannot replace an active single-block retirement claim for block_metadata id %', OLD.block_metadata_id;
    END IF;
    IF OLD.state = 'deleting'
        AND NEW.state = 'deleted_pending_verification'
        AND NEW.claim_token IS DISTINCT FROM OLD.claim_token THEN
        RAISE EXCEPTION 'cannot change claim owner while recording single-block object deletion for block_metadata id %', OLD.block_metadata_id;
    END IF;

    IF OLD.state = 'deleting' AND NEW.state = 'deleted_pending_verification' THEN
        SELECT single_block_object_key_main, single_block_object_deleted_at
        INTO shadow_single_block_key, shadow_deleted_at
        FROM block_consolidation_shadow
        WHERE block_metadata_id = OLD.block_metadata_id;
        IF shadow_single_block_key IS NOT NULL
            OR shadow_deleted_at IS NULL
            OR shadow_deleted_at IS DISTINCT FROM NEW.deleted_at THEN
            RAISE EXCEPTION 'shadow metadata is not durably cleared for block_metadata id %', OLD.block_metadata_id;
        END IF;
    END IF;

    IF OLD.delete_started_at IS NOT NULL AND NEW.delete_started_at IS DISTINCT FROM OLD.delete_started_at THEN
        RAISE EXCEPTION 'cannot change single-block retirement delete_started_at for block_metadata id %', OLD.block_metadata_id;
    END IF;
    IF OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS DISTINCT FROM OLD.deleted_at THEN
        RAISE EXCEPTION 'cannot change single-block retirement deleted_at for block_metadata id %', OLD.block_metadata_id;
    END IF;
    IF NEW.attempt_count < OLD.attempt_count THEN
        RAISE EXCEPTION 'cannot decrease single-block retirement attempt_count for block_metadata id %', OLD.block_metadata_id;
    END IF;
    IF OLD.state = 'deleted_verified' AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'cannot change a verified single-block retirement for block_metadata id %', OLD.block_metadata_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_single_block_retention_transition_trigger
BEFORE UPDATE ON block_single_block_retention
FOR EACH ROW
EXECUTE FUNCTION enforce_single_block_object_retirement_transition();

-- +goose StatementBegin
CREATE FUNCTION forbid_single_block_object_retirement_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'single-block retirement audit manifests cannot be deleted for block_metadata id %', OLD.block_metadata_id;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_single_block_retention_delete_trigger
BEFORE DELETE ON block_single_block_retention
FOR EACH ROW
EXECUTE FUNCTION forbid_single_block_object_retirement_delete();

DROP INDEX idx_block_single_block_retention_pending;
CREATE INDEX idx_block_single_block_retention_pending
    ON block_single_block_retention (tag, height, block_metadata_id)
    WHERE state IN ('eligible', 'deleting', 'deleted_pending_verification');

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM block_single_block_retention) THEN
        RAISE EXCEPTION 'cannot roll back hardened single-block retirement schema while retirement manifests exist';
    END IF;
    IF EXISTS (SELECT 1 FROM block_metadata WHERE single_block_retention_fenced_at IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot roll back hardened single-block retirement schema while metadata fences exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP INDEX IF EXISTS idx_block_single_block_retention_pending;

DROP TABLE IF EXISTS cscb_retirement_safety_observation;

DROP TRIGGER IF EXISTS block_single_block_retention_delete_trigger ON block_single_block_retention;
DROP FUNCTION IF EXISTS forbid_single_block_object_retirement_delete();

DROP TRIGGER IF EXISTS block_single_block_retention_transition_trigger ON block_single_block_retention;
DROP FUNCTION IF EXISTS enforce_single_block_object_retirement_transition();

DROP TRIGGER IF EXISTS block_single_block_retention_insert_trigger ON block_single_block_retention;
DROP FUNCTION IF EXISTS enforce_single_block_object_retirement_insert();

ALTER TABLE block_single_block_retention
    DROP CONSTRAINT IF EXISTS block_single_block_retention_lifecycle_check,
    DROP CONSTRAINT IF EXISTS block_single_block_retention_immutable_version_ids_check,
    DROP CONSTRAINT IF EXISTS block_single_block_retention_state_check,
    DROP COLUMN IF EXISTS verified_at,
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS claim_token;

ALTER TABLE block_single_block_retention
    ADD CONSTRAINT block_single_block_retention_state_check CHECK (
        state IN ('eligible', 'deleting', 'deleted_verified')
    ),
    ADD CONSTRAINT block_single_block_retention_check CHECK (
        (state = 'eligible' AND single_block_object_key_main IS NOT NULL AND single_block_object_key_main <> '' AND single_block_object_etag <> '' AND delete_started_at IS NULL AND deleted_at IS NULL)
        OR
        (state = 'deleting' AND single_block_object_key_main IS NOT NULL AND single_block_object_key_main <> '' AND single_block_object_etag <> '' AND delete_started_at IS NOT NULL AND last_attempt_at IS NOT NULL AND attempt_count > 0 AND deleted_at IS NULL)
        OR
        (state = 'deleted_verified' AND single_block_object_key_main IS NULL AND single_block_object_etag = '' AND outcome <> '' AND delete_started_at IS NOT NULL AND last_attempt_at IS NOT NULL AND attempt_count > 0 AND deleted_at IS NOT NULL)
    );

CREATE INDEX idx_block_single_block_retention_pending
    ON block_single_block_retention (tag, height, block_metadata_id)
    WHERE state IN ('eligible', 'deleting');

DROP TRIGGER IF EXISTS block_consolidation_shadow_single_block_retirement_trigger ON block_consolidation_shadow;
DROP FUNCTION IF EXISTS enforce_block_consolidation_shadow_single_block_retirement();

DROP TRIGGER IF EXISTS block_metadata_single_block_retirement_fence_trigger ON block_metadata;
DROP FUNCTION IF EXISTS enforce_block_metadata_single_block_retirement_fence();

ALTER TABLE block_metadata
    DROP CONSTRAINT IF EXISTS block_metadata_single_block_retirement_fence,
    DROP COLUMN IF EXISTS single_block_retention_fenced_at;
