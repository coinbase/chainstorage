-- +goose Up
CREATE TABLE cscb_repair_manifest (
    id BIGSERIAL PRIMARY KEY,
    tag INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('preparing', 'prepared', 'restored', 'verified', 'completed')),
    bucket TEXT NOT NULL CHECK (bucket <> ''),
    old_consolidated_object_key_main TEXT NOT NULL CHECK (old_consolidated_object_key_main <> ''),
    old_consolidated_object_version_id TEXT,
    old_consolidated_object_etag TEXT,
    old_consolidated_object_bytes BIGINT,
    start_height BIGINT NOT NULL CHECK (start_height >= 0),
    end_height BIGINT NOT NULL CHECK (end_height > start_height),
    canonical_block_count BIGINT NOT NULL CHECK (canonical_block_count >= 0),
    total_block_count BIGINT NOT NULL CHECK (total_block_count >= canonical_block_count),
    row_set_sha256 CHAR(64) NOT NULL CHECK (row_set_sha256 ~ '^[0-9a-f]{64}$'),
    new_consolidated_object_key_main TEXT,
    new_consolidated_object_version_id TEXT,
    new_consolidated_object_etag TEXT,
    new_consolidated_object_bytes BIGINT,
    outcome TEXT NOT NULL DEFAULT '',
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    restored_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tag, old_consolidated_object_key_main),
    CHECK (
        (
            state = 'preparing'
            AND old_consolidated_object_version_id IS NULL
            AND old_consolidated_object_etag IS NULL
            AND old_consolidated_object_bytes IS NULL
        )
        OR (
            state <> 'preparing'
            AND old_consolidated_object_version_id IS NOT NULL
            AND BTRIM(old_consolidated_object_version_id) <> ''
            AND LOWER(BTRIM(old_consolidated_object_version_id)) <> 'null'
            AND old_consolidated_object_etag IS NOT NULL
            AND old_consolidated_object_etag <> ''
            AND old_consolidated_object_bytes IS NOT NULL
            AND old_consolidated_object_bytes > 0
        )
    ),
    CHECK (
        (
            state IN ('preparing', 'prepared')
            AND outcome = ''
            AND restored_at IS NULL
            AND verified_at IS NULL
            AND completed_at IS NULL
            AND new_consolidated_object_key_main IS NULL
            AND new_consolidated_object_version_id IS NULL
            AND new_consolidated_object_etag IS NULL
            AND new_consolidated_object_bytes IS NULL
        )
        OR (
            state = 'restored'
            AND outcome = ''
            AND restored_at IS NOT NULL
            AND verified_at IS NULL
            AND completed_at IS NULL
            AND new_consolidated_object_key_main IS NULL
            AND new_consolidated_object_version_id IS NULL
            AND new_consolidated_object_etag IS NULL
            AND new_consolidated_object_bytes IS NULL
        )
        OR (
            state = 'verified'
            AND outcome = ''
            AND restored_at IS NOT NULL
            AND verified_at IS NOT NULL
            AND verified_at >= restored_at
            AND completed_at IS NULL
            AND canonical_block_count > 0
            AND new_consolidated_object_key_main IS NOT NULL
            AND new_consolidated_object_key_main <> ''
            AND new_consolidated_object_key_main <> old_consolidated_object_key_main
            AND new_consolidated_object_version_id IS NOT NULL
            AND BTRIM(new_consolidated_object_version_id) <> ''
            AND LOWER(BTRIM(new_consolidated_object_version_id)) <> 'null'
            AND new_consolidated_object_etag IS NOT NULL
            AND new_consolidated_object_etag <> ''
            AND new_consolidated_object_bytes IS NOT NULL
            AND new_consolidated_object_bytes > 0
        )
        OR (
            state = 'completed'
            AND outcome = 'already_clean_storage_neutral'
            AND restored_at IS NULL
            AND verified_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND completed_at >= verified_at
            AND new_consolidated_object_key_main IS NULL
            AND new_consolidated_object_version_id IS NULL
            AND new_consolidated_object_etag IS NULL
            AND new_consolidated_object_bytes IS NULL
        )
        OR (
            state = 'completed'
            AND outcome <> ''
            AND outcome <> 'already_clean_storage_neutral'
            AND restored_at IS NOT NULL
            AND completed_at IS NOT NULL
            AND completed_at >= restored_at
            AND (
                (
                    canonical_block_count > 0
                    AND verified_at IS NOT NULL
                    AND verified_at >= restored_at
                    AND completed_at >= verified_at
                    AND new_consolidated_object_key_main IS NOT NULL
                    AND new_consolidated_object_key_main <> ''
                    AND new_consolidated_object_key_main <> old_consolidated_object_key_main
                    AND new_consolidated_object_version_id IS NOT NULL
                    AND BTRIM(new_consolidated_object_version_id) <> ''
                    AND LOWER(BTRIM(new_consolidated_object_version_id)) <> 'null'
                    AND new_consolidated_object_etag IS NOT NULL
                    AND new_consolidated_object_etag <> ''
                    AND new_consolidated_object_bytes IS NOT NULL
                    AND new_consolidated_object_bytes > 0
                )
                OR (
                    canonical_block_count = 0
                    AND verified_at IS NULL
                    AND new_consolidated_object_key_main IS NULL
                    AND new_consolidated_object_version_id IS NULL
                    AND new_consolidated_object_etag IS NULL
                    AND new_consolidated_object_bytes IS NULL
                )
            )
        )
    )
);

CREATE TABLE cscb_repair_block (
    repair_id BIGINT NOT NULL REFERENCES cscb_repair_manifest (id) ON DELETE RESTRICT,
    block_metadata_id BIGINT NOT NULL REFERENCES block_metadata (id) ON DELETE RESTRICT,
    canonical BOOLEAN NOT NULL,
    tag INTEGER NOT NULL,
    height BIGINT NOT NULL CHECK (height >= 0),
    hash VARCHAR(66),
    single_block_object_key_main TEXT CHECK (single_block_object_key_main IS NULL OR single_block_object_key_main <> ''),
    single_block_object_key_sha256 CHAR(64) NOT NULL CHECK (single_block_object_key_sha256 ~ '^[0-9a-f]{64}$'),
    single_block_object_version_id TEXT,
    single_block_object_etag TEXT,
    single_block_object_bytes BIGINT,
    payload_sha256 CHAR(64),
    old_byte_offset BIGINT NOT NULL CHECK (old_byte_offset >= 0),
    old_byte_length BIGINT NOT NULL CHECK (old_byte_length > 0),
    old_uncompressed_length BIGINT NOT NULL CHECK (old_uncompressed_length > 0),
    PRIMARY KEY (repair_id, block_metadata_id),
    UNIQUE (block_metadata_id),
    CHECK (
        (
            single_block_object_version_id IS NULL
            AND single_block_object_etag IS NULL
            AND single_block_object_bytes IS NULL
            AND payload_sha256 IS NULL
        )
        OR (
            single_block_object_version_id IS NOT NULL
            AND BTRIM(single_block_object_version_id) <> ''
            AND LOWER(BTRIM(single_block_object_version_id)) <> 'null'
            AND single_block_object_etag IS NOT NULL
            AND single_block_object_etag <> ''
            AND single_block_object_bytes IS NOT NULL
            AND single_block_object_bytes > 0
            AND payload_sha256 IS NOT NULL
            AND payload_sha256 ~ '^[0-9a-f]{64}$'
        )
    )
);

CREATE TABLE cscb_repair_execution (
    execution_key CHAR(64) PRIMARY KEY CHECK (execution_key ~ '^[0-9a-f]{64}$'),
    -- NULL is a durable "no candidate" result. This prevents a lost Temporal
    -- activity response from selecting newly appeared work on retry.
    repair_id BIGINT REFERENCES cscb_repair_manifest (id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cscb_repair_manifest_pending
    ON cscb_repair_manifest (tag, end_height DESC, id)
    WHERE state <> 'completed';

CREATE INDEX idx_cscb_repair_manifest_old_object
    ON cscb_repair_manifest (old_consolidated_object_key_main);

CREATE INDEX idx_cscb_repair_block_repair_height
    ON cscb_repair_block (repair_id, height, block_metadata_id);

CREATE INDEX idx_cscb_repair_block_metadata
    ON cscb_repair_block (block_metadata_id);

CREATE INDEX idx_cscb_repair_execution_repair
    ON cscb_repair_execution (repair_id);

-- Application writers acquire the repair tag lock before touching rows. These
-- triggers independently prevent any pinned old object key from being
-- reintroduced, including callers that claim a non-CSCB format.
-- +goose StatementBegin
CREATE FUNCTION prevent_cscb_repair_old_block_metadata_reference()
RETURNS TRIGGER AS $$
DECLARE
    pinned_old BOOLEAN;
BEGIN
    IF NEW.object_key_main IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM cscb_repair_manifest repair
        WHERE repair.old_consolidated_object_key_main = NEW.object_key_main
            AND NOT (
                repair.state = 'completed'
                AND repair.outcome = 'already_clean_storage_neutral'
            )
    ) INTO pinned_old;
    IF NEW.object_format = 1 OR pinned_old THEN
        PERFORM pg_advisory_xact_lock(hashtextextended(NEW.object_key_main, 1));
        SELECT EXISTS (
            SELECT 1
            FROM cscb_repair_manifest repair
            WHERE repair.old_consolidated_object_key_main = NEW.object_key_main
                AND NOT (
                    repair.state = 'completed'
                    AND repair.outcome = 'already_clean_storage_neutral'
                )
        ) INTO pinned_old;
    END IF;
    IF pinned_old THEN
        IF TG_OP = 'UPDATE'
            AND OLD.object_key_main IS NOT DISTINCT FROM NEW.object_key_main
            AND OLD.object_format IS NOT DISTINCT FROM NEW.object_format
            AND OLD.byte_offset IS NOT DISTINCT FROM NEW.byte_offset
            AND OLD.byte_length IS NOT DISTINCT FROM NEW.byte_length
            AND OLD.uncompressed_length IS NOT DISTINCT FROM NEW.uncompressed_length THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'cannot reference a pinned old CSCB object from block_metadata id %', NEW.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cscb_repair_old_block_metadata_reference_trigger
BEFORE INSERT OR UPDATE OF object_key_main, object_format, byte_offset, byte_length, uncompressed_length ON block_metadata
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_old_block_metadata_reference();

-- +goose StatementBegin
CREATE FUNCTION prevent_cscb_repair_old_shadow_reference()
RETURNS TRIGGER AS $$
DECLARE
    pinned_old BOOLEAN;
BEGIN
    IF NEW.consolidated_object_key_main IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM cscb_repair_manifest repair
        WHERE repair.old_consolidated_object_key_main = NEW.consolidated_object_key_main
            AND NOT (
                repair.state = 'completed'
                AND repair.outcome = 'already_clean_storage_neutral'
            )
    ) INTO pinned_old;
    IF NEW.object_format = 1 OR pinned_old THEN
        PERFORM pg_advisory_xact_lock(hashtextextended(NEW.consolidated_object_key_main, 1));
        SELECT EXISTS (
            SELECT 1
            FROM cscb_repair_manifest repair
            WHERE repair.old_consolidated_object_key_main = NEW.consolidated_object_key_main
                AND NOT (
                    repair.state = 'completed'
                    AND repair.outcome = 'already_clean_storage_neutral'
                )
        ) INTO pinned_old;
    END IF;
    IF pinned_old THEN
        IF TG_OP = 'UPDATE'
            AND OLD.consolidated_object_key_main IS NOT DISTINCT FROM NEW.consolidated_object_key_main
            AND OLD.object_format IS NOT DISTINCT FROM NEW.object_format
            AND OLD.byte_offset IS NOT DISTINCT FROM NEW.byte_offset
            AND OLD.byte_length IS NOT DISTINCT FROM NEW.byte_length
            AND OLD.uncompressed_length IS NOT DISTINCT FROM NEW.uncompressed_length THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'cannot reference a pinned old CSCB object from consolidation shadow for block_metadata id %', NEW.block_metadata_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cscb_repair_old_shadow_reference_trigger
BEFORE INSERT OR UPDATE OF consolidated_object_key_main, object_format, byte_offset, byte_length, uncompressed_length ON block_consolidation_shadow
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_old_shadow_reference();

-- +goose StatementBegin
CREATE FUNCTION enforce_cscb_repair_manifest_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.tag IS DISTINCT FROM OLD.tag
        OR NEW.bucket IS DISTINCT FROM OLD.bucket
        OR NEW.old_consolidated_object_key_main IS DISTINCT FROM OLD.old_consolidated_object_key_main
        OR NEW.start_height IS DISTINCT FROM OLD.start_height
        OR NEW.end_height IS DISTINCT FROM OLD.end_height
        OR NEW.canonical_block_count IS DISTINCT FROM OLD.canonical_block_count
        OR NEW.total_block_count IS DISTINCT FROM OLD.total_block_count
        OR NEW.row_set_sha256 IS DISTINCT FROM OLD.row_set_sha256
        OR NEW.prepared_at IS DISTINCT FROM OLD.prepared_at THEN
        RAISE EXCEPTION 'cannot change fenced CSCB repair manifest fields for repair id %', OLD.id;
    END IF;

    IF OLD.state = 'preparing' THEN
        IF NOT (
            OLD.old_consolidated_object_version_id IS NULL
            AND OLD.old_consolidated_object_etag IS NULL
            AND OLD.old_consolidated_object_bytes IS NULL
            AND NEW.old_consolidated_object_version_id IS NOT NULL
            AND NEW.old_consolidated_object_etag IS NOT NULL
            AND NEW.old_consolidated_object_bytes IS NOT NULL
        ) THEN
            RAISE EXCEPTION 'invalid CSCB repair source inspection for repair id %', OLD.id;
        END IF;
    ELSIF NEW.old_consolidated_object_version_id IS DISTINCT FROM OLD.old_consolidated_object_version_id
        OR NEW.old_consolidated_object_etag IS DISTINCT FROM OLD.old_consolidated_object_etag
        OR NEW.old_consolidated_object_bytes IS DISTINCT FROM OLD.old_consolidated_object_bytes THEN
        RAISE EXCEPTION 'cannot change inspected CSCB repair source for repair id %', OLD.id;
    END IF;

    IF NOT (
        (OLD.state = 'preparing' AND NEW.state IN ('prepared', 'completed'))
        OR (OLD.state = 'prepared' AND NEW.state IN ('prepared', 'restored'))
        OR (OLD.state = 'restored' AND NEW.state IN ('restored', 'verified', 'completed'))
        OR (OLD.state = 'verified' AND NEW.state IN ('verified', 'completed'))
        OR (OLD.state = 'completed' AND NEW.state = 'completed')
    ) THEN
        RAISE EXCEPTION 'invalid CSCB repair transition from % to % for repair id %', OLD.state, NEW.state, OLD.id;
    END IF;

    IF OLD.new_consolidated_object_key_main IS NOT NULL
        AND (
            NEW.new_consolidated_object_key_main IS DISTINCT FROM OLD.new_consolidated_object_key_main
            OR NEW.new_consolidated_object_version_id IS DISTINCT FROM OLD.new_consolidated_object_version_id
            OR NEW.new_consolidated_object_etag IS DISTINCT FROM OLD.new_consolidated_object_etag
            OR NEW.new_consolidated_object_bytes IS DISTINCT FROM OLD.new_consolidated_object_bytes
            OR NEW.verified_at IS DISTINCT FROM OLD.verified_at
        ) THEN
        RAISE EXCEPTION 'cannot change verified CSCB repair target for repair id %', OLD.id;
    END IF;

    IF OLD.restored_at IS NOT NULL AND NEW.restored_at IS DISTINCT FROM OLD.restored_at THEN
        RAISE EXCEPTION 'cannot change CSCB repair restore timestamp for repair id %', OLD.id;
    END IF;

    IF NEW.outcome IS DISTINCT FROM OLD.outcome
        AND NOT (
            (OLD.state = 'preparing' AND NEW.state = 'completed'
                AND NEW.outcome = 'already_clean_storage_neutral')
            OR ((OLD.state = 'verified' OR (OLD.state = 'restored' AND OLD.canonical_block_count = 0))
                AND NEW.state = 'completed' AND OLD.outcome = '' AND NEW.outcome <> '')
        ) THEN
        RAISE EXCEPTION 'cannot change CSCB repair outcome outside completion for repair id %', OLD.id;
    END IF;

    IF OLD.completed_at IS NOT NULL AND NEW.completed_at IS DISTINCT FROM OLD.completed_at THEN
        RAISE EXCEPTION 'cannot change completed CSCB repair timestamp for repair id %', OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cscb_repair_manifest_transition_trigger
BEFORE UPDATE ON cscb_repair_manifest
FOR EACH ROW
EXECUTE FUNCTION enforce_cscb_repair_manifest_transition();

-- Inspection fills S3 observations exactly once. Retirement may later scrub
-- the exact single-block path, retaining its hash and immutable object audit.
-- +goose StatementBegin
CREATE FUNCTION enforce_cscb_repair_block_audit_update()
RETURNS TRIGGER AS $$
DECLARE
    repair_state TEXT;
    retirement_deleted BOOLEAN;
BEGIN
    SELECT state INTO repair_state FROM cscb_repair_manifest WHERE id = OLD.repair_id;

    IF repair_state = 'preparing'
        AND NEW.repair_id IS NOT DISTINCT FROM OLD.repair_id
        AND NEW.block_metadata_id IS NOT DISTINCT FROM OLD.block_metadata_id
        AND NEW.canonical IS NOT DISTINCT FROM OLD.canonical
        AND NEW.tag IS NOT DISTINCT FROM OLD.tag
        AND NEW.height IS NOT DISTINCT FROM OLD.height
        AND NEW.hash IS NOT DISTINCT FROM OLD.hash
        AND NEW.single_block_object_key_main IS NOT DISTINCT FROM OLD.single_block_object_key_main
        AND NEW.single_block_object_key_sha256 IS NOT DISTINCT FROM OLD.single_block_object_key_sha256
        AND OLD.single_block_object_version_id IS NULL
        AND OLD.single_block_object_etag IS NULL
        AND OLD.single_block_object_bytes IS NULL
        AND OLD.payload_sha256 IS NULL
        AND NEW.single_block_object_version_id IS NOT NULL
        AND NEW.single_block_object_etag IS NOT NULL
        AND NEW.single_block_object_bytes IS NOT NULL
        AND NEW.payload_sha256 IS NOT NULL
        AND NEW.old_byte_offset IS NOT DISTINCT FROM OLD.old_byte_offset
        AND NEW.old_byte_length IS NOT DISTINCT FROM OLD.old_byte_length
        AND NEW.old_uncompressed_length IS NOT DISTINCT FROM OLD.old_uncompressed_length THEN
        RETURN NEW;
    END IF;

    IF repair_state = 'completed'
        AND OLD.single_block_object_key_main IS NOT NULL
        AND NEW.single_block_object_key_main IS NULL
        AND NEW.repair_id IS NOT DISTINCT FROM OLD.repair_id
        AND NEW.block_metadata_id IS NOT DISTINCT FROM OLD.block_metadata_id
        AND NEW.canonical IS NOT DISTINCT FROM OLD.canonical
        AND NEW.tag IS NOT DISTINCT FROM OLD.tag
        AND NEW.height IS NOT DISTINCT FROM OLD.height
        AND NEW.hash IS NOT DISTINCT FROM OLD.hash
        AND NEW.single_block_object_key_sha256 IS NOT DISTINCT FROM OLD.single_block_object_key_sha256
        AND NEW.single_block_object_version_id IS NOT DISTINCT FROM OLD.single_block_object_version_id
        AND NEW.single_block_object_etag IS NOT DISTINCT FROM OLD.single_block_object_etag
        AND NEW.single_block_object_bytes IS NOT DISTINCT FROM OLD.single_block_object_bytes
        AND NEW.payload_sha256 IS NOT DISTINCT FROM OLD.payload_sha256
        AND NEW.old_byte_offset IS NOT DISTINCT FROM OLD.old_byte_offset
        AND NEW.old_byte_length IS NOT DISTINCT FROM OLD.old_byte_length
        AND NEW.old_uncompressed_length IS NOT DISTINCT FROM OLD.old_uncompressed_length THEN
        SELECT EXISTS (
            SELECT 1
            FROM block_single_block_retention retirement
            WHERE retirement.block_metadata_id = OLD.block_metadata_id
                AND retirement.state IN ('deleted_pending_verification', 'completed')
                AND retirement.deleted_at IS NOT NULL
        ) INTO retirement_deleted;
        IF retirement_deleted THEN
            RETURN NEW;
        END IF;
    END IF;

    RAISE EXCEPTION 'cannot mutate CSCB repair block audit for metadata id %', OLD.block_metadata_id;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cscb_repair_block_update_trigger
BEFORE UPDATE ON cscb_repair_block
FOR EACH ROW
EXECUTE FUNCTION enforce_cscb_repair_block_audit_update();

-- +goose StatementBegin
CREATE FUNCTION prevent_cscb_repair_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'CSCB repair audit rows are immutable';
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER cscb_repair_block_delete_trigger
BEFORE DELETE ON cscb_repair_block
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_audit_mutation();

CREATE TRIGGER cscb_repair_execution_update_trigger
BEFORE UPDATE ON cscb_repair_execution
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_audit_mutation();

CREATE TRIGGER cscb_repair_execution_delete_trigger
BEFORE DELETE ON cscb_repair_execution
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_audit_mutation();

CREATE TRIGGER cscb_repair_manifest_delete_trigger
BEFORE DELETE ON cscb_repair_manifest
FOR EACH ROW
EXECUTE FUNCTION prevent_cscb_repair_audit_mutation();

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    LOCK TABLE cscb_repair_execution, cscb_repair_block, cscb_repair_manifest
        IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM cscb_repair_manifest)
        OR EXISTS (SELECT 1 FROM cscb_repair_execution) THEN
        RAISE EXCEPTION 'cannot roll back CSCB repair migration while repair manifests or execution bindings exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS cscb_repair_manifest_delete_trigger ON cscb_repair_manifest;
DROP TRIGGER IF EXISTS cscb_repair_execution_delete_trigger ON cscb_repair_execution;
DROP TRIGGER IF EXISTS cscb_repair_execution_update_trigger ON cscb_repair_execution;
DROP TRIGGER IF EXISTS cscb_repair_block_delete_trigger ON cscb_repair_block;
DROP FUNCTION IF EXISTS prevent_cscb_repair_audit_mutation();

DROP TRIGGER IF EXISTS cscb_repair_block_update_trigger ON cscb_repair_block;
DROP FUNCTION IF EXISTS enforce_cscb_repair_block_audit_update();

DROP TRIGGER IF EXISTS cscb_repair_old_shadow_reference_trigger ON block_consolidation_shadow;
DROP FUNCTION IF EXISTS prevent_cscb_repair_old_shadow_reference();

DROP TRIGGER IF EXISTS cscb_repair_old_block_metadata_reference_trigger ON block_metadata;
DROP FUNCTION IF EXISTS prevent_cscb_repair_old_block_metadata_reference();

DROP TRIGGER IF EXISTS cscb_repair_manifest_transition_trigger ON cscb_repair_manifest;
DROP FUNCTION IF EXISTS enforce_cscb_repair_manifest_transition();

DROP INDEX IF EXISTS idx_cscb_repair_execution_repair;
DROP INDEX IF EXISTS idx_cscb_repair_block_repair_height;
DROP INDEX IF EXISTS idx_cscb_repair_manifest_old_object;
DROP INDEX IF EXISTS idx_cscb_repair_manifest_pending;
DROP TABLE IF EXISTS cscb_repair_execution;
DROP TABLE IF EXISTS cscb_repair_block;
DROP TABLE IF EXISTS cscb_repair_manifest;
