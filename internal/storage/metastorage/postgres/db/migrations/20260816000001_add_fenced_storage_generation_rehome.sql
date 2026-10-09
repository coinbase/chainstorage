-- +goose Up
-- A completed single-block retirement deliberately freezes the consolidated
-- placement. Legacy CSCB objects copied byte-for-byte into a configured storage
-- generation therefore need an explicit, object-scoped exception rather than a
-- disabled trigger or an unrestricted UPDATE.
CREATE TABLE block_storage_generation_rehome (
    id BIGSERIAL PRIMARY KEY,
    evidence_sha256 CHAR(64) NOT NULL UNIQUE CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    tag INTEGER NOT NULL,
    object_key_main TEXT NOT NULL CHECK (object_key_main <> ''),
    source_generation TEXT NOT NULL CHECK (source_generation = 'legacy'),
    target_generation TEXT NOT NULL CHECK (target_generation ~ '^v[1-9][0-9]*$'),
    source_bucket TEXT NOT NULL CHECK (source_bucket <> ''),
    source_version_id TEXT NOT NULL CHECK (
        BTRIM(source_version_id) <> '' AND LOWER(BTRIM(source_version_id)) <> 'null'
    ),
    source_etag TEXT NOT NULL CHECK (source_etag <> ''),
    destination_bucket TEXT NOT NULL CHECK (destination_bucket <> ''),
    destination_version_id TEXT NOT NULL CHECK (
        BTRIM(destination_version_id) <> '' AND LOWER(BTRIM(destination_version_id)) <> 'null'
    ),
    destination_etag TEXT NOT NULL CHECK (destination_etag <> ''),
    object_bytes BIGINT NOT NULL CHECK (object_bytes > 0),
    start_height BIGINT NOT NULL CHECK (start_height >= 0),
    end_height BIGINT NOT NULL CHECK (end_height > start_height),
    expected_block_count BIGINT NOT NULL CHECK (expected_block_count > 0),
    expected_canonical_count BIGINT NOT NULL CHECK (
        expected_canonical_count >= 0 AND expected_canonical_count <= expected_block_count
    ),
    expected_fenced_count BIGINT NOT NULL CHECK (
        expected_fenced_count > 0 AND expected_fenced_count <= expected_block_count
    ),
    expected_deleted_verified_count BIGINT NOT NULL CHECK (
        expected_deleted_verified_count = expected_fenced_count
    ),
    state TEXT NOT NULL CHECK (state IN ('executing', 'completed')),
    executed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tag, object_key_main, source_generation, target_generation),
    CHECK (source_bucket <> destination_bucket),
    CHECK (
        (state = 'executing' AND completed_at IS NULL)
        OR (state = 'completed' AND completed_at IS NOT NULL AND completed_at >= executed_at)
    )
);

-- The evidence is immutable. The only permitted mutation completes the exact
-- transaction that inserted the executing row and changed both placements.
-- +goose StatementBegin
CREATE FUNCTION enforce_block_storage_generation_rehome_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'block storage generation rehome audit rows are immutable';
    END IF;

    IF OLD.state = 'executing'
        AND NEW.state = 'completed'
        AND OLD.id IS NOT DISTINCT FROM NEW.id
        AND OLD.evidence_sha256 IS NOT DISTINCT FROM NEW.evidence_sha256
        AND OLD.tag IS NOT DISTINCT FROM NEW.tag
        AND OLD.object_key_main IS NOT DISTINCT FROM NEW.object_key_main
        AND OLD.source_generation IS NOT DISTINCT FROM NEW.source_generation
        AND OLD.target_generation IS NOT DISTINCT FROM NEW.target_generation
        AND OLD.source_bucket IS NOT DISTINCT FROM NEW.source_bucket
        AND OLD.source_version_id IS NOT DISTINCT FROM NEW.source_version_id
        AND OLD.source_etag IS NOT DISTINCT FROM NEW.source_etag
        AND OLD.destination_bucket IS NOT DISTINCT FROM NEW.destination_bucket
        AND OLD.destination_version_id IS NOT DISTINCT FROM NEW.destination_version_id
        AND OLD.destination_etag IS NOT DISTINCT FROM NEW.destination_etag
        AND OLD.object_bytes IS NOT DISTINCT FROM NEW.object_bytes
        AND OLD.start_height IS NOT DISTINCT FROM NEW.start_height
        AND OLD.end_height IS NOT DISTINCT FROM NEW.end_height
        AND OLD.expected_block_count IS NOT DISTINCT FROM NEW.expected_block_count
        AND OLD.expected_canonical_count IS NOT DISTINCT FROM NEW.expected_canonical_count
        AND OLD.expected_fenced_count IS NOT DISTINCT FROM NEW.expected_fenced_count
        AND OLD.expected_deleted_verified_count IS NOT DISTINCT FROM NEW.expected_deleted_verified_count
        AND OLD.executed_at IS NOT DISTINCT FROM NEW.executed_at
        AND OLD.completed_at IS NULL
        AND NEW.completed_at IS NOT NULL THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'cannot mutate block storage generation rehome audit id %', OLD.id;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_storage_generation_rehome_update_trigger
BEFORE UPDATE ON block_storage_generation_rehome
FOR EACH ROW
EXECUTE FUNCTION enforce_block_storage_generation_rehome_audit_mutation();

CREATE TRIGGER block_storage_generation_rehome_delete_trigger
BEFORE DELETE ON block_storage_generation_rehome
FOR EACH ROW
EXECUTE FUNCTION enforce_block_storage_generation_rehome_audit_mutation();

-- Replace the primary-placement trigger with a narrowly scoped exception. The
-- executing manifest is visible only inside the transaction performing the
-- rehome, and every fenced block must already have a verified retirement.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_block_metadata_storage_generation_mutation()
RETURNS TRIGGER AS $$
DECLARE
    pinned_old BOOLEAN;
    approved_rehome BOOLEAN;
    retirement_verified BOOLEAN;
BEGIN
    IF OLD.single_block_retention_fenced_at IS NOT NULL
        AND NEW.storage_generation IS DISTINCT FROM OLD.storage_generation THEN
        SELECT EXISTS (
            SELECT 1
            FROM block_storage_generation_rehome rehome
            WHERE rehome.state = 'executing'
                AND rehome.tag = OLD.tag
                AND rehome.object_key_main = OLD.object_key_main
                AND rehome.source_generation = 'legacy'
                AND OLD.storage_generation IS NULL
                AND rehome.target_generation = NEW.storage_generation
        ) INTO approved_rehome;
        IF NOT approved_rehome THEN
            RAISE EXCEPTION 'cannot change storage generation after single-block object retirement is fenced for block_metadata id %', OLD.id;
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM block_single_block_retention retirement
            JOIN block_consolidation_shadow shadow
                ON shadow.block_metadata_id = retirement.block_metadata_id
            WHERE retirement.block_metadata_id = OLD.id
                AND retirement.state = 'deleted_verified'
                AND retirement.deleted_at IS NOT NULL
                AND retirement.verified_at IS NOT NULL
                AND retirement.consolidated_object_key_main = OLD.object_key_main
                AND shadow.single_block_object_deleted_at IS NOT NULL
                AND shadow.consolidated_object_key_main = OLD.object_key_main
        ) INTO retirement_verified;
        IF NOT retirement_verified THEN
            RAISE EXCEPTION 'cannot rehome fenced storage generation without verified single-block retirement for block_metadata id %', OLD.id;
        END IF;
    END IF;

    IF NEW.storage_generation IS DISTINCT FROM OLD.storage_generation
        AND NEW.object_key_main IS NOT NULL THEN
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
        IF pinned_old THEN
            RAISE EXCEPTION 'cannot change storage generation for a pinned old CSCB object from block_metadata id %', OLD.id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- Apply the same manifest requirement to retired shadow placements. The
-- deleted single-block generation itself remains immutable and is not rehomed.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_block_consolidation_shadow_storage_generation_mutation()
RETURNS TRIGGER AS $$
DECLARE
    pinned_old BOOLEAN;
    approved_rehome BOOLEAN;
    retirement_verified BOOLEAN;
BEGIN
    IF OLD.single_block_object_deleted_at IS NOT NULL
        AND NEW.single_block_storage_generation IS DISTINCT FROM OLD.single_block_storage_generation THEN
        RAISE EXCEPTION 'cannot change deleted single-block object storage generation for block_metadata id %', OLD.block_metadata_id;
    END IF;

    IF OLD.single_block_object_deleted_at IS NOT NULL
        AND NEW.consolidated_storage_generation IS DISTINCT FROM OLD.consolidated_storage_generation THEN
        SELECT EXISTS (
            SELECT 1
            FROM block_storage_generation_rehome rehome
            WHERE rehome.state = 'executing'
                AND rehome.tag = OLD.tag
                AND rehome.object_key_main = OLD.consolidated_object_key_main
                AND rehome.source_generation = 'legacy'
                AND OLD.consolidated_storage_generation IS NULL
                AND rehome.target_generation = NEW.consolidated_storage_generation
        ) INTO approved_rehome;
        IF NOT approved_rehome THEN
            RAISE EXCEPTION 'cannot change retired consolidated storage generation without an executing rehome manifest for block_metadata id %', OLD.block_metadata_id;
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM block_single_block_retention retirement
            WHERE retirement.block_metadata_id = OLD.block_metadata_id
                AND retirement.state = 'deleted_verified'
                AND retirement.deleted_at IS NOT NULL
                AND retirement.verified_at IS NOT NULL
                AND retirement.consolidated_object_key_main = OLD.consolidated_object_key_main
        ) INTO retirement_verified;
        IF NOT retirement_verified THEN
            RAISE EXCEPTION 'cannot rehome retired consolidated storage generation without verified single-block retirement for block_metadata id %', OLD.block_metadata_id;
        END IF;
    END IF;

    IF NEW.consolidated_storage_generation IS DISTINCT FROM OLD.consolidated_storage_generation
        AND NEW.consolidated_object_key_main IS NOT NULL THEN
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
        IF pinned_old THEN
            RAISE EXCEPTION 'cannot change storage generation for a pinned old CSCB object from consolidation shadow for block_metadata id %', OLD.block_metadata_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
-- Completed rows are permanent evidence for a placement transition. Refuse to
-- remove the exception schema after it has been used.
-- +goose StatementBegin
DO $$
BEGIN
    LOCK TABLE block_storage_generation_rehome IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM block_storage_generation_rehome) THEN
        RAISE EXCEPTION 'cannot roll back fenced storage generation rehome migration after execution';
    END IF;
END $$;
-- +goose StatementEnd

-- Restore the original storage-generation guards.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_block_metadata_storage_generation_mutation()
RETURNS TRIGGER AS $$
DECLARE
    pinned_old BOOLEAN;
BEGIN
    IF OLD.single_block_retention_fenced_at IS NOT NULL
        AND NEW.storage_generation IS DISTINCT FROM OLD.storage_generation THEN
        RAISE EXCEPTION 'cannot change storage generation after single-block object retirement is fenced for block_metadata id %', OLD.id;
    END IF;

    IF NEW.storage_generation IS DISTINCT FROM OLD.storage_generation
        AND NEW.object_key_main IS NOT NULL THEN
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
        IF pinned_old THEN
            RAISE EXCEPTION 'cannot change storage generation for a pinned old CSCB object from block_metadata id %', OLD.id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION enforce_block_consolidation_shadow_storage_generation_mutation()
RETURNS TRIGGER AS $$
DECLARE
    pinned_old BOOLEAN;
BEGIN
    IF OLD.single_block_object_deleted_at IS NOT NULL
        AND NEW.single_block_storage_generation IS DISTINCT FROM OLD.single_block_storage_generation THEN
        RAISE EXCEPTION 'cannot change deleted single-block object storage generation for block_metadata id %', OLD.block_metadata_id;
    END IF;

    IF NEW.consolidated_storage_generation IS DISTINCT FROM OLD.consolidated_storage_generation
        AND NEW.consolidated_object_key_main IS NOT NULL THEN
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
        IF pinned_old THEN
            RAISE EXCEPTION 'cannot change storage generation for a pinned old CSCB object from consolidation shadow for block_metadata id %', OLD.block_metadata_id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS block_storage_generation_rehome_delete_trigger ON block_storage_generation_rehome;
DROP TRIGGER IF EXISTS block_storage_generation_rehome_update_trigger ON block_storage_generation_rehome;
DROP FUNCTION IF EXISTS enforce_block_storage_generation_rehome_audit_mutation();
DROP TABLE IF EXISTS block_storage_generation_rehome;
