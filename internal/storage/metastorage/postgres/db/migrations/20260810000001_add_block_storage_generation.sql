-- +goose Up
-- NULL is the immutable legacy bucket. Adding nullable columns keeps this
-- migration metadata-only while giving new rows a compact, extensible physical
-- storage generation such as "v2" or "v3". Keep the checks NOT VALID so this
-- deployment does not scan the large existing tables; PostgreSQL still
-- enforces them for every new or updated row. Validate them separately after
-- the application rollout.
ALTER TABLE block_metadata
    ADD COLUMN storage_generation TEXT,
    ADD CONSTRAINT block_metadata_storage_generation_check
        CHECK (storage_generation IS NULL OR storage_generation ~ '^v[1-9][0-9]*$') NOT VALID;

ALTER TABLE block_consolidation_shadow
    ADD COLUMN single_block_storage_generation TEXT,
    ADD COLUMN consolidated_storage_generation TEXT,
    ADD CONSTRAINT block_consolidation_shadow_single_block_storage_generation_check
        CHECK (
            single_block_storage_generation IS NULL
            OR single_block_storage_generation ~ '^v[1-9][0-9]*$'
        ) NOT VALID,
    ADD CONSTRAINT block_consolidation_shadow_consolidated_storage_generation_check
        CHECK (
            consolidated_storage_generation IS NULL
            OR consolidated_storage_generation ~ '^v[1-9][0-9]*$'
        ) NOT VALID;

-- Storage generation is part of a block's object placement. Protect it with
-- the same retention and repair invariants that already guard the object key
-- and byte placement columns.
-- +goose StatementBegin
CREATE FUNCTION enforce_block_metadata_storage_generation_mutation()
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

CREATE TRIGGER block_metadata_storage_generation_mutation_trigger
BEFORE UPDATE OF storage_generation ON block_metadata
FOR EACH ROW
EXECUTE FUNCTION enforce_block_metadata_storage_generation_mutation();

-- +goose StatementBegin
CREATE FUNCTION enforce_block_consolidation_shadow_storage_generation_mutation()
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

CREATE TRIGGER block_consolidation_shadow_storage_generation_mutation_trigger
BEFORE UPDATE OF single_block_storage_generation, consolidated_storage_generation ON block_consolidation_shadow
FOR EACH ROW
EXECUTE FUNCTION enforce_block_consolidation_shadow_storage_generation_mutation();

-- +goose Down
-- Dropping these columns is safe only while the generation registry is still
-- dormant. Serialize with all placement writers before checking so a v2/v3
-- row cannot appear between the check and the column drop.
-- +goose StatementBegin
LOCK TABLE block_metadata, block_consolidation_shadow IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM block_metadata
        WHERE storage_generation IS NOT NULL
    ) OR EXISTS (
        SELECT 1
        FROM block_consolidation_shadow
        WHERE single_block_storage_generation IS NOT NULL
            OR consolidated_storage_generation IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'cannot roll back block storage generation migration while non-legacy rows exist';
    END IF;
END;
$$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS block_consolidation_shadow_storage_generation_mutation_trigger ON block_consolidation_shadow;
DROP FUNCTION IF EXISTS enforce_block_consolidation_shadow_storage_generation_mutation();

DROP TRIGGER IF EXISTS block_metadata_storage_generation_mutation_trigger ON block_metadata;
DROP FUNCTION IF EXISTS enforce_block_metadata_storage_generation_mutation();

ALTER TABLE block_consolidation_shadow
    DROP CONSTRAINT IF EXISTS block_consolidation_shadow_consolidated_storage_generation_check,
    DROP CONSTRAINT IF EXISTS block_consolidation_shadow_single_block_storage_generation_check,
    DROP COLUMN IF EXISTS consolidated_storage_generation,
    DROP COLUMN IF EXISTS single_block_storage_generation;

ALTER TABLE block_metadata
    DROP CONSTRAINT IF EXISTS block_metadata_storage_generation_check,
    DROP COLUMN IF EXISTS storage_generation;
