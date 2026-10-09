-- +goose Up
ALTER TABLE block_single_block_retention
    ADD COLUMN single_block_delete_marker_version_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];

-- +goose StatementBegin
CREATE FUNCTION validate_single_block_retirement_version_ids(version_ids TEXT[], allow_empty BOOLEAN)
RETURNS BOOLEAN AS $$
    SELECT version_ids IS NOT NULL
        AND (allow_empty OR cardinality(version_ids) > 0)
        AND NOT EXISTS (
            SELECT 1
            FROM unnest(version_ids) AS version_id
            WHERE version_id IS NULL
                OR BTRIM(version_id) = ''
                OR LOWER(BTRIM(version_id)) = 'null'
        )
        AND cardinality(version_ids) = (
            SELECT COUNT(DISTINCT version_id)
            FROM unnest(version_ids) AS version_id
        );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
-- +goose StatementEnd

ALTER TABLE block_single_block_retention
    DROP CONSTRAINT block_single_block_retention_single_block_object_version__check,
    DROP CONSTRAINT block_single_block_retention_immutable_version_ids_check,
    ADD CONSTRAINT block_single_block_retention_immutable_version_ids_check CHECK (
        validate_single_block_retirement_version_ids(single_block_object_version_ids, FALSE)
        AND validate_single_block_retirement_version_ids(single_block_delete_marker_version_ids, TRUE)
        AND consolidated_object_version_id IS NOT NULL
        AND BTRIM(consolidated_object_version_id) <> ''
        AND LOWER(BTRIM(consolidated_object_version_id)) <> 'null'
    );

-- +goose StatementBegin
CREATE FUNCTION enforce_single_block_retirement_delete_markers_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.single_block_delete_marker_version_ids IS DISTINCT FROM OLD.single_block_delete_marker_version_ids THEN
        RAISE EXCEPTION 'cannot change pinned retirement delete-marker versions for block_metadata id %', OLD.block_metadata_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER block_single_block_retention_delete_markers_immutable_trigger
BEFORE UPDATE OF single_block_delete_marker_version_ids ON block_single_block_retention
FOR EACH ROW
EXECUTE FUNCTION enforce_single_block_retirement_delete_markers_immutable();

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM block_single_block_retention
        WHERE cardinality(single_block_object_version_ids) <> 1
            OR cardinality(single_block_delete_marker_version_ids) <> 0
    ) THEN
        RAISE EXCEPTION 'cannot roll back multi-version retirement while multi-version manifests exist';
    END IF;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS block_single_block_retention_delete_markers_immutable_trigger
    ON block_single_block_retention;
DROP FUNCTION IF EXISTS enforce_single_block_retirement_delete_markers_immutable();

ALTER TABLE block_single_block_retention
    DROP CONSTRAINT block_single_block_retention_immutable_version_ids_check,
    ADD CONSTRAINT block_single_block_retention_single_block_object_version__check CHECK (
        cardinality(single_block_object_version_ids) = 1
        AND single_block_object_version_ids[1] <> ''
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
    DROP COLUMN single_block_delete_marker_version_ids;

DROP FUNCTION IF EXISTS validate_single_block_retirement_version_ids(TEXT[], BOOLEAN);
