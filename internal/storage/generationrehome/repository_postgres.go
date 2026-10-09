package generationrehome

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/cscbrepairlock"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const legacyGeneration = "legacy"

type (
	PostgresRepository struct {
		db *sql.DB
	}

	rowQueryer interface {
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
)

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) Inspect(
	ctx context.Context,
	tag uint32,
	objectKey string,
	targetGeneration string,
	evidenceSHA256 string,
) (Inspection, error) {
	if r.db == nil {
		return Inspection{}, xerrors.New("postgres db is required")
	}
	if err := validateInspectionKey(objectKey, targetGeneration, evidenceSHA256); err != nil {
		return Inspection{}, err
	}
	return inspectCohort(ctx, r.db, tag, objectKey, targetGeneration, evidenceSHA256)
}

func (r *PostgresRepository) Rehome(ctx context.Context, req RehomeRequest) (bool, error) {
	if r.db == nil {
		return false, xerrors.New("postgres db is required")
	}
	if err := validateRehomeRequest(req); err != nil {
		return false, err
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, xerrors.Errorf("failed to begin storage generation rehome: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := cscbrepairlock.AcquireTag(ctx, tx, req.Tag); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 1))`,
		req.Object.ObjectKey,
	); err != nil {
		return false, xerrors.Errorf("failed to lock storage generation rehome object %q: %w", req.Object.ObjectKey, err)
	}

	inspection, err := inspectCohort(
		ctx,
		tx,
		req.Tag,
		req.Object.ObjectKey,
		req.TargetGeneration,
		req.Object.EvidenceSHA256,
	)
	if err != nil {
		return false, err
	}
	alreadyTarget, err := validateInspection(req.Object, inspection)
	if err != nil {
		return false, err
	}
	if alreadyTarget {
		return true, nil
	}

	const insertAudit = `
		INSERT INTO block_storage_generation_rehome (
			evidence_sha256, tag, object_key_main, source_generation, target_generation,
			source_bucket, source_version_id, source_etag,
			destination_bucket, destination_version_id, destination_etag, object_bytes,
			start_height, end_height, expected_block_count, expected_canonical_count,
			expected_fenced_count, expected_deleted_verified_count, state
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, $10, $11, $12,
			$13, $14, $15, $16,
			$17, $18, 'executing'
		)
		RETURNING id`
	var auditID int64
	if err := tx.QueryRowContext(
		ctx,
		insertAudit,
		req.Object.EvidenceSHA256,
		req.Tag,
		req.Object.ObjectKey,
		legacyGeneration,
		req.TargetGeneration,
		req.SourceBucket,
		req.Object.Source.VersionID,
		req.Object.Source.ETag,
		req.DestinationBucket,
		req.Object.Destination.VersionID,
		req.Object.Destination.ETag,
		req.Object.Destination.Bytes,
		req.Object.StartHeight,
		req.Object.EndHeight,
		req.Object.ExpectedRows,
		req.Object.ExpectedCanonical,
		req.Object.ExpectedFenced,
		req.Object.ExpectedDeleted,
	).Scan(&auditID); err != nil {
		return false, xerrors.Errorf("failed to insert storage generation rehome audit for %q: %w", req.Object.ObjectKey, err)
	}

	const updateShadow = `
		UPDATE block_consolidation_shadow shadow
		SET consolidated_storage_generation = $3
		FROM block_metadata bm
		WHERE shadow.block_metadata_id = bm.id
			AND bm.tag = $1
			AND bm.object_key_main = $2
			AND bm.storage_generation IS NULL
			AND shadow.consolidated_object_key_main = bm.object_key_main
			AND shadow.consolidated_storage_generation IS NULL`
	result, err := tx.ExecContext(ctx, updateShadow, req.Tag, req.Object.ObjectKey, req.TargetGeneration)
	if err != nil {
		return false, xerrors.Errorf("failed to rehome shadow placement for %q: %w", req.Object.ObjectKey, err)
	}
	if err := requireRowsAffected(result, req.Object.ExpectedRows, "shadow placement", req.Object.ObjectKey); err != nil {
		return false, err
	}

	const updatePrimary = `
		UPDATE block_metadata
		SET storage_generation = $3
		WHERE tag = $1
			AND object_key_main = $2
			AND storage_generation IS NULL`
	result, err = tx.ExecContext(ctx, updatePrimary, req.Tag, req.Object.ObjectKey, req.TargetGeneration)
	if err != nil {
		return false, xerrors.Errorf("failed to rehome primary placement for %q: %w", req.Object.ObjectKey, err)
	}
	if err := requireRowsAffected(result, req.Object.ExpectedRows, "primary placement", req.Object.ObjectKey); err != nil {
		return false, err
	}

	const completeAudit = `
		UPDATE block_storage_generation_rehome
		SET state = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND state = 'executing'`
	result, err = tx.ExecContext(ctx, completeAudit, auditID)
	if err != nil {
		return false, xerrors.Errorf("failed to complete storage generation rehome audit for %q: %w", req.Object.ObjectKey, err)
	}
	if err := requireRowsAffected(result, 1, "rehome audit", req.Object.ObjectKey); err != nil {
		return false, err
	}

	completed, err := inspectCohort(
		ctx,
		tx,
		req.Tag,
		req.Object.ObjectKey,
		req.TargetGeneration,
		req.Object.EvidenceSHA256,
	)
	if err != nil {
		return false, err
	}
	completedTarget, err := validateInspection(req.Object, completed)
	if err != nil {
		return false, xerrors.Errorf("post-rehome validation failed for %q: %w", req.Object.ObjectKey, err)
	}
	if !completedTarget {
		return false, xerrors.Errorf("post-rehome validation did not reach target generation for %q", req.Object.ObjectKey)
	}

	if err := tx.Commit(); err != nil {
		return false, xerrors.Errorf("failed to commit storage generation rehome for %q: %w", req.Object.ObjectKey, err)
	}
	return false, nil
}

func inspectCohort(
	ctx context.Context,
	q rowQueryer,
	tag uint32,
	objectKey string,
	targetGeneration string,
	evidenceSHA256 string,
) (Inspection, error) {
	const query = `
		WITH cohort AS (
			SELECT
				bm.height,
				cb.block_metadata_id IS NOT NULL AS canonical,
				bm.single_block_retention_fenced_at IS NOT NULL AS fenced,
				bm.storage_generation AS primary_generation,
				shadow.block_metadata_id IS NULL AS missing_shadow,
				CASE
					WHEN shadow.block_metadata_id IS NULL THEN FALSE
					ELSE NOT (
						bm.object_format = $5
						AND bm.byte_offset IS NOT NULL AND bm.byte_offset >= 0
						AND bm.byte_length IS NOT NULL AND bm.byte_length > 0
						AND bm.uncompressed_length IS NOT NULL AND bm.uncompressed_length > 0
						AND shadow.tag = bm.tag
						AND shadow.height = bm.height
						AND shadow.hash IS NOT DISTINCT FROM bm.hash
						AND shadow.consolidated_object_key_main = bm.object_key_main
						AND shadow.object_format = bm.object_format
						AND shadow.byte_offset = bm.byte_offset
						AND shadow.byte_length = bm.byte_length
						AND shadow.uncompressed_length IS NOT DISTINCT FROM bm.uncompressed_length
					)
				END AS invalid_placement,
				shadow.consolidated_storage_generation,
				retirement.block_metadata_id IS NOT NULL
					AND retirement.state = 'deleted_verified'
					AND retirement.deleted_at IS NOT NULL
					AND retirement.verified_at IS NOT NULL
					AND retirement.consolidated_object_key_main = bm.object_key_main
					AND shadow.single_block_object_deleted_at IS NOT NULL AS deleted_verified,
				retirement.block_metadata_id IS NOT NULL
					AND NOT (
						retirement.state = 'deleted_verified'
						AND retirement.deleted_at IS NOT NULL
						AND retirement.verified_at IS NOT NULL
						AND retirement.consolidated_object_key_main = bm.object_key_main
						AND shadow.single_block_object_deleted_at IS NOT NULL
					) AS active_retention,
				EXISTS (
					SELECT 1
					FROM cscb_repair_manifest repair
					WHERE repair.old_consolidated_object_key_main = bm.object_key_main
						AND NOT (
							repair.state = 'completed'
							AND repair.outcome = 'already_clean_storage_neutral'
						)
				) AS pinned_repair
			FROM block_metadata bm
			LEFT JOIN canonical_blocks cb
				ON cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
			LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
			LEFT JOIN block_single_block_retention retirement ON retirement.block_metadata_id = bm.id
			WHERE bm.tag = $1 AND bm.object_key_main = $2
		)
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE canonical),
			COALESCE(MIN(height), 0),
			COALESCE(MAX(height) + 1, 0),
			COUNT(*) FILTER (WHERE missing_shadow),
			COUNT(*) FILTER (WHERE invalid_placement),
			COUNT(*) FILTER (WHERE fenced),
			COUNT(*) FILTER (WHERE deleted_verified),
			COUNT(*) FILTER (WHERE active_retention),
			COUNT(*) FILTER (WHERE pinned_repair),
			COUNT(*) FILTER (WHERE primary_generation IS NULL),
			COUNT(*) FILTER (WHERE primary_generation = $3),
			COUNT(*) FILTER (WHERE primary_generation IS NOT NULL AND primary_generation <> $3),
			COUNT(*) FILTER (WHERE consolidated_storage_generation IS NULL),
			COUNT(*) FILTER (WHERE consolidated_storage_generation = $3),
			COUNT(*) FILTER (WHERE consolidated_storage_generation IS NOT NULL AND consolidated_storage_generation <> $3),
			EXISTS (
				SELECT 1
				FROM block_storage_generation_rehome audit
				WHERE audit.tag = $1
					AND audit.object_key_main = $2
					AND audit.source_generation = 'legacy'
					AND audit.target_generation = $3
					AND audit.evidence_sha256 = $4
					AND audit.state = 'completed'
			)
		FROM cohort`

	var inspection Inspection
	if err := q.QueryRowContext(
		ctx,
		query,
		tag,
		objectKey,
		targetGeneration,
		evidenceSHA256,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
	).Scan(
		&inspection.TotalRows,
		&inspection.CanonicalRows,
		&inspection.StartHeight,
		&inspection.EndHeight,
		&inspection.MissingShadowRows,
		&inspection.InvalidPlacementRows,
		&inspection.FencedRows,
		&inspection.DeletedVerifiedRows,
		&inspection.ActiveRetentionRows,
		&inspection.PinnedRepairRows,
		&inspection.PrimaryLegacyRows,
		&inspection.PrimaryTargetRows,
		&inspection.PrimaryOtherRows,
		&inspection.ConsolidatedLegacyRows,
		&inspection.ConsolidatedTargetRows,
		&inspection.ConsolidatedOtherRows,
		&inspection.CompletedAudit,
	); err != nil {
		return Inspection{}, xerrors.Errorf("failed to inspect storage generation cohort for %q: %w", objectKey, err)
	}
	return inspection, nil
}

func validateInspectionKey(objectKey string, targetGeneration string, evidenceSHA256 string) error {
	if strings.TrimSpace(objectKey) == "" {
		return xerrors.New("storage generation rehome object key is required")
	}
	if !generationPattern.MatchString(targetGeneration) || targetGeneration != "v2" {
		return xerrors.Errorf("unsupported storage generation rehome target %q", targetGeneration)
	}
	if len(evidenceSHA256) != 64 {
		return xerrors.New("valid storage generation rehome evidence digest is required")
	}
	if _, err := hex.DecodeString(evidenceSHA256); err != nil || strings.ToLower(evidenceSHA256) != evidenceSHA256 {
		return xerrors.New("valid storage generation rehome evidence digest is required")
	}
	return nil
}

func validateRehomeRequest(req RehomeRequest) error {
	if err := validateInspectionKey(req.Object.ObjectKey, req.TargetGeneration, req.Object.EvidenceSHA256); err != nil {
		return err
	}
	if req.SourceBucket == "" || req.DestinationBucket == "" || req.SourceBucket == req.DestinationBucket {
		return xerrors.New("distinct source and destination buckets are required")
	}
	if req.Object.EndHeight <= req.Object.StartHeight || req.Object.ExpectedRows == 0 {
		return xerrors.New("valid storage generation rehome object range and row count are required")
	}
	if req.Object.ExpectedCanonical > req.Object.ExpectedRows ||
		req.Object.ExpectedFenced == 0 ||
		req.Object.ExpectedFenced > req.Object.ExpectedRows ||
		req.Object.ExpectedDeleted != req.Object.ExpectedFenced {
		return xerrors.New("valid storage generation rehome cohort counts are required")
	}
	if !immutableVersionID(req.Object.Source.VersionID) || !immutableVersionID(req.Object.Destination.VersionID) ||
		req.Object.Source.ETag == "" || req.Object.Destination.ETag == "" ||
		req.Object.Source.Bytes == 0 || req.Object.Source.Bytes != req.Object.Destination.Bytes {
		return xerrors.New("valid immutable storage generation rehome object versions are required")
	}
	if req.Object.EvidenceSHA256 != evidenceSHA256(req.Object) {
		return xerrors.New("storage generation rehome evidence digest mismatch")
	}
	return nil
}

func requireRowsAffected(result sql.Result, expected uint64, name string, objectKey string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to count %s rows for %q: %w", name, objectKey, err)
	}
	if rows < 0 || uint64(rows) != expected {
		return xerrors.Errorf("%s guard failed for %q: rows=%d expected=%d", name, objectKey, rows, expected)
	}
	return nil
}
