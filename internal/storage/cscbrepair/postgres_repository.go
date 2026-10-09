package cscbrepair

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/lib/pq"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/cscbrepairlock"
	"github.com/coinbase/chainstorage/internal/storage/retirementlock"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const manifestColumns = `
	id, tag, state, bucket,
	old_consolidated_object_key_main, old_consolidated_object_version_id,
	old_consolidated_object_etag, old_consolidated_object_bytes,
	start_height, end_height, canonical_block_count, total_block_count, row_set_sha256,
	new_consolidated_object_key_main, new_consolidated_object_version_id,
	new_consolidated_object_etag, new_consolidated_object_bytes,
	outcome, prepared_at, restored_at, verified_at,
	completed_at`

const repairPromotionRetentionCommitGrace = time.Hour

type (
	PostgresRepository struct {
		db *sql.DB
	}

	rowScanner interface {
		Scan(dest ...any) error
	}

	queryer interface {
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
)

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) FindByExecutionKey(
	ctx context.Context,
	executionKey string,
) (*Manifest, bool, error) {
	if err := r.validateDB(); err != nil {
		return nil, false, err
	}
	if !validExecutionKey(executionKey) {
		return nil, false, xerrors.New("valid CSCB repair execution key is required")
	}
	var repairID sql.NullInt64
	if err := r.db.QueryRowContext(ctx, `
		SELECT repair_id
		FROM cscb_repair_execution
		WHERE execution_key = $1`, executionKey).Scan(&repairID); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, xerrors.Errorf("failed to find CSCB repair execution binding: %w", err)
	}
	if !repairID.Valid {
		return nil, true, nil
	}
	manifest, err := loadManifest(ctx, r.db, repairID.Int64, false)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

func (r *PostgresRepository) FindPending(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if endHeight <= startHeight {
		return nil, xerrors.New("valid CSCB repair range is required")
	}
	query := fmt.Sprintf(`
			SELECT %s
			FROM cscb_repair_manifest
			WHERE tag = $1
				AND state <> $2
				AND end_height > $3
				AND start_height < $4
			ORDER BY id ASC
			LIMIT 1`, manifestColumns)
	manifest, err := scanManifest(r.db.QueryRowContext(
		ctx,
		query,
		tag,
		StateCompleted,
		startHeight,
		endHeight,
	))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to find pending CSCB repair: %w", err)
	}
	if err := loadManifestBlocks(ctx, r.db, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (r *PostgresRepository) FindByObjectKey(
	ctx context.Context,
	tag uint32,
	objectKey string,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if objectKey == "" {
		return nil, xerrors.New("CSCB repair object key is required")
	}
	query := fmt.Sprintf(`
			SELECT %s
			FROM cscb_repair_manifest
			WHERE tag = $1
				AND old_consolidated_object_key_main = $2
			LIMIT 1`, manifestColumns)
	manifest, err := scanManifest(r.db.QueryRowContext(ctx, query, tag, objectKey))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to find CSCB repair manifest for object %q: %w", objectKey, err)
	}
	if err := loadManifestBlocks(ctx, r.db, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (r *PostgresRepository) FindNextCandidate(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if endHeight <= startHeight {
		return nil, nil
	}
	objectKey, err := findNextCandidateObject(ctx, r.db, tag, startHeight, endHeight)
	if err != nil {
		return nil, err
	}
	if objectKey == "" {
		if err := requireNoShadowOnlyCandidates(ctx, r.db, tag, startHeight, endHeight); err != nil {
			return nil, err
		}
		return nil, nil
	}
	manifest, err := loadCandidateByKey(ctx, r.db, tag, objectKey)
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

func (r *PostgresRepository) FindCandidateByObjectKey(
	ctx context.Context,
	tag uint32,
	objectKey string,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if objectKey == "" {
		return nil, xerrors.New("CSCB repair object key is required")
	}
	return loadCandidateByKey(ctx, r.db, tag, objectKey)
}

func (r *PostgresRepository) ListCandidateObjectKeys(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	limit int,
) ([]string, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if endHeight <= startHeight {
		return nil, xerrors.New("valid CSCB repair range is required")
	}
	if limit <= 0 {
		return nil, xerrors.New("CSCB repair candidate limit must be positive")
	}

	objectKeys := make([]string, 0, limit)
	pendingRows, err := r.db.QueryContext(ctx, `
		SELECT old_consolidated_object_key_main, start_height, end_height
		FROM cscb_repair_manifest
		WHERE tag = $1
			AND state <> $2
			AND end_height > $3
			AND start_height < $4
		ORDER BY end_height DESC, old_consolidated_object_key_main DESC
		LIMIT $5`, tag, StateCompleted, startHeight, endHeight, limit)
	if err != nil {
		return nil, xerrors.Errorf("failed to list pending CSCB repair candidates: %w", err)
	}
	for pendingRows.Next() {
		var objectKey string
		var candidateStart, candidateEnd uint64
		if err := pendingRows.Scan(&objectKey, &candidateStart, &candidateEnd); err != nil {
			_ = pendingRows.Close()
			return nil, xerrors.Errorf("failed to scan pending CSCB repair candidate: %w", err)
		}
		if candidateStart < startHeight || candidateEnd > endHeight {
			_ = pendingRows.Close()
			return nil, xerrors.Errorf(
				"pending CSCB repair object %q exceeds approved range: object=[%d, %d) approved=[%d, %d)",
				objectKey,
				candidateStart,
				candidateEnd,
				startHeight,
				endHeight,
			)
		}
		objectKeys = append(objectKeys, objectKey)
	}
	if err := pendingRows.Err(); err != nil {
		_ = pendingRows.Close()
		return nil, xerrors.Errorf("failed to iterate pending CSCB repair candidates: %w", err)
	}
	if err := pendingRows.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close pending CSCB repair candidates: %w", err)
	}

	remaining := limit - len(objectKeys)
	if remaining > 0 {
		activeRows, err := r.db.QueryContext(ctx, `
			WITH overlapping_keys AS (
				SELECT DISTINCT bm.object_key_main
				FROM block_metadata bm
				WHERE bm.tag = $1
					AND bm.height >= $2
					AND bm.height < $3
					AND bm.object_format = $4
					AND bm.object_key_main IS NOT NULL
					AND bm.object_key_main <> ''
					AND NOT EXISTS (
						SELECT 1
						FROM cscb_repair_manifest repair
						WHERE repair.tag = bm.tag
							AND (
								repair.old_consolidated_object_key_main = bm.object_key_main
								OR repair.new_consolidated_object_key_main = bm.object_key_main
							)
					)
			)
			SELECT bm.object_key_main, MIN(bm.height), MAX(bm.height)
			FROM block_metadata bm
			JOIN overlapping_keys candidate ON candidate.object_key_main = bm.object_key_main
			WHERE bm.tag = $1
				AND bm.object_format = $4
				AND bm.object_key_main IS NOT NULL
				AND bm.object_key_main <> ''
			GROUP BY bm.object_key_main
			ORDER BY MAX(bm.height) DESC, bm.object_key_main DESC
			LIMIT $5`,
			tag,
			startHeight,
			endHeight,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			remaining,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to list active CSCB repair candidates: %w", err)
		}
		for activeRows.Next() {
			var objectKey string
			var minHeight, maxHeight uint64
			if err := activeRows.Scan(&objectKey, &minHeight, &maxHeight); err != nil {
				_ = activeRows.Close()
				return nil, xerrors.Errorf("failed to scan active CSCB repair candidate: %w", err)
			}
			if minHeight < startHeight || maxHeight >= endHeight {
				_ = activeRows.Close()
				return nil, xerrors.Errorf(
					"CSCB repair object %q crosses approved range: object=[%d, %d] approved=[%d, %d)",
					objectKey,
					minHeight,
					maxHeight,
					startHeight,
					endHeight,
				)
			}
			objectKeys = append(objectKeys, objectKey)
		}
		if err := activeRows.Err(); err != nil {
			_ = activeRows.Close()
			return nil, xerrors.Errorf("failed to iterate active CSCB repair candidates: %w", err)
		}
		if err := activeRows.Close(); err != nil {
			return nil, xerrors.Errorf("failed to close active CSCB repair candidates: %w", err)
		}
	}

	if len(objectKeys) == 0 {
		if err := requireNoShadowOnlyCandidates(ctx, r.db, tag, startHeight, endHeight); err != nil {
			return nil, err
		}
	}
	return objectKeys, nil
}

func findNextCandidateObject(
	ctx context.Context,
	q queryer,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) (string, error) {
	if endHeight <= startHeight {
		return "", nil
	}
	const query = `
		WITH overlapping_keys AS (
			SELECT DISTINCT bm.object_key_main
			FROM block_metadata bm
			WHERE bm.tag = $1
				AND bm.height >= $2
				AND bm.height < $3
				AND bm.object_format = $4
				AND bm.object_key_main IS NOT NULL
				AND bm.object_key_main <> ''
				AND NOT EXISTS (
					SELECT 1
					FROM cscb_repair_manifest repair
					WHERE repair.tag = bm.tag
						AND (
							repair.old_consolidated_object_key_main = bm.object_key_main
							OR repair.new_consolidated_object_key_main = bm.object_key_main
						)
				)
		)
		SELECT bm.object_key_main, MIN(bm.height), MAX(bm.height)
		FROM block_metadata bm
		JOIN overlapping_keys candidate ON candidate.object_key_main = bm.object_key_main
		WHERE bm.tag = $1
			AND bm.object_format = $4
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
		GROUP BY bm.object_key_main
		ORDER BY MAX(bm.height) DESC, bm.object_key_main DESC
		LIMIT 1`
	var objectKey string
	var minHeight, maxHeight uint64
	if err := q.QueryRowContext(
		ctx,
		query,
		tag,
		startHeight,
		endHeight,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	).Scan(&objectKey, &minHeight, &maxHeight); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", xerrors.Errorf("failed to find next active CSCB repair candidate: %w", err)
	}
	if minHeight < startHeight || maxHeight >= endHeight {
		return "", xerrors.Errorf(
			"CSCB repair object %q crosses approved range: object=[%d, %d] approved=[%d, %d)",
			objectKey,
			minHeight,
			maxHeight,
			startHeight,
			endHeight,
		)
	}
	return objectKey, nil
}

func requireNoShadowOnlyCandidates(
	ctx context.Context,
	q queryer,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) error {
	const query = `
		SELECT shadow.consolidated_object_key_main, MIN(bm.height), MAX(bm.height)
		FROM block_consolidation_shadow shadow
		JOIN block_metadata bm ON bm.id = shadow.block_metadata_id
			AND bm.tag = shadow.tag
			AND bm.height = shadow.height
			AND bm.hash IS NOT DISTINCT FROM shadow.hash
		WHERE shadow.tag = $1
			AND shadow.height >= $2
			AND shadow.height < $3
			AND shadow.object_format = $4
			AND shadow.consolidated_object_key_main IS NOT NULL
			AND shadow.consolidated_object_key_main <> ''
			AND (
				bm.object_key_main IS DISTINCT FROM shadow.consolidated_object_key_main
				OR bm.object_format <> $4
			)
			AND NOT EXISTS (
				SELECT 1
				FROM cscb_repair_manifest repair
				WHERE repair.tag = shadow.tag
					AND (
						repair.old_consolidated_object_key_main = shadow.consolidated_object_key_main
						OR repair.new_consolidated_object_key_main = shadow.consolidated_object_key_main
					)
			)
		GROUP BY shadow.consolidated_object_key_main
		ORDER BY MAX(bm.height) DESC, shadow.consolidated_object_key_main DESC
		LIMIT 1`
	var objectKey string
	var minHeight, maxHeight uint64
	if err := q.QueryRowContext(
		ctx,
		query,
		tag,
		startHeight,
		endHeight,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	).Scan(&objectKey, &minHeight, &maxHeight); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return xerrors.Errorf("failed to inspect shadow-only CSCB repair candidates: %w", err)
	}
	return xerrors.Errorf(
		"CSCB repair range contains shadow-only consolidated object %q at heights [%d, %d]",
		objectKey,
		minHeight,
		maxHeight,
	)
}

func (r *PostgresRepository) FenceCandidate(ctx context.Context, manifest *Manifest) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if err := validateFencedInput(manifest); err != nil {
		return nil, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair fencing: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockRepairTag(ctx, tx, manifest.Tag); err != nil {
		return nil, err
	}
	if err := lockCandidateRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKey(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	current, err := loadCandidateByKey(ctx, tx, manifest.Tag, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, err
	}
	if err := compareCandidateRows(current, manifest); err != nil {
		return nil, err
	}
	if err := requireNoUnexpectedOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey, manifest.Blocks); err != nil {
		return nil, err
	}

	const insertManifest = `
		INSERT INTO cscb_repair_manifest (
			tag, state, bucket, old_consolidated_object_key_main,
			start_height, end_height, canonical_block_count, total_block_count, row_set_sha256
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tag, old_consolidated_object_key_main) DO NOTHING
		RETURNING id`
	var repairID int64
	err = tx.QueryRowContext(
		ctx,
		insertManifest,
		manifest.Tag,
		StatePreparing,
		manifest.Bucket,
		manifest.OldConsolidatedObjectKey,
		manifest.StartHeight,
		manifest.EndHeight,
		manifest.CanonicalBlockCount,
		manifest.TotalBlockCount,
		manifest.RowSetSHA256,
	).Scan(&repairID)
	inserted := err == nil
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("failed to insert CSCB repair manifest: %w", err)
	}

	if inserted {
		const insertBlock = `
			INSERT INTO cscb_repair_block (
				repair_id, block_metadata_id, canonical, tag, height, hash,
				single_block_object_key_main, single_block_object_key_sha256,
				old_byte_offset, old_byte_length, old_uncompressed_length
			) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, $9, $10, $11)`
		for _, block := range manifest.Blocks {
			if _, err := tx.ExecContext(
				ctx,
				insertBlock,
				repairID,
				block.BlockMetadataID,
				block.Canonical,
				block.Tag,
				block.Height,
				block.Hash,
				block.SingleBlockObjectKey,
				block.SingleBlockObjectKeySHA256,
				block.OldByteOffset,
				block.OldByteLength,
				block.OldUncompressedLength,
			); err != nil {
				return nil, xerrors.Errorf("failed to insert CSCB repair block metadata_id=%d: %w", block.BlockMetadataID, err)
			}
		}
	} else {
		const existingID = `
			SELECT id
			FROM cscb_repair_manifest
			WHERE tag = $1 AND old_consolidated_object_key_main = $2`
		if err := tx.QueryRowContext(ctx, existingID, manifest.Tag, manifest.OldConsolidatedObjectKey).Scan(&repairID); err != nil {
			return nil, xerrors.Errorf("failed to load concurrent CSCB repair manifest: %w", err)
		}
	}

	fenced, err := loadManifest(ctx, tx, repairID, false)
	if err != nil {
		return nil, err
	}
	if !sameFencedManifest(fenced, manifest) {
		return nil, xerrors.Errorf("CSCB repair manifest conflict for old object %q", manifest.OldConsolidatedObjectKey)
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair fence: %w", err)
	}
	return fenced, nil
}

func (r *PostgresRepository) RecordInspection(
	ctx context.Context,
	repairID int64,
	oldObject ObjectVersion,
	blocks []Block,
	alreadyClean bool,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if err := validateInspectionInput(oldObject, blocks); err != nil {
		return nil, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin recording CSCB repair inspection: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	manifest, err := lockManifestForRepair(ctx, tx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State != StatePreparing {
		if !sameInspection(manifest, oldObject, blocks, alreadyClean) {
			return nil, xerrors.Errorf("CSCB repair inspection changed for repair id %d", repairID)
		}
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent CSCB repair inspection: %w", err)
		}
		return manifest, nil
	}
	if len(blocks) != len(manifest.Blocks) {
		return nil, xerrors.Errorf("CSCB repair inspection block count changed: expected=%d actual=%d", len(manifest.Blocks), len(blocks))
	}
	if err := lockCandidateRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKey(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	current, err := loadCandidateByKey(ctx, tx, manifest.Tag, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, err
	}
	if err := compareCandidateRows(current, manifest); err != nil {
		return nil, err
	}
	if err := requireNoUnexpectedOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey, manifest.Blocks); err != nil {
		return nil, err
	}

	const updateBlock = `
		UPDATE cscb_repair_block
		SET single_block_object_version_id = $3,
			single_block_object_etag = $4,
			single_block_object_bytes = $5,
			payload_sha256 = $6
		WHERE repair_id = $1
			AND block_metadata_id = $2
			AND single_block_object_key_main = $7
			AND single_block_object_key_sha256 = $8
			AND single_block_object_version_id IS NULL
			AND single_block_object_etag IS NULL
			AND single_block_object_bytes IS NULL
			AND payload_sha256 IS NULL`
	for i := range blocks {
		block := &blocks[i]
		expected := manifest.Blocks[i]
		if block.BlockMetadataID != expected.BlockMetadataID ||
			block.SingleBlockObjectKey != expected.SingleBlockObjectKey ||
			block.SingleBlockObjectKeySHA256 != expected.SingleBlockObjectKeySHA256 {
			return nil, xerrors.Errorf("CSCB repair inspection row changed for metadata_id=%d", expected.BlockMetadataID)
		}
		result, err := tx.ExecContext(
			ctx,
			updateBlock,
			manifest.ID,
			block.BlockMetadataID,
			block.SingleBlockObjectVersion.VersionID,
			block.SingleBlockObjectVersion.ETag,
			block.SingleBlockObjectVersion.Bytes,
			block.PayloadSHA256,
			block.SingleBlockObjectKey,
			block.SingleBlockObjectKeySHA256,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to record CSCB repair block inspection metadata_id=%d: %w", block.BlockMetadataID, err)
		}
		if err := requireRowsAffected(result, 1, "record CSCB repair block inspection"); err != nil {
			return nil, err
		}
	}

	state := StatePrepared
	outcome := ""
	terminal := false
	if alreadyClean {
		state = StateCompleted
		outcome = alreadyCleanOutcome
		terminal = true
	}
	const updateManifest = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			old_consolidated_object_version_id = $3,
			old_consolidated_object_etag = $4,
			old_consolidated_object_bytes = $5,
			outcome = $6,
			verified_at = CASE WHEN $7 THEN clock_timestamp() ELSE NULL END,
			completed_at = CASE WHEN $7 THEN clock_timestamp() ELSE NULL END,
			updated_at = clock_timestamp()
		WHERE id = $1 AND state = $8`
	result, err := tx.ExecContext(
		ctx,
		updateManifest,
		manifest.ID,
		state,
		oldObject.VersionID,
		oldObject.ETag,
		oldObject.Bytes,
		outcome,
		terminal,
		StatePreparing,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to record CSCB repair inspection: %w", err)
	}
	if err := requireRowsAffected(result, 1, "record CSCB repair inspection"); err != nil {
		return nil, err
	}
	inspected, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair inspection: %w", err)
	}
	return inspected, nil
}

func (r *PostgresRepository) BindExecutionKey(
	ctx context.Context,
	executionKey string,
	repairID int64,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if !validExecutionKey(executionKey) || repairID <= 0 {
		return nil, xerrors.New("valid CSCB repair execution binding is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair execution binding: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insert = `
		INSERT INTO cscb_repair_execution (execution_key, repair_id)
		VALUES ($1, $2)
		ON CONFLICT (execution_key) DO NOTHING`
	if _, err := tx.ExecContext(ctx, insert, executionKey, repairID); err != nil {
		return nil, xerrors.Errorf("failed to bind CSCB repair execution: %w", err)
	}
	var boundRepairID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT repair_id
		FROM cscb_repair_execution
		WHERE execution_key = $1
		FOR UPDATE`, executionKey).Scan(&boundRepairID); err != nil {
		return nil, xerrors.Errorf("failed to load CSCB repair execution binding: %w", err)
	}
	if !boundRepairID.Valid {
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit CSCB repair no-candidate execution binding: %w", err)
		}
		return nil, nil
	}
	if boundRepairID.Int64 != repairID {
		return nil, xerrors.Errorf(
			"CSCB repair execution key is already bound to repair id %d, requested %d",
			boundRepairID.Int64,
			repairID,
		)
	}
	manifest, err := loadManifest(ctx, tx, boundRepairID.Int64, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair execution binding: %w", err)
	}
	return manifest, nil
}

func (r *PostgresRepository) BindNoCandidateExecution(
	ctx context.Context,
	executionKey string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if !validExecutionKey(executionKey) {
		return nil, xerrors.New("valid CSCB repair execution key is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair no-candidate execution binding: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockRepairTag(ctx, tx, tag); err != nil {
		return nil, err
	}
	objectKey, err := findNextCandidateObject(ctx, tx, tag, startHeight, endHeight)
	if err != nil {
		return nil, xerrors.Errorf("failed terminal CSCB repair candidate check: %w", err)
	}
	if objectKey != "" {
		return nil, xerrors.Errorf("CSCB repair candidate appeared before range completion: object=%q", objectKey)
	}
	if err := requireNoShadowOnlyCandidates(ctx, tx, tag, startHeight, endHeight); err != nil {
		return nil, xerrors.Errorf("failed terminal CSCB repair shadow check: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO cscb_repair_execution (execution_key, repair_id)
		VALUES ($1, NULL)
		ON CONFLICT (execution_key) DO NOTHING`, executionKey); err != nil {
		return nil, xerrors.Errorf("failed to bind CSCB repair no-candidate execution: %w", err)
	}
	var boundRepairID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT repair_id
		FROM cscb_repair_execution
		WHERE execution_key = $1
		FOR UPDATE`, executionKey).Scan(&boundRepairID); err != nil {
		return nil, xerrors.Errorf("failed to load CSCB repair no-candidate execution binding: %w", err)
	}
	var manifest *Manifest
	if boundRepairID.Valid {
		manifest, err = loadManifest(ctx, tx, boundRepairID.Int64, false)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair no-candidate execution binding: %w", err)
	}
	return manifest, nil
}

func (r *PostgresRepository) Get(ctx context.Context, repairID int64) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	return loadManifest(ctx, r.db, repairID, false)
}

func (r *PostgresRepository) RestoreToSingleBlock(
	ctx context.Context,
	repairID int64,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair restore: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	manifest, err := lockManifestForRepair(ctx, tx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State != StatePrepared {
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent CSCB repair restore: %w", err)
		}
		return manifest, nil
	}
	if err := lockCandidateRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	current, err := loadCandidateByKey(ctx, tx, manifest.Tag, manifest.OldConsolidatedObjectKey)
	if err != nil {
		return nil, err
	}
	if err := compareCandidateRows(current, manifest); err != nil {
		return nil, err
	}

	if manifest.CanonicalBlockCount > 0 {
		const unrelatedCanonical = `
			SELECT bm.id
			FROM canonical_blocks cb
			JOIN block_metadata bm ON bm.id = cb.block_metadata_id
				AND bm.tag = cb.tag
				AND bm.height = cb.height
			WHERE cb.tag = $1
				AND cb.height >= $2
				AND cb.height < $3
				AND bm.skipped = FALSE
				AND NOT EXISTS (
					SELECT 1 FROM cscb_repair_block block
					WHERE block.repair_id = $4 AND block.block_metadata_id = bm.id
				)
			LIMIT 1`
		var unrelatedID int64
		err = tx.QueryRowContext(
			ctx,
			unrelatedCanonical,
			manifest.Tag,
			manifest.StartHeight,
			manifest.EndHeight,
			manifest.ID,
		).Scan(&unrelatedID)
		if err != nil && err != sql.ErrNoRows {
			return nil, xerrors.Errorf("failed to check CSCB repair range isolation: %w", err)
		}
		if err == nil {
			return nil, xerrors.Errorf("CSCB repair range contains unrelated canonical metadata_id=%d", unrelatedID)
		}
	}

	const restoreMetadata = `
		UPDATE block_metadata bm
		SET object_key_main = block.single_block_object_key_main,
			object_format = $2,
			byte_offset = NULL,
			byte_length = NULL,
			uncompressed_length = NULL
		FROM cscb_repair_block block
		WHERE block.repair_id = $1
			AND bm.id = block.block_metadata_id
			AND bm.tag = block.tag
			AND bm.height = block.height
			AND bm.hash IS NOT DISTINCT FROM block.hash
			AND bm.skipped = FALSE
			AND bm.single_block_retention_fenced_at IS NULL
			AND bm.storage_generation IS NULL
			AND bm.object_key_main = $3
			AND bm.object_format = $4
			AND bm.byte_offset = block.old_byte_offset
			AND bm.byte_length = block.old_byte_length
			AND bm.uncompressed_length = block.old_uncompressed_length`
	result, err := tx.ExecContext(
		ctx,
		restoreMetadata,
		manifest.ID,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		manifest.OldConsolidatedObjectKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to restore active single-block metadata: %w", err)
	}
	if err := requireRowsAffected(result, manifest.TotalBlockCount, "restore active single-block metadata"); err != nil {
		return nil, err
	}

	const deleteOldShadows = `
		DELETE FROM block_consolidation_shadow shadow
		USING cscb_repair_block block
		WHERE block.repair_id = $1
			AND shadow.block_metadata_id = block.block_metadata_id
			AND shadow.tag = block.tag
			AND shadow.height = block.height
			AND shadow.hash IS NOT DISTINCT FROM block.hash
			AND shadow.single_block_object_key_main = block.single_block_object_key_main
			AND shadow.single_block_object_deleted_at IS NULL
			AND shadow.single_block_storage_generation IS NULL
			AND shadow.consolidated_storage_generation IS NULL
			AND shadow.consolidated_object_key_main = $2
			AND shadow.object_format = $3
			AND shadow.byte_offset = block.old_byte_offset
			AND shadow.byte_length = block.old_byte_length
			AND shadow.uncompressed_length = block.old_uncompressed_length`
	result, err = tx.ExecContext(
		ctx,
		deleteOldShadows,
		manifest.ID,
		manifest.OldConsolidatedObjectKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to remove invalidated CSCB shadows: %w", err)
	}
	if err := requireRowsAffected(result, manifest.TotalBlockCount, "remove invalidated CSCB shadows"); err != nil {
		return nil, err
	}

	const markRestored = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			restored_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1 AND state = $3`
	result, err = tx.ExecContext(ctx, markRestored, manifest.ID, StateRestored, StatePrepared)
	if err != nil {
		return nil, xerrors.Errorf("failed to mark CSCB repair restored: %w", err)
	}
	if err := requireRowsAffected(result, 1, "mark CSCB repair restored"); err != nil {
		return nil, err
	}
	restored, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair restore: %w", err)
	}
	return restored, nil
}

func (r *PostgresRepository) GetRebuilt(ctx context.Context, repairID int64) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	manifest, err := loadManifest(ctx, r.db, repairID, false)
	if err != nil {
		return nil, err
	}
	if err := loadRebuiltBlocks(ctx, r.db, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func (r *PostgresRepository) RecordVerified(
	ctx context.Context,
	repairID int64,
	objectKey string,
	object ObjectVersion,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if objectKey == "" || object.VersionID == "" || object.ETag == "" || object.Bytes == 0 {
		return nil, xerrors.New("verified CSCB object identity is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin recording verified CSCB repair: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	manifest, err := lockManifestForRepair(ctx, tx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateVerified || manifest.State == StateCompleted {
		if manifest.NewConsolidatedObjectKey != objectKey ||
			manifest.NewConsolidatedObjectVersion.VersionID != object.VersionID ||
			manifest.NewConsolidatedObjectVersion.ETag != object.ETag ||
			manifest.NewConsolidatedObjectVersion.Bytes != object.Bytes {
			return nil, xerrors.Errorf("verified CSCB repair identity changed for repair id %d", manifest.ID)
		}
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent verified CSCB repair: %w", err)
		}
		return manifest, nil
	}
	if manifest.State != StateRestored {
		return nil, xerrors.Errorf("CSCB repair must be restored before verification: id=%d state=%s", manifest.ID, manifest.State)
	}
	if err := lockRepairRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKeys(ctx, tx, manifest.OldConsolidatedObjectKey, objectKey); err != nil {
		return nil, err
	}
	if err := loadRebuiltBlocks(ctx, tx, manifest); err != nil {
		return nil, err
	}
	if err := validateRebuiltMetadata(manifest, objectKey); err != nil {
		return nil, err
	}
	if err := requireExactNewReferences(ctx, tx, manifest, objectKey); err != nil {
		return nil, err
	}
	if err := requireNoOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}

	const update = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			new_consolidated_object_key_main = $3,
			new_consolidated_object_version_id = $4,
			new_consolidated_object_etag = $5,
			new_consolidated_object_bytes = $6,
			verified_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1 AND state = $7`
	result, err := tx.ExecContext(
		ctx,
		update,
		manifest.ID,
		StateVerified,
		objectKey,
		object.VersionID,
		object.ETag,
		object.Bytes,
		StateRestored,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to record verified CSCB repair: %w", err)
	}
	if err := requireRowsAffected(result, 1, "record verified CSCB repair"); err != nil {
		return nil, err
	}
	verified, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit verified CSCB repair: %w", err)
	}
	return verified, nil
}

func (r *PostgresRepository) PromoteCompleted(
	ctx context.Context,
	repairID int64,
	objectKey string,
	object ObjectVersion,
	placements []RebuiltPlacement,
	singleBlockObjectRetention time.Duration,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if objectKey == "" || object.VersionID == "" || object.ETag == "" || object.Bytes == 0 {
		return nil, xerrors.New("verified CSCB object identity is required")
	}
	if singleBlockObjectRetention <= 0 {
		return nil, xerrors.New("positive single-block object retention is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin atomic CSCB repair promotion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	manifest, err := lockManifestForRepair(ctx, tx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateCompleted {
		if manifest.NewConsolidatedObjectKey != objectKey ||
			manifest.NewConsolidatedObjectVersion != object {
			return nil, xerrors.Errorf("completed CSCB repair identity changed for repair id %d", manifest.ID)
		}
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent atomic CSCB repair promotion: %w", err)
		}
		return manifest, nil
	}
	if manifest.State != StatePrepared && manifest.State != StateRestored {
		return nil, xerrors.Errorf(
			"CSCB repair must be prepared or restored before atomic promotion: id=%d state=%s",
			manifest.ID,
			manifest.State,
		)
	}
	placementsByID, err := validateRebuiltPlacements(manifest, objectKey, placements)
	if err != nil {
		return nil, err
	}
	if err := lockRepairRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKeys(ctx, tx, manifest.OldConsolidatedObjectKey, objectKey); err != nil {
		return nil, err
	}
	if err := requireNoUnexpectedOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey, manifest.Blocks); err != nil {
		return nil, err
	}
	var promotionTime time.Time
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&promotionTime); err != nil {
		return nil, xerrors.Errorf("failed to capture CSCB repair promotion time: %w", err)
	}
	if manifest.State == StatePrepared {
		const markRestored = `
			UPDATE cscb_repair_manifest
			SET state = $2,
				restored_at = $3,
				updated_at = $3
			WHERE id = $1 AND state = $4`
		result, err := tx.ExecContext(ctx, markRestored, manifest.ID, StateRestored, promotionTime, StatePrepared)
		if err != nil {
			return nil, xerrors.Errorf("failed to mark CSCB repair restored during atomic promotion: %w", err)
		}
		if err := requireRowsAffected(result, 1, "mark CSCB repair restored during atomic promotion"); err != nil {
			return nil, err
		}
		manifest.State = StateRestored
		manifest.RestoredAt = &promotionTime
	}
	deleteAfter := promotionTime.Add(singleBlockObjectRetention + repairPromotionRetentionCommitGrace)

	for _, block := range manifest.Blocks {
		placement := placementsByID[block.BlockMetadataID]
		if err := validateRepairPromotionRow(ctx, tx, manifest, block, objectKey, placement); err != nil {
			return nil, err
		}

		const upsertShadow = `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11, $11, $12)
			ON CONFLICT (block_metadata_id) DO UPDATE SET
				tag = EXCLUDED.tag,
				height = EXCLUDED.height,
				hash = EXCLUDED.hash,
				single_block_object_key_main = EXCLUDED.single_block_object_key_main,
				consolidated_object_key_main = EXCLUDED.consolidated_object_key_main,
				object_format = EXCLUDED.object_format,
				byte_offset = EXCLUDED.byte_offset,
				byte_length = EXCLUDED.byte_length,
				uncompressed_length = EXCLUDED.uncompressed_length,
				validated_at = EXCLUDED.validated_at,
				single_block_retention_started_at = EXCLUDED.single_block_retention_started_at,
				single_block_delete_after = EXCLUDED.single_block_delete_after
			WHERE block_consolidation_shadow.tag = EXCLUDED.tag
				AND block_consolidation_shadow.height = EXCLUDED.height
				AND block_consolidation_shadow.hash IS NOT DISTINCT FROM EXCLUDED.hash
				AND block_consolidation_shadow.single_block_object_key_main = EXCLUDED.single_block_object_key_main
				AND block_consolidation_shadow.single_block_storage_generation IS NULL
				AND block_consolidation_shadow.consolidated_storage_generation IS NULL
				AND block_consolidation_shadow.single_block_object_deleted_at IS NULL
			RETURNING block_metadata_id`
		var shadowID int64
		if err := tx.QueryRowContext(
			ctx,
			upsertShadow,
			block.BlockMetadataID,
			block.Tag,
			block.Height,
			block.Hash,
			block.SingleBlockObjectKey,
			objectKey,
			int32(placement.ObjectFormat),
			placement.ByteOffset,
			placement.ByteLength,
			placement.UncompressedLength,
			promotionTime,
			deleteAfter,
		).Scan(&shadowID); err != nil {
			return nil, xerrors.Errorf("failed to upsert repaired CSCB shadow for metadata_id=%d: %w", block.BlockMetadataID, err)
		}

		const promoteMetadata = `
			UPDATE block_metadata
			SET object_key_main = $5,
				object_format = $6,
				byte_offset = $7,
				byte_length = $8,
				uncompressed_length = $9
			WHERE id = $1
				AND tag = $2
				AND height = $3
				AND hash IS NOT DISTINCT FROM NULLIF($4, '')
				AND skipped = FALSE
				AND storage_generation IS NULL
				AND single_block_retention_fenced_at IS NULL`
		result, err := tx.ExecContext(
			ctx,
			promoteMetadata,
			block.BlockMetadataID,
			block.Tag,
			block.Height,
			block.Hash,
			objectKey,
			int32(placement.ObjectFormat),
			placement.ByteOffset,
			placement.ByteLength,
			placement.UncompressedLength,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to promote repaired CSCB metadata_id=%d: %w", block.BlockMetadataID, err)
		}
		if err := requireRowsAffected(result, 1, "promote repaired CSCB metadata"); err != nil {
			return nil, err
		}
	}

	if err := requireNoOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	if err := requireExactRepairedReferences(ctx, tx, manifest, objectKey); err != nil {
		return nil, err
	}
	const markVerified = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			new_consolidated_object_key_main = $3,
			new_consolidated_object_version_id = $4,
			new_consolidated_object_etag = $5,
			new_consolidated_object_bytes = $6,
			restored_at = COALESCE(restored_at, $7),
			verified_at = $7,
			updated_at = $7
		WHERE id = $1 AND state = $8`
	result, err := tx.ExecContext(
		ctx,
		markVerified,
		manifest.ID,
		StateVerified,
		objectKey,
		object.VersionID,
		object.ETag,
		object.Bytes,
		promotionTime,
		StateRestored,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to mark atomically promoted CSCB repair verified: %w", err)
	}
	if err := requireRowsAffected(result, 1, "mark atomically promoted CSCB repair verified"); err != nil {
		return nil, err
	}
	const markCompleted = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			outcome = $3,
			completed_at = $4,
			updated_at = $4
		WHERE id = $1 AND state = $5`
	result, err = tx.ExecContext(
		ctx,
		markCompleted,
		manifest.ID,
		StateCompleted,
		completedOutcome,
		promotionTime,
		StateVerified,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to complete atomically promoted CSCB repair: %w", err)
	}
	if err := requireRowsAffected(result, 1, "complete atomically promoted CSCB repair"); err != nil {
		return nil, err
	}
	completed, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := loadRebuiltBlocks(ctx, tx, completed); err != nil {
		return nil, err
	}
	if err := validateRebuiltMetadata(completed, objectKey); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit atomic CSCB repair promotion: %w", err)
	}
	return completed, nil
}

func (r *PostgresRepository) CompleteRetainingOldObject(
	ctx context.Context,
	repairID int64,
	outcome string,
) (*Manifest, error) {
	if err := r.validateDB(); err != nil {
		return nil, err
	}
	if outcome == "" {
		return nil, xerrors.New("CSCB repair completion outcome is required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin CSCB repair completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	manifest, err := lockManifestForRepair(ctx, tx, repairID)
	if err != nil {
		return nil, err
	}
	if manifest.State == StateCompleted {
		if err := tx.Commit(); err != nil {
			return nil, xerrors.Errorf("failed to commit idempotent CSCB repair completion: %w", err)
		}
		return manifest, nil
	}
	if manifest.CanonicalBlockCount == 0 {
		if manifest.State != StateRestored {
			return nil, xerrors.Errorf("zero-canonical CSCB repair must be restored before completion: id=%d state=%s", manifest.ID, manifest.State)
		}
	} else if manifest.State != StateVerified {
		return nil, xerrors.Errorf("CSCB repair must be verified before completion: id=%d state=%s", manifest.ID, manifest.State)
	}
	if err := lockRepairRows(ctx, tx, manifest.Blocks); err != nil {
		return nil, err
	}
	if err := lockConsolidatedObjectKeys(
		ctx,
		tx,
		manifest.OldConsolidatedObjectKey,
		manifest.NewConsolidatedObjectKey,
	); err != nil {
		return nil, err
	}
	if err := loadRebuiltBlocks(ctx, tx, manifest); err != nil {
		return nil, err
	}
	if err := validateRebuiltMetadata(manifest, manifest.NewConsolidatedObjectKey); err != nil {
		return nil, err
	}
	if manifest.CanonicalBlockCount > 0 {
		var err error
		if allRepairRowsOnObject(manifest, manifest.NewConsolidatedObjectKey) {
			err = requireExactRepairedReferences(ctx, tx, manifest, manifest.NewConsolidatedObjectKey)
		} else {
			err = requireExactNewReferences(ctx, tx, manifest, manifest.NewConsolidatedObjectKey)
		}
		if err != nil {
			return nil, err
		}
	}
	if err := requireNoOldReferences(ctx, tx, manifest.OldConsolidatedObjectKey); err != nil {
		return nil, err
	}
	const update = `
		UPDATE cscb_repair_manifest
		SET state = $2,
			outcome = $3,
			completed_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $1 AND state = $4`
	result, err := tx.ExecContext(ctx, update, manifest.ID, StateCompleted, outcome, manifest.State)
	if err != nil {
		return nil, xerrors.Errorf("failed to complete CSCB repair: %w", err)
	}
	if err := requireRowsAffected(result, 1, "complete CSCB repair"); err != nil {
		return nil, err
	}
	completed, err := loadManifest(ctx, tx, manifest.ID, false)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit CSCB repair completion: %w", err)
	}
	return completed, nil
}

func (r *PostgresRepository) validateDB() error {
	if r == nil || r.db == nil {
		return xerrors.New("postgres db is required for CSCB repair")
	}
	return nil
}

func loadCandidateByKey(ctx context.Context, q queryer, tag uint32, objectKey string) (*Manifest, error) {
	const query = `
		SELECT
			bm.id,
			EXISTS (
				SELECT 1 FROM canonical_blocks cb
				WHERE cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
			) AS canonical,
			bm.tag,
			bm.height,
			COALESCE(bm.hash, ''),
			bm.skipped,
			(bm.single_block_retention_fenced_at IS NOT NULL),
			(retirement.block_metadata_id IS NOT NULL),
			COALESCE(bm.storage_generation, ''),
			COALESCE(shadow.single_block_storage_generation, ''),
			COALESCE(shadow.consolidated_storage_generation, ''),
			COALESCE(shadow.single_block_object_key_main, ''),
			(shadow.single_block_object_deleted_at IS NOT NULL),
			COALESCE(shadow.consolidated_object_key_main, ''),
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length
		FROM block_metadata bm
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		LEFT JOIN block_single_block_retention retirement ON retirement.block_metadata_id = bm.id
		WHERE bm.tag = $1
			AND bm.object_key_main = $2
			AND bm.object_key_main <> ''
			AND bm.object_format = $3
		ORDER BY bm.height ASC, bm.id ASC`
	rows, err := q.QueryContext(ctx, query, tag, objectKey, api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH)
	if err != nil {
		return nil, xerrors.Errorf("failed to query active CSCB repair rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	manifest := &Manifest{
		Tag:                      tag,
		OldConsolidatedObjectKey: objectKey,
		StartHeight:              ^uint64(0),
	}
	for rows.Next() {
		var block Block
		var oldOffset, oldLength, oldUncompressed sql.NullInt64
		if err := rows.Scan(
			&block.BlockMetadataID,
			&block.Canonical,
			&block.Tag,
			&block.Height,
			&block.Hash,
			&block.Skipped,
			&block.RetirementFenced,
			&block.RetirementManifestExists,
			&block.StorageGeneration,
			&block.SingleBlockStorageGeneration,
			&block.ConsolidatedStorageGeneration,
			&block.SingleBlockObjectKey,
			&block.SingleBlockObjectDeleted,
			&block.OldConsolidatedObjectKey,
			&oldOffset,
			&oldLength,
			&oldUncompressed,
		); err != nil {
			return nil, xerrors.Errorf("failed to scan active CSCB repair row: %w", err)
		}
		if oldOffset.Valid && oldOffset.Int64 >= 0 {
			block.OldByteOffset = uint64(oldOffset.Int64)
		}
		if oldLength.Valid && oldLength.Int64 >= 0 {
			block.OldByteLength = uint64(oldLength.Int64)
		}
		if oldUncompressed.Valid && oldUncompressed.Int64 >= 0 {
			block.OldUncompressedLength = uint64(oldUncompressed.Int64)
		}
		manifest.Blocks = append(manifest.Blocks, block)
		manifest.TotalBlockCount++
		if block.Canonical {
			manifest.CanonicalBlockCount++
		}
		if block.Height < manifest.StartHeight {
			manifest.StartHeight = block.Height
		}
		if block.Height == ^uint64(0) {
			return nil, xerrors.New("CSCB repair row height overflows exclusive end height")
		}
		if block.Height+1 > manifest.EndHeight {
			manifest.EndHeight = block.Height + 1
		}
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate active CSCB repair rows: %w", err)
	}
	if len(manifest.Blocks) == 0 {
		return nil, xerrors.Errorf("active CSCB repair object %q has no metadata references", objectKey)
	}
	return manifest, nil
}

func loadManifest(ctx context.Context, q queryer, repairID int64, forUpdate bool) (*Manifest, error) {
	suffix := ""
	if forUpdate {
		suffix = " FOR UPDATE"
	}
	query := fmt.Sprintf(`SELECT %s FROM cscb_repair_manifest WHERE id = $1%s`, manifestColumns, suffix)
	manifest, err := scanManifest(q.QueryRowContext(ctx, query, repairID))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, xerrors.Errorf("CSCB repair manifest not found: id=%d", repairID)
		}
		return nil, xerrors.Errorf("failed to load CSCB repair manifest: %w", err)
	}
	if err := loadManifestBlocks(ctx, q, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func scanManifest(row rowScanner) (*Manifest, error) {
	var manifest Manifest
	var state string
	var oldVersionID, oldETag sql.NullString
	var oldBytes sql.NullInt64
	var newKey, newVersionID, newETag sql.NullString
	var newBytes sql.NullInt64
	var restoredAt, verifiedAt, completedAt sql.NullTime
	if err := row.Scan(
		&manifest.ID,
		&manifest.Tag,
		&state,
		&manifest.Bucket,
		&manifest.OldConsolidatedObjectKey,
		&oldVersionID,
		&oldETag,
		&oldBytes,
		&manifest.StartHeight,
		&manifest.EndHeight,
		&manifest.CanonicalBlockCount,
		&manifest.TotalBlockCount,
		&manifest.RowSetSHA256,
		&newKey,
		&newVersionID,
		&newETag,
		&newBytes,
		&manifest.Outcome,
		&manifest.PreparedAt,
		&restoredAt,
		&verifiedAt,
		&completedAt,
	); err != nil {
		return nil, err
	}
	manifest.State = State(state)
	manifest.OldConsolidatedObjectVersion.VersionID = oldVersionID.String
	manifest.OldConsolidatedObjectVersion.ETag = oldETag.String
	if oldBytes.Valid && oldBytes.Int64 > 0 {
		manifest.OldConsolidatedObjectVersion.Bytes = uint64(oldBytes.Int64)
	}
	manifest.NewConsolidatedObjectKey = newKey.String
	manifest.NewConsolidatedObjectVersion.VersionID = newVersionID.String
	manifest.NewConsolidatedObjectVersion.ETag = newETag.String
	if newBytes.Valid && newBytes.Int64 > 0 {
		manifest.NewConsolidatedObjectVersion.Bytes = uint64(newBytes.Int64)
	}
	manifest.RestoredAt = nullableTime(restoredAt)
	manifest.VerifiedAt = nullableTime(verifiedAt)
	manifest.CompletedAt = nullableTime(completedAt)
	return &manifest, nil
}

func loadManifestBlocks(ctx context.Context, q queryer, manifest *Manifest) error {
	const query = `
		SELECT block.block_metadata_id, block.canonical, block.tag, block.height, COALESCE(block.hash, ''),
			COALESCE(bm.storage_generation, ''),
			COALESCE(shadow.single_block_storage_generation, ''),
			COALESCE(shadow.consolidated_storage_generation, ''),
			COALESCE(block.single_block_object_key_main, ''), block.single_block_object_key_sha256,
			COALESCE(block.single_block_object_version_id, ''),
			COALESCE(block.single_block_object_etag, ''), COALESCE(block.single_block_object_bytes, 0),
			COALESCE(block.payload_sha256, ''),
			block.old_byte_offset, block.old_byte_length, block.old_uncompressed_length
		FROM cscb_repair_block block
		JOIN block_metadata bm ON bm.id = block.block_metadata_id
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE block.repair_id = $1
		ORDER BY block.height ASC, block.block_metadata_id ASC`
	rows, err := q.QueryContext(ctx, query, manifest.ID)
	if err != nil {
		return xerrors.Errorf("failed to query CSCB repair blocks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	manifest.Blocks = nil
	for rows.Next() {
		var block Block
		block.OldConsolidatedObjectKey = manifest.OldConsolidatedObjectKey
		if err := rows.Scan(
			&block.BlockMetadataID,
			&block.Canonical,
			&block.Tag,
			&block.Height,
			&block.Hash,
			&block.StorageGeneration,
			&block.SingleBlockStorageGeneration,
			&block.ConsolidatedStorageGeneration,
			&block.SingleBlockObjectKey,
			&block.SingleBlockObjectKeySHA256,
			&block.SingleBlockObjectVersion.VersionID,
			&block.SingleBlockObjectVersion.ETag,
			&block.SingleBlockObjectVersion.Bytes,
			&block.PayloadSHA256,
			&block.OldByteOffset,
			&block.OldByteLength,
			&block.OldUncompressedLength,
		); err != nil {
			return xerrors.Errorf("failed to scan CSCB repair block: %w", err)
		}
		manifest.Blocks = append(manifest.Blocks, block)
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed to iterate CSCB repair blocks: %w", err)
	}
	if manifest.ID != 0 && uint64(len(manifest.Blocks)) != manifest.TotalBlockCount {
		return xerrors.Errorf(
			"CSCB repair manifest block count mismatch: id=%d expected=%d actual=%d",
			manifest.ID,
			manifest.TotalBlockCount,
			len(manifest.Blocks),
		)
	}
	return nil
}

func loadRebuiltBlocks(ctx context.Context, q queryer, manifest *Manifest) error {
	const query = `
		SELECT
			block.block_metadata_id,
			EXISTS (
				SELECT 1 FROM canonical_blocks cb
				WHERE cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
			) AS canonical,
			COALESCE(bm.object_key_main, ''),
			bm.object_format,
			COALESCE(bm.storage_generation, ''),
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length,
			COALESCE(shadow.consolidated_object_key_main, ''),
			COALESCE(shadow.single_block_storage_generation, ''),
			COALESCE(shadow.consolidated_storage_generation, ''),
			shadow.validated_at,
			shadow.single_block_retention_started_at,
			shadow.single_block_delete_after
		FROM cscb_repair_block block
		JOIN block_metadata bm ON bm.id = block.block_metadata_id
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE block.repair_id = $1
		ORDER BY block.height ASC, block.block_metadata_id ASC`
	rows, err := q.QueryContext(ctx, query, manifest.ID)
	if err != nil {
		return xerrors.Errorf("failed to query rebuilt CSCB repair rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	blocksByID := make(map[int64]*Block, len(manifest.Blocks))
	for i := range manifest.Blocks {
		blocksByID[manifest.Blocks[i].BlockMetadataID] = &manifest.Blocks[i]
	}
	seen := 0
	for rows.Next() {
		var id int64
		var canonical bool
		var activeOffset, activeLength, activeUncompressed sql.NullInt64
		var validatedAt, retentionStartedAt, deleteAfter sql.NullTime
		var activeKey, newKey string
		var storageGeneration, singleBlockStorageGeneration, consolidatedStorageGeneration string
		var objectFormat int32
		if err := rows.Scan(
			&id,
			&canonical,
			&activeKey,
			&objectFormat,
			&storageGeneration,
			&activeOffset,
			&activeLength,
			&activeUncompressed,
			&newKey,
			&singleBlockStorageGeneration,
			&consolidatedStorageGeneration,
			&validatedAt,
			&retentionStartedAt,
			&deleteAfter,
		); err != nil {
			return xerrors.Errorf("failed to scan rebuilt CSCB repair row: %w", err)
		}
		block, ok := blocksByID[id]
		if !ok {
			return xerrors.Errorf("rebuilt CSCB repair returned unexpected metadata_id=%d", id)
		}
		if canonical != block.Canonical {
			return xerrors.Errorf("canonical identity changed during CSCB repair for metadata_id=%d", id)
		}
		block.ActiveObjectKey = activeKey
		block.ActiveObjectFormat = objectFormat
		block.StorageGeneration = storageGeneration
		block.SingleBlockStorageGeneration = singleBlockStorageGeneration
		block.ConsolidatedStorageGeneration = consolidatedStorageGeneration
		block.NewConsolidatedObjectKey = newKey
		block.NewByteOffset = nullableUint64(activeOffset)
		block.NewByteLength = nullableUint64(activeLength)
		block.NewUncompressedLength = nullableUint64(activeUncompressed)
		block.NewValidatedAt = nullableTime(validatedAt)
		block.NewRetentionStartedAt = nullableTime(retentionStartedAt)
		block.NewSingleBlockDeleteAfter = nullableTime(deleteAfter)
		seen++
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed to iterate rebuilt CSCB repair rows: %w", err)
	}
	if uint64(seen) != manifest.TotalBlockCount {
		return xerrors.Errorf("rebuilt CSCB repair row count mismatch: expected=%d actual=%d", manifest.TotalBlockCount, seen)
	}
	return nil
}

func lockManifestForRepair(ctx context.Context, tx *sql.Tx, repairID int64) (*Manifest, error) {
	var tag uint32
	if err := tx.QueryRowContext(ctx, `SELECT tag FROM cscb_repair_manifest WHERE id = $1`, repairID).Scan(&tag); err != nil {
		if err == sql.ErrNoRows {
			return nil, xerrors.Errorf("CSCB repair manifest not found: id=%d", repairID)
		}
		return nil, xerrors.Errorf("failed to load CSCB repair tag: %w", err)
	}
	if err := lockRepairTag(ctx, tx, tag); err != nil {
		return nil, err
	}
	return loadManifest(ctx, tx, repairID, true)
}

func lockCandidateRows(ctx context.Context, tx *sql.Tx, blocks []Block) error {
	ordered := append([]Block(nil), blocks...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Tag != ordered[j].Tag {
			return ordered[i].Tag < ordered[j].Tag
		}
		if ordered[i].Height != ordered[j].Height {
			return ordered[i].Height < ordered[j].Height
		}
		if ordered[i].Hash != ordered[j].Hash {
			return ordered[i].Hash < ordered[j].Hash
		}
		return ordered[i].BlockMetadataID < ordered[j].BlockMetadataID
	})
	ids := make([]int64, 0, len(ordered))
	for _, block := range ordered {
		if err := retirementlock.Acquire(ctx, tx, block.Tag, block.Height, block.Hash); err != nil {
			return err
		}
		ids = append(ids, block.BlockMetadataID)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT bm.id
		FROM block_metadata bm
		JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE bm.id = ANY($1)
		ORDER BY bm.id
		FOR UPDATE OF bm, shadow`, pq.Array(ids))
	if err != nil {
		return xerrors.Errorf("failed to lock CSCB repair metadata rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed to iterate locked CSCB repair rows: %w", err)
	}
	if count != len(ids) {
		return xerrors.Errorf("CSCB repair lock count mismatch: expected=%d actual=%d", len(ids), count)
	}
	return nil
}

func lockConsolidatedObjectKey(ctx context.Context, tx *sql.Tx, objectKey string) error {
	if objectKey == "" {
		return xerrors.New("consolidated object key is required for CSCB repair lock")
	}
	if _, err := tx.ExecContext(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 1))`,
		objectKey,
	); err != nil {
		return xerrors.Errorf("failed to acquire CSCB repair object lock: %w", err)
	}
	return nil
}

func lockConsolidatedObjectKeys(ctx context.Context, tx *sql.Tx, objectKeys ...string) error {
	unique := make(map[string]struct{}, len(objectKeys))
	ordered := make([]string, 0, len(objectKeys))
	for _, objectKey := range objectKeys {
		if objectKey == "" {
			continue
		}
		if _, ok := unique[objectKey]; ok {
			continue
		}
		unique[objectKey] = struct{}{}
		ordered = append(ordered, objectKey)
	}
	if len(ordered) == 0 {
		return xerrors.New("at least one consolidated object key is required for CSCB repair locks")
	}
	sort.Strings(ordered)
	for _, objectKey := range ordered {
		if err := lockConsolidatedObjectKey(ctx, tx, objectKey); err != nil {
			return err
		}
	}
	return nil
}

func lockRepairTag(ctx context.Context, tx *sql.Tx, tag uint32) error {
	return cscbrepairlock.AcquireTag(ctx, tx, tag)
}

func lockRepairRows(ctx context.Context, tx *sql.Tx, blocks []Block) error {
	ordered := append([]Block(nil), blocks...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Tag != ordered[j].Tag {
			return ordered[i].Tag < ordered[j].Tag
		}
		if ordered[i].Height != ordered[j].Height {
			return ordered[i].Height < ordered[j].Height
		}
		if ordered[i].Hash != ordered[j].Hash {
			return ordered[i].Hash < ordered[j].Hash
		}
		return ordered[i].BlockMetadataID < ordered[j].BlockMetadataID
	})
	ids := make([]int64, 0, len(ordered))
	for _, block := range ordered {
		if err := retirementlock.Acquire(ctx, tx, block.Tag, block.Height, block.Hash); err != nil {
			return err
		}
		ids = append(ids, block.BlockMetadataID)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM block_metadata
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return xerrors.Errorf("failed to lock rebuilt block metadata: %w", err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return xerrors.Errorf("failed to iterate locked rebuilt block metadata: %w", err)
	}
	if err := rows.Close(); err != nil {
		return xerrors.Errorf("failed to close rebuilt block metadata rows: %w", err)
	}
	if count != len(ids) {
		return xerrors.Errorf("rebuilt metadata lock count mismatch: expected=%d actual=%d", len(ids), count)
	}
	shadowRows, err := tx.QueryContext(ctx, `
		SELECT block_metadata_id FROM block_consolidation_shadow
		WHERE block_metadata_id = ANY($1)
		ORDER BY block_metadata_id
		FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return xerrors.Errorf("failed to lock rebuilt consolidation shadows: %w", err)
	}
	defer func() { _ = shadowRows.Close() }()
	for shadowRows.Next() {
	}
	return shadowRows.Err()
}

func validateRepairPromotionRow(
	ctx context.Context,
	tx *sql.Tx,
	manifest *Manifest,
	block Block,
	newObjectKey string,
	placement RebuiltPlacement,
) error {
	const query = `
		SELECT
			EXISTS (
				SELECT 1 FROM canonical_blocks cb
				WHERE cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
			),
			COALESCE(bm.object_key_main, ''),
			bm.object_format,
			COALESCE(bm.storage_generation, ''),
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length,
			bm.skipped,
			(bm.single_block_retention_fenced_at IS NOT NULL),
			EXISTS (
				SELECT 1 FROM block_single_block_retention retirement
				WHERE retirement.block_metadata_id = bm.id
			),
			(shadow.block_metadata_id IS NOT NULL),
			COALESCE(shadow.single_block_object_key_main, ''),
			COALESCE(shadow.consolidated_object_key_main, ''),
			shadow.object_format,
			COALESCE(shadow.single_block_storage_generation, ''),
			COALESCE(shadow.consolidated_storage_generation, ''),
			shadow.byte_offset,
			shadow.byte_length,
			shadow.uncompressed_length,
			(shadow.single_block_object_deleted_at IS NOT NULL)
		FROM block_metadata bm
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE bm.id = $1
			AND bm.tag = $2
			AND bm.height = $3
			AND bm.hash IS NOT DISTINCT FROM NULLIF($4, '')`
	var canonical, skipped, fenced, retirementManifest, shadowExists, singleBlockDeleted bool
	var activeKey, shadowSingleKey, shadowConsolidatedKey string
	var storageGeneration, singleBlockStorageGeneration, consolidatedStorageGeneration string
	var activeFormat int32
	var activeOffset, activeLength, activeUncompressed sql.NullInt64
	var shadowFormat, shadowOffset, shadowLength, shadowUncompressed sql.NullInt64
	if err := tx.QueryRowContext(
		ctx,
		query,
		block.BlockMetadataID,
		block.Tag,
		block.Height,
		block.Hash,
	).Scan(
		&canonical,
		&activeKey,
		&activeFormat,
		&storageGeneration,
		&activeOffset,
		&activeLength,
		&activeUncompressed,
		&skipped,
		&fenced,
		&retirementManifest,
		&shadowExists,
		&shadowSingleKey,
		&shadowConsolidatedKey,
		&shadowFormat,
		&singleBlockStorageGeneration,
		&consolidatedStorageGeneration,
		&shadowOffset,
		&shadowLength,
		&shadowUncompressed,
		&singleBlockDeleted,
	); err != nil {
		if err == sql.ErrNoRows {
			return xerrors.Errorf("CSCB repair metadata identity changed for metadata_id=%d", block.BlockMetadataID)
		}
		return xerrors.Errorf("failed to validate CSCB repair promotion row metadata_id=%d: %w", block.BlockMetadataID, err)
	}
	if canonical != block.Canonical || skipped || fenced || retirementManifest || singleBlockDeleted {
		return xerrors.Errorf("CSCB repair promotion row is blocked or changed for metadata_id=%d", block.BlockMetadataID)
	}
	if storageGeneration != "" || singleBlockStorageGeneration != "" || consolidatedStorageGeneration != "" {
		return xerrors.Errorf(
			"CSCB repair requires legacy storage generation for metadata_id=%d: primary=%q single_block_shadow=%q consolidated_shadow=%q",
			block.BlockMetadataID,
			storageGeneration,
			singleBlockStorageGeneration,
			consolidatedStorageGeneration,
		)
	}
	oldLayout := activeKey == manifest.OldConsolidatedObjectKey &&
		activeFormat == int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) &&
		nullableUint64(activeOffset) == block.OldByteOffset &&
		nullableUint64(activeLength) == block.OldByteLength &&
		nullableUint64(activeUncompressed) == block.OldUncompressedLength
	singleLayout := activeKey == block.SingleBlockObjectKey &&
		activeFormat == int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK) &&
		!activeOffset.Valid && !activeLength.Valid && !activeUncompressed.Valid
	newLayout := activeKey == newObjectKey &&
		activeFormat == int32(placement.ObjectFormat) &&
		nullableUint64(activeOffset) == placement.ByteOffset &&
		nullableUint64(activeLength) == placement.ByteLength &&
		nullableUint64(activeUncompressed) == placement.UncompressedLength
	if !oldLayout && !singleLayout && !newLayout {
		return xerrors.Errorf("CSCB repair active placement changed for metadata_id=%d", block.BlockMetadataID)
	}
	if !shadowExists {
		if !singleLayout {
			return xerrors.Errorf("CSCB repair shadow is missing for metadata_id=%d", block.BlockMetadataID)
		}
		return nil
	}
	if shadowSingleKey != block.SingleBlockObjectKey || !shadowFormat.Valid ||
		shadowFormat.Int64 != int64(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) {
		return xerrors.Errorf("CSCB repair shadow identity changed for metadata_id=%d", block.BlockMetadataID)
	}
	oldShadow := shadowConsolidatedKey == manifest.OldConsolidatedObjectKey &&
		nullableUint64(shadowOffset) == block.OldByteOffset &&
		nullableUint64(shadowLength) == block.OldByteLength &&
		nullableUint64(shadowUncompressed) == block.OldUncompressedLength
	newShadow := shadowConsolidatedKey == newObjectKey &&
		nullableUint64(shadowOffset) == placement.ByteOffset &&
		nullableUint64(shadowLength) == placement.ByteLength &&
		nullableUint64(shadowUncompressed) == placement.UncompressedLength
	if !oldShadow && !newShadow {
		return xerrors.Errorf("CSCB repair shadow placement changed for metadata_id=%d", block.BlockMetadataID)
	}
	return nil
}

func compareCandidateRows(current *Manifest, pinned *Manifest) error {
	if current == nil || pinned == nil {
		return xerrors.New("CSCB repair candidate is required")
	}
	if current.Tag != pinned.Tag ||
		current.OldConsolidatedObjectKey != pinned.OldConsolidatedObjectKey ||
		current.StartHeight != pinned.StartHeight ||
		current.EndHeight != pinned.EndHeight ||
		current.CanonicalBlockCount != pinned.CanonicalBlockCount ||
		current.TotalBlockCount != pinned.TotalBlockCount ||
		len(current.Blocks) != len(pinned.Blocks) {
		return xerrors.Errorf("active CSCB repair candidate changed for old object %q", pinned.OldConsolidatedObjectKey)
	}
	for i := range pinned.Blocks {
		a := current.Blocks[i]
		b := pinned.Blocks[i]
		if a.BlockMetadataID != b.BlockMetadataID ||
			a.Canonical != b.Canonical ||
			a.Tag != b.Tag ||
			a.Height != b.Height ||
			a.Hash != b.Hash ||
			a.Skipped ||
			a.RetirementFenced ||
			a.RetirementManifestExists ||
			a.StorageGeneration != b.StorageGeneration ||
			a.SingleBlockStorageGeneration != b.SingleBlockStorageGeneration ||
			a.ConsolidatedStorageGeneration != b.ConsolidatedStorageGeneration ||
			a.SingleBlockObjectKey != b.SingleBlockObjectKey ||
			a.SingleBlockObjectDeleted ||
			a.OldConsolidatedObjectKey != b.OldConsolidatedObjectKey ||
			a.OldByteOffset != b.OldByteOffset ||
			a.OldByteLength != b.OldByteLength ||
			a.OldUncompressedLength != b.OldUncompressedLength {
			return xerrors.Errorf("active CSCB repair row changed for metadata_id=%d", b.BlockMetadataID)
		}
	}
	return nil
}

func validateRebuiltMetadata(manifest *Manifest, objectKey string) error {
	if err := validateLegacyStorageGenerations(manifest.Blocks); err != nil {
		return err
	}
	if manifest.RestoredAt == nil {
		return xerrors.Errorf("CSCB repair restore timestamp is missing: id=%d", manifest.ID)
	}
	canonicalCount := uint64(0)
	for _, block := range manifest.Blocks {
		if block.Canonical {
			canonicalCount++
		}
		if block.ActiveObjectKey != objectKey {
			if block.Canonical {
				return xerrors.Errorf("canonical row is not on the rebuilt CSCB: metadata_id=%d", block.BlockMetadataID)
			}
			if block.ActiveObjectKey != block.SingleBlockObjectKey ||
				block.ActiveObjectFormat != int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK) ||
				block.NewConsolidatedObjectKey != "" ||
				block.NewByteOffset != 0 ||
				block.NewByteLength != 0 ||
				block.NewUncompressedLength != 0 ||
				block.NewValidatedAt != nil ||
				block.NewRetentionStartedAt != nil ||
				block.NewSingleBlockDeleteAfter != nil {
				return xerrors.Errorf("non-canonical row did not remain on single-block storage: metadata_id=%d", block.BlockMetadataID)
			}
			continue
		}
		if block.NewConsolidatedObjectKey != objectKey ||
			block.ActiveObjectFormat != int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) ||
			block.NewByteLength == 0 ||
			block.NewUncompressedLength == 0 ||
			block.NewValidatedAt == nil ||
			block.NewRetentionStartedAt == nil ||
			block.NewSingleBlockDeleteAfter == nil ||
			block.NewRetentionStartedAt.Before(*manifest.RestoredAt) ||
			!block.NewSingleBlockDeleteAfter.After(*block.NewRetentionStartedAt) {
			return xerrors.Errorf("canonical row is not on a freshly promoted CSCB: metadata_id=%d", block.BlockMetadataID)
		}
		if block.NewSingleBlockDeleteAfter.Before(block.NewRetentionStartedAt.Add(72 * time.Hour)) {
			return xerrors.Errorf(
				"canonical row does not have the required 72-hour single-block retention: metadata_id=%d",
				block.BlockMetadataID,
			)
		}
	}
	if canonicalCount != manifest.CanonicalBlockCount {
		return xerrors.Errorf("rebuilt canonical block count mismatch: expected=%d actual=%d", manifest.CanonicalBlockCount, canonicalCount)
	}
	if canonicalCount == 0 {
		if objectKey != "" {
			return xerrors.Errorf("zero-canonical CSCB repair unexpectedly produced object %q", objectKey)
		}
		return nil
	}
	if objectKey == "" {
		return xerrors.New("rebuilt CSCB object key is missing")
	}
	if objectKey == manifest.OldConsolidatedObjectKey {
		return xerrors.Errorf("rebuilt CSCB reused dirty object key %q", objectKey)
	}
	return nil
}

func requireNoUnexpectedOldReferences(ctx context.Context, q queryer, objectKey string, blocks []Block) error {
	ids := make([]int64, 0, len(blocks))
	for _, block := range blocks {
		ids = append(ids, block.BlockMetadataID)
	}
	const query = `
		SELECT
			(SELECT COUNT(*)
			 FROM block_metadata
			 WHERE object_key_main = $1
				AND object_key_main <> ''
				AND NOT (id = ANY($2)))
			+
			(SELECT COUNT(*)
			 FROM block_consolidation_shadow
			 WHERE consolidated_object_key_main = $1
				AND consolidated_object_key_main <> ''
				AND NOT (block_metadata_id = ANY($2)))`
	var references uint64
	if err := q.QueryRowContext(ctx, query, objectKey, pq.Array(ids)).Scan(&references); err != nil {
		return xerrors.Errorf("failed to count unexpected dirty CSCB references: %w", err)
	}
	if references != 0 {
		return xerrors.Errorf("dirty CSCB %q has %d references outside the pinned row set", objectKey, references)
	}
	return nil
}

func requireExactNewReferences(ctx context.Context, q queryer, manifest *Manifest, objectKey string) error {
	ids := make([]int64, 0, manifest.CanonicalBlockCount)
	for _, block := range manifest.Blocks {
		if block.Canonical {
			ids = append(ids, block.BlockMetadataID)
		}
	}
	if len(ids) == 0 || uint64(len(ids)) != manifest.CanonicalBlockCount {
		return xerrors.Errorf(
			"rebuilt CSCB expected canonical reference count mismatch: expected=%d actual=%d",
			manifest.CanonicalBlockCount,
			len(ids),
		)
	}
	const query = `
		SELECT
			(SELECT COUNT(*)
			 FROM block_metadata
			 WHERE object_key_main = $1
				AND object_key_main <> ''
				AND NOT (id = ANY($2)))
			+
			(SELECT COUNT(*)
			 FROM block_consolidation_shadow
			 WHERE consolidated_object_key_main = $1
				AND consolidated_object_key_main <> ''
				AND NOT (block_metadata_id = ANY($2)))`
	var references uint64
	if err := q.QueryRowContext(ctx, query, objectKey, pq.Array(ids)).Scan(&references); err != nil {
		return xerrors.Errorf("failed to count unexpected rebuilt CSCB references: %w", err)
	}
	if references != 0 {
		return xerrors.Errorf("rebuilt CSCB %q has %d references outside the pinned canonical row set", objectKey, references)
	}
	return nil
}

func requireExactRepairedReferences(ctx context.Context, q queryer, manifest *Manifest, objectKey string) error {
	ids := make([]int64, 0, len(manifest.Blocks))
	for _, block := range manifest.Blocks {
		ids = append(ids, block.BlockMetadataID)
	}
	if len(ids) == 0 || len(ids) != len(manifest.Blocks) {
		return xerrors.Errorf(
			"rebuilt CSCB expected reference count mismatch: expected=%d actual=%d",
			len(manifest.Blocks),
			len(ids),
		)
	}
	const query = `
		SELECT
			(SELECT COUNT(*)
			 FROM block_metadata
			 WHERE object_key_main = $1
				AND object_key_main <> ''
				AND storage_generation IS NULL
				AND id = ANY($2))
			+
			(SELECT COUNT(*)
			 FROM block_consolidation_shadow
			 WHERE consolidated_object_key_main = $1
				AND consolidated_object_key_main <> ''
				AND single_block_storage_generation IS NULL
				AND consolidated_storage_generation IS NULL
				AND block_metadata_id = ANY($2)),
			(SELECT COUNT(*)
			 FROM block_metadata
			 WHERE object_key_main = $1
				AND object_key_main <> ''
				AND NOT (id = ANY($2)))
			+
			(SELECT COUNT(*)
			 FROM block_consolidation_shadow
			 WHERE consolidated_object_key_main = $1
				AND consolidated_object_key_main <> ''
				AND NOT (block_metadata_id = ANY($2)))`
	var expectedReferences, unexpectedReferences uint64
	if err := q.QueryRowContext(ctx, query, objectKey, pq.Array(ids)).Scan(&expectedReferences, &unexpectedReferences); err != nil {
		return xerrors.Errorf("failed to count rebuilt CSCB references: %w", err)
	}
	want := uint64(len(ids) * 2)
	if expectedReferences != want || unexpectedReferences != 0 {
		return xerrors.Errorf(
			"rebuilt CSCB %q reference mismatch: expected=%d actual=%d unexpected=%d",
			objectKey,
			want,
			expectedReferences,
			unexpectedReferences,
		)
	}
	return nil
}

func allRepairRowsOnObject(manifest *Manifest, objectKey string) bool {
	if manifest == nil || objectKey == "" || len(manifest.Blocks) == 0 {
		return false
	}
	for _, block := range manifest.Blocks {
		if block.ActiveObjectKey != objectKey || block.NewConsolidatedObjectKey != objectKey {
			return false
		}
	}
	return true
}

func requireNoOldReferences(ctx context.Context, q queryer, objectKey string) error {
	const query = `
		SELECT
			(SELECT COUNT(*)
			 FROM block_metadata
			 WHERE object_key_main = $1 AND object_key_main <> '')
			+
			(SELECT COUNT(*)
			 FROM block_consolidation_shadow
			 WHERE consolidated_object_key_main = $1 AND consolidated_object_key_main <> '')`
	var references uint64
	if err := q.QueryRowContext(ctx, query, objectKey).Scan(&references); err != nil {
		return xerrors.Errorf("failed to count old CSCB references: %w", err)
	}
	if references != 0 {
		return xerrors.Errorf("old CSCB %q still has %d database references", objectKey, references)
	}
	return nil
}

func validateFencedInput(manifest *Manifest) error {
	if manifest == nil || manifest.Tag == 0 || manifest.Bucket == "" ||
		manifest.State != StatePreparing ||
		manifest.OldConsolidatedObjectKey == "" ||
		manifest.EndHeight <= manifest.StartHeight ||
		manifest.TotalBlockCount != uint64(len(manifest.Blocks)) ||
		len(manifest.RowSetSHA256) != 64 || manifest.RowSetSHA256 != rowSetSHA256(manifest) {
		return xerrors.New("complete CSCB repair fence is required")
	}
	if err := validateLegacyStorageGenerations(manifest.Blocks); err != nil {
		return err
	}
	canonicalCount := uint64(0)
	for _, block := range manifest.Blocks {
		if block.BlockMetadataID == 0 || block.Tag != manifest.Tag || block.SingleBlockObjectKey == "" ||
			len(block.SingleBlockObjectKeySHA256) != 64 ||
			block.SingleBlockObjectKeySHA256 != objectKeySHA256(block.SingleBlockObjectKey) ||
			block.OldConsolidatedObjectKey != manifest.OldConsolidatedObjectKey ||
			block.OldByteLength == 0 || block.OldUncompressedLength == 0 {
			return xerrors.Errorf("incomplete CSCB repair fence block metadata_id=%d", block.BlockMetadataID)
		}
		if block.Canonical {
			canonicalCount++
		}
	}
	if canonicalCount != manifest.CanonicalBlockCount {
		return xerrors.Errorf("CSCB repair fence canonical count mismatch: expected=%d actual=%d", manifest.CanonicalBlockCount, canonicalCount)
	}
	return nil
}

func validateInspectionInput(oldObject ObjectVersion, blocks []Block) error {
	if !immutableVersionID(oldObject.VersionID) || oldObject.ETag == "" || oldObject.Bytes == 0 || len(blocks) == 0 {
		return xerrors.New("complete CSCB repair object inspection is required")
	}
	for _, block := range blocks {
		if block.BlockMetadataID == 0 || block.SingleBlockObjectKey == "" ||
			block.SingleBlockObjectKeySHA256 != objectKeySHA256(block.SingleBlockObjectKey) ||
			!immutableVersionID(block.SingleBlockObjectVersion.VersionID) ||
			block.SingleBlockObjectVersion.ETag == "" || block.SingleBlockObjectVersion.Bytes == 0 ||
			len(block.PayloadSHA256) != 64 {
			return xerrors.Errorf("incomplete CSCB repair inspection block metadata_id=%d", block.BlockMetadataID)
		}
	}
	return nil
}

func sameFencedManifest(actual *Manifest, expected *Manifest) bool {
	if actual == nil || expected == nil ||
		actual.Tag != expected.Tag ||
		actual.State != StatePreparing ||
		actual.Bucket != expected.Bucket ||
		actual.OldConsolidatedObjectKey != expected.OldConsolidatedObjectKey ||
		actual.StartHeight != expected.StartHeight ||
		actual.EndHeight != expected.EndHeight ||
		actual.CanonicalBlockCount != expected.CanonicalBlockCount ||
		actual.TotalBlockCount != expected.TotalBlockCount ||
		actual.RowSetSHA256 != expected.RowSetSHA256 ||
		len(actual.Blocks) != len(expected.Blocks) {
		return false
	}
	for i := range actual.Blocks {
		a := actual.Blocks[i]
		b := expected.Blocks[i]
		if a.BlockMetadataID != b.BlockMetadataID || a.Canonical != b.Canonical ||
			a.Tag != b.Tag || a.Height != b.Height || a.Hash != b.Hash ||
			a.SingleBlockObjectKey != b.SingleBlockObjectKey ||
			a.SingleBlockObjectKeySHA256 != b.SingleBlockObjectKeySHA256 ||
			a.OldByteOffset != b.OldByteOffset || a.OldByteLength != b.OldByteLength ||
			a.OldUncompressedLength != b.OldUncompressedLength {
			return false
		}
	}
	return true
}

func sameInspection(actual *Manifest, oldObject ObjectVersion, blocks []Block, alreadyClean bool) bool {
	expectedState := StatePrepared
	expectedOutcome := ""
	if alreadyClean {
		expectedState = StateCompleted
		expectedOutcome = alreadyCleanOutcome
	}
	if actual == nil || actual.State != expectedState || actual.Outcome != expectedOutcome ||
		actual.OldConsolidatedObjectVersion != oldObject || len(actual.Blocks) != len(blocks) {
		return false
	}
	for i := range blocks {
		a := actual.Blocks[i]
		b := blocks[i]
		if a.BlockMetadataID != b.BlockMetadataID ||
			a.SingleBlockObjectKey != b.SingleBlockObjectKey ||
			a.SingleBlockObjectKeySHA256 != b.SingleBlockObjectKeySHA256 ||
			a.SingleBlockObjectVersion != b.SingleBlockObjectVersion ||
			a.PayloadSHA256 != b.PayloadSHA256 {
			return false
		}
	}
	return true
}

func requireRowsAffected(result sql.Result, expected uint64, action string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to inspect %s result: %w", action, err)
	}
	if uint64(rows) != expected {
		return xerrors.Errorf("%s guard failed: expected=%d actual=%d", action, expected, rows)
	}
	return nil
}

func nullableUint64(value sql.NullInt64) uint64 {
	if !value.Valid || value.Int64 < 0 {
		return 0
	}
	return uint64(value.Int64)
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
