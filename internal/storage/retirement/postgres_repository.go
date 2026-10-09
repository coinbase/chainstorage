package retirement

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/retirementlock"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const metadataRowColumns = `
	bm.id,
	(cb.block_metadata_id IS NOT NULL) AS is_canonical,
	bm.tag,
	bm.height,
	COALESCE(bm.hash, ''),
	bm.skipped,
	COALESCE(bm.object_key_main, ''),
	bm.object_format,
	bm.storage_generation,
	bm.byte_offset,
	bm.byte_length,
	bm.uncompressed_length,
	bm.single_block_retention_fenced_at,
	shadow.block_metadata_id,
	shadow.tag,
	shadow.height,
	shadow.hash,
	shadow.single_block_object_key_main,
	shadow.single_block_storage_generation,
	shadow.consolidated_object_key_main,
	shadow.consolidated_storage_generation,
	shadow.object_format,
	shadow.byte_offset,
	shadow.byte_length,
	shadow.uncompressed_length,
	shadow.validated_at,
	shadow.single_block_retention_started_at,
	shadow.single_block_delete_after,
	shadow.single_block_object_deleted_at,
	shadow.format_version,
	retirement.block_metadata_id,
	retirement.state,
	retirement.bucket,
	retirement.single_block_object_key_main,
	retirement.single_block_object_key_sha256,
	retirement.single_block_object_version_ids,
	retirement.single_block_delete_marker_version_ids,
	retirement.single_block_object_etag,
	retirement.single_block_object_bytes,
	retirement.consolidated_object_key_main,
	retirement.consolidated_object_version_id,
	retirement.consolidated_object_etag,
	retirement.consolidated_byte_offset,
	retirement.consolidated_byte_length,
	retirement.consolidated_uncompressed_length,
	retirement.payload_sha256,
	retirement.outcome,
	retirement.attempt_count,
	retirement.claim_token,
	retirement.claim_expires_at,
	retirement.prepared_at,
	retirement.delete_started_at,
	retirement.last_attempt_at,
	retirement.deleted_at,
	retirement.verified_at`

// pendingRetirementStatesSQL renders the pending retirement states as SQL
// literals.
//
// idx_block_single_block_retention_pending is a partial index whose predicate
// lists these exact states. Postgres can only use it when the query supplies
// the states as literals: it cannot prove that a parameterized `state IN
// ($n, ...)` implies the index predicate, so bind parameters silently degrade
// every pending lookup to a full scan of the manifest table. That table only
// grows (finalized manifests are retained as audit records), so the regression
// scales with history — it timed out retention activities in production once
// the table reached ~520k rows (INF-1330).
var pendingRetirementStatesSQL = renderRetirementStateLiterals(
	RetirementStateEligible,
	RetirementStateDeleting,
	RetirementStateDeletedPendingVerification,
)

var retirementStateLiteralPattern = regexp.MustCompile(`^[a-z][a-z_]*$`)

// renderRetirementStateLiterals builds the quoted, comma-separated literal list
// embedded in the queries above. The states are compile-time constants and are
// never caller input; the pattern guard exists so that a future state name
// needing escaping fails loudly at startup instead of producing malformed SQL.
func renderRetirementStateLiterals(states ...string) string {
	quoted := make([]string, 0, len(states))
	for _, state := range states {
		if !retirementStateLiteralPattern.MatchString(state) {
			panic(fmt.Sprintf("retirement state %q cannot be inlined as a SQL literal", state))
		}
		quoted = append(quoted, "'"+state+"'")
	}
	return strings.Join(quoted, ", ")
}

const metadataRowFrom = `
	FROM canonical_blocks cb
	JOIN block_metadata bm ON bm.id = cb.block_metadata_id AND bm.tag = cb.tag AND bm.height = cb.height
	LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
	LEFT JOIN block_single_block_retention retirement ON retirement.block_metadata_id = bm.id`

const metadataRowByIDFrom = `
	FROM block_metadata bm
	LEFT JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id AND cb.tag = bm.tag AND cb.height = bm.height
	LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
	LEFT JOIN block_single_block_retention retirement ON retirement.block_metadata_id = bm.id`

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error) {
	if r.db == nil {
		return nil, xerrors.New("postgres db is required")
	}
	if endHeight <= startHeight {
		return nil, nil
	}
	if limit == 0 {
		limit = endHeight - startHeight
	}

	query := fmt.Sprintf(`
		SELECT %s
		%s
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
		ORDER BY cb.height ASC
		LIMIT $4`, metadataRowColumns, metadataRowFrom)
	rows, err := r.db.QueryContext(ctx, query, tag, startHeight, endHeight, limit)
	if err != nil {
		return nil, xerrors.Errorf("failed to query retirement metadata rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make([]MetadataRow, 0)
	for rows.Next() {
		row, err := scanMetadataRow(rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan retirement metadata row: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate retirement metadata rows: %w", err)
	}
	return result, nil
}

func (r *PostgresRepository) GetMetadataRow(ctx context.Context, blockMetadataID int64) (MetadataRow, error) {
	if r.db == nil {
		return MetadataRow{}, xerrors.New("postgres db is required")
	}
	query := fmt.Sprintf(`
		SELECT %s
		%s
		WHERE bm.id = $1`, metadataRowColumns, metadataRowByIDFrom)
	row, err := scanMetadataRow(r.db.QueryRowContext(ctx, query, blockMetadataID))
	if err != nil {
		if err == sql.ErrNoRows {
			return MetadataRow{}, xerrors.Errorf("retirement metadata row not found: block_metadata_id=%d", blockMetadataID)
		}
		return MetadataRow{}, xerrors.Errorf("failed to get retirement metadata row: %w", err)
	}
	return row, nil
}

func (r *PostgresRepository) PrepareRetirement(
	ctx context.Context,
	manifest RetirementManifest,
	expectedStorageGeneration string,
) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if !isValidStorageGeneration(expectedStorageGeneration) {
		return xerrors.Errorf("unsupported expected storage generation %q", expectedStorageGeneration)
	}
	if manifest.State != RetirementStateEligible {
		return xerrors.Errorf("prepared retirement must be eligible, got %q", manifest.State)
	}
	if err := validatePreparedManifest(manifest); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin retirement preparation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := retirementlock.Acquire(ctx, tx, manifest.Tag, manifest.Height, manifest.Hash); err != nil {
		return err
	}

	const lockMetadata = `
		SELECT bm.id, CURRENT_TIMESTAMP
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id AND bm.tag = cb.tag AND bm.height = cb.height
		JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		WHERE bm.id = $1
			AND shadow.validated_at IS NOT NULL
			AND shadow.single_block_retention_started_at IS NOT NULL
			AND shadow.single_block_delete_after IS NOT NULL
			AND shadow.single_block_delete_after <= CURRENT_TIMESTAMP
			AND shadow.single_block_object_deleted_at IS NULL
			AND NOT EXISTS (
				SELECT 1
				FROM cscb_repair_block repair_block
				JOIN cscb_repair_manifest repair ON repair.id = repair_block.repair_id
				WHERE repair_block.block_metadata_id = bm.id
					AND repair.state <> 'completed'
			)
		FOR UPDATE OF cb, bm, shadow`
	var lockedID int64
	var databasePreparedAt time.Time
	if err := tx.QueryRowContext(ctx, lockMetadata, manifest.BlockMetadataID).Scan(&lockedID, &databasePreparedAt); err != nil {
		return xerrors.Errorf("failed to lock canonical retirement metadata: %w", err)
	}
	query := fmt.Sprintf(`
		SELECT %s
		%s
		WHERE bm.id = $1`, metadataRowColumns, metadataRowFrom)
	row, err := scanMetadataRow(tx.QueryRowContext(ctx, query, lockedID))
	if err != nil {
		return xerrors.Errorf("failed to lock retirement metadata row: %w", err)
	}
	if err := validateManifestMetadata(row, manifest, expectedStorageGeneration); err != nil {
		return err
	}
	const fenceMetadata = `
		UPDATE block_metadata
		SET single_block_retention_fenced_at = COALESCE(single_block_retention_fenced_at, $2)
		WHERE id = $1
			AND object_key_main = $3
			AND object_format = $4
			AND byte_offset = $5
			AND byte_length = $6
			AND uncompressed_length = $7
			AND storage_generation IS NOT DISTINCT FROM NULLIF($8, '')`
	result, err := tx.ExecContext(
		ctx,
		fenceMetadata,
		manifest.BlockMetadataID,
		databasePreparedAt,
		manifest.ConsolidatedObjectKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		manifest.ConsolidatedByteOffset,
		manifest.ConsolidatedByteLength,
		manifest.ConsolidatedUncompressedLength,
		expectedStorageGeneration,
	)
	if err != nil {
		return xerrors.Errorf("failed to fence retired block metadata: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return xerrors.Errorf("retired block metadata fence guard failed: block_metadata_id=%d rows=%d err=%v", manifest.BlockMetadataID, rows, err)
	}

	const insert = `
		INSERT INTO block_single_block_retention (
			block_metadata_id, tag, height, hash, state, bucket,
			single_block_object_key_main, single_block_object_key_sha256, single_block_object_version_ids,
			single_block_delete_marker_version_ids,
			single_block_object_etag, single_block_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, outcome, prepared_at, updated_at
		)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $21)
		ON CONFLICT (block_metadata_id) DO NOTHING`
	deleteMarkerVersionIDs := manifest.SingleBlockDeleteMarkerVersionIDs
	if deleteMarkerVersionIDs == nil {
		deleteMarkerVersionIDs = []string{}
	}
	if _, err := tx.ExecContext(ctx, insert,
		manifest.BlockMetadataID,
		manifest.Tag,
		manifest.Height,
		manifest.Hash,
		manifest.State,
		manifest.Bucket,
		manifest.SingleBlockObjectKey,
		manifest.SingleBlockObjectKeySHA256,
		pq.Array(manifest.SingleBlockObjectVersionIDs),
		pq.Array(deleteMarkerVersionIDs),
		manifest.SingleBlockObjectETag,
		manifest.SingleBlockObjectBytes,
		manifest.ConsolidatedObjectKey,
		manifest.ConsolidatedObjectVersionID,
		manifest.ConsolidatedObjectETag,
		manifest.ConsolidatedByteOffset,
		manifest.ConsolidatedByteLength,
		manifest.ConsolidatedUncompressedLength,
		manifest.PayloadSHA256,
		manifest.Outcome,
		databasePreparedAt,
	); err != nil {
		return xerrors.Errorf("failed to persist retirement manifest: %w", err)
	}

	existing, err := getManifestForUpdate(ctx, tx, manifest.BlockMetadataID)
	if err != nil {
		return err
	}
	if !samePreparedManifest(existing, manifest) {
		return xerrors.Errorf("retirement manifest conflict: block_metadata_id=%d state=%s", manifest.BlockMetadataID, existing.State)
	}
	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("failed to commit retirement manifest: %w", err)
	}
	return nil
}

func (r *PostgresRepository) ObserveRetentionSafety(
	ctx context.Context,
	bucket string,
	consolidatedObjectKey string,
	configurationSHA256 string,
) (time.Time, time.Time, error) {
	if r.db == nil {
		return time.Time{}, time.Time{}, xerrors.New("postgres db is required")
	}
	if bucket == "" || consolidatedObjectKey == "" || !isSHA256Hex(configurationSHA256) {
		return time.Time{}, time.Time{}, xerrors.New("valid retirement safety observation is required")
	}
	const query = `
		INSERT INTO cscb_retirement_safety_observation (
			bucket, consolidated_object_key_main, configuration_sha256,
			first_observed_at, last_observed_at
		)
		SELECT $1, $2, $3, database_now, database_now
		FROM (SELECT clock_timestamp() AS database_now) clock
		ON CONFLICT (bucket, consolidated_object_key_main) DO UPDATE
		SET configuration_sha256 = EXCLUDED.configuration_sha256,
			first_observed_at = CASE
				WHEN cscb_retirement_safety_observation.configuration_sha256 = EXCLUDED.configuration_sha256
					THEN cscb_retirement_safety_observation.first_observed_at
				ELSE EXCLUDED.first_observed_at
			END,
			last_observed_at = EXCLUDED.last_observed_at
		RETURNING first_observed_at, last_observed_at`
	var firstObservedAt time.Time
	var observedAt time.Time
	if err := r.db.QueryRowContext(
		ctx,
		query,
		bucket,
		consolidatedObjectKey,
		configurationSHA256,
	).Scan(&firstObservedAt, &observedAt); err != nil {
		return time.Time{}, time.Time{}, xerrors.Errorf("failed to persist CSCB retirement safety observation: %w", err)
	}
	return firstObservedAt, observedAt, nil
}

func (r *PostgresRepository) ClaimRetirement(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	claimedAt time.Time,
	claimExpiresAt time.Time,
) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if claimToken == "" || claimedAt.IsZero() || !claimExpiresAt.After(claimedAt) {
		return xerrors.New("valid retirement claim token and lease are required")
	}
	leaseMicros := claimExpiresAt.Sub(claimedAt).Microseconds()
	if leaseMicros <= 0 {
		return xerrors.New("retirement claim lease must be at least one microsecond")
	}
	const query = `
		UPDATE block_single_block_retention
		SET state = CASE WHEN state = $2 THEN $3 ELSE state END,
			claim_token = $4,
			claim_expires_at = CURRENT_TIMESTAMP + ($5 * INTERVAL '1 microsecond'),
			delete_started_at = CASE WHEN state = $2 THEN COALESCE(delete_started_at, CURRENT_TIMESTAMP) ELSE delete_started_at END,
			last_attempt_at = CURRENT_TIMESTAMP,
			attempt_count = attempt_count + 1,
			outcome = CASE WHEN state = $6 THEN 'verification_started' ELSE 'delete_started' END,
			updated_at = CURRENT_TIMESTAMP
		WHERE block_metadata_id = $1
			AND (
				(state = $2 AND claim_token IS NULL AND claim_expires_at IS NULL)
				OR
				(state IN ($3, $6) AND claim_expires_at <= CURRENT_TIMESTAMP)
			)`
	result, err := r.db.ExecContext(
		ctx,
		query,
		blockMetadataID,
		RetirementStateEligible,
		RetirementStateDeleting,
		claimToken,
		leaseMicros,
		RetirementStateDeletedPendingVerification,
	)
	if err != nil {
		return xerrors.Errorf("failed to claim retirement: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to inspect retirement claim transition: %w", err)
	}
	if rows == 1 {
		return nil
	}
	return xerrors.Errorf("%w: block_metadata_id=%d", ErrRetirementClaimUnavailable, blockMetadataID)
}

func (r *PostgresRepository) RenewRetirementClaim(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	renewedAt time.Time,
	claimExpiresAt time.Time,
) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if claimToken == "" || renewedAt.IsZero() || !claimExpiresAt.After(renewedAt) {
		return xerrors.New("valid retirement claim token and lease are required")
	}
	leaseMicros := claimExpiresAt.Sub(renewedAt).Microseconds()
	if leaseMicros <= 0 {
		return xerrors.New("retirement claim lease must be at least one microsecond")
	}
	const query = `
		UPDATE block_single_block_retention
		SET claim_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 microsecond'),
			last_attempt_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE block_metadata_id = $1
			AND state IN ($4, $5)
			AND claim_token = $2
			AND claim_expires_at > CURRENT_TIMESTAMP`
	result, err := r.db.ExecContext(
		ctx,
		query,
		blockMetadataID,
		claimToken,
		leaseMicros,
		RetirementStateDeleting,
		RetirementStateDeletedPendingVerification,
	)
	if err != nil {
		return xerrors.Errorf("failed to renew retirement claim: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to inspect retirement claim renewal: %w", err)
	}
	if rows == 1 {
		return nil
	}
	return xerrors.Errorf("%w: block_metadata_id=%d", ErrRetirementClaimUnavailable, blockMetadataID)
}

// ReleaseRetirementClaim moves the claim's expiry to now. Only the lease
// changes: state, outcome, and attempt bookkeeping stay as the last
// RecordRetirementOutcome left them, and the row is merely available to the
// next attempt at once, which is what ClaimRetirement's takeover branch
// checks. The outcome column holds one value per row, so the interrupted
// attempt's outcome remains readable only until the next attempt claims the
// row and records its own — the same as for any retried attempt; what
// survives the takeover is attempt_count and last_attempt_at.
func (r *PostgresRepository) ReleaseRetirementClaim(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if claimToken == "" {
		return xerrors.New("retirement claim token is required")
	}
	const query = `
		UPDATE block_single_block_retention
		SET claim_expires_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE block_metadata_id = $1
			AND claim_token = $2
			AND state IN ($3, $4)
			AND claim_expires_at > CURRENT_TIMESTAMP`
	if _, err := r.db.ExecContext(
		ctx,
		query,
		blockMetadataID,
		claimToken,
		RetirementStateDeleting,
		RetirementStateDeletedPendingVerification,
	); err != nil {
		return xerrors.Errorf("failed to release retirement claim: %w", err)
	}
	return nil
}

func (r *PostgresRepository) RecordRetirementOutcome(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	outcome string,
	attemptedAt time.Time,
) error {
	if r.db == nil {
		return xerrors.New("postgres db is required")
	}
	if claimToken == "" || outcome == "" {
		return xerrors.New("retirement claim token and outcome are required")
	}
	const query = `
		UPDATE block_single_block_retention
		SET outcome = $3,
			last_attempt_at = CURRENT_TIMESTAMP,
			updated_at = CURRENT_TIMESTAMP
		WHERE block_metadata_id = $1
			AND state IN ($4, $5)
			AND claim_token = $2`
	result, err := r.db.ExecContext(
		ctx,
		query,
		blockMetadataID,
		claimToken,
		outcome,
		RetirementStateDeleting,
		RetirementStateDeletedPendingVerification,
	)
	if err != nil {
		return xerrors.Errorf("failed to record retirement outcome: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to inspect retirement outcome update: %w", err)
	}
	if rows == 1 {
		return nil
	}
	return xerrors.Errorf("%w: block_metadata_id=%d", ErrRetirementClaimUnavailable, blockMetadataID)
}

func (r *PostgresRepository) RecordRetirementObjectDeleted(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	outcome string,
) (time.Time, error) {
	if r.db == nil {
		return time.Time{}, xerrors.New("postgres db is required")
	}
	if claimToken == "" || outcome == "" {
		return time.Time{}, xerrors.New("claim token and outcome are required to record object deletion")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, xerrors.Errorf("failed to begin recording retirement object deletion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	metadata, err := lockFinalizationMetadata(ctx, tx, blockMetadataID)
	if err != nil {
		return time.Time{}, err
	}
	manifest, err := getManifestForUpdate(ctx, tx, blockMetadataID)
	if err != nil {
		return time.Time{}, err
	}
	if !finalizationMetadataMatchesManifest(metadata, manifest) {
		return time.Time{}, xerrors.Errorf("active or shadow CSCB metadata changed before retirement finalization: block_metadata_id=%d", blockMetadataID)
	}
	if manifest.State == RetirementStateDeletedPendingVerification {
		if manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" || manifest.Outcome == "" ||
			manifest.DeleteStartedAt == nil || manifest.LastAttemptAt == nil || manifest.DeletedAt == nil ||
			manifest.VerifiedAt != nil || manifest.ClaimToken != claimToken || manifest.ClaimExpiresAt == nil ||
			metadata.shadowSingleBlockKey.Valid || !metadata.shadowDeletedAt.Valid ||
			!metadata.shadowDeletedAt.Time.Equal(*manifest.DeletedAt) {
			return time.Time{}, xerrors.Errorf("pending-verification retirement metadata is inconsistent: block_metadata_id=%d", blockMetadataID)
		}
		if err := clearCompletedRepairSingleBlockPath(ctx, tx, blockMetadataID, ""); err != nil {
			return time.Time{}, err
		}
		if err := tx.Commit(); err != nil {
			return time.Time{}, xerrors.Errorf("failed to commit idempotent retirement object deletion: %w", err)
		}
		return *manifest.DeletedAt, nil
	}
	if manifest.State != RetirementStateDeleting {
		return time.Time{}, xerrors.Errorf("retirement must be deleting before recording object deletion: block_metadata_id=%d state=%s", blockMetadataID, manifest.State)
	}
	if manifest.ClaimToken != claimToken || manifest.ClaimExpiresAt == nil {
		return time.Time{}, xerrors.Errorf("%w: block_metadata_id=%d", ErrRetirementClaimUnavailable, blockMetadataID)
	}
	if !metadata.shadowSingleBlockKey.Valid || metadata.shadowSingleBlockKey.String != manifest.SingleBlockObjectKey || metadata.shadowDeletedAt.Valid {
		return time.Time{}, xerrors.Errorf("single-block shadow metadata changed before retirement finalization: block_metadata_id=%d", blockMetadataID)
	}
	var databaseDeletedAt time.Time
	if err := tx.QueryRowContext(ctx, `
		SELECT database_now
		FROM (SELECT clock_timestamp() AS database_now) clock
		WHERE database_now < $1`, *manifest.ClaimExpiresAt).Scan(&databaseDeletedAt); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, xerrors.Errorf("%w: block_metadata_id=%d", ErrRetirementClaimUnavailable, blockMetadataID)
		}
		return time.Time{}, xerrors.Errorf("failed to read database deletion timestamp: %w", err)
	}

	const clearShadow = `
		UPDATE block_consolidation_shadow
		SET single_block_object_key_main = NULL,
			single_block_object_deleted_at = $3
		WHERE block_metadata_id = $1
			AND single_block_object_key_main = $2
			AND single_block_object_deleted_at IS NULL`
	result, err := tx.ExecContext(ctx, clearShadow, blockMetadataID, manifest.SingleBlockObjectKey, databaseDeletedAt)
	if err != nil {
		return time.Time{}, xerrors.Errorf("failed to clear retired shadow path: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return time.Time{}, xerrors.Errorf("retired shadow path clear guard failed: block_metadata_id=%d rows=%d err=%v", blockMetadataID, rows, err)
	}

	const recordDeleted = `
		UPDATE block_single_block_retention
		SET state = $2,
			single_block_object_key_main = NULL,
			single_block_object_etag = '',
			outcome = $3,
			deleted_at = $4,
			last_attempt_at = $4,
			updated_at = $4
		WHERE block_metadata_id = $1
			AND state = $5
			AND claim_token = $6
			AND claim_expires_at > $7`
	result, err = tx.ExecContext(
		ctx,
		recordDeleted,
		blockMetadataID,
		RetirementStateDeletedPendingVerification,
		outcome,
		databaseDeletedAt,
		RetirementStateDeleting,
		claimToken,
		databaseDeletedAt,
	)
	if err != nil {
		return time.Time{}, xerrors.Errorf("failed to record retirement object deletion: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return time.Time{}, xerrors.Errorf("%w: block_metadata_id=%d rows=%d err=%v", ErrRetirementClaimUnavailable, blockMetadataID, rows, err)
	}
	if err := clearCompletedRepairSingleBlockPath(ctx, tx, blockMetadataID, manifest.SingleBlockObjectKey); err != nil {
		return time.Time{}, err
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, xerrors.Errorf("failed to commit retirement object deletion: %w", err)
	}
	return databaseDeletedAt, nil
}

func clearCompletedRepairSingleBlockPath(
	ctx context.Context,
	tx *sql.Tx,
	blockMetadataID int64,
	expectedObjectKey string,
) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE cscb_repair_block
		SET single_block_object_key_main = NULL
		WHERE block_metadata_id = $1
			AND single_block_object_key_main IS NOT NULL
			AND ($2 = '' OR single_block_object_key_main = $2)`, blockMetadataID, expectedObjectKey)
	if err != nil {
		return xerrors.Errorf("failed to clear retired CSCB repair single-block path: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return xerrors.Errorf("failed to inspect retired CSCB repair path clear: %w", err)
	}
	if rows > 1 {
		return xerrors.Errorf("retired CSCB repair path clear guard failed: block_metadata_id=%d rows=%d", blockMetadataID, rows)
	}
	return nil
}

func (r *PostgresRepository) FinalizeRetirement(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	outcome string,
) (time.Time, error) {
	if r.db == nil {
		return time.Time{}, xerrors.New("postgres db is required")
	}
	if claimToken == "" || outcome == "" {
		return time.Time{}, xerrors.New("claim token and outcome are required for retirement finalization")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, xerrors.Errorf("failed to begin retirement finalization: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	metadata, err := lockFinalizationMetadata(ctx, tx, blockMetadataID)
	if err != nil {
		return time.Time{}, err
	}
	manifest, err := getManifestForUpdate(ctx, tx, blockMetadataID)
	if err != nil {
		return time.Time{}, err
	}
	if !finalizationMetadataMatchesManifest(metadata, manifest) {
		return time.Time{}, xerrors.Errorf("active or shadow CSCB metadata changed before retirement finalization: block_metadata_id=%d", blockMetadataID)
	}
	if manifest.State == RetirementStateDeletedVerified {
		if !finalizedMetadataIsConsistent(metadata, manifest) {
			return time.Time{}, xerrors.Errorf("finalized retirement metadata is inconsistent: block_metadata_id=%d", blockMetadataID)
		}
		if err := tx.Commit(); err != nil {
			return time.Time{}, xerrors.Errorf("failed to commit idempotent retirement finalization: %w", err)
		}
		return *manifest.VerifiedAt, nil
	}
	if manifest.State != RetirementStateDeletedPendingVerification {
		return time.Time{}, xerrors.Errorf("retirement must be pending verification before finalization: block_metadata_id=%d state=%s", blockMetadataID, manifest.State)
	}
	if manifest.ClaimToken != claimToken || manifest.ClaimExpiresAt == nil {
		return time.Time{}, xerrors.Errorf("%w: block_metadata_id=%d", ErrRetirementClaimUnavailable, blockMetadataID)
	}
	if manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" || manifest.DeletedAt == nil || manifest.VerifiedAt != nil ||
		metadata.shadowSingleBlockKey.Valid || !metadata.shadowDeletedAt.Valid ||
		!metadata.shadowDeletedAt.Time.Equal(*manifest.DeletedAt) {
		return time.Time{}, xerrors.Errorf("pending-verification retirement metadata is inconsistent: block_metadata_id=%d", blockMetadataID)
	}
	var databaseVerifiedAt time.Time
	if err := tx.QueryRowContext(ctx, `
		SELECT database_now
		FROM (SELECT clock_timestamp() AS database_now) clock
		WHERE database_now < $1`, *manifest.ClaimExpiresAt).Scan(&databaseVerifiedAt); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, xerrors.Errorf("%w: block_metadata_id=%d", ErrRetirementClaimUnavailable, blockMetadataID)
		}
		return time.Time{}, xerrors.Errorf("failed to read database verification timestamp: %w", err)
	}

	const finalize = `
		UPDATE block_single_block_retention
		SET state = $2,
			claim_token = NULL,
			claim_expires_at = NULL,
			outcome = $3,
			verified_at = $4,
			last_attempt_at = $4,
			updated_at = $4
		WHERE block_metadata_id = $1
			AND state = $5
			AND claim_token = $6
			AND claim_expires_at > $7`
	result, err := tx.ExecContext(
		ctx,
		finalize,
		blockMetadataID,
		RetirementStateDeletedVerified,
		outcome,
		databaseVerifiedAt,
		RetirementStateDeletedPendingVerification,
		claimToken,
		databaseVerifiedAt,
	)
	if err != nil {
		return time.Time{}, xerrors.Errorf("failed to finalize retirement manifest: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return time.Time{}, xerrors.Errorf("%w: block_metadata_id=%d rows=%d err=%v", ErrRetirementClaimUnavailable, blockMetadataID, rows, err)
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, xerrors.Errorf("failed to commit retirement finalization: %w", err)
	}
	return databaseVerifiedAt, nil
}

func finalizedMetadataIsConsistent(metadata finalizationMetadata, manifest RetirementManifest) bool {
	return manifest.SingleBlockObjectKey == "" &&
		manifest.SingleBlockObjectETag == "" &&
		manifest.Outcome != "" &&
		manifest.DeleteStartedAt != nil &&
		manifest.LastAttemptAt != nil &&
		manifest.DeletedAt != nil &&
		manifest.VerifiedAt != nil &&
		manifest.ClaimToken == "" &&
		manifest.ClaimExpiresAt == nil &&
		!metadata.shadowSingleBlockKey.Valid &&
		metadata.shadowDeletedAt.Valid &&
		metadata.shadowDeletedAt.Time.Equal(*manifest.DeletedAt)
}

type finalizationMetadata struct {
	shadowTag                int64
	shadowHeight             int64
	shadowHash               sql.NullString
	shadowSingleBlockKey     sql.NullString
	shadowDeletedAt          sql.NullTime
	shadowConsolidatedKey    string
	shadowObjectFormat       int64
	shadowByteOffset         int64
	shadowByteLength         int64
	shadowUncompressedLength int64
	shadowValidatedAt        sql.NullTime
	primaryTag               int64
	primaryHeight            int64
	primaryHash              sql.NullString
	primaryObjectKey         sql.NullString
	primaryObjectFormat      int64
	primaryByteOffset        sql.NullInt64
	primaryByteLength        sql.NullInt64
	primaryUncompressed      sql.NullInt64
	primarySkipped           bool
	primaryRetirementFenced  sql.NullTime
}

func lockFinalizationMetadata(ctx context.Context, tx *sql.Tx, blockMetadataID int64) (finalizationMetadata, error) {
	const query = `
		SELECT
			shadow.tag,
			shadow.height,
			shadow.hash,
			shadow.single_block_object_key_main,
			shadow.single_block_object_deleted_at,
			shadow.consolidated_object_key_main,
			shadow.object_format,
			shadow.byte_offset,
			shadow.byte_length,
			shadow.uncompressed_length,
			shadow.validated_at,
			bm.tag,
			bm.height,
			bm.hash,
			bm.object_key_main,
			bm.object_format,
			bm.byte_offset,
			bm.byte_length,
			bm.uncompressed_length,
			bm.skipped,
			bm.single_block_retention_fenced_at
		FROM block_consolidation_shadow shadow
		JOIN block_metadata bm ON bm.id = shadow.block_metadata_id
		WHERE shadow.block_metadata_id = $1
		FOR UPDATE OF shadow, bm`
	var metadata finalizationMetadata
	if err := tx.QueryRowContext(ctx, query, blockMetadataID).Scan(
		&metadata.shadowTag,
		&metadata.shadowHeight,
		&metadata.shadowHash,
		&metadata.shadowSingleBlockKey,
		&metadata.shadowDeletedAt,
		&metadata.shadowConsolidatedKey,
		&metadata.shadowObjectFormat,
		&metadata.shadowByteOffset,
		&metadata.shadowByteLength,
		&metadata.shadowUncompressedLength,
		&metadata.shadowValidatedAt,
		&metadata.primaryTag,
		&metadata.primaryHeight,
		&metadata.primaryHash,
		&metadata.primaryObjectKey,
		&metadata.primaryObjectFormat,
		&metadata.primaryByteOffset,
		&metadata.primaryByteLength,
		&metadata.primaryUncompressed,
		&metadata.primarySkipped,
		&metadata.primaryRetirementFenced,
	); err != nil {
		return finalizationMetadata{}, xerrors.Errorf("failed to lock retirement finalization metadata: %w", err)
	}
	return metadata, nil
}

func finalizationMetadataMatchesManifest(metadata finalizationMetadata, manifest RetirementManifest) bool {
	return metadata.shadowValidatedAt.Valid &&
		metadata.primaryRetirementFenced.Valid &&
		metadata.shadowTag >= 0 && uint32(metadata.shadowTag) == manifest.Tag &&
		metadata.shadowHeight >= 0 && uint64(metadata.shadowHeight) == manifest.Height &&
		nullableHashMatches(metadata.shadowHash, manifest.Hash) &&
		metadata.primaryTag >= 0 && uint32(metadata.primaryTag) == manifest.Tag &&
		metadata.primaryHeight >= 0 && uint64(metadata.primaryHeight) == manifest.Height &&
		nullableHashMatches(metadata.primaryHash, manifest.Hash) &&
		metadata.shadowConsolidatedKey == manifest.ConsolidatedObjectKey &&
		metadata.shadowObjectFormat == int64(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) &&
		metadata.shadowByteOffset >= 0 && uint64(metadata.shadowByteOffset) == manifest.ConsolidatedByteOffset &&
		metadata.shadowByteLength > 0 && uint64(metadata.shadowByteLength) == manifest.ConsolidatedByteLength &&
		metadata.shadowUncompressedLength > 0 && uint64(metadata.shadowUncompressedLength) == manifest.ConsolidatedUncompressedLength &&
		!metadata.primarySkipped &&
		metadata.primaryObjectKey.Valid && metadata.primaryObjectKey.String == manifest.ConsolidatedObjectKey &&
		metadata.primaryObjectFormat == int64(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH) &&
		metadata.primaryByteOffset.Valid && metadata.primaryByteOffset.Int64 >= 0 && uint64(metadata.primaryByteOffset.Int64) == manifest.ConsolidatedByteOffset &&
		metadata.primaryByteLength.Valid && metadata.primaryByteLength.Int64 > 0 && uint64(metadata.primaryByteLength.Int64) == manifest.ConsolidatedByteLength &&
		metadata.primaryUncompressed.Valid && metadata.primaryUncompressed.Int64 > 0 && uint64(metadata.primaryUncompressed.Int64) == manifest.ConsolidatedUncompressedLength
}

func nullableHashMatches(value sql.NullString, expected string) bool {
	if !value.Valid {
		return expected == ""
	}
	return value.String == expected
}

func (r *PostgresRepository) ListPendingRetirements(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit uint64,
) ([]RetirementManifest, error) {
	if r.db == nil {
		return nil, xerrors.New("postgres db is required")
	}
	if endHeight <= startHeight {
		return nil, nil
	}
	if limit == 0 {
		limit = endHeight - startHeight
	}
	query := `
		SELECT
			retention.block_metadata_id, retention.tag, retention.height, COALESCE(retention.hash, ''),
			retention.state, retention.bucket,
			retention.single_block_object_key_main, retention.single_block_object_key_sha256,
			retention.single_block_object_version_ids, retention.single_block_delete_marker_version_ids,
			retention.single_block_object_etag, retention.single_block_object_bytes,
			retention.consolidated_object_key_main, retention.consolidated_object_version_id,
			retention.consolidated_object_etag, retention.consolidated_byte_offset,
			retention.consolidated_byte_length, retention.consolidated_uncompressed_length,
			retention.payload_sha256, retention.outcome, retention.attempt_count,
			retention.claim_token, retention.claim_expires_at, retention.prepared_at,
			retention.delete_started_at, retention.last_attempt_at, retention.deleted_at,
			retention.verified_at
		FROM block_single_block_retention retention
		JOIN block_consolidation_shadow shadow
			ON shadow.block_metadata_id = retention.block_metadata_id
			AND shadow.tag = retention.tag
			AND shadow.height = retention.height
		WHERE retention.tag = $1
			AND retention.height >= $2
			AND retention.height < $3
			AND retention.state IN (` + pendingRetirementStatesSQL + `)
			AND shadow.single_block_delete_after IS NOT NULL
			AND shadow.single_block_delete_after <= $4
		ORDER BY retention.height ASC
		LIMIT $5`
	rows, err := r.db.QueryContext(
		ctx,
		query,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		limit,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to list pending retirements: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make([]RetirementManifest, 0)
	for rows.Next() {
		manifest, err := scanManifest(rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan pending retirement: %w", err)
		}
		result = append(result, manifest)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate pending retirements: %w", err)
	}
	return result, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanMetadataRow(source scanner) (MetadataRow, error) {
	var row MetadataRow
	var primaryObjectFormat int64
	var primaryStorageGeneration sql.NullString
	var primaryByteOffset sql.NullInt64
	var primaryByteLength sql.NullInt64
	var primaryUncompressedLength sql.NullInt64
	var primaryRetirementFencedAt sql.NullTime
	var shadowBlockMetadataID sql.NullInt64
	var shadowTag sql.NullInt64
	var shadowHeight sql.NullInt64
	var shadowHash sql.NullString
	var shadowSingleBlockKey sql.NullString
	var shadowSingleBlockStorageGeneration sql.NullString
	var shadowConsolidatedKey sql.NullString
	var shadowConsolidatedStorageGeneration sql.NullString
	var shadowObjectFormat sql.NullInt64
	var shadowByteOffset sql.NullInt64
	var shadowByteLength sql.NullInt64
	var shadowUncompressedLength sql.NullInt64
	var shadowValidatedAt sql.NullTime
	var shadowSingleBlockRetentionStartedAt sql.NullTime
	var shadowSingleBlockDeleteAfter sql.NullTime
	var shadowSingleBlockObjectDeletedAt sql.NullTime
	var shadowFormatVersion sql.NullInt64
	var retirementBlockMetadataID sql.NullInt64
	var retirementState sql.NullString
	var retirementBucket sql.NullString
	var retirementSingleBlockKey sql.NullString
	var retirementSingleBlockKeySHA256 sql.NullString
	var retirementVersionIDs pq.StringArray
	var retirementDeleteMarkerVersionIDs pq.StringArray
	var retirementSingleBlockETag sql.NullString
	var retirementSingleBlockBytes sql.NullInt64
	var retirementConsolidatedKey sql.NullString
	var retirementConsolidatedVersionID sql.NullString
	var retirementConsolidatedETag sql.NullString
	var retirementByteOffset sql.NullInt64
	var retirementByteLength sql.NullInt64
	var retirementUncompressedLength sql.NullInt64
	var retirementPayloadSHA256 sql.NullString
	var retirementOutcome sql.NullString
	var retirementAttemptCount sql.NullInt64
	var retirementClaimToken sql.NullString
	var retirementClaimExpiresAt sql.NullTime
	var retirementPreparedAt sql.NullTime
	var retirementDeleteStartedAt sql.NullTime
	var retirementLastAttemptAt sql.NullTime
	var retirementDeletedAt sql.NullTime
	var retirementVerifiedAt sql.NullTime
	if err := source.Scan(
		&row.BlockMetadataID,
		&row.Canonical,
		&row.Tag,
		&row.Height,
		&row.Hash,
		&row.Skipped,
		&row.PrimaryObjectKey,
		&primaryObjectFormat,
		&primaryStorageGeneration,
		&primaryByteOffset,
		&primaryByteLength,
		&primaryUncompressedLength,
		&primaryRetirementFencedAt,
		&shadowBlockMetadataID,
		&shadowTag,
		&shadowHeight,
		&shadowHash,
		&shadowSingleBlockKey,
		&shadowSingleBlockStorageGeneration,
		&shadowConsolidatedKey,
		&shadowConsolidatedStorageGeneration,
		&shadowObjectFormat,
		&shadowByteOffset,
		&shadowByteLength,
		&shadowUncompressedLength,
		&shadowValidatedAt,
		&shadowSingleBlockRetentionStartedAt,
		&shadowSingleBlockDeleteAfter,
		&shadowSingleBlockObjectDeletedAt,
		&shadowFormatVersion,
		&retirementBlockMetadataID,
		&retirementState,
		&retirementBucket,
		&retirementSingleBlockKey,
		&retirementSingleBlockKeySHA256,
		&retirementVersionIDs,
		&retirementDeleteMarkerVersionIDs,
		&retirementSingleBlockETag,
		&retirementSingleBlockBytes,
		&retirementConsolidatedKey,
		&retirementConsolidatedVersionID,
		&retirementConsolidatedETag,
		&retirementByteOffset,
		&retirementByteLength,
		&retirementUncompressedLength,
		&retirementPayloadSHA256,
		&retirementOutcome,
		&retirementAttemptCount,
		&retirementClaimToken,
		&retirementClaimExpiresAt,
		&retirementPreparedAt,
		&retirementDeleteStartedAt,
		&retirementLastAttemptAt,
		&retirementDeletedAt,
		&retirementVerifiedAt,
	); err != nil {
		return MetadataRow{}, err
	}
	row.PrimaryObjectFormat = api.BlockObjectFormat(primaryObjectFormat)
	row.PrimaryStorageGeneration = primaryStorageGeneration.String
	row.PrimaryByteOffset = nullableUint64(primaryByteOffset)
	row.PrimaryByteLength = nullableUint64(primaryByteLength)
	row.PrimaryUncompressedLength = nullableUint64(primaryUncompressedLength)
	row.RetirementFencedAt = nullableTime(primaryRetirementFencedAt)
	if shadowBlockMetadataID.Valid {
		row.SingleBlockObjectKey = shadowSingleBlockKey.String
		row.Shadow = &ConsolidationShadow{
			Tag:                           uint32(shadowTag.Int64),
			Height:                        uint64(shadowHeight.Int64),
			Hash:                          shadowHash.String,
			SingleBlockObjectKey:          shadowSingleBlockKey.String,
			SingleBlockStorageGeneration:  shadowSingleBlockStorageGeneration.String,
			ConsolidatedObjectKey:         shadowConsolidatedKey.String,
			ConsolidatedStorageGeneration: shadowConsolidatedStorageGeneration.String,
			ObjectFormat:                  api.BlockObjectFormat(shadowObjectFormat.Int64),
			ByteOffset:                    nullableUint64(shadowByteOffset),
			ByteLength:                    nullableUint64(shadowByteLength),
			UncompressedLength:            nullableUint64(shadowUncompressedLength),
			FormatVersion:                 int(shadowFormatVersion.Int64),
		}
		row.Shadow.ValidatedAt = nullableTime(shadowValidatedAt)
		row.Shadow.SingleBlockRetentionStartedAt = nullableTime(shadowSingleBlockRetentionStartedAt)
		row.Shadow.SingleBlockDeleteAfter = nullableTime(shadowSingleBlockDeleteAfter)
		row.Shadow.SingleBlockObjectDeletedAt = nullableTime(shadowSingleBlockObjectDeletedAt)
	}
	if retirementBlockMetadataID.Valid {
		row.Retirement = &RetirementManifest{
			BlockMetadataID:                   retirementBlockMetadataID.Int64,
			Tag:                               row.Tag,
			Height:                            row.Height,
			Hash:                              row.Hash,
			State:                             retirementState.String,
			Bucket:                            retirementBucket.String,
			SingleBlockObjectKey:              retirementSingleBlockKey.String,
			SingleBlockObjectKeySHA256:        retirementSingleBlockKeySHA256.String,
			SingleBlockObjectVersionIDs:       append([]string(nil), retirementVersionIDs...),
			SingleBlockDeleteMarkerVersionIDs: append([]string(nil), retirementDeleteMarkerVersionIDs...),
			SingleBlockObjectETag:             retirementSingleBlockETag.String,
			SingleBlockObjectBytes:            nullableUint64(retirementSingleBlockBytes),
			ConsolidatedObjectKey:             retirementConsolidatedKey.String,
			ConsolidatedObjectVersionID:       retirementConsolidatedVersionID.String,
			ConsolidatedObjectETag:            retirementConsolidatedETag.String,
			ConsolidatedByteOffset:            nullableUint64(retirementByteOffset),
			ConsolidatedByteLength:            nullableUint64(retirementByteLength),
			ConsolidatedUncompressedLength:    nullableUint64(retirementUncompressedLength),
			PayloadSHA256:                     retirementPayloadSHA256.String,
			Outcome:                           retirementOutcome.String,
			AttemptCount:                      int(retirementAttemptCount.Int64),
			ClaimToken:                        retirementClaimToken.String,
			ClaimExpiresAt:                    nullableTime(retirementClaimExpiresAt),
			PreparedAt:                        retirementPreparedAt.Time,
			DeleteStartedAt:                   nullableTime(retirementDeleteStartedAt),
			LastAttemptAt:                     nullableTime(retirementLastAttemptAt),
			DeletedAt:                         nullableTime(retirementDeletedAt),
			VerifiedAt:                        nullableTime(retirementVerifiedAt),
		}
	}
	return row, nil
}

func getManifestForUpdate(ctx context.Context, tx *sql.Tx, blockMetadataID int64) (RetirementManifest, error) {
	const query = `
		SELECT
			block_metadata_id, tag, height, COALESCE(hash, ''), state, bucket,
			single_block_object_key_main, single_block_object_key_sha256, single_block_object_version_ids,
			single_block_delete_marker_version_ids,
			single_block_object_etag, single_block_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, outcome, attempt_count, claim_token, claim_expires_at,
			prepared_at, delete_started_at, last_attempt_at, deleted_at, verified_at
		FROM block_single_block_retention
		WHERE block_metadata_id = $1
		FOR UPDATE`
	manifest, err := scanManifest(tx.QueryRowContext(ctx, query, blockMetadataID))
	if err != nil {
		return RetirementManifest{}, xerrors.Errorf("failed to lock retirement manifest: %w", err)
	}
	return manifest, nil
}

func scanManifest(source scanner) (RetirementManifest, error) {
	var manifest RetirementManifest
	var hash sql.NullString
	var singleBlockKey sql.NullString
	var versionIDs pq.StringArray
	var deleteMarkerVersionIDs pq.StringArray
	var singleBlockBytes int64
	var byteOffset int64
	var byteLength int64
	var uncompressedLength int64
	var attemptCount int
	var claimToken sql.NullString
	var claimExpiresAt sql.NullTime
	var deleteStartedAt sql.NullTime
	var lastAttemptAt sql.NullTime
	var deletedAt sql.NullTime
	var verifiedAt sql.NullTime
	if err := source.Scan(
		&manifest.BlockMetadataID,
		&manifest.Tag,
		&manifest.Height,
		&hash,
		&manifest.State,
		&manifest.Bucket,
		&singleBlockKey,
		&manifest.SingleBlockObjectKeySHA256,
		&versionIDs,
		&deleteMarkerVersionIDs,
		&manifest.SingleBlockObjectETag,
		&singleBlockBytes,
		&manifest.ConsolidatedObjectKey,
		&manifest.ConsolidatedObjectVersionID,
		&manifest.ConsolidatedObjectETag,
		&byteOffset,
		&byteLength,
		&uncompressedLength,
		&manifest.PayloadSHA256,
		&manifest.Outcome,
		&attemptCount,
		&claimToken,
		&claimExpiresAt,
		&manifest.PreparedAt,
		&deleteStartedAt,
		&lastAttemptAt,
		&deletedAt,
		&verifiedAt,
	); err != nil {
		return RetirementManifest{}, err
	}
	if singleBlockBytes < 0 || byteOffset < 0 || byteLength < 0 || uncompressedLength < 0 || attemptCount < 0 {
		return RetirementManifest{}, xerrors.New("retirement manifest contains negative byte fields")
	}
	manifest.Hash = hash.String
	manifest.SingleBlockObjectKey = singleBlockKey.String
	manifest.SingleBlockObjectVersionIDs = append([]string(nil), versionIDs...)
	manifest.SingleBlockDeleteMarkerVersionIDs = append([]string(nil), deleteMarkerVersionIDs...)
	manifest.SingleBlockObjectBytes = uint64(singleBlockBytes)
	manifest.ConsolidatedByteOffset = uint64(byteOffset)
	manifest.ConsolidatedByteLength = uint64(byteLength)
	manifest.ConsolidatedUncompressedLength = uint64(uncompressedLength)
	manifest.AttemptCount = attemptCount
	manifest.ClaimToken = claimToken.String
	manifest.ClaimExpiresAt = nullableTime(claimExpiresAt)
	manifest.DeleteStartedAt = nullableTime(deleteStartedAt)
	manifest.LastAttemptAt = nullableTime(lastAttemptAt)
	manifest.DeletedAt = nullableTime(deletedAt)
	manifest.VerifiedAt = nullableTime(verifiedAt)
	return manifest, nil
}

func validateManifestMetadata(
	row MetadataRow,
	manifest RetirementManifest,
	expectedStorageGeneration string,
) error {
	if !row.Canonical || row.BlockMetadataID != manifest.BlockMetadataID || row.Tag != manifest.Tag || row.Height != manifest.Height || row.Hash != manifest.Hash {
		return xerrors.Errorf("canonical metadata changed before retirement: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if row.Skipped || row.Shadow == nil || !validShadowReference(row, row.Shadow) || row.SingleBlockObjectKey != manifest.SingleBlockObjectKey {
		return xerrors.Errorf("single-block shadow metadata changed before retirement: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if row.PrimaryStorageGeneration != expectedStorageGeneration ||
		row.Shadow.SingleBlockStorageGeneration != expectedStorageGeneration ||
		row.Shadow.ConsolidatedStorageGeneration != expectedStorageGeneration {
		return xerrors.Errorf("storage generation changed before retirement: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if row.Shadow.ValidatedAt == nil || row.Shadow.SingleBlockRetentionStartedAt == nil || row.Shadow.SingleBlockDeleteAfter == nil ||
		row.Shadow.SingleBlockObjectDeletedAt != nil {
		return xerrors.Errorf("retirement eligibility changed before manifest persistence: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if row.PrimaryObjectKey != manifest.ConsolidatedObjectKey ||
		row.PrimaryObjectFormat != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH ||
		row.PrimaryByteOffset != manifest.ConsolidatedByteOffset ||
		row.PrimaryByteLength != manifest.ConsolidatedByteLength ||
		row.PrimaryUncompressedLength != manifest.ConsolidatedUncompressedLength {
		return xerrors.Errorf("consolidated metadata changed before retirement: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	return nil
}

func validatePreparedManifest(manifest RetirementManifest) error {
	if manifest.BlockMetadataID <= 0 || manifest.Bucket == "" || manifest.SingleBlockObjectKey == "" ||
		manifest.SingleBlockObjectKeySHA256 != keySHA256(manifest.SingleBlockObjectKey) ||
		!validPinnedVersionIDs(
			firstString(manifest.SingleBlockObjectVersionIDs),
			manifest.SingleBlockObjectVersionIDs,
			manifest.SingleBlockDeleteMarkerVersionIDs,
		) ||
		manifest.SingleBlockObjectETag == "" || manifest.SingleBlockObjectBytes == 0 ||
		manifest.ConsolidatedObjectKey == "" || !isImmutableVersionID(manifest.ConsolidatedObjectVersionID) || manifest.ConsolidatedObjectETag == "" ||
		manifest.ConsolidatedByteLength == 0 || manifest.ConsolidatedUncompressedLength == 0 ||
		!isSHA256Hex(manifest.PayloadSHA256) || manifest.PreparedAt.IsZero() {
		return xerrors.Errorf("retirement manifest is incomplete: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	return nil
}

func samePreparedManifest(existing RetirementManifest, expected RetirementManifest) bool {
	if existing.State != RetirementStateEligible && existing.State != RetirementStateDeleting {
		return false
	}
	return existing.BlockMetadataID == expected.BlockMetadataID &&
		existing.Tag == expected.Tag &&
		existing.Height == expected.Height &&
		existing.Hash == expected.Hash &&
		existing.Bucket == expected.Bucket &&
		existing.SingleBlockObjectKey == expected.SingleBlockObjectKey &&
		existing.SingleBlockObjectKeySHA256 == expected.SingleBlockObjectKeySHA256 &&
		sameStrings(existing.SingleBlockObjectVersionIDs, expected.SingleBlockObjectVersionIDs) &&
		sameStrings(existing.SingleBlockDeleteMarkerVersionIDs, expected.SingleBlockDeleteMarkerVersionIDs) &&
		existing.SingleBlockObjectETag == expected.SingleBlockObjectETag &&
		existing.SingleBlockObjectBytes == expected.SingleBlockObjectBytes &&
		existing.ConsolidatedObjectKey == expected.ConsolidatedObjectKey &&
		existing.ConsolidatedObjectVersionID == expected.ConsolidatedObjectVersionID &&
		existing.ConsolidatedObjectETag == expected.ConsolidatedObjectETag &&
		existing.ConsolidatedByteOffset == expected.ConsolidatedByteOffset &&
		existing.ConsolidatedByteLength == expected.ConsolidatedByteLength &&
		existing.ConsolidatedUncompressedLength == expected.ConsolidatedUncompressedLength &&
		existing.PayloadSHA256 == expected.PayloadSHA256
}

func sameStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	copy := value.Time
	return &copy
}

func nullableUint64(value sql.NullInt64) uint64 {
	if !value.Valid || value.Int64 < 0 {
		return 0
	}
	return uint64(value.Int64)
}
