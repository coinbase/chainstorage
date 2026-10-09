package retirement

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const integrationRetentionBucket = "integration-bucket"

func TestIntegrationPostgresRepositoryRetirementStateMachine(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retirement integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_000_000_000 + unique%100_000_000)
	height := uint64(8_000_000_000 + unique%100_000_000)
	// Exercise the complete lifecycle for SQL NULL hashes. Repository reads
	// normalize NULL to an empty string, while writes use NULLIF consistently.
	hash := ""
	singleBlockKey := fmt.Sprintf("single-block/%d.gzip", height)
	cscbKey := fmt.Sprintf("consolidated/%d.cscb.gzip", height)
	validatedAt := time.Now().UTC().Add(-96 * time.Hour)
	retiredAt := validatedAt
	retireAfter := retiredAt.Add(72 * time.Hour)
	var blockMetadataID int64
	err = db.QueryRowContext(ctx, `
		INSERT INTO block_metadata (
			height, tag, hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length
		) VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, FALSE, $7, $8, $9, $10)
		RETURNING id`,
		height,
		tag,
		hash,
		height-1,
		cscbKey,
		time.Now().UTC().Unix(),
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		0,
		128,
		128,
	).Scan(&blockMetadataID)
	require.NoError(err)
	defer func() {
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_single_block_retention WHERE block_metadata_id = $1`, blockMetadataID)
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE block_metadata_id = $1`, blockMetadataID)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
	}()
	_, err = db.ExecContext(ctx, `
		INSERT INTO canonical_blocks (height, block_metadata_id, tag)
		VALUES ($1, $2, $3)`, height, blockMetadataID, tag)
	require.NoError(err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, single_block_object_key_main,
			consolidated_object_key_main, object_format, byte_offset, byte_length,
			uncompressed_length, validated_at, single_block_retention_started_at, single_block_delete_after
		) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		blockMetadataID,
		tag,
		height,
		hash,
		singleBlockKey,
		cscbKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		0,
		128,
		128,
		validatedAt,
		retiredAt,
		retireAfter,
	)
	require.NoError(err)

	repo := NewPostgresRepository(db)
	preparedAt := time.Now().UTC().Add(7 * 24 * time.Hour)
	manifest := RetirementManifest{
		BlockMetadataID:                   blockMetadataID,
		Tag:                               tag,
		Height:                            height,
		Hash:                              hash,
		State:                             RetirementStateEligible,
		Bucket:                            "integration-bucket",
		SingleBlockObjectKey:              singleBlockKey,
		SingleBlockObjectKeySHA256:        keySHA256(singleBlockKey),
		SingleBlockObjectVersionIDs:       []string{"single-block-v2", "single-block-v1"},
		SingleBlockDeleteMarkerVersionIDs: []string{"delete-marker-v1"},
		SingleBlockObjectETag:             "single-block-etag",
		SingleBlockObjectBytes:            256,
		ConsolidatedObjectKey:             cscbKey,
		ConsolidatedObjectVersionID:       "cscb-v1",
		ConsolidatedObjectETag:            "cscb-etag",
		ConsolidatedByteOffset:            0,
		ConsolidatedByteLength:            128,
		ConsolidatedUncompressedLength:    128,
		PayloadSHA256:                     keySHA256("payload"),
		PreparedAt:                        preparedAt,
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_single_block_retention (
			block_metadata_id, tag, height, hash, state, bucket,
			single_block_object_key_main, single_block_object_key_sha256, single_block_object_version_ids,
			single_block_object_etag, single_block_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, prepared_at
		) VALUES ($1, $2, $3, NULLIF($4, ''), 'deleted_verified', $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, CURRENT_TIMESTAMP)`,
		blockMetadataID, tag, height, hash, manifest.Bucket, singleBlockKey, manifest.SingleBlockObjectKeySHA256,
		pq.Array(manifest.SingleBlockObjectVersionIDs), manifest.SingleBlockObjectETag, manifest.SingleBlockObjectBytes,
		cscbKey, manifest.ConsolidatedObjectVersionID, manifest.ConsolidatedObjectETag,
		manifest.ConsolidatedByteOffset, manifest.ConsolidatedByteLength, manifest.ConsolidatedUncompressedLength,
		manifest.PayloadSHA256,
	)
	require.Error(err)
	require.Contains(err.Error(), "must be inserted in eligible state")
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET single_block_delete_after = CURRENT_TIMESTAMP + INTERVAL '1 hour' WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	err = repo.PrepareRetirement(ctx, manifest, "")
	require.Error(err)
	require.Contains(err.Error(), "failed to lock canonical retirement metadata")
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET single_block_delete_after = $2 WHERE block_metadata_id = $1`, blockMetadataID, retireAfter)
	require.NoError(err)
	require.NoError(repo.PrepareRetirement(ctx, manifest, ""))
	_, err = db.ExecContext(ctx, `DELETE FROM block_single_block_retention WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "audit manifests cannot be deleted")
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_single_block_retention (
			block_metadata_id, tag, height, hash, state, bucket,
			single_block_object_key_main, single_block_object_key_sha256, single_block_object_version_ids,
			single_block_object_etag, single_block_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, prepared_at
		) VALUES ($1, $2, $3, NULLIF($4, ''), 'eligible', $5, $6, $7, ARRAY['null'], $8, $9,
			$10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP)
		ON CONFLICT (block_metadata_id) DO NOTHING`,
		blockMetadataID, tag, height, hash, manifest.Bucket, singleBlockKey, manifest.SingleBlockObjectKeySHA256,
		manifest.SingleBlockObjectETag, manifest.SingleBlockObjectBytes, cscbKey, manifest.ConsolidatedObjectVersionID,
		manifest.ConsolidatedObjectETag, manifest.ConsolidatedByteOffset, manifest.ConsolidatedByteLength,
		manifest.ConsolidatedUncompressedLength, manifest.PayloadSHA256,
	)
	require.Error(err)
	require.Contains(err.Error(), "block_single_block_retention_immutable_version_ids_check")
	_, err = db.ExecContext(ctx, `
		INSERT INTO block_single_block_retention (
			block_metadata_id, tag, height, hash, state, bucket,
			single_block_object_key_main, single_block_object_key_sha256, single_block_object_version_ids,
			single_block_object_etag, single_block_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, prepared_at
		) VALUES ($1, $2, $3, NULLIF($4, ''), 'eligible', $5, $6, $7, ARRAY[NULL]::TEXT[], $8, $9,
			$10, $11, $12, $13, $14, $15, $16, CURRENT_TIMESTAMP)
		ON CONFLICT (block_metadata_id) DO NOTHING`,
		blockMetadataID, tag, height, hash, manifest.Bucket, singleBlockKey, manifest.SingleBlockObjectKeySHA256,
		manifest.SingleBlockObjectETag, manifest.SingleBlockObjectBytes, cscbKey, manifest.ConsolidatedObjectVersionID,
		manifest.ConsolidatedObjectETag, manifest.ConsolidatedByteOffset, manifest.ConsolidatedByteLength,
		manifest.ConsolidatedUncompressedLength, manifest.PayloadSHA256,
	)
	require.Error(err)
	require.Contains(err.Error(), "block_single_block_retention_immutable_version_ids_check")
	firstObservedAt, observedAt, err := repo.ObserveRetentionSafety(ctx, manifest.Bucket, cscbKey, keySHA256("safe-configuration-v1"))
	require.NoError(err)
	require.Equal(firstObservedAt, observedAt)
	sameFirstObservedAt, laterObservedAt, err := repo.ObserveRetentionSafety(ctx, manifest.Bucket, cscbKey, keySHA256("safe-configuration-v1"))
	require.NoError(err)
	require.Equal(firstObservedAt, sameFirstObservedAt)
	require.False(laterObservedAt.Before(observedAt))
	resetFirstObservedAt, resetObservedAt, err := repo.ObserveRetentionSafety(ctx, manifest.Bucket, cscbKey, keySHA256("safe-configuration-v2"))
	require.NoError(err)
	require.Equal(resetFirstObservedAt, resetObservedAt)
	require.False(resetFirstObservedAt.Before(laterObservedAt))
	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET state = 'deleted_verified' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "invalid single-block retirement transition")
	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET consolidated_object_etag = 'mutated' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change pinned retirement manifest fields")
	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET single_block_delete_marker_version_ids = ARRAY['mutated-marker'] WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change pinned retirement delete-marker versions")

	startedAt := preparedAt.Add(24 * time.Hour)
	claimToken := "integration-claim"
	require.NoError(repo.ClaimRetirement(ctx, blockMetadataID, claimToken, startedAt, startedAt.Add(time.Hour)))
	var databaseClaimExpiresAt time.Time
	require.NoError(db.QueryRowContext(ctx, `SELECT claim_expires_at FROM block_single_block_retention WHERE block_metadata_id = $1`, blockMetadataID).Scan(&databaseClaimExpiresAt))
	require.WithinDuration(time.Now().UTC().Add(time.Hour), databaseClaimExpiresAt, 5*time.Second)
	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET claim_token = 'stolen-claim' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot replace an active single-block retirement claim")
	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET state = 'deleted_pending_verification', claim_token = 'stolen-claim' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change claim owner while recording single-block object deletion")
	err = repo.ClaimRetirement(ctx, blockMetadataID, "competing-claim", startedAt.Add(48*time.Hour), startedAt.Add(49*time.Hour))
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	err = repo.RenewRetirementClaim(ctx, blockMetadataID, "competing-claim", startedAt.Add(time.Second), startedAt.Add(2*time.Hour))
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	require.NoError(repo.RenewRetirementClaim(ctx, blockMetadataID, claimToken, startedAt.Add(time.Second), startedAt.Add(2*time.Hour)))
	// Releasing the held claim expires it at once (INF-1603): the releaser's
	// token can no longer renew or record, and the replacement claim below
	// takes the row over without waiting out the lease — the same transition
	// the manual expiry used to force here. Releasing again, or with a
	// foreign token, is a no-op rather than an error.
	require.NoError(repo.ReleaseRetirementClaim(ctx, blockMetadataID, claimToken))
	require.NoError(repo.ReleaseRetirementClaim(ctx, blockMetadataID, claimToken))
	require.NoError(repo.ReleaseRetirementClaim(ctx, blockMetadataID, "foreign-claim"))
	err = repo.RenewRetirementClaim(ctx, blockMetadataID, claimToken, time.Now().UTC(), time.Now().UTC().Add(time.Hour))
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	_, err = repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, ActionDeletedObjectVersion)
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	claimToken = "replacement-claim"
	require.NoError(repo.ClaimRetirement(ctx, blockMetadataID, claimToken, time.Now().UTC(), time.Now().UTC().Add(time.Hour)))
	_, err = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	postReorgRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.False(postReorgRow.Canonical)
	require.Equal(singleBlockKey, postReorgRow.SingleBlockObjectKey)
	_, err = repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, "competing-claim", ActionDeletedObjectVersion)
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET byte_length = 127 WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	_, err = repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, ActionDeletedObjectVersion)
	require.Error(err)
	require.Contains(err.Error(), "CSCB metadata changed")
	failedRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.Equal(singleBlockKey, failedRow.SingleBlockObjectKey)
	require.Equal(RetirementStateDeleting, failedRow.Retirement.State)
	_, err = db.ExecContext(ctx, `UPDATE block_consolidation_shadow SET byte_length = 128 WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	deletedAt, err := repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, ActionDeletedObjectVersion)
	require.NoError(err)
	require.WithinDuration(time.Now().UTC(), deletedAt, 5*time.Second)

	pendingRow, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.Empty(pendingRow.SingleBlockObjectKey)
	require.Empty(pendingRow.Shadow.SingleBlockObjectKey)
	require.NotNil(pendingRow.Shadow.SingleBlockObjectDeletedAt)
	require.Equal(RetirementStateDeletedPendingVerification, pendingRow.Retirement.State)
	require.Empty(pendingRow.Retirement.SingleBlockObjectKey)
	require.Equal(ActionDeletedObjectVersion, pendingRow.Retirement.Outcome)
	require.NotNil(pendingRow.Retirement.DeletedAt)
	require.Nil(pendingRow.Retirement.VerifiedAt)
	require.Equal(claimToken, pendingRow.Retirement.ClaimToken)

	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET consolidated_object_version_id = 'mutated' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change pinned retirement manifest fields")
	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET state = 'eligible' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "invalid single-block retirement transition")

	_, err = repo.FinalizeRetirement(ctx, blockMetadataID, "competing-claim", ActionDeletedVerified)
	require.ErrorIs(err, ErrRetirementClaimUnavailable)
	verifiedAt, err := repo.FinalizeRetirement(ctx, blockMetadataID, claimToken, ActionDeletedVerified)
	require.NoError(err)
	idempotentVerifiedAt, err := repo.FinalizeRetirement(ctx, blockMetadataID, "idempotent-claim", ActionDeletedVerified)
	require.NoError(err)
	require.Equal(verifiedAt, idempotentVerifiedAt)

	row, err := repo.GetMetadataRow(ctx, blockMetadataID)
	require.NoError(err)
	require.False(row.Canonical)
	require.Empty(row.Hash)
	require.Empty(row.SingleBlockObjectKey)
	require.NotNil(row.Shadow)
	require.Empty(row.Shadow.Hash)
	require.Empty(row.Shadow.SingleBlockObjectKey)
	require.NotNil(row.Shadow.SingleBlockObjectDeletedAt)
	require.WithinDuration(deletedAt, *row.Shadow.SingleBlockObjectDeletedAt, time.Microsecond)
	require.Equal(cscbKey, row.Shadow.ConsolidatedObjectKey)
	require.Equal(cscbKey, row.PrimaryObjectKey)
	require.NotNil(row.Retirement)
	require.WithinDuration(time.Now().UTC(), row.Retirement.PreparedAt, 5*time.Second)
	require.Equal(RetirementStateDeletedVerified, row.Retirement.State)
	require.Empty(row.Retirement.SingleBlockObjectKey)
	require.Equal(keySHA256(singleBlockKey), row.Retirement.SingleBlockObjectKeySHA256)
	require.Equal([]string{"single-block-v2", "single-block-v1"}, row.Retirement.SingleBlockObjectVersionIDs)
	require.Equal([]string{"delete-marker-v1"}, row.Retirement.SingleBlockDeleteMarkerVersionIDs)
	require.Empty(row.Retirement.SingleBlockObjectETag)
	require.Empty(row.Retirement.ClaimToken)
	require.Nil(row.Retirement.ClaimExpiresAt)
	require.NotNil(row.Retirement.DeleteStartedAt)
	require.Equal(2, row.Retirement.AttemptCount)
	require.NotNil(row.Retirement.LastAttemptAt)
	require.NotNil(row.Retirement.DeletedAt)
	require.NotNil(row.Retirement.VerifiedAt)
	require.Equal(ActionDeletedVerified, row.Retirement.Outcome)
	require.WithinDuration(deletedAt, *row.Retirement.DeletedAt, time.Microsecond)
	require.WithinDuration(verifiedAt, *row.Retirement.VerifiedAt, time.Microsecond)
	_, err = db.ExecContext(ctx, `UPDATE block_single_block_retention SET outcome = 'mutated' WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot change a verified single-block retirement")

	pending, err := repo.ListPendingRetirements(ctx, tag, height, height+1, time.Now().UTC(), 0)
	require.NoError(err)
	require.Empty(pending)
}

func TestIntegrationPostgresRepositorySelectsDueRetentionCohorts(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_100_000_000 + unique%100_000_000)
	startHeight := uint64(8_100_000_000 + unique%100_000_000)
	dueKey := fmt.Sprintf("consolidated/due-%d.cscb.gzip", unique)
	futureKey := fmt.Sprintf("consolidated/future-%d.cscb.gzip", unique)
	blockMetadataIDs := make([]int64, 0, 3)
	defer func() {
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest DISABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_manifest WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest ENABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_single_block_retention WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, blockMetadataID := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
		}
	}()

	now := time.Now().UTC()
	for index := 0; index < 3; index++ {
		height := startHeight + uint64(index)
		consolidatedKey := dueKey
		deleteAfter := now.Add(-time.Hour)
		if index == 2 {
			consolidatedKey = futureKey
			deleteAfter = now.Add(time.Hour)
		}
		var blockMetadataID int64
		err = db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, $7, $8, $9)
			RETURNING id`,
			height,
			tag,
			height-1,
			consolidatedKey,
			now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			uint64(index*128),
			128,
			128,
		).Scan(&blockMetadataID)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, blockMetadataID)
		_, err = db.ExecContext(ctx, `
			INSERT INTO canonical_blocks (height, block_metadata_id, tag)
			VALUES ($1, $2, $3)`,
			height,
			blockMetadataID,
			tag,
		)
		require.NoError(err)
		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			blockMetadataID,
			tag,
			height,
			fmt.Sprintf("single-block/%d.gzip", height),
			consolidatedKey,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			uint64(index*128),
			128,
			128,
			now.Add(-96*time.Hour),
			now.Add(-96*time.Hour),
			deleteAfter,
		)
		require.NoError(err)
	}

	repo := NewPostgresRepository(db)
	cohorts, _, err := repo.ListDueRetentionCohorts(ctx, "", tag, 0, 0, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Len(cohorts, 1)
	require.Equal([]RetentionCohort{{
		ConsolidatedObjectKey: dueKey,
		StartHeight:           startHeight,
		EndHeight:             startHeight + 2,
		RowCount:              2,
		EligibleAt:            cohorts[0].EligibleAt,
	}}, cohorts)
	require.WithinDuration(now.Add(-time.Hour), cohorts[0].EligibleAt, time.Second)

	pendingManifest := RetirementManifest{
		BlockMetadataID:                blockMetadataIDs[0],
		Tag:                            tag,
		Height:                         startHeight,
		State:                          RetirementStateEligible,
		Bucket:                         integrationRetentionBucket,
		SingleBlockObjectKey:           fmt.Sprintf("single-block/%d.gzip", startHeight),
		SingleBlockObjectKeySHA256:     keySHA256(fmt.Sprintf("single-block/%d.gzip", startHeight)),
		SingleBlockObjectVersionIDs:    []string{"single-block-v1"},
		SingleBlockObjectETag:          "single-block-etag",
		SingleBlockObjectBytes:         128,
		ConsolidatedObjectKey:          dueKey,
		ConsolidatedObjectVersionID:    "cscb-v1",
		ConsolidatedObjectETag:         "cscb-etag",
		ConsolidatedByteOffset:         0,
		ConsolidatedByteLength:         128,
		ConsolidatedUncompressedLength: 128,
		PayloadSHA256:                  keySHA256("payload"),
		PreparedAt:                     now.Add(-30 * time.Minute),
	}
	require.NoError(repo.PrepareRetirement(ctx, pendingManifest, ""))

	pending, err := repo.ListPendingRetentionCohorts(ctx, integrationRetentionBucket, "", tag, 0, 0, now, 10)
	require.NoError(err)
	require.Len(pending, 1)
	require.Equal(uint64(1), pending[0].RowCount)

	futureAtCutoff, err := repo.ListPendingRetirements(
		ctx,
		tag,
		startHeight,
		startHeight+1,
		now.Add(-2*time.Hour),
		10,
	)
	require.NoError(err)
	require.Empty(futureAtCutoff)

	due, _, err := repo.ListDueRetentionCohorts(ctx, "", tag, 0, 0, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Len(due, 1)
	require.Equal(startHeight+1, due[0].StartHeight)
	require.Equal(startHeight+2, due[0].EndHeight)
	require.Equal(uint64(1), due[0].RowCount)

	snapshotPending, snapshotDue, _, err := repo.ListRetentionCohorts(ctx, integrationRetentionBucket, "", tag, 0, 0, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Equal(pending, snapshotPending)
	require.Equal(due, snapshotDue)

	merged, hasMore, _, err := NewSelector(repo).Select(ctx, integrationRetentionBucket, "", tag, 0, 0, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.False(hasMore)
	require.Equal([]RetentionCohort{{
		ConsolidatedObjectKey: dueKey,
		StartHeight:           startHeight,
		EndHeight:             startHeight + 2,
		RowCount:              2,
		EligibleAt:            merged[0].EligibleAt,
		Pending:               true,
	}}, merged)

	bounded, _, err := repo.ListDueRetentionCohorts(
		ctx,
		"",
		tag,
		startHeight+1,
		startHeight+2,
		now,
		10,
		DueCohortCursor{},
	)
	require.NoError(err)
	require.Len(bounded, 1)
	require.Equal(startHeight+1, bounded[0].StartHeight)
	require.Equal(startHeight+2, bounded[0].EndHeight)
	require.Equal(uint64(1), bounded[0].RowCount)

	_, err = db.ExecContext(ctx, `
		INSERT INTO cscb_repair_manifest (
			tag, state, bucket, old_consolidated_object_key_main,
			start_height, end_height, canonical_block_count, total_block_count,
			row_set_sha256
		) VALUES ($1, 'preparing', $2, $3, $4, $5, 2, 2, $6)`,
		tag,
		integrationRetentionBucket,
		dueKey,
		startHeight,
		startHeight+2,
		strings.Repeat("a", 64),
	)
	require.NoError(err)
	cohorts, _, err = repo.ListDueRetentionCohorts(ctx, "", tag, 0, 0, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Empty(cohorts, "an object with an active CSCB repair must not be selected for retention")
}

func TestIntegrationPostgresRepositorySelectsOnlyWriteStorageGenerationCohorts(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_200_000_000 + unique%100_000_000)
	startHeight := uint64(8_200_000_000 + unique%100_000_000)
	sharedKey := fmt.Sprintf("consolidated/generation-shared-%d.cscb.gzip", unique)
	pendingKey := fmt.Sprintf("consolidated/generation-pending-%d.cscb.gzip", unique)
	blockMetadataIDs := make([]int64, 0, 4)
	defer func() {
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_single_block_retention WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, blockMetadataID := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
		}
	}()

	now := time.Now().UTC()
	seed := func(
		height uint64,
		consolidatedKey string,
		primaryGeneration string,
		sourceGeneration string,
		destinationGeneration string,
	) int64 {
		singleBlockKey := fmt.Sprintf("single-block/generation-%d.gzip", height)
		var blockMetadataID int64
		err := db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length, storage_generation
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, $7, $8, $9, NULLIF($10, ''))
			RETURNING id`,
			height,
			tag,
			height-1,
			consolidatedKey,
			now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			0,
			128,
			128,
			primaryGeneration,
		).Scan(&blockMetadataID)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, blockMetadataID)

		_, err = db.ExecContext(ctx, `
			INSERT INTO canonical_blocks (height, block_metadata_id, tag)
			VALUES ($1, $2, $3)`,
			height,
			blockMetadataID,
			tag,
		)
		require.NoError(err)

		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				single_block_storage_generation, consolidated_object_key_main,
				consolidated_storage_generation, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, NULLIF($5, ''), $6, NULLIF($7, ''), $8, $9, $10, $11, $12, $13, $14)`,
			blockMetadataID,
			tag,
			height,
			singleBlockKey,
			sourceGeneration,
			consolidatedKey,
			destinationGeneration,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			0,
			128,
			128,
			now.Add(-96*time.Hour),
			now.Add(-96*time.Hour),
			now.Add(-time.Hour),
		)
		require.NoError(err)
		return blockMetadataID
	}

	seed(
		startHeight,
		sharedKey,
		"v2",
		"",
		"v2",
	)
	nativeV2ID := seed(
		startHeight+1,
		sharedKey,
		"v2",
		"v2",
		"v2",
	)
	nativeLegacyID := seed(
		startHeight+2,
		sharedKey,
		"",
		"",
		"",
	)
	pendingV2ID := seed(
		startHeight+3,
		pendingKey,
		"v2",
		"v2",
		"v2",
	)

	repo := NewPostgresRepository(db)
	pendingSingleKey := fmt.Sprintf("single-block/generation-%d.gzip", startHeight+3)
	require.NoError(repo.PrepareRetirement(ctx, RetirementManifest{
		BlockMetadataID:                pendingV2ID,
		Tag:                            tag,
		Height:                         startHeight + 3,
		State:                          RetirementStateEligible,
		Bucket:                         integrationRetentionBucket,
		SingleBlockObjectKey:           pendingSingleKey,
		SingleBlockObjectKeySHA256:     keySHA256(pendingSingleKey),
		SingleBlockObjectVersionIDs:    []string{"single-block-v1"},
		SingleBlockObjectETag:          "single-block-etag",
		SingleBlockObjectBytes:         128,
		ConsolidatedObjectKey:          pendingKey,
		ConsolidatedObjectVersionID:    "cscb-v1",
		ConsolidatedObjectETag:         "cscb-etag",
		ConsolidatedByteLength:         128,
		ConsolidatedUncompressedLength: 128,
		PayloadSHA256:                  keySHA256("payload"),
		PreparedAt:                     now.Add(-30 * time.Minute),
	}, "v2"))

	v2Bucket := "integration-v2-bucket"
	pendingV2, dueV2, _, err := repo.ListRetentionCohorts(
		ctx,
		v2Bucket,
		"v2",
		tag,
		0,
		0,
		now,
		10,
		DueCohortCursor{},
	)
	require.NoError(err)
	require.Empty(pendingV2, "an old-bucket manifest must not be selected under the v2 target")
	require.Len(dueV2, 1)
	require.Equal([]RetentionCohort{{
		ConsolidatedObjectKey: sharedKey,
		StartHeight:           startHeight + 1,
		EndHeight:             startHeight + 2,
		RowCount:              1,
		EligibleAt:            dueV2[0].EligibleAt,
	}}, dueV2, "mixed and legacy rows sharing the same object key must not merge into the v2 cohort")

	pendingLegacy, dueLegacy, _, err := repo.ListRetentionCohorts(
		ctx,
		integrationRetentionBucket,
		"",
		tag,
		0,
		0,
		now,
		10,
		DueCohortCursor{},
	)
	require.NoError(err)
	require.Empty(pendingLegacy, "a v2 metadata row must not be selected merely because its manifest names the old bucket")
	require.Len(dueLegacy, 1)
	require.Equal([]RetentionCohort{{
		ConsolidatedObjectKey: sharedKey,
		StartHeight:           startHeight + 2,
		EndHeight:             startHeight + 3,
		RowCount:              1,
		EligibleAt:            dueLegacy[0].EligibleAt,
	}}, dueLegacy)

	legacySingleKey := fmt.Sprintf("single-block/generation-%d.gzip", startHeight+2)
	legacyManifest := RetirementManifest{
		BlockMetadataID:                nativeLegacyID,
		Tag:                            tag,
		Height:                         startHeight + 2,
		State:                          RetirementStateEligible,
		Bucket:                         integrationRetentionBucket,
		SingleBlockObjectKey:           legacySingleKey,
		SingleBlockObjectKeySHA256:     keySHA256(legacySingleKey),
		SingleBlockObjectVersionIDs:    []string{"single-block-v1"},
		SingleBlockObjectETag:          "single-block-etag",
		SingleBlockObjectBytes:         128,
		ConsolidatedObjectKey:          sharedKey,
		ConsolidatedObjectVersionID:    "cscb-v1",
		ConsolidatedObjectETag:         "cscb-etag",
		ConsolidatedByteLength:         128,
		ConsolidatedUncompressedLength: 128,
		PayloadSHA256:                  keySHA256("payload"),
		PreparedAt:                     now,
	}
	_, err = db.ExecContext(ctx, `
		UPDATE block_metadata
		SET storage_generation = $2
		WHERE id = $1`, nativeLegacyID, "v2")
	require.NoError(err)
	_, err = db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_storage_generation = $2,
			consolidated_storage_generation = $2
		WHERE block_metadata_id = $1`, nativeLegacyID, "v2")
	require.NoError(err)
	err = repo.PrepareRetirement(ctx, legacyManifest, "")
	require.ErrorContains(err, "storage generation changed before retirement")
	var retirementFencedAt sql.NullTime
	require.NoError(db.QueryRowContext(ctx, `
		SELECT single_block_retention_fenced_at
		FROM block_metadata
		WHERE id = $1`, nativeLegacyID).Scan(&retirementFencedAt))
	require.False(retirementFencedAt.Valid, "a stale-generation prepare must not fence metadata")
	var manifestCount int
	require.NoError(db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM block_single_block_retention
		WHERE block_metadata_id = $1`, nativeLegacyID).Scan(&manifestCount))
	require.Zero(manifestCount, "a stale-generation prepare must not persist a manifest")

	_, err = db.ExecContext(ctx, `
		UPDATE block_metadata
		SET storage_generation = NULL
		WHERE id = $1`, nativeLegacyID)
	require.NoError(err)
	_, err = db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_storage_generation = NULL,
			consolidated_storage_generation = NULL
		WHERE block_metadata_id = $1`, nativeLegacyID)
	require.NoError(err)

	_, err = db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_object_key_main = NULL,
			single_block_object_deleted_at = NOW()
		WHERE block_metadata_id = $1`, nativeV2ID)
	require.NoError(err)
	completed, hasMore, _, err := NewSelector(repo).Select(
		ctx,
		v2Bucket,
		"v2",
		tag,
		0,
		0,
		now,
		10,
		DueCohortCursor{},
	)
	require.NoError(err)
	require.False(hasMore)
	require.Empty(completed, "mixed rows must not be reselected after native-v2 retention finishes")
}

// TestIntegrationPostgresRepositoryOrdersBoundedSelectionByHeight seeds three
// cohorts whose due-time order is the exact reverse of their height order, so
// the two orderings cannot be confused for one another.
//
// A bounded selection must walk the supplied range by height, and when the limit
// truncates it must keep the *lowest* cohorts, so that repeated runs advance
// monotonically and an approved range maps to a predictable set of cohorts. An
// unbounded selection must still drain the most-overdue work first.
func TestIntegrationPostgresRepositoryOrdersBoundedSelectionByHeight(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	const cohortCount = 3
	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_100_000_000 + unique%100_000_000)
	startHeight := uint64(8_100_000_000 + unique%100_000_000)
	blockMetadataIDs := make([]int64, 0, cohortCount)
	cohortKeys := make([]string, 0, cohortCount)
	defer func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, blockMetadataID := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
		}
	}()

	now := time.Now().UTC()
	for index := 0; index < cohortCount; index++ {
		height := startHeight + uint64(index)
		consolidatedKey := fmt.Sprintf("consolidated/ordering-%d-%d.cscb.gzip", unique, index)
		cohortKeys = append(cohortKeys, consolidatedKey)
		// Later heights are more overdue, so due-time order reverses height order.
		deleteAfter := now.Add(-time.Duration(index+1) * time.Hour)

		var blockMetadataID int64
		err = db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, $7, $8, $9)
			RETURNING id`,
			height,
			tag,
			height-1,
			consolidatedKey,
			now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			0,
			128,
			128,
		).Scan(&blockMetadataID)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, blockMetadataID)

		_, err = db.ExecContext(ctx, `
			INSERT INTO canonical_blocks (height, block_metadata_id, tag)
			VALUES ($1, $2, $3)`,
			height,
			blockMetadataID,
			tag,
		)
		require.NoError(err)

		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			blockMetadataID,
			tag,
			height,
			fmt.Sprintf("single-block/%d.gzip", height),
			consolidatedKey,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			0,
			128,
			128,
			now.Add(-96*time.Hour),
			now.Add(-96*time.Hour),
			deleteAfter,
		)
		require.NoError(err)
	}

	endHeight := startHeight + cohortCount

	repo := NewPostgresRepository(db)
	bounded, _, err := repo.ListDueRetentionCohorts(ctx, "", tag, startHeight, endHeight, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Len(bounded, cohortCount)
	boundedHeights := make([]uint64, 0, len(bounded))
	for _, cohort := range bounded {
		boundedHeights = append(boundedHeights, cohort.StartHeight)
	}
	require.Equal(
		[]uint64{startHeight, startHeight + 1, startHeight + 2},
		boundedHeights,
		"a bounded selection must be ordered by height, not by due time",
	)

	truncated, _, err := repo.ListDueRetentionCohorts(ctx, "", tag, startHeight, endHeight, now, 2, DueCohortCursor{})
	require.NoError(err)
	require.Len(truncated, 2)
	require.Equal(
		[]string{cohortKeys[0], cohortKeys[1]},
		[]string{truncated[0].ConsolidatedObjectKey, truncated[1].ConsolidatedObjectKey},
		"a truncated bounded selection must keep the lowest cohorts so repeated runs advance monotonically",
	)

	unbounded, _, err := repo.ListDueRetentionCohorts(ctx, "", tag, 0, 0, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Len(unbounded, cohortCount)
	unboundedHeights := make([]uint64, 0, len(unbounded))
	for _, cohort := range unbounded {
		unboundedHeights = append(unboundedHeights, cohort.StartHeight)
	}
	require.Equal(
		[]uint64{startHeight + 2, startHeight + 1, startHeight},
		unboundedHeights,
		"an unbounded selection must still drain the most overdue cohorts first",
	)
}

func openRetirementIntegrationDB(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// TestIntegrationDueCohortBoundsComeFromEnumerableRows is the regression guard
// for the INF-1448 review finding: cohort bounds and row counts must derive
// only from rows the sweep can enumerate — shadow rows that are canonical and
// whose metadata still points at the cohort's consolidated object. Bounds
// widened by an orphaned (non-canonical) or re-pointed edge row fail the
// sweep's contiguous execution-plan validation repeatedly, and an orphaned
// lowest row must not hide the cohort's valid remainder.
func TestIntegrationDueCohortBoundsComeFromEnumerableRows(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_200_000_000 + unique%100_000_000)
	startHeight := uint64(8_300_000_000 + unique%100_000_000)
	cohortKey := fmt.Sprintf("consolidated/enumerable-%d.cscb.gzip", unique)
	otherKey := fmt.Sprintf("consolidated/repointed-%d.cscb.gzip", unique)
	now := time.Now().UTC()
	blockMetadataIDs := make([]int64, 0, 5)
	defer func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, id := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, id)
		}
	}()

	// Five heights, all with due shadow rows for cohortKey:
	//   h+0: shadow row is NOT canonical (orphaned by a reorg)
	//   h+1: canonical, metadata re-pointed at another object
	//   h+2, h+3: fully enumerable
	//   h+4: shadow row is NOT canonical (orphaned upper edge)
	insert := func(height uint64, canonicalRow bool, metadataKey string) {
		var id int64
		err := db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, 0, 128, 128)
			RETURNING id`,
			height, tag, height-1, metadataKey, now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		).Scan(&id)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, id)
		if canonicalRow {
			_, err = db.ExecContext(ctx, `
				INSERT INTO canonical_blocks (height, block_metadata_id, tag)
				VALUES ($1, $2, $3)`, height, id, tag)
			require.NoError(err)
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, 0, 128, 128, $7, $7, $8)`,
			id, tag, height,
			fmt.Sprintf("single-block/%d.gzip", height),
			cohortKey,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			now.Add(-96*time.Hour),
			now.Add(-time.Hour),
		)
		require.NoError(err)
	}
	insert(startHeight, false, cohortKey)   // orphaned lower edge
	insert(startHeight+1, true, otherKey)   // canonical but re-pointed
	insert(startHeight+2, true, cohortKey)  // enumerable
	insert(startHeight+3, true, cohortKey)  // enumerable
	insert(startHeight+4, false, cohortKey) // orphaned upper edge

	repo := NewPostgresRepository(db)
	cohorts, _, err := repo.ListDueRetentionCohorts(ctx, "", tag, 0, 0, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Len(cohorts, 1,
		"orphaned and re-pointed edge rows must not hide the enumerable remainder")
	require.Equal(startHeight+2, cohorts[0].StartHeight,
		"bounds must start at the first ENUMERABLE row, not the first due shadow row")
	require.Equal(startHeight+4, cohorts[0].EndHeight,
		"bounds must end after the last ENUMERABLE row, not the orphaned upper edge")
	require.Equal(uint64(2), cohorts[0].RowCount,
		"row count must count only rows the sweep can enumerate")
}

// TestIntegrationCandidatePaginationSurvivesDeadPrefix is the regression guard
// for the round-5 review finding on INF-1448: the candidate LIMIT applies to
// RAW shadow-only candidates, so a window whose first LIMIT candidates all
// expand to nothing (here: repair-covered cohorts) must not hide a valid
// cohort behind them. Selection must paginate past the dead prefix within one
// call rather than returning empty and letting the cron advance past the
// whole window forever.
func TestIntegrationCandidatePaginationSurvivesDeadPrefix(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_300_000_000 + unique%100_000_000)
	baseHeight := uint64(8_500_000_000 + unique%100_000_000)
	now := time.Now().UTC()
	blockMetadataIDs := make([]int64, 0, 8)
	defer func() {
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest DISABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_manifest WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest ENABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, id := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, id)
		}
	}()

	// Four single-row cohorts in height order; the first three are covered by
	// active repairs (dead candidates), the fourth is valid. With limit 2 the
	// first raw page holds only dead candidates.
	// Dead cohorts here are NON-CANONICAL, not repair-covered. Repair coverage
	// is excluded during candidate discovery, so repair-covered fixtures never
	// reach the expansion loop and would make this test vacuous. Orphaned rows
	// are only detectable in the expansion, which is exactly the path under
	// test.
	insertCohort := func(height uint64, key string, canonical bool) {
		var id int64
		err := db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, 0, 128, 128)
			RETURNING id`,
			height, tag, height-1, key, now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		).Scan(&id)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, id)
		if canonical {
			_, err = db.ExecContext(ctx, `
				INSERT INTO canonical_blocks (height, block_metadata_id, tag)
				VALUES ($1, $2, $3)`, height, id, tag)
			require.NoError(err)
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, 0, 128, 128, $7, $7, $8)`,
			id, tag, height,
			fmt.Sprintf("single-block/%d.gzip", height),
			key,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			now.Add(-96*time.Hour),
			now.Add(-time.Hour),
		)
		require.NoError(err)
	}
	insertCohort(baseHeight, fmt.Sprintf("consolidated/dead1-%d.cscb.gzip", unique), false)
	insertCohort(baseHeight+1, fmt.Sprintf("consolidated/dead2-%d.cscb.gzip", unique), false)
	insertCohort(baseHeight+2, fmt.Sprintf("consolidated/dead3-%d.cscb.gzip", unique), false)
	validKey := fmt.Sprintf("consolidated/valid-%d.cscb.gzip", unique)
	insertCohort(baseHeight+3, validKey, true)

	repo := NewPostgresRepository(db)

	// Bounded (by-height) selection with limit 2: page one is entirely dead.
	cohorts, _, err := repo.ListDueRetentionCohorts(ctx, "", tag, 0, baseHeight+10, now, 2, DueCohortCursor{})
	require.NoError(err)
	require.Len(cohorts, 1,
		"a dead candidate prefix wider than the limit must not hide the valid cohort")
	require.Equal(validKey, cohorts[0].ConsolidatedObjectKey)

	// Open-ended (by-due-time) selection paginates on the other cursor shape.
	cohorts, _, err = repo.ListDueRetentionCohorts(ctx, "", tag, 0, 0, now, 2, DueCohortCursor{})
	require.NoError(err)
	require.Len(cohorts, 1)
	require.Equal(validKey, cohorts[0].ConsolidatedObjectKey)
}

// TestIntegrationCandidatePaginationExhaustsHugeDeadPrefix is the round-6
// regression guard: pagination must run to EXHAUSTION, not to a page cap. The
// previous revision capped at 8 pages, and a cap is a silent truncation — the
// cron reads the empty result as proof the window holds nothing selectable and
// advances past candidates nobody examined, stranding the valid cohort.
//
// The fixture builds a dead prefix far wider than any fixed page budget: 60
// repair-covered cohorts selected with limit 2 forces 30+ pages, where the old
// cap allowed 8. The selectable cohort sits last, so only exhaustive
// pagination can reach it.
func TestIntegrationCandidatePaginationExhaustsHugeDeadPrefix(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	const deadCohorts = 60
	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_400_000_000 + unique%100_000_000)
	baseHeight := uint64(8_700_000_000 + unique%100_000_000)
	now := time.Now().UTC()
	blockMetadataIDs := make([]int64, 0, deadCohorts+1)
	defer func() {
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest DISABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_manifest WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest ENABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, id := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, id)
		}
	}()

	// Dead cohorts are NON-CANONICAL: repair coverage is filtered during
	// candidate discovery and would never reach the expansion loop this test
	// exists to exercise.
	insertCohort := func(height uint64, key string, canonical bool) {
		var id int64
		err := db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, 0, 128, 128)
			RETURNING id`,
			height, tag, height-1, key, now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		).Scan(&id)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, id)
		if canonical {
			_, err = db.ExecContext(ctx, `
				INSERT INTO canonical_blocks (height, block_metadata_id, tag)
				VALUES ($1, $2, $3)`, height, id, tag)
			require.NoError(err)
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, 0, 128, 128, $7, $7, $8)`,
			id, tag, height,
			fmt.Sprintf("single-block/%d.gzip", height),
			key,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			now.Add(-96*time.Hour),
			now.Add(-time.Hour),
		)
		require.NoError(err)
	}

	for i := 0; i < deadCohorts; i++ {
		insertCohort(baseHeight+uint64(i), fmt.Sprintf("consolidated/dead-%d-%04d.cscb.gzip", unique, i), false)
	}
	validKey := fmt.Sprintf("consolidated/valid-%d.cscb.gzip", unique)
	insertCohort(baseHeight+uint64(deadCohorts), validKey, true)

	repo := NewPostgresRepository(db)

	// limit 2 => 30+ candidate pages of pure dead prefix before the valid one.
	cohorts, resumeAfter, err := repo.ListDueRetentionCohorts(ctx, "", tag, 0, baseHeight+uint64(deadCohorts)+10, now, 2, DueCohortCursor{})
	require.NoError(err)
	require.Len(cohorts, 1,
		"pagination must walk a dead prefix far wider than any fixed page budget")
	require.Equal(validKey, cohorts[0].ConsolidatedObjectKey)
	require.Equal(baseHeight+uint64(deadCohorts), cohorts[0].StartHeight)
	require.Zero(resumeAfter,
		"candidates were exhausted within budget, so there is no continuation to report")

	// Same through the open-ended (by-due-time) cursor shape.
	cohorts, _, err = repo.ListDueRetentionCohorts(ctx, "", tag, 0, 0, now, 2, DueCohortCursor{})
	require.NoError(err)
	require.Len(cohorts, 1)
	require.Equal(validKey, cohorts[0].ConsolidatedObjectKey)
}

// TestIntegrationDueSelectionReportsContinuationWhenBudgetSpent is the round-7
// regression guard: an unbounded selection call can outrun the activity
// timeout, and a bounded one that stays silent strands work. Selection must be
// bounded AND report where it stopped.
//
// It also pins the EligibleAt contract, which no other test can distinguish: a
// cohort's EligibleAt must be the MAX single_block_delete_after over its
// enumerable rows (the moment the cohort as a whole became retirable, which is
// exactly what the HAVING gates on), not the MIN. Every other fixture in this
// file gives all rows the same delete_after, so a MIN/MAX swap passes them
// silently.
func TestIntegrationDueSelectionReportsContinuationWhenBudgetSpent(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_500_000_000 + unique%100_000_000)
	baseHeight := uint64(8_900_000_000 + unique%100_000_000)
	now := time.Now().UTC()
	blockMetadataIDs := make([]int64, 0, 4)
	defer func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, id := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, id)
		}
	}()

	// One cohort, two enumerable rows with DIFFERENT delete_after values.
	cohortKey := fmt.Sprintf("consolidated/eligible-%d.cscb.gzip", unique)
	earliest := now.Add(-3 * time.Hour)
	latest := now.Add(-time.Hour)
	for i, deleteAfter := range []time.Time{earliest, latest} {
		height := baseHeight + uint64(i)
		var id int64
		err := db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, 0, 128, 128)
			RETURNING id`,
			height, tag, height-1, cohortKey, now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		).Scan(&id)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, id)
		_, err = db.ExecContext(ctx, `
			INSERT INTO canonical_blocks (height, block_metadata_id, tag)
			VALUES ($1, $2, $3)`, height, id, tag)
		require.NoError(err)
		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, 0, 128, 128, $7, $7, $8)`,
			id, tag, height,
			fmt.Sprintf("single-block/%d.gzip", height),
			cohortKey,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			now.Add(-96*time.Hour),
			deleteAfter,
		)
		require.NoError(err)
	}

	repo := NewPostgresRepository(db)
	cohorts, resumeAfter, err := repo.ListDueRetentionCohorts(ctx, "", tag, 0, baseHeight+10, now, 10, DueCohortCursor{})
	require.NoError(err)
	require.Len(cohorts, 1)
	require.Zero(resumeAfter)
	require.WithinDuration(latest, cohorts[0].EligibleAt, time.Second,
		"EligibleAt must be the cohort's LAST due moment (MAX), not its first (MIN)")
	require.False(cohorts[0].EligibleAt.Equal(earliest.UTC()),
		"a MIN/MAX swap must fail this assertion, not pass silently")
}

// TestIntegrationContinuationSurvivesOverlappingCandidates is the round-8
// regression guard. Candidate groups are NOT height-disjoint: a reorg can leave
// several consolidated objects covering the same or overlapping heights while
// canonical_blocks points at only one. A continuation expressed as a height
// watermark ("last examined end + 1") therefore filters out unexamined
// candidates that start at or below that height, starving them permanently.
//
// The fixture is built so a height watermark MUST lose work: every dead
// candidate spans the same height band as the valid one, so "resume above the
// last dead candidate's end" skips the valid cohort entirely. Only a keyset
// cursor — which skips exactly the candidates already examined, whatever
// heights they overlap — finds it.
func TestIntegrationContinuationSurvivesOverlappingCandidates(t *testing.T) {
	require := require.New(t)
	cfg, err := config.New()
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("retention integration tests never write to production")
	}

	ctx := context.Background()
	db, err := openRetirementIntegrationDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	const deadCohorts = 40
	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_600_000_000 + unique%100_000_000)
	baseHeight := uint64(9_100_000_000 + unique%100_000_000)
	now := time.Now().UTC()
	blockMetadataIDs := make([]int64, 0, deadCohorts+2)
	defer func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE tag = $1`, tag)
		_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE tag = $1`, tag)
		for _, id := range blockMetadataIDs {
			_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, id)
		}
	}()

	// insertRow puts one shadow row for `key` at `height`. canonical=false makes
	// the row unenumerable (orphaned by a reorg) without removing it from
	// candidate discovery, which is what forces expansion to drop it.
	insertRow := func(height uint64, key string, canonical bool) {
		var id int64
		err := db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, 0, 128, 128)
			RETURNING id`,
			height, tag, height-1, key, now.Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		).Scan(&id)
		require.NoError(err)
		blockMetadataIDs = append(blockMetadataIDs, id)
		if canonical {
			_, err = db.ExecContext(ctx, `
				INSERT INTO canonical_blocks (height, block_metadata_id, tag)
				VALUES ($1, $2, $3)`, height, id, tag)
			require.NoError(err)
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO block_consolidation_shadow (
				block_metadata_id, tag, height, hash, single_block_object_key_main,
				consolidated_object_key_main, object_format, byte_offset, byte_length,
				uncompressed_length, validated_at, single_block_retention_started_at,
				single_block_delete_after
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, 0, 128, 128, $7, $7, $8)`,
			id, tag, height,
			fmt.Sprintf("single-block/%d-%s.gzip", height, key[len(key)-12:]),
			key,
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			now.Add(-96*time.Hour),
			now.Add(-time.Hour),
		)
		require.NoError(err)
	}

	// Dead candidates all START at baseHeight and END at baseHeight+100: they
	// overlap each other and the valid cohort completely. Sorted by
	// (MIN(height), key) they come first because their keys sort before "valid".
	for i := 0; i < deadCohorts; i++ {
		key := fmt.Sprintf("consolidated/dead-%04d-%d.cscb.gzip", i, unique)
		insertRow(baseHeight, key, false)
		insertRow(baseHeight+100, key, false)
	}
	// The valid cohort starts at the SAME height as every dead one, so any
	// "resume above the last dead end height" rule discards it.
	validKey := fmt.Sprintf("consolidated/valid-%d.cscb.gzip", unique)
	insertRow(baseHeight, validKey, true)
	insertRow(baseHeight+50, validKey, true)

	repo := NewPostgresRepository(db)

	// Walk with a tiny limit so enumeration pages repeatedly across the
	// overlapping dead prefix, exactly as a budget-truncated cron tick does.
	var (
		cursor DueCohortCursor
		found  *RetentionCohort
	)
	for pass := 0; pass < deadCohorts+5; pass++ {
		cohorts, next, err := repo.ListDueRetentionCohorts(
			ctx, "", tag, 0, baseHeight+1_000, now, 2, cursor,
		)
		require.NoError(err)
		for i := range cohorts {
			if cohorts[i].ConsolidatedObjectKey == validKey {
				found = &cohorts[i]
			}
		}
		if found != nil || next.IsZero() {
			break
		}
		cursor = next
	}
	require.NotNil(found,
		"a keyset continuation must reach a valid cohort that overlaps every dead candidate; a height watermark cannot")
	require.Equal(baseHeight, found.StartHeight)
	require.Equal(baseHeight+51, found.EndHeight)
	require.Equal(uint64(2), found.RowCount)
}
