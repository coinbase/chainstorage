package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepairlock"
	"github.com/coinbase/chainstorage/internal/storage/generationrehome"
)

func TestIntegrationFencedStorageGenerationRehomeGuard(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}
	ctx := context.Background()
	cfg, err := config.New()
	require.NoError(t, err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	if cfg.Env() == config.EnvProduction {
		t.Skip("fenced generation rehome integration tests never write to production")
	}
	db, err := newDBConnection(ctx, cfg.AWS.Postgres)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, runMigrations(ctx, db))

	t.Run("fenced update requires executing manifest", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		blockMetadataID, _, _, _ := insertFencedRehomeFixture(t, ctx, tx, uint64(time.Now().UnixNano()))

		_, err = tx.ExecContext(ctx, `UPDATE block_metadata SET storage_generation = 'v2' WHERE id = $1`, blockMetadataID)
		require.ErrorContains(t, err, "cannot change storage generation after single-block object retirement is fenced")
	})

	t.Run("exact manifest permits only consolidated generation rehome", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		blockMetadataID, tag, _, objectKey := insertFencedRehomeFixture(t, ctx, tx, uint64(time.Now().UnixNano())+1)

		var auditID int64
		err = tx.QueryRowContext(ctx, `
			INSERT INTO block_storage_generation_rehome (
				evidence_sha256, tag, object_key_main, source_generation, target_generation,
				source_bucket, source_version_id, source_etag,
				destination_bucket, destination_version_id, destination_etag, object_bytes,
				start_height, end_height, expected_block_count, expected_canonical_count,
				expected_fenced_count, expected_deleted_verified_count, state
			) SELECT
				$1, bm.tag, bm.object_key_main, 'legacy', 'v2',
				'legacy-bucket', 'legacy-version', 'legacy-etag',
				'v2-bucket', 'v2-version', 'v2-etag', 128,
				bm.height, bm.height + 1, 1, 1, 1, 1, 'executing'
			FROM block_metadata bm
			WHERE bm.id = $2
			RETURNING id`, fmt.Sprintf("%064x", blockMetadataID), blockMetadataID).Scan(&auditID)
		require.NoError(t, err)

		_, err = tx.ExecContext(ctx, `
			UPDATE block_consolidation_shadow
			SET consolidated_storage_generation = 'v2'
			WHERE block_metadata_id = $1`, blockMetadataID)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `UPDATE block_metadata SET storage_generation = 'v2' WHERE id = $1`, blockMetadataID)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `
			UPDATE block_storage_generation_rehome
			SET state = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
			WHERE id = $1`, auditID)
		require.NoError(t, err)

		var primaryGeneration, consolidatedGeneration sql.NullString
		var singleBlockGeneration sql.NullString
		err = tx.QueryRowContext(ctx, `
			SELECT bm.storage_generation, shadow.consolidated_storage_generation, shadow.single_block_storage_generation
			FROM block_metadata bm
			JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
			WHERE bm.id = $1 AND bm.tag = $2 AND bm.object_key_main = $3`, blockMetadataID, tag, objectKey).Scan(
			&primaryGeneration,
			&consolidatedGeneration,
			&singleBlockGeneration,
		)
		require.NoError(t, err)
		require.Equal(t, "v2", primaryGeneration.String)
		require.Equal(t, "v2", consolidatedGeneration.String)
		require.False(t, singleBlockGeneration.Valid)
	})

	t.Run("repository transaction is verified and idempotent", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, tag, height, objectKey := insertFencedRehomeFixture(t, ctx, tx, uint64(time.Now().UnixNano())+2)
		require.NoError(t, tx.Commit())

		ledger := fmt.Sprintf(`{"type":"audit_object","object_key":%q,"min_height":%d,"end_height_exclusive":%d,"referenced_rows":1,"canonical_rows":1,"valid_placement_rows":1,"consistent_shadow_rows":1,"fenced_rows":1,"deleted_retention_rows":1,"copy_eligible":true}
{"type":"copy_verified","object_key":%q,"source_size":128,"source_etag":"source-etag","source_version_id":"source-version","destination_size":128,"destination_etag":"destination-etag","destination_version_id":"destination-version","verification":"complete"}
`, objectKey, height, height+1, objectKey)
		objects, err := generationrehome.LoadFencedCopyLedger(strings.NewReader(ledger))
		require.NoError(t, err)
		require.Len(t, objects, 1)
		object := objects[0]
		repository := generationrehome.NewPostgresRepository(db)

		before, err := repository.Inspect(ctx, tag, objectKey, "v2", object.EvidenceSHA256)
		require.NoError(t, err)
		require.Equal(t, uint64(1), before.PrimaryLegacyRows)
		require.False(t, before.CompletedAudit)

		alreadyTarget, err := repository.Rehome(ctx, generationrehome.RehomeRequest{
			Tag:               tag,
			SourceBucket:      "legacy-bucket",
			DestinationBucket: "v2-bucket",
			TargetGeneration:  "v2",
			Object:            object,
		})
		require.NoError(t, err)
		require.False(t, alreadyTarget)

		after, err := repository.Inspect(ctx, tag, objectKey, "v2", object.EvidenceSHA256)
		require.NoError(t, err)
		require.Equal(t, uint64(1), after.PrimaryTargetRows)
		require.Equal(t, uint64(1), after.ConsolidatedTargetRows)
		require.True(t, after.CompletedAudit)

		alreadyTarget, err = repository.Rehome(ctx, generationrehome.RehomeRequest{
			Tag:               tag,
			SourceBucket:      "legacy-bucket",
			DestinationBucket: "v2-bucket",
			TargetGeneration:  "v2",
			Object:            object,
		})
		require.NoError(t, err)
		require.True(t, alreadyTarget)
	})

	t.Run("repository waits for repair tag lock before object lock", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, tag, height, objectKey := insertFencedRehomeFixture(t, ctx, tx, uint64(time.Now().UnixNano())+3)
		require.NoError(t, tx.Commit())

		ledger := fmt.Sprintf(`{"type":"audit_object","object_key":%q,"min_height":%d,"end_height_exclusive":%d,"referenced_rows":1,"canonical_rows":1,"valid_placement_rows":1,"consistent_shadow_rows":1,"fenced_rows":1,"deleted_retention_rows":1,"copy_eligible":true}
{"type":"copy_verified","object_key":%q,"source_size":128,"source_etag":"source-etag","source_version_id":"source-version","destination_size":128,"destination_etag":"destination-etag","destination_version_id":"destination-version","verification":"complete"}
`, objectKey, height, height+1, objectKey)
		objects, err := generationrehome.LoadFencedCopyLedger(strings.NewReader(ledger))
		require.NoError(t, err)
		require.Len(t, objects, 1)
		req := generationrehome.RehomeRequest{
			Tag:               tag,
			SourceBucket:      "legacy-bucket",
			DestinationBucket: "v2-bucket",
			TargetGeneration:  "v2",
			Object:            objects[0],
		}

		lockTx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = lockTx.Rollback() }()
		require.NoError(t, cscbrepairlock.AcquireTag(ctx, lockTx, tag))

		blockedCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		_, err = generationrehome.NewPostgresRepository(db).Rehome(blockedCtx, req)
		require.ErrorContains(t, err, "failed to acquire CSCB repair tag lock")
		require.NoError(t, lockTx.Rollback())

		alreadyTarget, err := generationrehome.NewPostgresRepository(db).Rehome(ctx, req)
		require.NoError(t, err)
		require.False(t, alreadyTarget)
	})
}

func insertFencedRehomeFixture(t *testing.T, ctx context.Context, tx *sql.Tx, unique uint64) (int64, uint32, uint64, string) {
	t.Helper()
	tag := uint32(1_700_000_000 + unique%100_000_000)
	height := uint64(8_000_000_000) + unique%100_000_000
	hash := fmt.Sprintf("0x%064x", unique)
	objectKey := fmt.Sprintf("2/consolidated/%d-%d.cscb.zstd", height, unique)
	now := time.Now().UTC()

	var blockMetadataID int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO block_metadata (
			height, tag, hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length,
			single_block_retention_fenced_at
		) VALUES ($1, $2, $3, $4, $5, $6, FALSE, 1, 0, 64, 128, $7)
		RETURNING id`, height, tag, hash, height-1, objectKey, now.Unix(), now).Scan(&blockMetadataID)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO canonical_blocks (height, block_metadata_id, tag)
		VALUES ($1, $2, $3)`, height, blockMetadataID, tag)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, legacy_object_key_main,
			single_block_object_key_main, consolidated_object_key_main, object_format,
			byte_offset, byte_length, uncompressed_length, validated_at,
			single_block_object_deleted_at
		) VALUES ($1, $2, $3, $4, NULL, NULL, $5, 1, 0, 64, 128, $6, $7)`,
		blockMetadataID, tag, height, hash, objectKey, now, now,
	)
	require.NoError(t, err)

	// The fixture starts after the normal retirement state machine has completed;
	// disable only its insert gate while retaining every generation rehome guard.
	_, err = tx.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_insert_trigger`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO block_single_block_retention (
			block_metadata_id, tag, height, hash, state, bucket,
			single_block_object_key_main, single_block_object_key_sha256,
			single_block_object_version_ids, single_block_object_etag, single_block_object_bytes,
			consolidated_object_key_main, consolidated_object_version_id, consolidated_object_etag,
			consolidated_byte_offset, consolidated_byte_length, consolidated_uncompressed_length,
			payload_sha256, outcome, attempt_count, prepared_at, delete_started_at,
			last_attempt_at, deleted_at, verified_at
		) VALUES (
			$1, $2, $3, $4, 'deleted_verified', 'legacy-bucket',
			NULL, $5, ARRAY['single-version'], '', 128,
			$6, 'consolidated-version', 'consolidated-etag', 0, 64, 128,
			$7, 'deleted_and_verified', 1, $8, $8, $8, $8, $8
		)`,
		blockMetadataID,
		tag,
		height,
		hash,
		fmt.Sprintf("%064x", unique+1),
		objectKey,
		fmt.Sprintf("%064x", unique+2),
		now,
	)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_insert_trigger`)
	require.NoError(t, err)
	return blockMetadataID, tag, height, objectKey
}
