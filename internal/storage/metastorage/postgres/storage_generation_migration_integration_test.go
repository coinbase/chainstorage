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
)

const storageGenerationMigrationPath = "db/migrations/20260810000001_add_block_storage_generation.sql"

func TestIntegrationStorageGenerationMigrationDownRejectsNonLegacyRows(t *testing.T) {
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
		t.Skip("storage generation migration integration tests never write to production")
	}

	db, err := newDBConnection(ctx, cfg.AWS.Postgres)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.NoError(t, runMigrations(ctx, db))
	downGuard := loadStorageGenerationDownGuard(t)

	tests := []struct {
		name   string
		insert func(*testing.T, context.Context, *sql.Tx, uint32, uint64, string)
	}{
		{
			name: "primary generation",
			insert: func(t *testing.T, ctx context.Context, tx *sql.Tx, tag uint32, height uint64, hash string) {
				_, err := tx.ExecContext(ctx, `
					INSERT INTO block_metadata (
						height, tag, hash, parent_height, object_key_main, timestamp, skipped,
						object_format, storage_generation
					) VALUES ($1, $2, $3, $4, $5, $6, FALSE, 0, 'v2')`,
					height, tag, hash, height-1, fmt.Sprintf("2/%d/%s", height, hash), time.Now().Unix(),
				)
				require.NoError(t, err)
			},
		},
		{
			name: "single-block shadow generation",
			insert: func(t *testing.T, ctx context.Context, tx *sql.Tx, tag uint32, height uint64, hash string) {
				insertStorageGenerationShadow(t, ctx, tx, tag, height, hash, "v2", "")
			},
		},
		{
			name: "consolidated shadow generation",
			insert: func(t *testing.T, ctx context.Context, tx *sql.Tx, tag uint32, height uint64, hash string) {
				insertStorageGenerationShadow(t, ctx, tx, tag, height, hash, "", "v2")
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()

			unique := uint64(time.Now().UnixNano()) + uint64(index)
			tag := uint32(1_800_000_000 + unique%100_000_000)
			height := uint64(9_000_000_000) + unique%100_000_000
			hash := fmt.Sprintf("0x%064x", unique)
			test.insert(t, ctx, tx, tag, height, hash)

			_, err = tx.ExecContext(ctx, downGuard)
			require.ErrorContains(t, err, "cannot roll back block storage generation migration while non-legacy rows exist")
		})
	}
}

func insertStorageGenerationShadow(
	t *testing.T,
	ctx context.Context,
	tx *sql.Tx,
	tag uint32,
	height uint64,
	hash string,
	singleBlockGeneration string,
	consolidatedGeneration string,
) {
	t.Helper()
	singleBlockKey := fmt.Sprintf("2/%d/%s", height, hash)
	var blockMetadataID int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO block_metadata (
			height, tag, hash, parent_height, object_key_main, timestamp, skipped, object_format
		) VALUES ($1, $2, $3, $4, $5, $6, FALSE, 0)
		RETURNING id`,
		height, tag, hash, height-1, singleBlockKey, time.Now().Unix(),
	).Scan(&blockMetadataID)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, legacy_object_key_main,
			single_block_object_key_main, consolidated_object_key_main, object_format,
			byte_offset, byte_length, uncompressed_length, validated_at,
			single_block_storage_generation, consolidated_storage_generation
		) VALUES ($1, $2, $3, $4, $5, $5, $6, 1, 0, 128, 256, $7, NULLIF($8, ''), NULLIF($9, ''))`,
		blockMetadataID,
		tag,
		height,
		hash,
		singleBlockKey,
		fmt.Sprintf("2/consolidated/%d.cscb.zstd", height),
		time.Now().UTC(),
		singleBlockGeneration,
		consolidatedGeneration,
	)
	require.NoError(t, err)
}

func loadStorageGenerationDownGuard(t *testing.T) string {
	t.Helper()
	migration, err := embedMigrations.ReadFile(storageGenerationMigrationPath)
	require.NoError(t, err)
	down := strings.SplitN(string(migration), "-- +goose Down", 2)
	require.Len(t, down, 2)
	statement := strings.SplitN(down[1], "-- +goose StatementBegin", 2)
	require.Len(t, statement, 2)
	guard := strings.SplitN(statement[1], "-- +goose StatementEnd", 2)
	require.Len(t, guard, 2)
	return strings.TrimSpace(guard[0])
}
