package cscbrepair_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

func TestIntegrationCSCBRepairReferenceIndexMigration(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}
	require := require.New(t)
	ctx := context.Background()

	cfg, err := config.New(
		config.WithEnvironment(config.EnvLocal),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(err)
	require.NotNil(cfg.AWS.Postgres)
	configureRepairTestEnvironment(t, cfg.AWS.Postgres)

	db, err := openRepairDB(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	defer func() { _ = db.Close() }()

	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))
	requireCandidateIndex(t, db)
	requireReferenceIndexes(t, db, "hash")
	requireNoTemporaryIndexes(t, db)
	requireIndexPlans(t, db)

	// Production rolls this migration down before applying the corrected index
	// definitions. Exercise that exact clean down/up path.
	require.NoError(goose.DownToContext(ctx, db, "db/migrations", 20260714000001))
	requireNoRepairIndexes(t, db)

	require.NoError(goose.UpContext(ctx, db, "db/migrations"))
	requireCandidateIndex(t, db)
	requireReferenceIndexes(t, db, "hash")
	requireNoTemporaryIndexes(t, db)
	requireIndexPlans(t, db)
}

func requireCandidateIndex(t *testing.T, db *sql.DB) {
	t.Helper()
	var method, definition string
	var valid, ready bool
	err := db.QueryRow(`
		SELECT access_method.amname, index_state.indisvalid, index_state.indisready,
			pg_get_indexdef(index_state.indexrelid)
		FROM pg_index index_state
		JOIN pg_class index_class ON index_class.oid = index_state.indexrelid
		JOIN pg_namespace index_namespace ON index_namespace.oid = index_class.relnamespace
		JOIN pg_am access_method ON access_method.oid = index_class.relam
		WHERE index_namespace.nspname = current_schema()
			AND index_class.relname = 'idx_block_metadata_cscb_repair_candidate'`).Scan(
		&method,
		&valid,
		&ready,
		&definition,
	)
	require.NoError(t, err)
	require.Equal(t, "btree", method)
	require.True(t, valid)
	require.True(t, ready)
	require.Contains(t, definition, "(tag, height DESC, object_key_main, id)")
	require.Contains(t, definition, "object_format = 1")
}

func requireNoRepairIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM pg_class index_class
		JOIN pg_namespace index_namespace ON index_namespace.oid = index_class.relnamespace
		WHERE index_namespace.nspname = current_schema()
			AND index_class.relname IN (
				'idx_block_metadata_cscb_repair_candidate',
				'idx_block_metadata_object_key_reference',
				'idx_block_consolidation_shadow_object_key_reference',
				'idx_block_metadata_cscb_repair_candidate_new',
				'idx_block_metadata_object_key_reference_hash_new',
				'idx_block_consolidation_shadow_object_key_reference_hash_new'
			)`).Scan(&count)
	require.NoError(t, err)
	require.Zero(t, count)
}

func requireNoTemporaryIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM pg_class index_class
		JOIN pg_namespace index_namespace ON index_namespace.oid = index_class.relnamespace
		WHERE index_namespace.nspname = current_schema()
			AND index_class.relname IN (
				'idx_block_metadata_cscb_repair_candidate_new',
				'idx_block_metadata_object_key_reference_hash_new',
				'idx_block_consolidation_shadow_object_key_reference_hash_new'
			)`).Scan(&count)
	require.NoError(t, err)
	require.Zero(t, count)
}

func requireReferenceIndexes(t *testing.T, db *sql.DB, expectedMethod string) {
	t.Helper()
	indexes := []struct {
		name   string
		column string
	}{
		{name: "idx_block_metadata_object_key_reference", column: "object_key_main"},
		{name: "idx_block_consolidation_shadow_object_key_reference", column: "consolidated_object_key_main"},
	}
	for _, index := range indexes {
		requireReferenceIndex(t, db, index.name, index.column, expectedMethod)
	}
}

func requireReferenceIndex(t *testing.T, db *sql.DB, indexName string, column string, expectedMethod string) {
	t.Helper()
	var method, definition string
	var valid, ready bool
	err := db.QueryRow(`
		SELECT access_method.amname, index_state.indisvalid, index_state.indisready,
			pg_get_indexdef(index_state.indexrelid)
		FROM pg_index index_state
		JOIN pg_class index_class ON index_class.oid = index_state.indexrelid
		JOIN pg_namespace index_namespace ON index_namespace.oid = index_class.relnamespace
		JOIN pg_am access_method ON access_method.oid = index_class.relam
		WHERE index_namespace.nspname = current_schema()
			AND index_class.relname = $1`, indexName).Scan(
		&method,
		&valid,
		&ready,
		&definition,
	)
	require.NoError(t, err)
	require.Equal(t, expectedMethod, method)
	require.True(t, valid)
	require.True(t, ready)
	if expectedMethod == "hash" {
		require.Contains(t, definition, "USING hash ("+column+")")
	}
}

func requireIndexPlans(t *testing.T, db *sql.DB) {
	t.Helper()
	const candidateListQuery = `
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
		LIMIT $5`
	const shadowOnlyCandidateQuery = `
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
	requireReferenceIndexPlan(t, db, `
		SELECT COUNT(*)
		FROM block_metadata
		WHERE object_key_main = $1
			AND object_key_main <> ''
			AND NOT (id = ANY($2))`, "idx_block_metadata_object_key_reference", "consolidated/test.cscb", pq.Array([]int64{1}))
	requireReferenceIndexPlan(t, db, `
		SELECT COUNT(*)
		FROM block_consolidation_shadow
		WHERE consolidated_object_key_main = $1
			AND consolidated_object_key_main <> ''
			AND NOT (block_metadata_id = ANY($2))`, "idx_block_consolidation_shadow_object_key_reference", "consolidated/test.cscb", pq.Array([]int64{1}))
	requireReferenceIndexPlan(t, db, `
		SELECT object_key_main
		FROM block_metadata
		WHERE tag = $1
			AND height >= $2
			AND height < $3
			AND object_format = $4
			AND object_key_main IS NOT NULL
			AND object_key_main <> ''`, "idx_block_metadata_cscb_repair_candidate", uint32(2), uint64(1000), uint64(2000), 1)
	requireReferenceIndexPlan(t, db, candidateListQuery, "idx_block_metadata_cscb_repair_candidate", uint32(2), uint64(1000), uint64(2000), 1, 10)
	requireReferenceIndexPlan(t, db, candidateListQuery, "idx_block_metadata_object_key_reference", uint32(2), uint64(1000), uint64(2000), 1, 10)
	requireReferenceIndexPlan(t, db, shadowOnlyCandidateQuery, "idx_block_consolidation_shadow_tag_height", uint32(2), uint64(1000), uint64(2000), 1)
	requireReferenceIndexPlan(t, db, shadowOnlyCandidateQuery, "block_metadata_pkey", uint32(2), uint64(1000), uint64(2000), 1)
}

func requireReferenceIndexPlan(t *testing.T, db *sql.DB, query string, indexName string, args ...any) {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`SET LOCAL enable_seqscan = off`)
	require.NoError(t, err)

	rows, err := tx.Query("EXPLAIN (COSTS OFF) "+query, args...)
	require.NoError(t, err)
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	require.Contains(t, plan.String(), indexName)
}
