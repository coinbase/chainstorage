package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepairlock"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// BenchmarkPersistBlockMetas compares the set-based PersistBlockMetas against a verbatim copy of the
// previous per-block implementation (persistBlockMetasLegacy) on the same schema. It needs the
// docker-compose Postgres:
//
//	docker-compose -f docker-compose-testing.yml -f docker-compose-benchmark.yml up -d postgres toxiproxy
//	TEST_TYPE=integration go test ./internal/storage/metastorage/postgres -run='^$' -bench=BenchmarkPersistBlockMetas -benchtime=20x
//
// Run it a second time with CHAINSTORAGE_AWS_POSTGRES_PORT=5434 after adding a 1 ms latency toxic to
// the toxiproxy listener to approximate Aurora round-trip latency; see docker-compose-benchmark.yml.
func BenchmarkPersistBlockMetas(b *testing.B) {
	env := newPersistBenchEnv(b)
	ctx := context.Background()

	impls := []struct {
		name    string
		persist func(ctx context.Context, blocks []*api.BlockMetadata) error
	}{
		{
			name: "legacy",
			persist: func(ctx context.Context, blocks []*api.BlockMetadata) error {
				return persistBlockMetasLegacy(ctx, env.db, true, blocks, nil)
			},
		},
		{
			name: "batched",
			persist: func(ctx context.Context, blocks []*api.BlockMetadata) error {
				return env.accessor.PersistBlockMetas(ctx, true, blocks, nil)
			},
		},
	}
	scenarios := []struct {
		name  string
		build func(size int) (setup []*api.BlockMetadata, batch []*api.BlockMetadata)
	}{
		{name: "fresh", build: benchFreshBatch},
		{name: "replay", build: benchReplayBatch},
		{name: "mixed", build: benchMixedBatch},
		{name: "reorg", build: benchReorgBatch},
	}
	sizes := []int{100, 1000, 2500}
	defaultChunk := persistBlockMetasChunkSize
	b.Cleanup(func() { persistBlockMetasChunkSize = defaultChunk })

	for _, impl := range impls {
		for _, scenario := range scenarios {
			for _, size := range sizes {
				chunks := []int{0}
				if impl.name == "batched" {
					chunks = []int{defaultChunk}
					if size == sizes[len(sizes)-1] && (scenario.name == "fresh" || scenario.name == "replay") {
						chunks = []int{250, 500, defaultChunk, size}
					}
				}
				for _, chunk := range chunks {
					name := fmt.Sprintf("%s/%s/%d", impl.name, scenario.name, size)
					if chunk > 0 {
						name += fmt.Sprintf("/chunk=%d", chunk)
					}
					b.Run(name, func(b *testing.B) {
						if chunk > 0 {
							persistBlockMetasChunkSize = chunk
						}
						var total time.Duration
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							b.StopTimer()
							env.truncate(b)
							setup, batch := scenario.build(size)
							if len(setup) > 0 {
								require.NoError(b, env.accessor.PersistBlockMetas(ctx, true, setup, nil))
							}
							b.StartTimer()
							start := time.Now()
							err := impl.persist(ctx, batch)
							total += time.Since(start)
							b.StopTimer()
							require.NoError(b, err)
						}
						b.ReportMetric(float64(total.Microseconds())/1000/float64(b.N), "ms/batch")
						b.ReportMetric(float64(size)*float64(b.N)/total.Seconds(), "blocks/s")
					})
				}
			}
		}
	}
}

// The pure-Go helpers below are what the set-based path adds on top of the SQL round trips.
func BenchmarkPartitionAndDedupe(b *testing.B) {
	_, batch := benchReorgBatch(2500)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		regular, skipped := partitionBlocksForPersist(batch)
		_ = dedupeBlocksKeepLast(regular, regularBlockKey)
		_ = dedupeBlocksKeepLast(skipped, skippedBlockKey)
	}
}

func BenchmarkBlockMetadataColumnArrays(b *testing.B) {
	_, batch := benchMixedBatch(2500)
	regular, _ := partitionBlocksForPersist(batch)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = blockMetadataColumnArrays(regular)
	}
}

func BenchmarkResolveCanonicalRows(b *testing.B) {
	_, batch := benchReorgBatch(2500)
	ids := newPersistedBlockIDs(len(batch))
	for i, block := range batch {
		if block.Skipped {
			ids.skipped[skippedBlockKey(block)] = int64(i)
		} else {
			ids.regular[regularBlockKey(block)] = int64(i)
		}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := resolveCanonicalRows(batch, ids); err != nil {
			b.Fatal(err)
		}
	}
}

type persistBenchEnv struct {
	accessor internal.MetaStorage
	db       *sql.DB
}

func newPersistBenchEnv(b *testing.B) *persistBenchEnv {
	cfg, err := config.New()
	require.NoError(b, err)
	if !cfg.IsIntegrationTest() || cfg.AWS.Postgres == nil {
		b.Skip("requires TEST_TYPE=integration and a Postgres configuration")
	}
	cfg.Chain.BlockStartHeight = 0
	ctx := context.Background()
	db, err := newDBConnection(ctx, cfg.AWS.Postgres)
	require.NoError(b, err)
	require.NoError(b, runMigrations(ctx, db))

	var accessor internal.MetaStorage
	app := testapp.New(
		b,
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		fx.Populate(&accessor),
	)
	env := &persistBenchEnv{accessor: accessor, db: db}
	b.Cleanup(func() {
		env.truncate(b)
		app.Close()
		_ = db.Close()
	})
	env.truncate(b)
	return env
}

// truncate mirrors blockStorageTestSuite.TearDownTest.
func (e *persistBenchEnv) truncate(b *testing.B) {
	ctx := context.Background()
	_, err := e.db.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_delete_trigger`)
	require.NoError(b, err)
	defer func() {
		_, err := e.db.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_delete_trigger`)
		require.NoError(b, err)
	}()
	for _, table := range []string{"block_events", "block_single_block_retention", "block_consolidation_shadow", "canonical_blocks", "block_metadata"} {
		_, err := e.db.ExecContext(ctx, "DELETE FROM "+table)
		require.NoError(b, err)
	}
}

const benchTag = uint32(1)

// benchFreshBatch: every row takes the INSERT path.
func benchFreshBatch(size int) ([]*api.BlockMetadata, []*api.BlockMetadata) {
	return nil, testutil.MakeBlockMetadatasFromStartHeight(0, size, benchTag)
}

// benchReplayBatch: every row takes the ON CONFLICT DO UPDATE path, so the placement guards run.
func benchReplayBatch(size int) ([]*api.BlockMetadata, []*api.BlockMetadata) {
	setup := testutil.MakeBlockMetadatasFromStartHeight(0, size, benchTag)
	batch := make([]*api.BlockMetadata, len(setup))
	for i, block := range setup {
		batch[i] = proto.Clone(block).(*api.BlockMetadata)
		batch[i].ObjectKeyMain += ".replayed"
	}
	return setup, batch
}

// benchMixedBatch: 5% skipped blocks, so both groups and both partial unique indexes are exercised.
func benchMixedBatch(size int) ([]*api.BlockMetadata, []*api.BlockMetadata) {
	batch := testutil.MakeBlockMetadatasFromStartHeight(0, size, benchTag)
	for i := 20; i < len(batch); i += 20 {
		batch[i] = &api.BlockMetadata{Tag: benchTag, Height: batch[i].Height, Skipped: true}
		if i+1 < len(batch) {
			batch[i+1].ParentHash = batch[i-1].Hash
			batch[i+1].ParentHeight = batch[i-1].Height
		}
	}
	return nil, batch
}

// benchReorgBatch: 5% of the blocks appear twice (same hash, replayed with an empty parent hash) and
// 1% of the heights additionally carry a skipped duplicate, so the dedupe and last-wins logic run.
func benchReorgBatch(size int) ([]*api.BlockMetadata, []*api.BlockMetadata) {
	chain := testutil.MakeBlockMetadatasFromStartHeight(0, size, benchTag)
	batch := make([]*api.BlockMetadata, 0, size+size/20+size/100)
	for i, block := range chain {
		batch = append(batch, block)
		if i%20 == 10 {
			duplicate := proto.Clone(block).(*api.BlockMetadata)
			duplicate.ParentHash = ""
			duplicate.ParentHeight = 0
			batch = append(batch, duplicate)
		}
	}
	for i := 50; i < len(chain); i += 100 {
		batch = append(batch, &api.BlockMetadata{Tag: benchTag, Height: chain[i].Height, Skipped: true})
	}
	return nil, batch
}

// persistBlockMetasLegacy is a verbatim copy of the per-block PersistBlockMetas implementation that
// this package used before INF-1675 (two round trips per block). It exists only for the benchmark.
func persistBlockMetasLegacy(ctx context.Context, db *sql.DB, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	if len(blocks) == 0 {
		return nil
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Height < blocks[j].Height
	})
	if err := parser.ValidateChain(blocks, lastBlock); err != nil {
		return xerrors.Errorf("failed to validate chain: %w", err)
	}

	txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tx, err := db.BeginTx(txCtx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	tags := make([]uint32, 0, len(blocks))
	for _, block := range blocks {
		if block != nil {
			tags = append(tags, block.Tag)
		}
	}
	if err := cscbrepairlock.AcquireTagsShared(txCtx, tx, tags); err != nil {
		return err
	}

	blockMetadataSkippedQuery := `
		INSERT INTO block_metadata (
			height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length, storage_generation
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (tag, height) WHERE skipped = true DO UPDATE SET
			hash = EXCLUDED.hash,
			parent_hash = EXCLUDED.parent_hash,
			parent_height = EXCLUDED.parent_height,
			object_key_main = EXCLUDED.object_key_main,
			timestamp = EXCLUDED.timestamp,
			skipped = EXCLUDED.skipped,
			object_format = EXCLUDED.object_format,
			byte_offset = EXCLUDED.byte_offset,
			byte_length = EXCLUDED.byte_length,
			uncompressed_length = EXCLUDED.uncompressed_length,
			storage_generation = EXCLUDED.storage_generation
		RETURNING id`

	blockMetadataRegularQuery := `
		INSERT INTO block_metadata (
			height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length, storage_generation
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (tag, hash) WHERE hash IS NOT NULL AND NOT skipped DO UPDATE SET
			parent_hash = EXCLUDED.parent_hash,
			parent_height = EXCLUDED.parent_height,
			object_key_main = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.object_key_main
				ELSE EXCLUDED.object_key_main
			END,
			timestamp = EXCLUDED.timestamp,
			skipped = EXCLUDED.skipped,
			object_format = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.object_format
				ELSE EXCLUDED.object_format
			END,
			byte_offset = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.byte_offset
				ELSE EXCLUDED.byte_offset
			END,
			byte_length = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.byte_length
				ELSE EXCLUDED.byte_length
			END,
			uncompressed_length = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.uncompressed_length
				ELSE EXCLUDED.uncompressed_length
			END,
			storage_generation = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.storage_generation
				ELSE EXCLUDED.storage_generation
			END
		RETURNING id`

	canonicalQuery := `
		INSERT INTO canonical_blocks (height, block_metadata_id, tag)
		VALUES ($1, $2, $3)
		ON CONFLICT (height, tag) DO UPDATE
		SET block_metadata_id = EXCLUDED.block_metadata_id`

	for _, block := range blocks {
		tsProto := block.GetTimestamp()
		var unixTimestamp int64
		if tsProto != nil {
			unixTimestamp = tsProto.GetSeconds()
		}
		var parentHeight uint64
		if block.Height != 0 {
			parentHeight = block.ParentHeight
		}
		var blockId int64
		query := blockMetadataRegularQuery
		if block.Skipped {
			query = blockMetadataSkippedQuery
		}
		byteOffset, byteLength, uncompressedLength := blockObjectByteFields(block)
		err = tx.QueryRowContext(txCtx, query,
			block.Height,
			block.Tag,
			block.Hash,
			block.ParentHash,
			parentHeight,
			block.ObjectKeyMain,
			unixTimestamp,
			block.Skipped,
			int32(block.GetObjectFormat()),
			byteOffset,
			byteLength,
			uncompressedLength,
			nullableStorageGeneration(block.GetStorageGeneration()),
		).Scan(&blockId)
		if err != nil {
			return xerrors.Errorf("failed to insert block metadata for height %d: %w", block.Height, err)
		}
		_, err = tx.ExecContext(txCtx, canonicalQuery, block.Height, blockId, block.Tag)
		if err != nil {
			return xerrors.Errorf("failed to insert canonical block for height %d: %w", block.Height, err)
		}
	}

	if updateWatermark && len(blocks) > 0 {
		highestBlock := blocks[len(blocks)-1]
		watermarkQuery := `
			UPDATE canonical_blocks
			SET is_watermark = TRUE
			WHERE tag = $1 AND height = $2`
		if _, err = tx.ExecContext(txCtx, watermarkQuery, highestBlock.Tag, highestBlock.Height); err != nil {
			return xerrors.Errorf("failed to update watermark for height %d: %w", highestBlock.Height, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return xerrors.Errorf("failed to commit transaction: %w", err)
	}
	committed = true
	return nil
}
