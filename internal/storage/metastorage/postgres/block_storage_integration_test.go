package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	tag = 1
)

type blockStorageTestSuite struct {
	suite.Suite
	accessor internal.MetaStorage
	config   *config.Config
	db       *sql.DB
}

func (s *blockStorageTestSuite) SetupSuite() {
	require := testutil.Require(s.T())
	cfg, err := config.New()
	require.NoError(err)
	if !cfg.IsIntegrationTest() || cfg.AWS.Postgres == nil {
		return
	}

	db, err := newDBConnection(context.Background(), cfg.AWS.Postgres)
	require.NoError(err)
	defer db.Close()
	require.NoError(runMigrations(context.Background(), db))
}

func (s *blockStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var accessor internal.MetaStorage
	cfg, err := config.New()
	require.NoError(err)

	// Skip tests if Postgres is not configured
	if cfg.AWS.Postgres == nil {
		s.T().Skip("Postgres not configured, skipping test suite")
		return
	}

	// Set the starting block height
	cfg.Chain.BlockStartHeight = 10
	s.config = cfg
	// Create a new test application with Postgres configuration
	app := testapp.New(
		s.T(),
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&accessor),
	)
	defer app.Close()
	s.accessor = accessor

	// Get database connection for cleanup
	db, err := newDBConnection(context.Background(), cfg.AWS.Postgres)
	require.NoError(err)
	s.db = db
}

func (s *blockStorageTestSuite) TearDownTest() {
	if s.db != nil {
		ctx := context.Background()
		s.T().Log("Clearing database tables after test")
		_, err := s.db.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_delete_trigger`)
		if err != nil {
			s.T().Errorf("Failed to disable retirement audit delete trigger for test cleanup: %v", err)
			return
		}
		defer func() {
			if _, err := s.db.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_delete_trigger`); err != nil {
				s.T().Errorf("Failed to restore retirement audit delete trigger after test cleanup: %v", err)
			}
		}()
		// Clear all tables in reverse order due to foreign key constraints
		tables := []string{"block_events", "block_single_block_retention", "block_consolidation_shadow", "canonical_blocks", "block_metadata"}
		for _, table := range tables {
			_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", table))
			if err != nil {
				s.T().Logf("Failed to clear table %s: %v", table, err)
			}
		}
	}
}

func (s *blockStorageTestSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *blockStorageTestSuite) TestPersistBlockMetasPreservesRetirementFencedCSCBPlacement() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	block := testutil.MakeBlockMetadatasFromStartHeight(s.config.Chain.BlockStartHeight, 1, tag)[0]
	block.ObjectKeyMain = "single-block/fenced-block.gzip"
	block.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	block.ByteOffset = 0
	block.ByteLength = 0
	block.UncompressedLength = 0
	guardStorage, ok := s.accessor.(internal.SingleBlockUploadGuardStorage)
	require.True(ok)
	uploadContext, cancelUpload := context.WithCancel(ctx)
	uploadGuard, err := guardStorage.AcquireSingleBlockUploadGuard(uploadContext, block.Tag, block.Height, block.Hash)
	require.NoError(err)
	require.NotNil(uploadGuard)
	require.False(uploadGuard.RetirementFenced())
	cancelUpload()
	defer func() { _ = uploadGuard.Release() }()
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block}, nil))

	const cscbKey = "consolidated/fenced-batch.cscb.gzip"
	var blockMetadataID int64
	err = s.db.QueryRowContext(ctx, `
		UPDATE block_metadata
		SET object_key_main = $1,
			object_format = $2,
			byte_offset = $3,
			byte_length = $4,
			uncompressed_length = $5
		WHERE tag = $6 AND hash = $7
		RETURNING id`,
		cscbKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		64,
		128,
		256,
		block.Tag,
		block.Hash,
	).Scan(&blockMetadataID)
	require.NoError(err)

	validatedAt := time.Now().UTC().Add(-96 * time.Hour)
	retireAfter := validatedAt.Add(72 * time.Hour)
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, single_block_object_key_main,
			consolidated_object_key_main, object_format, byte_offset, byte_length,
			uncompressed_length, validated_at, single_block_retention_started_at, single_block_delete_after
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12)`,
		blockMetadataID,
		block.Tag,
		block.Height,
		block.Hash,
		block.ObjectKeyMain,
		cscbKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		64,
		128,
		256,
		validatedAt,
		retireAfter,
	)
	require.NoError(err)

	preparedAt := time.Now().UTC()
	repo := retirement.NewPostgresRepository(s.db)
	manifest := retirement.RetirementManifest{
		BlockMetadataID:                blockMetadataID,
		Tag:                            block.Tag,
		Height:                         block.Height,
		Hash:                           block.Hash,
		State:                          retirement.RetirementStateEligible,
		Bucket:                         "integration-bucket",
		SingleBlockObjectKey:           block.ObjectKeyMain,
		SingleBlockObjectKeySHA256:     sha256Hex(block.ObjectKeyMain),
		SingleBlockObjectVersionIDs:    []string{"single-block-v1"},
		SingleBlockObjectETag:          "single-block-etag",
		SingleBlockObjectBytes:         512,
		ConsolidatedObjectKey:          cscbKey,
		ConsolidatedObjectVersionID:    "cscb-v1",
		ConsolidatedObjectETag:         "cscb-etag",
		ConsolidatedByteOffset:         64,
		ConsolidatedByteLength:         128,
		ConsolidatedUncompressedLength: 256,
		PayloadSHA256:                  strings.Repeat("a", 64),
		PreparedAt:                     preparedAt,
	}
	prepareDone := make(chan error, 1)
	go func() {
		prepareDone <- repo.PrepareRetirement(ctx, manifest, "")
	}()
	select {
	case err := <-prepareDone:
		require.Failf("retirement preparation bypassed upload guard", "unexpected result: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(uploadGuard.Release())
	require.NoError(<-prepareDone)

	fencedGuard, err := guardStorage.AcquireSingleBlockUploadGuard(ctx, block.Tag, block.Height, block.Hash)
	require.NoError(err)
	require.NotNil(fencedGuard)
	require.True(fencedGuard.RetirementFenced())
	require.NoError(fencedGuard.Release())
	_, err = s.db.ExecContext(ctx, `
		UPDATE block_metadata
		SET object_key_main = $2,
			object_format = $3,
			byte_offset = NULL,
			byte_length = NULL,
			uncompressed_length = NULL
		WHERE id = $1`,
		blockMetadataID,
		"single-block/direct-sql-regression.gzip",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
	)
	require.Error(err)
	require.Contains(err.Error(), "cannot change CSCB placement after single-block object retirement is fenced")

	_, err = s.db.ExecContext(ctx, `
		UPDATE block_metadata
		SET storage_generation = $2
		WHERE id = $1`, blockMetadataID, "v2")
	require.Error(err)
	require.Contains(err.Error(), "cannot change storage generation after single-block object retirement is fenced")

	replayedSingleBlock := proto.Clone(block).(*api.BlockMetadata)
	replayedSingleBlock.ObjectKeyMain = "single-block/replayed-block.gzip"
	replayedSingleBlock.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	replayedSingleBlock.StorageGeneration = "v2"
	require.NoError(s.accessor.PersistBlockMetas(ctx, false, []*api.BlockMetadata{replayedSingleBlock}, nil))

	for _, fetched := range []*api.BlockMetadata{
		mustGetBlockByHash(s.T(), s.accessor, block),
		mustGetBlockByHeight(s.T(), s.accessor, block),
	} {
		require.Equal(cscbKey, fetched.ObjectKeyMain)
		require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, fetched.ObjectFormat)
		require.Equal(uint64(64), fetched.ByteOffset)
		require.Equal(uint64(128), fetched.ByteLength)
		require.Equal(uint64(256), fetched.UncompressedLength)
		require.Equal("", fetched.GetStorageGeneration())
	}

	claimToken := "normal-read-path-claim"
	claimedAt := time.Now().UTC()
	require.NoError(repo.ClaimRetirement(ctx, blockMetadataID, claimToken, claimedAt, claimedAt.Add(time.Hour)))
	_, err = repo.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, retirement.ActionDeletedObjectVersion)
	require.NoError(err)
	require.NoError(s.accessor.PersistBlockMetas(ctx, false, []*api.BlockMetadata{replayedSingleBlock}, nil))
	for _, fetched := range []*api.BlockMetadata{
		mustGetBlockByHash(s.T(), s.accessor, block),
		mustGetBlockByHeight(s.T(), s.accessor, block),
	} {
		require.Equal(cscbKey, fetched.ObjectKeyMain)
		require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, fetched.ObjectFormat)
	}
	_, err = repo.FinalizeRetirement(ctx, blockMetadataID, claimToken, retirement.ActionDeletedVerified)
	require.NoError(err)
	require.NoError(s.accessor.PersistBlockMetas(ctx, false, []*api.BlockMetadata{replayedSingleBlock}, nil))

	for _, fetched := range []*api.BlockMetadata{
		mustGetBlockByHash(s.T(), s.accessor, block),
		mustGetBlockByHeight(s.T(), s.accessor, block),
	} {
		require.Equal(cscbKey, fetched.ObjectKeyMain)
		require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, fetched.ObjectFormat)
		require.Equal(uint64(64), fetched.ByteOffset)
		require.Equal(uint64(128), fetched.ByteLength)
		require.Equal(uint64(256), fetched.UncompressedLength)
	}

	var clearedSingleBlockPath sql.NullString
	var singleBlockDeletedAt sql.NullTime
	err = s.db.QueryRowContext(ctx, `
		SELECT single_block_object_key_main, single_block_object_deleted_at
		FROM block_consolidation_shadow
		WHERE block_metadata_id = $1`, blockMetadataID).Scan(&clearedSingleBlockPath, &singleBlockDeletedAt)
	require.NoError(err)
	require.False(clearedSingleBlockPath.Valid)
	require.True(singleBlockDeletedAt.Valid)

	_, err = s.db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_object_key_main = $2,
			single_block_object_deleted_at = NULL
		WHERE block_metadata_id = $1`, blockMetadataID, "single-block/direct-shadow-regression.gzip")
	require.Error(err)
	require.Contains(err.Error(), "cannot restore or rewrite deleted single-block object metadata")

	_, err = s.db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_object_deleted_at = single_block_object_deleted_at + INTERVAL '1 second'
		WHERE block_metadata_id = $1`, blockMetadataID)
	require.Error(err)
	require.Contains(err.Error(), "cannot restore or rewrite deleted single-block object metadata")
}

func (s *blockStorageTestSuite) TestSingleBlockUploadGuardMatchesNullableHash() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	height := s.config.Chain.BlockStartHeight + 1

	var blockMetadataID int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO block_metadata (
			height, tag, hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length
		) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, 0, 128, 128)
		RETURNING id`,
		height,
		tag,
		height-1,
		"consolidated/null-hash.cscb.gzip",
		time.Now().UTC().Unix(),
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	).Scan(&blockMetadataID)
	require.NoError(err)
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO canonical_blocks (height, block_metadata_id, tag)
		VALUES ($1, $2, $3)`, height, blockMetadataID, tag)
	require.NoError(err)
	var nonCanonicalBlockMetadataID int64
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO block_metadata (
			height, tag, hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length
		) VALUES ($1, $2, NULL, $3, $4, $5, FALSE, $6, 0, 128, 128)
		RETURNING id`,
		height,
		tag,
		height-1,
		"consolidated/non-canonical-null-hash.cscb.gzip",
		time.Now().UTC().Unix(),
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
	).Scan(&nonCanonicalBlockMetadataID)
	require.NoError(err)
	_, err = s.db.ExecContext(ctx, `
		UPDATE block_metadata
		SET single_block_retention_fenced_at = CURRENT_TIMESTAMP
		WHERE id = $1`, nonCanonicalBlockMetadataID)
	require.NoError(err)

	guardStorage, ok := s.accessor.(internal.SingleBlockUploadGuardStorage)
	require.True(ok)
	guard, err := guardStorage.AcquireSingleBlockUploadGuard(ctx, tag, height, "")
	require.NoError(err)
	require.NotNil(guard)
	require.False(guard.RetirementFenced())
	canonicalUpdateConn, err := s.db.Conn(ctx)
	require.NoError(err)
	defer canonicalUpdateConn.Close()
	_, err = canonicalUpdateConn.ExecContext(ctx, "SET lock_timeout = '100ms'")
	require.NoError(err)
	_, err = canonicalUpdateConn.ExecContext(ctx, `
		UPDATE canonical_blocks
		SET block_metadata_id = $1
		WHERE tag = $2 AND height = $3`, nonCanonicalBlockMetadataID, tag, height)
	require.ErrorContains(err, "canceling statement due to lock timeout")
	require.NoError(guard.Release())
	_, err = canonicalUpdateConn.ExecContext(ctx, "SET lock_timeout = 0")
	require.NoError(err)
	_, err = canonicalUpdateConn.ExecContext(ctx, `
		UPDATE canonical_blocks
		SET block_metadata_id = $1
		WHERE tag = $2 AND height = $3`, nonCanonicalBlockMetadataID, tag, height)
	require.NoError(err)
	fencedGuard, err := guardStorage.AcquireSingleBlockUploadGuard(ctx, tag, height, "")
	require.NoError(err)
	require.NotNil(fencedGuard)
	require.True(fencedGuard.RetirementFenced())
	require.NoError(fencedGuard.Release())
}

func (s *blockStorageTestSuite) TestSingleBlockUploadGuardMatchesExactHash() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	height := s.config.Chain.BlockStartHeight + 3

	var fencedBlockMetadataID int64
	for _, blockHash := range []string{"unfenced-hash", "fenced-hash"} {
		var blockMetadataID int64
		err := s.db.QueryRowContext(ctx, `
			INSERT INTO block_metadata (
				height, tag, hash, parent_height, object_key_main, timestamp, skipped,
				object_format, byte_offset, byte_length, uncompressed_length
			) VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, 0, 128, 128)
			RETURNING id`,
			height,
			tag,
			blockHash,
			height-1,
			"consolidated/"+blockHash+".cscb.gzip",
			time.Now().UTC().Unix(),
			api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		).Scan(&blockMetadataID)
		require.NoError(err)
		if blockHash == "fenced-hash" {
			fencedBlockMetadataID = blockMetadataID
		}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE block_metadata
		SET single_block_retention_fenced_at = CURRENT_TIMESTAMP
		WHERE id = $1`, fencedBlockMetadataID)
	require.NoError(err)

	guardStorage, ok := s.accessor.(internal.SingleBlockUploadGuardStorage)
	require.True(ok)
	unfencedGuard, err := guardStorage.AcquireSingleBlockUploadGuard(ctx, tag, height, "unfenced-hash")
	require.NoError(err)
	require.False(unfencedGuard.RetirementFenced())
	require.NoError(unfencedGuard.Release())
	fencedGuard, err := guardStorage.AcquireSingleBlockUploadGuard(ctx, tag, height, "fenced-hash")
	require.NoError(err)
	require.True(fencedGuard.RetirementFenced())
	require.NoError(fencedGuard.Release())
}

func (s *blockStorageTestSuite) TestSingleBlockUploadGuardWithoutHashAllowsMissingCanonicalMetadata() {
	require := testutil.Require(s.T())
	guardStorage, ok := s.accessor.(internal.SingleBlockUploadGuardStorage)
	require.True(ok)

	guard, err := guardStorage.AcquireSingleBlockUploadGuard(
		context.Background(),
		tag,
		s.config.Chain.BlockStartHeight+4,
		"",
	)
	require.NoError(err)
	require.NotNil(guard)
	require.False(guard.RetirementFenced())
	require.NoError(guard.Release())
}

func (s *blockStorageTestSuite) TestSingleBlockShadowCompatibilityColumnsStaySynchronized() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	height := s.config.Chain.BlockStartHeight + 2
	validatedAt := time.Now().UTC().Add(-96 * time.Hour).Truncate(time.Microsecond)
	deleteAfter := validatedAt.Add(72 * time.Hour)

	var blockMetadataID int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO block_metadata (
			height, tag, hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length
		) VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8, $9, $10)
		RETURNING id`,
		height,
		tag,
		"compatibility-hash",
		height-1,
		"consolidated/compatibility.cscb.gzip",
		time.Now().UTC().Unix(),
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		64,
		128,
		256,
	).Scan(&blockMetadataID)
	require.NoError(err)

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, legacy_object_key_main,
			consolidated_object_key_main, object_format, byte_offset, byte_length,
			uncompressed_length, validated_at, legacy_object_retired_at, legacy_object_retire_after
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12)`,
		blockMetadataID,
		tag,
		height,
		"compatibility-hash",
		"single-block/compatibility.gzip",
		"consolidated/compatibility.cscb.gzip",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		64,
		128,
		256,
		validatedAt,
		deleteAfter,
	)
	require.NoError(err)

	var canonicalKey string
	var canonicalStartedAt time.Time
	var canonicalDeleteAfter time.Time
	err = s.db.QueryRowContext(ctx, `
		SELECT single_block_object_key_main, single_block_retention_started_at, single_block_delete_after
		FROM block_consolidation_shadow
		WHERE block_metadata_id = $1`, blockMetadataID).Scan(&canonicalKey, &canonicalStartedAt, &canonicalDeleteAfter)
	require.NoError(err)
	require.Equal("single-block/compatibility.gzip", canonicalKey)
	require.WithinDuration(validatedAt, canonicalStartedAt, 0)
	require.WithinDuration(deleteAfter, canonicalDeleteAfter, 0)

	deletedAt := time.Now().UTC().Truncate(time.Microsecond)
	_, err = s.db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_object_key_main = NULL,
			single_block_object_deleted_at = $2
		WHERE block_metadata_id = $1`, blockMetadataID, deletedAt)
	require.NoError(err)

	var compatibilityKey sql.NullString
	var canonicalDeletedAt time.Time
	err = s.db.QueryRowContext(ctx, `
		SELECT legacy_object_key_main, single_block_object_deleted_at
		FROM block_consolidation_shadow
		WHERE block_metadata_id = $1`, blockMetadataID).Scan(&compatibilityKey, &canonicalDeletedAt)
	require.NoError(err)
	require.False(compatibilityKey.Valid)
	require.WithinDuration(deletedAt, canonicalDeletedAt, 0)
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func mustGetBlockByHash(t *testing.T, accessor internal.MetaStorage, block *api.BlockMetadata) *api.BlockMetadata {
	t.Helper()
	result, err := accessor.GetBlockByHash(context.Background(), block.Tag, block.Height, block.Hash)
	require.NoError(t, err)
	return result
}

func mustGetBlockByHeight(t *testing.T, accessor internal.MetaStorage, block *api.BlockMetadata) *api.BlockMetadata {
	t.Helper()
	result, err := accessor.GetBlockByHeight(context.Background(), block.Tag, block.Height)
	require.NoError(t, err)
	return result
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByMaxWriteSize() {
	tests := []struct {
		totalBlocks int
	}{
		{totalBlocks: 2},
		{totalBlocks: 4},
		{totalBlocks: 8},
		{totalBlocks: 64},
		// Around the set-based insert chunk boundary, and the production backfiller batch_size.
		{totalBlocks: persistBlockMetasChunkSize - 1},
		{totalBlocks: persistBlockMetasChunkSize},
		{totalBlocks: persistBlockMetasChunkSize + 1},
		{totalBlocks: 2500},
	}
	for _, test := range tests {
		s.T().Run(fmt.Sprintf("test %d blocks", test.totalBlocks), func(t *testing.T) {
			s.runTestPersistBlockMetas(test.totalBlocks)
		})
	}
}

func (s *blockStorageTestSuite) runTestPersistBlockMetas(totalBlocks int) {
	require := testutil.Require(s.T())
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, totalBlocks, tag)
	zaptest.NewLogger(s.T())
	ctx := context.TODO()

	// shuffle it to make sure it still works
	shuffleSeed := time.Now().UnixNano()
	rand.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })
	logger := zaptest.NewLogger(s.T())
	logger.Info("shuffled blocks", zap.Int64("seed", shuffleSeed))

	fmt.Println("Persisting blocks")
	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	if err != nil {
		panic(err)
	}

	expectedLatestBlock := proto.Clone(blocks[totalBlocks-1])

	// fetch range with missing item
	fmt.Println("Fetching range with missing item")
	_, err = s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(totalBlocks+100))
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	// fetch valid range
	fmt.Println("Fetching valid range")
	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(totalBlocks))
	if err != nil {
		panic(err)
	}
	sort.Slice(fetchedBlocks, func(i, j int) bool {
		return fetchedBlocks[i].Height < fetchedBlocks[j].Height
	})
	assert.Len(s.T(), fetchedBlocks, int(totalBlocks))

	for i := 0; i < len(blocks); i++ {
		//get block by height
		// fetch block through three ways, should always return identical result
		fetchedBlockMeta, err := s.accessor.GetBlockByHeight(ctx, tag, blocks[i].Height)
		if err != nil {
			panic(err)
		}
		s.equalProto(blocks[i], fetchedBlockMeta)

		fetchedBlockMeta, err = s.accessor.GetBlockByHash(ctx, tag, blocks[i].Height, blocks[i].Hash)
		if err != nil {
			panic(err)
		}
		s.equalProto(blocks[i], fetchedBlockMeta)

		fetchedBlockMeta, err = s.accessor.GetBlockByHash(ctx, tag, blocks[i].Height, "")
		if err != nil {
			panic(err)
		}
		s.equalProto(blocks[i], fetchedBlockMeta)

		s.equalProto(blocks[i], fetchedBlocks[i])
	}

	fetchedBlocksMeta, err := s.accessor.GetBlocksByHeights(ctx, tag, []uint64{startHeight + 1, startHeight + uint64(totalBlocks/2), startHeight, startHeight + uint64(totalBlocks) - 1})
	if err != nil {
		fmt.Println("Error fetching blocks by heights", err)
		panic(err)
	}
	assert.Len(s.T(), fetchedBlocksMeta, 4)
	s.equalProto(blocks[1], fetchedBlocksMeta[0])
	s.equalProto(blocks[totalBlocks/2], fetchedBlocksMeta[1])
	s.equalProto(blocks[0], fetchedBlocksMeta[2])
	s.equalProto(blocks[totalBlocks-1], fetchedBlocksMeta[3])

	fetchedBlockMeta, err := s.accessor.GetLatestBlock(ctx, tag)
	if err != nil {
		fmt.Println("Error fetching latest block", err)
		panic(err)
	}
	s.equalProto(expectedLatestBlock, fetchedBlockMeta)

}

func (s *blockStorageTestSuite) TestPersistBlockMetasByInvalidChain() {
	require := testutil.Require(s.T())
	blocks := testutil.MakeBlockMetadatas(100, tag)
	blocks[73].Hash = "0xdeadbeef"
	err := s.accessor.PersistBlockMetas(context.Background(), true, blocks, nil)
	require.Error(err)
	require.True(xerrors.Is(err, parser.ErrInvalidChain))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByInvalidLastBlock() {
	require := testutil.Require(s.T())
	blocks := testutil.MakeBlockMetadatasFromStartHeight(1_000_000, 100, tag)
	lastBlock := testutil.MakeBlockMetadata(999_999, tag)
	lastBlock.Hash = "0xdeadbeef"
	err := s.accessor.PersistBlockMetas(context.Background(), true, blocks, lastBlock)
	require.Error(err)
	require.True(xerrors.Is(err, parser.ErrInvalidChain))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasWithSkippedBlocks() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 100, tag)
	// Mark 37th block as skipped and point the next block to the previous block.
	blocks[37] = &api.BlockMetadata{
		Tag:     tag,
		Height:  startHeight + 37,
		Skipped: true,
	}
	blocks[38].ParentHeight = blocks[36].Height
	blocks[38].ParentHash = blocks[36].Hash
	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+100)
	require.NoError(err)
	require.Equal(blocks, fetchedBlocks)
}

func (s *blockStorageTestSuite) TestPersistBlockMetas() {
	s.runTestPersistBlockMetas(10)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasNotContinuous() {
	blocks := testutil.MakeBlockMetadatas(10, tag)
	blocks[2] = blocks[9]
	err := s.accessor.PersistBlockMetas(context.TODO(), true, blocks[:9], nil)
	assert.NotNil(s.T(), err)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasDuplicatedHeights() {
	blocks := testutil.MakeBlockMetadatas(10, tag)
	blocks[9].Height = 2
	err := s.accessor.PersistBlockMetas(context.TODO(), true, blocks, nil)
	assert.NotNil(s.T(), err)
}

func (s *blockStorageTestSuite) TestGetBlocksNotExist() {
	_, err := s.accessor.GetLatestBlock(context.TODO(), tag)
	assert.True(s.T(), xerrors.Is(err, errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestGetBlockByHeightInvalidHeight() {
	_, err := s.accessor.GetBlockByHeight(context.TODO(), tag, 0)
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightsInvalidHeight() {
	_, err := s.accessor.GetBlocksByHeights(context.TODO(), tag, []uint64{0})
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightsBlockNotFound() {
	_, err := s.accessor.GetBlocksByHeights(context.TODO(), tag, []uint64{15})
	assert.True(s.T(), xerrors.Is(err, errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestGetBlockByHashInvalidHeight() {
	_, err := s.accessor.GetBlockByHash(context.TODO(), tag, 0, "0x0")
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlockByHashQueryUsesPartialIndex() {
	require := testutil.Require(s.T())

	tx, err := s.db.BeginTx(context.Background(), nil)
	require.NoError(err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(context.Background(), "SET LOCAL enable_seqscan = off")
	require.NoError(err)

	rows, err := tx.QueryContext(context.Background(), "EXPLAIN "+blockMetadataByHashQuery(), tag, s.config.Chain.BlockStartHeight, "0x0")
	require.NoError(err)
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(rows.Err())

	plan := strings.Join(lines, "\n")
	assert.Contains(s.T(), plan, "unique_tag_hash_regular")
	assert.Contains(s.T(), plan, "Index Cond: ((tag = 1) AND ((hash)::text = '0x0'::text))")
	assert.NotContains(s.T(), plan, "Seq Scan")
}

func (s *blockStorageTestSuite) TestSingleBlockUploadGuardByHashQueryUsesPartialIndex() {
	require := testutil.Require(s.T())

	tx, err := s.db.BeginTx(context.Background(), nil)
	require.NoError(err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(context.Background(), "SET LOCAL enable_seqscan = off")
	require.NoError(err)

	rows, err := tx.QueryContext(
		context.Background(),
		"EXPLAIN "+singleBlockUploadGuardByHashQuery(),
		tag,
		s.config.Chain.BlockStartHeight,
		"0x0",
	)
	require.NoError(err)
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(rows.Err())

	plan := strings.Join(lines, "\n")
	assert.Contains(s.T(), plan, "unique_tag_hash_regular")
	assert.Contains(s.T(), plan, "Index Cond: ((tag = 1) AND ((hash)::text = '0x0'::text))")
	assert.NotContains(s.T(), plan, "Seq Scan")
}

func (s *blockStorageTestSuite) TestSingleBlockUploadGuardWithoutHashQueryUsesCanonicalIndexes() {
	require := testutil.Require(s.T())

	tx, err := s.db.BeginTx(context.Background(), nil)
	require.NoError(err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(context.Background(), "SET LOCAL enable_seqscan = off")
	require.NoError(err)

	rows, err := tx.QueryContext(
		context.Background(),
		"EXPLAIN "+singleBlockUploadGuardWithoutHashQuery(),
		tag,
		s.config.Chain.BlockStartHeight,
	)
	require.NoError(err)
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(rows.Err())

	plan := strings.Join(lines, "\n")
	assert.Contains(s.T(), plan, "Index Scan using canonical_blocks_")
	assert.Contains(s.T(), plan, "Index Cond: ((height = '10'::bigint) AND (tag = 1))")
	assert.Contains(s.T(), plan, "block_metadata_pkey")
	assert.Regexp(s.T(), `Index Cond: \(id = \$[0-9]+\)`, plan)
	assert.NotContains(s.T(), plan, "Seq Scan")
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightRangeInvalidRange() {
	_, err := s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 100, 100)
	assert.True(s.T(), xerrors.Is(err, errors.ErrOutOfRange))

	_, err = s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 0, s.config.Chain.BlockStartHeight)
	assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) equalProto(x, y any) {
	if diff := cmp.Diff(x, y, protocmp.Transform()); diff != "" {
		assert.FailNow(s.T(), diff)
	}
}

func (s *blockStorageTestSuite) getBlockMetadataID(ctx context.Context, block *api.BlockMetadata) int64 {
	require := testutil.Require(s.T())
	var blockMetadataID int64
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id FROM block_metadata WHERE tag = $1 AND height = $2 AND hash = $3 AND object_key_main = $4`,
		block.GetTag(),
		block.GetHeight(),
		block.GetHash(),
		block.GetObjectKeyMain(),
	).Scan(&blockMetadataID)
	require.NoError(err)
	return blockMetadataID
}

type consolidationShadowAudit struct {
	SingleBlockObjectKey  string
	ConsolidatedObjectKey string
	RetiredAt             *time.Time
	RetireAfter           *time.Time
}

func (s *blockStorageTestSuite) getConsolidationShadowAudit(ctx context.Context, block *api.BlockMetadata) consolidationShadowAudit {
	require := testutil.Require(s.T())
	var singleBlockObjectKey, consolidatedObjectKey string
	var retiredAt sql.NullTime
	var retireAfter sql.NullTime
	err := s.db.QueryRowContext(
		ctx,
		`SELECT shadow.single_block_object_key_main, shadow.consolidated_object_key_main,
			shadow.single_block_retention_started_at, shadow.single_block_delete_after
		 FROM block_metadata bm
		 JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
		 WHERE bm.tag = $1 AND bm.height = $2 AND bm.hash = $3`,
		block.GetTag(),
		block.GetHeight(),
		block.GetHash(),
	).Scan(&singleBlockObjectKey, &consolidatedObjectKey, &retiredAt, &retireAfter)
	require.NoError(err)
	audit := consolidationShadowAudit{
		SingleBlockObjectKey:  singleBlockObjectKey,
		ConsolidatedObjectKey: consolidatedObjectKey,
	}
	if retiredAt.Valid {
		value := retiredAt.Time
		audit.RetiredAt = &value
	}
	if retireAfter.Valid {
		value := retireAfter.Time
		audit.RetireAfter = &value
	}
	return audit
}

func (s *blockStorageTestSuite) insertConsolidationShadow(
	ctx context.Context,
	block *api.BlockMetadata,
	consolidatedObjectKey string,
	byteOffset uint64,
	byteLength uint64,
	uncompressedLength uint64,
	validated bool,
	singleBlockObjectKeyMain string,
) {
	if singleBlockObjectKeyMain == "" {
		singleBlockObjectKeyMain = block.GetObjectKeyMain()
	}
	s.insertConsolidationShadowWithIdentity(
		ctx,
		block,
		consolidatedObjectKey,
		byteOffset,
		byteLength,
		uncompressedLength,
		validated,
		singleBlockObjectKeyMain,
		block.GetTag(),
		block.GetHeight(),
		block.GetHash(),
	)
}

func (s *blockStorageTestSuite) insertConsolidationShadowWithIdentity(
	ctx context.Context,
	block *api.BlockMetadata,
	consolidatedObjectKey string,
	byteOffset uint64,
	byteLength uint64,
	uncompressedLength uint64,
	validated bool,
	singleBlockObjectKeyMain string,
	shadowTag uint32,
	shadowHeight uint64,
	shadowHash string,
) {
	require := testutil.Require(s.T())
	var validatedAt any
	if validated {
		validatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, single_block_object_key_main, consolidated_object_key_main,
			object_format, byte_offset, byte_length, uncompressed_length, validated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		s.getBlockMetadataID(ctx, block),
		shadowTag,
		shadowHeight,
		shadowHash,
		singleBlockObjectKeyMain,
		consolidatedObjectKey,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
		byteOffset,
		byteLength,
		uncompressedLength,
		validatedAt,
	)
	require.NoError(err)
}

func (s *blockStorageTestSuite) insertInvalidConsolidationShadow(
	ctx context.Context,
	block *api.BlockMetadata,
	consolidatedObjectKey string,
	objectFormat api.BlockObjectFormat,
	byteOffset uint64,
	byteLength uint64,
	uncompressedLength uint64,
) {
	require := testutil.Require(s.T())
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, single_block_object_key_main, consolidated_object_key_main,
			object_format, byte_offset, byte_length, uncompressed_length, validated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		s.getBlockMetadataID(ctx, block),
		block.GetTag(),
		block.GetHeight(),
		block.GetHash(),
		block.GetObjectKeyMain(),
		consolidatedObjectKey,
		int32(objectFormat),
		byteOffset,
		byteLength,
		uncompressedLength,
		time.Now().UTC(),
	)
	require.NoError(err)
}

func expectedConsolidationShadow(
	block *api.BlockMetadata,
	consolidatedObjectKey string,
	byteOffset uint64,
	byteLength uint64,
	uncompressedLength uint64,
) *api.BlockMetadata {
	shadow := proto.Clone(block).(*api.BlockMetadata)
	shadow.ObjectKeyMain = consolidatedObjectKey
	shadow.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	shadow.ByteOffset = byteOffset
	shadow.ByteLength = byteLength
	shadow.UncompressedLength = uncompressedLength
	return shadow
}

func (s *blockStorageTestSuite) TestGetConsolidationShadowPredicates() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 4, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/validated.cscb.zstd", 10, 20, 20, true, "")
	s.insertConsolidationShadow(ctx, blocks[1], "consolidated/unvalidated.cscb.zstd", 30, 40, 40, false, "")
	s.insertConsolidationShadow(ctx, blocks[2], "consolidated/wrong-single-block-key.cscb.zstd", 50, 60, 60, true, "single-block/key/does/not/match")

	expected := expectedConsolidationShadow(blocks[0], "consolidated/validated.cscb.zstd", 10, 20, 20)
	actual, err := s.accessor.GetBlockConsolidationShadow(ctx, blocks[0])
	require.NoError(err)
	s.equalProto(expected, actual)

	_, err = s.accessor.GetBlockConsolidationShadow(ctx, blocks[1])
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	_, err = s.accessor.GetBlockConsolidationShadow(ctx, blocks[2])
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	_, err = s.accessor.GetBlockConsolidationShadow(ctx, blocks[3])
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	skipped := &api.BlockMetadata{Tag: tag, Height: startHeight + 10, Skipped: true}
	_, err = s.accessor.GetBlockConsolidationShadow(ctx, skipped)
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestGetBlocksConsolidationShadowPreservesOrderAndMisses() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 3, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/first.cscb.zstd", 100, 200, 200, true, "")
	s.insertConsolidationShadow(ctx, blocks[2], "consolidated/third.cscb.zstd", 300, 400, 400, true, "")
	skipped := &api.BlockMetadata{Tag: tag, Height: startHeight + 99, Skipped: true}

	actual, err := s.accessor.GetBlocksConsolidationShadow(ctx, []*api.BlockMetadata{blocks[2], blocks[1], skipped, blocks[0]})
	require.NoError(err)
	require.Len(actual, 4)
	s.equalProto(expectedConsolidationShadow(blocks[2], "consolidated/third.cscb.zstd", 300, 400, 400), actual[0])
	require.Nil(actual[1])
	require.Nil(actual[2])
	s.equalProto(expectedConsolidationShadow(blocks[0], "consolidated/first.cscb.zstd", 100, 200, 200), actual[3])
}

func (s *blockStorageTestSuite) TestGetBlocksMissingConsolidationShadowFiltersAndLimits() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 5, tag)
	blocks[2].ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	blocks[2].ByteOffset = 10
	blocks[2].ByteLength = 20
	blocks[2].UncompressedLength = 20
	blocks[4] = &api.BlockMetadata{
		Tag:     tag,
		Height:  startHeight + 4,
		Skipped: true,
	}

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/validated.cscb.zstd", 10, 20, 20, true, "")
	s.insertConsolidationShadow(ctx, blocks[1], "consolidated/unvalidated.cscb.zstd", 30, 40, 40, false, "")

	actual, err := s.accessor.GetBlocksMissingConsolidationShadow(ctx, tag, startHeight, startHeight+5, 2)
	require.NoError(err)
	require.Len(actual, 2)
	require.Equal(s.getBlockMetadataID(ctx, blocks[1]), actual[0].ID)
	s.equalProto(blocks[1], actual[0].Metadata)
	require.Equal(s.getBlockMetadataID(ctx, blocks[3]), actual[1].ID)
	s.equalProto(blocks[3], actual[1].Metadata)
	require.False(actual[0].Metadata.GetSkipped())
	require.False(actual[1].Metadata.GetSkipped())

	height, found, err := s.accessor.GetFirstBlockMissingConsolidationShadow(ctx, tag, startHeight, startHeight+5)
	require.NoError(err)
	require.True(found)
	require.Equal(blocks[1].GetHeight(), height)

	height, found, err = s.accessor.GetFirstBlockMissingConsolidationShadow(ctx, tag, blocks[2].GetHeight(), startHeight+5)
	require.NoError(err)
	require.True(found)
	require.Equal(blocks[3].GetHeight(), height)

	height, found, err = s.accessor.GetFirstBlockMissingConsolidationShadow(ctx, tag, startHeight, blocks[1].GetHeight())
	require.NoError(err)
	require.False(found)
	require.Zero(height)
}

func (s *blockStorageTestSuite) TestGetBlockConsolidationShadowStats() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 8, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/first.cscb.zstd", 10, 20, 20, true, "")
	s.insertConsolidationShadow(ctx, blocks[1], "consolidated/first.cscb.zstd", 30, 40, 40, true, "")
	s.insertConsolidationShadow(ctx, blocks[2], "consolidated/second.cscb.zstd", 50, 60, 60, true, "")
	s.insertConsolidationShadow(ctx, blocks[3], "consolidated/unvalidated.cscb.zstd", 70, 80, 80, false, "")
	s.insertConsolidationShadow(ctx, blocks[4], "consolidated/wrong-single-block-key.cscb.zstd", 90, 100, 100, true, "single-block/key/does/not/match")
	s.insertConsolidationShadowWithIdentity(ctx, blocks[5], "consolidated/wrong-tag.cscb.zstd", 110, 120, 120, true, blocks[5].GetObjectKeyMain(), tag+1, blocks[5].GetHeight(), blocks[5].GetHash())
	s.insertConsolidationShadowWithIdentity(ctx, blocks[6], "consolidated/wrong-height.cscb.zstd", 130, 140, 140, true, blocks[6].GetObjectKeyMain(), tag, blocks[6].GetHeight()+1000, blocks[6].GetHash())
	s.insertConsolidationShadowWithIdentity(ctx, blocks[7], "consolidated/wrong-hash.cscb.zstd", 150, 160, 160, true, blocks[7].GetObjectKeyMain(), tag, blocks[7].GetHeight(), "wrong-hash")

	stats, err := s.accessor.GetBlockConsolidationShadowStats(ctx, tag, startHeight, startHeight+8)
	require.NoError(err)
	require.Equal(uint64(2), stats.Objects)
	require.Equal(uint64(3), stats.Blocks)
	require.Equal(uint64(8), stats.EligibleBlocks)

	stats, err = s.accessor.GetBlockConsolidationShadowStats(ctx, tag, startHeight+1, startHeight+3)
	require.NoError(err)
	require.Equal(uint64(2), stats.Objects)
	require.Equal(uint64(2), stats.Blocks)
	require.Equal(uint64(2), stats.EligibleBlocks)
}

func (s *blockStorageTestSuite) TestGetBlockConsolidationShadowStatsCountsSameKeyInDifferentGenerationsSeparately() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 2, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)
	placements := make([]*internal.ConsolidationShadowPlacement, 0, len(blocks))
	for i, block := range blocks {
		consolidatedGeneration := ""
		if i == 1 {
			consolidatedGeneration = "v2"
		}
		placements = append(placements, &internal.ConsolidationShadowPlacement{
			BlockMetadataID:               s.getBlockMetadataID(ctx, block),
			Tag:                           block.GetTag(),
			Height:                        block.GetHeight(),
			Hash:                          block.GetHash(),
			SingleBlockObjectKeyMain:      block.GetObjectKeyMain(),
			SingleBlockStorageGeneration:  block.GetStorageGeneration(),
			ConsolidatedObjectKeyMain:     "consolidated/shared-key.cscb.zstd",
			ConsolidatedStorageGeneration: consolidatedGeneration,
			ObjectFormat:                  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                    uint64(i * 100),
			ByteLength:                    100,
			UncompressedLength:            100,
		})
	}
	require.NoError(s.accessor.PersistBlockConsolidationShadows(ctx, placements))

	stats, err := s.accessor.GetBlockConsolidationShadowStats(ctx, tag, startHeight, startHeight+2)
	require.NoError(err)
	require.Equal(uint64(2), stats.Objects)
	require.Equal(uint64(2), stats.Blocks)
	require.Equal(uint64(2), stats.EligibleBlocks)
}

func (s *blockStorageTestSuite) TestGetBlockConsolidationShadowStatsExcludesSkippedBlocks() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 3, tag)
	blocks[1] = &api.BlockMetadata{
		Tag:     tag,
		Height:  startHeight + 1,
		Skipped: true,
	}
	blocks[2].ParentHeight = blocks[0].Height
	blocks[2].ParentHash = blocks[0].Hash

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/first.cscb.zstd", 10, 20, 20, true, "")
	s.insertConsolidationShadow(ctx, blocks[1], "consolidated/skipped.cscb.zstd", 30, 40, 40, true, "")
	s.insertConsolidationShadow(ctx, blocks[2], "consolidated/second.cscb.zstd", 50, 60, 60, true, "")

	stats, err := s.accessor.GetBlockConsolidationShadowStats(ctx, tag, startHeight, startHeight+3)
	require.NoError(err)
	require.Equal(uint64(2), stats.Objects)
	require.Equal(uint64(2), stats.Blocks)
	require.Equal(uint64(2), stats.EligibleBlocks)
}

func (s *blockStorageTestSuite) TestGetBlockConsolidationShadowStatsRequiresPromotionValidShadows() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 5, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/valid.cscb.zstd", 10, 20, 20, true, "")
	s.insertInvalidConsolidationShadow(
		ctx,
		blocks[1],
		"consolidated/wrong-format.cscb.zstd",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		30,
		40,
		40,
	)
	s.insertInvalidConsolidationShadow(
		ctx,
		blocks[2],
		"consolidated/zero-byte-length.cscb.zstd",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		50,
		0,
		60,
	)
	s.insertInvalidConsolidationShadow(
		ctx,
		blocks[3],
		"consolidated/zero-uncompressed-length.cscb.zstd",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		70,
		80,
		0,
	)
	s.insertConsolidationShadow(ctx, blocks[4], "consolidated/promoted.cscb.zstd", 90, 100, 100, true, "")
	result, err := s.accessor.PromoteBlockConsolidationShadows(ctx, tag, blocks[4].GetHeight(), blocks[4].GetHeight()+1, 10, config.DefaultSingleBlockObjectRetention)
	require.NoError(err)
	require.Equal(uint64(1), result.Blocks)

	stats, err := s.accessor.GetBlockConsolidationShadowStats(ctx, tag, startHeight, startHeight+5)
	require.NoError(err)
	require.Equal(uint64(2), stats.Objects)
	require.Equal(uint64(2), stats.Blocks)
	require.Equal(uint64(5), stats.EligibleBlocks)
}

func (s *blockStorageTestSuite) TestPersistBlockConsolidationShadowsGuardsPrimaryIdentity() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 2, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	err = s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:           s.getBlockMetadataID(ctx, blocks[0]),
			Tag:                       blocks[0].GetTag(),
			Height:                    blocks[0].GetHeight(),
			Hash:                      blocks[0].GetHash(),
			SingleBlockObjectKeyMain:  blocks[0].GetObjectKeyMain(),
			ConsolidatedObjectKeyMain: "consolidated/first.cscb.zstd",
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                100,
			ByteLength:                200,
			UncompressedLength:        200,
		},
	})
	require.NoError(err)

	shadow, err := s.accessor.GetBlockConsolidationShadow(ctx, blocks[0])
	require.NoError(err)
	s.equalProto(expectedConsolidationShadow(blocks[0], "consolidated/first.cscb.zstd", 100, 200, 200), shadow)

	primary, err := s.accessor.GetBlockByHeight(ctx, blocks[0].GetTag(), blocks[0].GetHeight())
	require.NoError(err)
	s.equalProto(blocks[0], primary)

	err = s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:           s.getBlockMetadataID(ctx, blocks[1]),
			Tag:                       blocks[1].GetTag(),
			Height:                    blocks[1].GetHeight(),
			Hash:                      blocks[1].GetHash(),
			SingleBlockObjectKeyMain:  "single-block/key/does/not/match",
			ConsolidatedObjectKeyMain: "consolidated/wrong.cscb.zstd",
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                300,
			ByteLength:                400,
			UncompressedLength:        400,
		},
	})
	require.Error(err)

	_, err = s.accessor.GetBlockConsolidationShadow(ctx, blocks[1])
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestPersistBlockConsolidationShadowsRollsBackBatchOnIdentityMismatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 2, tag)

	require.NoError(s.accessor.PersistBlockMetas(ctx, true, blocks, nil))
	firstMetadataID := s.getBlockMetadataID(ctx, blocks[0])
	secondMetadataID := s.getBlockMetadataID(ctx, blocks[1])

	err := s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:           firstMetadataID,
			Tag:                       blocks[0].GetTag(),
			Height:                    blocks[0].GetHeight(),
			Hash:                      blocks[0].GetHash(),
			SingleBlockObjectKeyMain:  blocks[0].GetObjectKeyMain(),
			ConsolidatedObjectKeyMain: "consolidated/valid.cscb.zstd",
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:                200,
			UncompressedLength:        200,
		},
		{
			BlockMetadataID:           secondMetadataID,
			Tag:                       blocks[1].GetTag(),
			Height:                    blocks[1].GetHeight(),
			Hash:                      blocks[1].GetHash(),
			SingleBlockObjectKeyMain:  "single-block/key/does/not/match",
			ConsolidatedObjectKeyMain: "consolidated/invalid.cscb.zstd",
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:                200,
			UncompressedLength:        200,
		},
	})
	require.Error(err)
	require.Contains(err.Error(), fmt.Sprintf("metadata_id=%d", secondMetadataID))

	for _, block := range blocks {
		_, err := s.accessor.GetBlockConsolidationShadow(ctx, block)
		require.True(xerrors.Is(err, errors.ErrItemNotFound))
	}
}

func (s *blockStorageTestSuite) TestPersistBlockConsolidationShadowsRollsBackBatchOnDeletedShadow() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 2, tag)

	require.NoError(s.accessor.PersistBlockMetas(ctx, true, blocks, nil))
	firstMetadataID := s.getBlockMetadataID(ctx, blocks[0])
	secondMetadataID := s.getBlockMetadataID(ctx, blocks[1])
	secondPlacement := &internal.ConsolidationShadowPlacement{
		BlockMetadataID:           secondMetadataID,
		Tag:                       blocks[1].GetTag(),
		Height:                    blocks[1].GetHeight(),
		Hash:                      blocks[1].GetHash(),
		SingleBlockObjectKeyMain:  blocks[1].GetObjectKeyMain(),
		ConsolidatedObjectKeyMain: "consolidated/deleted.cscb.zstd",
		ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteLength:                200,
		UncompressedLength:        200,
	}
	require.NoError(s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{secondPlacement}))
	_, err := s.db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_object_key_main = NULL,
			single_block_object_deleted_at = NOW()
		WHERE block_metadata_id = $1`, secondMetadataID)
	require.NoError(err)

	err = s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:           firstMetadataID,
			Tag:                       blocks[0].GetTag(),
			Height:                    blocks[0].GetHeight(),
			Hash:                      blocks[0].GetHash(),
			SingleBlockObjectKeyMain:  blocks[0].GetObjectKeyMain(),
			ConsolidatedObjectKeyMain: "consolidated/valid.cscb.zstd",
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:                200,
			UncompressedLength:        200,
		},
		secondPlacement,
	})
	require.Error(err)
	require.Contains(err.Error(), fmt.Sprintf("metadata_id=%d", secondMetadataID))
	require.Contains(err.Error(), "existing shadow is not writable")

	_, err = s.accessor.GetBlockConsolidationShadow(ctx, blocks[0])
	require.True(xerrors.Is(err, errors.ErrItemNotFound))
	var deletedAt sql.NullTime
	require.NoError(s.db.QueryRowContext(
		ctx,
		`SELECT single_block_object_deleted_at FROM block_consolidation_shadow WHERE block_metadata_id = $1`,
		secondMetadataID,
	).Scan(&deletedAt))
	require.True(deletedAt.Valid)
}

func (s *blockStorageTestSuite) TestPersistBlockConsolidationShadowsBulkBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	const batchSize = 1000
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, batchSize, tag)

	require.NoError(s.accessor.PersistBlockMetas(ctx, true, blocks, nil))
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, height FROM block_metadata WHERE tag = $1 AND height >= $2 AND height < $3`,
		tag,
		startHeight,
		startHeight+batchSize,
	)
	require.NoError(err)
	defer rows.Close()
	metadataIDs := make(map[uint64]int64, batchSize)
	for rows.Next() {
		var metadataID int64
		var height uint64
		require.NoError(rows.Scan(&metadataID, &height))
		metadataIDs[height] = metadataID
	}
	require.NoError(rows.Err())
	require.Len(metadataIDs, batchSize)

	placements := make([]*internal.ConsolidationShadowPlacement, 0, batchSize)
	for i, block := range blocks {
		placements = append(placements, &internal.ConsolidationShadowPlacement{
			BlockMetadataID:           metadataIDs[block.GetHeight()],
			Tag:                       block.GetTag(),
			Height:                    block.GetHeight(),
			Hash:                      block.GetHash(),
			SingleBlockObjectKeyMain:  block.GetObjectKeyMain(),
			ConsolidatedObjectKeyMain: "consolidated/bulk.cscb.zstd",
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                uint64(i * 100),
			ByteLength:                100,
			UncompressedLength:        100,
		})
	}
	require.NoError(s.accessor.PersistBlockConsolidationShadows(ctx, placements))

	var shadowCount int
	require.NoError(s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM block_consolidation_shadow`).Scan(&shadowCount))
	require.Equal(batchSize, shadowCount)
}

func (s *blockStorageTestSuite) TestConsolidationShadowPromotionMovesStorageGenerationAtomically() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	block := testutil.MakeBlockMetadata(s.config.Chain.BlockStartHeight, tag)
	block.StorageGeneration = ""

	err := s.accessor.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block}, nil)
	require.NoError(err)
	err = s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:               s.getBlockMetadataID(ctx, block),
			Tag:                           block.GetTag(),
			Height:                        block.GetHeight(),
			Hash:                          block.GetHash(),
			SingleBlockObjectKeyMain:      block.GetObjectKeyMain(),
			SingleBlockStorageGeneration:  "",
			ConsolidatedObjectKeyMain:     "consolidated/v2.cscb.zstd",
			ConsolidatedStorageGeneration: "v2",
			ObjectFormat:                  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                    100,
			ByteLength:                    200,
			UncompressedLength:            200,
		},
	})
	require.NoError(err)

	shadow, err := s.accessor.GetBlockConsolidationShadow(ctx, block)
	require.NoError(err)
	require.Equal("v2", shadow.GetStorageGeneration())
	require.Equal("consolidated/v2.cscb.zstd", shadow.GetObjectKeyMain())

	result, err := s.accessor.PromoteBlockConsolidationShadows(
		ctx,
		block.GetTag(),
		block.GetHeight(),
		block.GetHeight()+1,
		1,
		config.DefaultSingleBlockObjectRetention,
	)
	require.NoError(err)
	require.Equal(uint64(1), result.Blocks)

	primary, err := s.accessor.GetBlockByHeight(ctx, block.GetTag(), block.GetHeight())
	require.NoError(err)
	require.Equal("v2", primary.GetStorageGeneration())
	require.Equal("consolidated/v2.cscb.zstd", primary.GetObjectKeyMain())
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, primary.GetObjectFormat())
}

func (s *blockStorageTestSuite) TestPersistBlockConsolidationShadowsRejectsStaleSourceGeneration() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	block := testutil.MakeBlockMetadata(s.config.Chain.BlockStartHeight, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block}, nil)
	require.NoError(err)
	err = s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:               s.getBlockMetadataID(ctx, block),
			Tag:                           block.GetTag(),
			Height:                        block.GetHeight(),
			Hash:                          block.GetHash(),
			SingleBlockObjectKeyMain:      block.GetObjectKeyMain(),
			SingleBlockStorageGeneration:  "v2",
			ConsolidatedObjectKeyMain:     "consolidated/v2.cscb.zstd",
			ConsolidatedStorageGeneration: "v2",
			ObjectFormat:                  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:                    200,
			UncompressedLength:            200,
		},
	})
	require.Error(err)

	_, err = s.accessor.GetBlockConsolidationShadow(ctx, block)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestPersistBlockConsolidationShadowsReplacesStaleGenerationAndResetsRetention() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	block := testutil.MakeBlockMetadata(s.config.Chain.BlockStartHeight, tag)
	originalSingleBlockKey := block.GetObjectKeyMain()

	require.NoError(s.accessor.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block}, nil))
	require.NoError(s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:               s.getBlockMetadataID(ctx, block),
			Tag:                           block.GetTag(),
			Height:                        block.GetHeight(),
			Hash:                          block.GetHash(),
			SingleBlockObjectKeyMain:      originalSingleBlockKey,
			SingleBlockStorageGeneration:  "",
			ConsolidatedObjectKeyMain:     "consolidated/legacy.cscb.zstd",
			ConsolidatedStorageGeneration: "",
			ObjectFormat:                  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:                    200,
			UncompressedLength:            200,
		},
	}))

	promotion, err := s.accessor.PromoteBlockConsolidationShadows(
		ctx,
		block.GetTag(),
		block.GetHeight(),
		block.GetHeight()+1,
		1,
		config.DefaultSingleBlockObjectRetention,
	)
	require.NoError(err)
	require.Equal(uint64(1), promotion.Blocks)

	// A replay writes the same deterministic key to the configured v2 write bucket. The
	// primary row moves first; the next consolidation must replace the now-stale
	// legacy shadow rather than carrying its legacy retirement clock forward.
	replayed := proto.Clone(block).(*api.BlockMetadata)
	replayed.ObjectKeyMain = originalSingleBlockKey
	replayed.StorageGeneration = "v2"
	require.NoError(s.accessor.PersistBlockMetas(ctx, false, []*api.BlockMetadata{replayed}, nil))
	require.NoError(s.accessor.PersistBlockConsolidationShadows(ctx, []*internal.ConsolidationShadowPlacement{
		{
			BlockMetadataID:               s.getBlockMetadataID(ctx, replayed),
			Tag:                           replayed.GetTag(),
			Height:                        replayed.GetHeight(),
			Hash:                          replayed.GetHash(),
			SingleBlockObjectKeyMain:      originalSingleBlockKey,
			SingleBlockStorageGeneration:  "v2",
			ConsolidatedObjectKeyMain:     "consolidated/v2.cscb.zstd",
			ConsolidatedStorageGeneration: "v2",
			ObjectFormat:                  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:                    200,
			UncompressedLength:            200,
		},
	}))

	shadow, err := s.accessor.GetBlockConsolidationShadow(ctx, replayed)
	require.NoError(err)
	require.Equal("v2", shadow.GetStorageGeneration())
	require.Equal("consolidated/v2.cscb.zstd", shadow.GetObjectKeyMain())

	var sourceGeneration sql.NullString
	var consolidatedGeneration sql.NullString
	var retentionStartedAt sql.NullTime
	var deleteAfter sql.NullTime
	err = s.db.QueryRowContext(ctx, `
		SELECT single_block_storage_generation, consolidated_storage_generation,
			single_block_retention_started_at, single_block_delete_after
		FROM block_consolidation_shadow
		WHERE block_metadata_id = $1`, s.getBlockMetadataID(ctx, replayed)).Scan(
		&sourceGeneration,
		&consolidatedGeneration,
		&retentionStartedAt,
		&deleteAfter,
	)
	require.NoError(err)
	require.Equal("v2", sourceGeneration.String)
	require.Equal("v2", consolidatedGeneration.String)
	require.False(retentionStartedAt.Valid)
	require.False(deleteAfter.Valid)
}

func (s *blockStorageTestSuite) TestPromoteBlockConsolidationShadowsPromotesValidatedShadows() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 3, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/first.cscb.zstd", 10, 20, 20, true, "")
	s.insertConsolidationShadow(ctx, blocks[1], "consolidated/second.cscb.zstd", 30, 40, 40, true, "")

	beforePromotion := time.Now().UTC()
	result, err := s.accessor.PromoteBlockConsolidationShadows(ctx, tag, startHeight, startHeight+3, 10, config.DefaultSingleBlockObjectRetention)
	require.NoError(err)
	require.Equal(uint64(2), result.Blocks)

	primary, err := s.accessor.GetBlockByHeight(ctx, tag, blocks[0].GetHeight())
	require.NoError(err)
	s.equalProto(expectedConsolidationShadow(blocks[0], "consolidated/first.cscb.zstd", 10, 20, 20), primary)

	primary, err = s.accessor.GetBlockByHeight(ctx, tag, blocks[1].GetHeight())
	require.NoError(err)
	s.equalProto(expectedConsolidationShadow(blocks[1], "consolidated/second.cscb.zstd", 30, 40, 40), primary)

	primary, err = s.accessor.GetBlockByHeight(ctx, tag, blocks[2].GetHeight())
	require.NoError(err)
	s.equalProto(blocks[2], primary)

	audit := s.getConsolidationShadowAudit(ctx, blocks[0])
	require.Equal(blocks[0].GetObjectKeyMain(), audit.SingleBlockObjectKey)
	require.Equal("consolidated/first.cscb.zstd", audit.ConsolidatedObjectKey)
	require.NotNil(audit.RetiredAt)
	require.NotNil(audit.RetireAfter)
	require.WithinDuration(beforePromotion, *audit.RetiredAt, time.Minute)
	require.WithinDuration(audit.RetiredAt.Add(config.DefaultSingleBlockObjectRetention), *audit.RetireAfter, time.Minute)

	stats, err := s.accessor.GetBlockConsolidationShadowStats(ctx, tag, startHeight, startHeight+3)
	require.NoError(err)
	require.Equal(uint64(2), stats.Objects)
	require.Equal(uint64(2), stats.Blocks)
	require.Equal(uint64(3), stats.EligibleBlocks)
}

func (s *blockStorageTestSuite) TestGetFirstPromotableBlockConsolidationShadowFiltersCandidates() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 6, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	s.insertInvalidConsolidationShadow(
		ctx,
		blocks[0],
		"consolidated/invalid.cscb.zstd",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		10,
		0,
		20,
	)
	s.insertConsolidationShadow(ctx, blocks[1], "consolidated/skipped.cscb.zstd", 10, 20, 20, true, "")
	_, err = s.db.ExecContext(
		ctx,
		`UPDATE block_metadata SET skipped = true WHERE tag = $1 AND height = $2 AND hash = $3`,
		blocks[1].GetTag(),
		blocks[1].GetHeight(),
		blocks[1].GetHash(),
	)
	require.NoError(err)
	s.insertConsolidationShadow(ctx, blocks[2], "consolidated/unvalidated.cscb.zstd", 10, 20, 20, false, "")
	s.insertConsolidationShadow(ctx, blocks[3], "consolidated/promoted.cscb.zstd", 10, 20, 20, true, "")
	result, err := s.accessor.PromoteBlockConsolidationShadows(ctx, tag, blocks[3].GetHeight(), blocks[3].GetHeight()+1, 10, config.DefaultSingleBlockObjectRetention)
	require.NoError(err)
	require.Equal(uint64(1), result.Blocks)
	s.insertConsolidationShadow(ctx, blocks[4], "consolidated/first-promotable.cscb.zstd", 10, 20, 20, true, "")

	height, found, err := s.accessor.GetFirstPromotableBlockConsolidationShadow(ctx, tag, startHeight, startHeight+uint64(len(blocks)))
	require.NoError(err)
	require.True(found)
	require.Equal(blocks[4].GetHeight(), height)

	height, found, err = s.accessor.GetFirstPromotableBlockConsolidationShadow(ctx, tag, startHeight, blocks[4].GetHeight())
	require.NoError(err)
	require.False(found)
	require.Zero(height)

	height, found, err = s.accessor.GetFirstPromotableBlockConsolidationShadow(ctx, tag, blocks[4].GetHeight()+1, startHeight+uint64(len(blocks)))
	require.NoError(err)
	require.False(found)
	require.Zero(height)
}

// TestPromotionPlansBoundTheUnconsolidatedIndexByHeight reproduces the
// statistics robinhood-mainnet had on 2026-10-08: the planner believes no
// block_metadata row is unpromoted (null_frac(byte_length) = 0) while many are.
// Under them every promotion statement must still read
// idx_block_metadata_unconsolidated by height. An unbounded scan of that index
// timed out every promotion once a backlog formed, and each failure made the
// backlog larger.
func (s *blockStorageTestSuite) TestPromotionPlansBoundTheUnconsolidatedIndexByHeight() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 200, tag)
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, blocks, nil))
	windowStart, windowEnd := startHeight+100, startHeight+110
	// Validated shadows for the window, as production holds one per row.
	for _, block := range blocks[100:110] {
		s.insertConsolidationShadow(ctx, block, "consolidated/window.cscb.zstd", 10, 20, 20, true, "")
	}

	_, err := s.db.ExecContext(ctx, `ALTER TABLE block_metadata SET (autovacuum_enabled = false)`)
	require.NoError(err)
	defer func() {
		_, err := s.db.ExecContext(ctx, `ALTER TABLE block_metadata RESET (autovacuum_enabled)`)
		require.NoError(err)
	}()
	// Analyze while every row looks promoted, then make every row unpromoted
	// again without re-analyzing. VACUUM first so page counts do not depend on
	// rows earlier tests deleted.
	for _, statement := range []string{
		`UPDATE block_metadata SET byte_length = 1 WHERE tag = 1`,
		`VACUUM block_metadata`,
		`VACUUM canonical_blocks`,
		`VACUUM block_consolidation_shadow`,
		`ANALYZE block_metadata`,
		`ANALYZE canonical_blocks`,
		`ANALYZE block_consolidation_shadow`,
		`UPDATE block_metadata SET byte_length = NULL WHERE tag = 1`,
	} {
		_, err := s.db.ExecContext(ctx, statement)
		require.NoError(err, statement)
	}

	cscb := int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH)
	statements := []struct {
		name  string
		query string
		args  []any
	}{
		{"promotionInvalidShadowQuery", promotionInvalidShadowQuery, []any{tag, windowStart, windowEnd, cscb}},
		{"promotionQuery", promotionQuery, []any{tag, windowStart, windowEnd, 10, cscb, config.DefaultSingleBlockObjectRetention.Microseconds()}},
	}
	for _, statement := range statements {
		for _, node := range s.explainPlanNodes(ctx, statement.query, statement.args...) {
			nodeType, _ := node["Node Type"].(string)
			if node["Relation Name"] != "block_metadata" || !strings.Contains(nodeType, "Scan") {
				continue
			}
			// The wedge's signature is a scan of block_metadata with filters
			// only: the partial index read without its (tag, height) key, or
			// the UPDATE driven from it against the candidates CTE. Any index
			// condition is a bounded lookup (the window on the partial index,
			// or a candidate's primary key or unique (tag, hash)).
			condition, _ := node["Index Cond"].(string)
			if node["Index Name"] == "idx_block_metadata_unconsolidated" {
				require.Contains(condition, "height", "%s reads idx_block_metadata_unconsolidated without a height bound: %v", statement.name, node)
				continue
			}
			require.NotEmpty(condition, "%s scans block_metadata with no index condition: %v", statement.name, node)
		}
	}
}

// explainPlanNodes returns every node of the statement's plan. Sequential and
// bitmap scans are disabled for the explain: a production-sized table is read
// by plain index scans here, and the small test tables would otherwise plan
// unlike one.
func (s *blockStorageTestSuite) explainPlanNodes(ctx context.Context, query string, args ...any) []map[string]any {
	require := testutil.Require(s.T())
	tx, err := s.db.BeginTx(ctx, nil)
	require.NoError(err)
	defer func() { _ = tx.Rollback() }()
	for _, setting := range []string{`SET LOCAL enable_seqscan = off`, `SET LOCAL enable_bitmapscan = off`} {
		_, err = tx.ExecContext(ctx, setting)
		require.NoError(err)
	}
	var raw []byte
	require.NoError(tx.QueryRowContext(ctx, "EXPLAIN (FORMAT JSON) "+query, args...).Scan(&raw))
	var plans []map[string]any
	require.NoError(json.Unmarshal(raw, &plans))
	require.NotEmpty(plans)
	var nodes []map[string]any
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		nodes = append(nodes, node)
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if childNode, ok := child.(map[string]any); ok {
				walk(childNode)
			}
		}
	}
	root, ok := plans[0]["Plan"].(map[string]any)
	require.True(ok, "EXPLAIN returned no plan")
	walk(root)
	return nodes
}

func (s *blockStorageTestSuite) TestPromoteBlockConsolidationShadowsMissingShadowNoOps() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 1, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	result, err := s.accessor.PromoteBlockConsolidationShadows(ctx, tag, startHeight, startHeight+1, 10, config.DefaultSingleBlockObjectRetention)
	require.NoError(err)
	require.Equal(uint64(0), result.Blocks)

	primary, err := s.accessor.GetBlockByHeight(ctx, tag, blocks[0].GetHeight())
	require.NoError(err)
	s.equalProto(blocks[0], primary)
}

func (s *blockStorageTestSuite) TestPromoteBlockConsolidationShadowsRejectsInvalidShadowMetadata() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 1, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)
	s.insertInvalidConsolidationShadow(
		ctx,
		blocks[0],
		"consolidated/invalid.cscb.zstd",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		10,
		20,
		20,
	)

	result, err := s.accessor.PromoteBlockConsolidationShadows(ctx, tag, startHeight, startHeight+1, 10, config.DefaultSingleBlockObjectRetention)
	require.Error(err)
	require.Nil(result)
	require.Contains(err.Error(), "invalid consolidation shadow metadata")

	primary, err := s.accessor.GetBlockByHeight(ctx, tag, blocks[0].GetHeight())
	require.NoError(err)
	s.equalProto(blocks[0], primary)
}

func (s *blockStorageTestSuite) TestPromoteBlockConsolidationShadowsSkipsStaleReorgMetadata() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 4, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)
	s.insertConsolidationShadow(ctx, blocks[3], "consolidated/stale.cscb.zstd", 10, 20, 20, true, "")

	reorgBlock := proto.Clone(blocks[3]).(*api.BlockMetadata)
	reorgBlock.Hash = "0xreorg"
	reorgBlock.ParentHash = blocks[2].GetHash()
	reorgBlock.ObjectKeyMain = "single-block/reorg.gzip"
	err = s.accessor.PersistBlockMetas(ctx, true, []*api.BlockMetadata{reorgBlock}, blocks[2])
	require.NoError(err)

	result, err := s.accessor.PromoteBlockConsolidationShadows(ctx, tag, reorgBlock.GetHeight(), reorgBlock.GetHeight()+1, 10, config.DefaultSingleBlockObjectRetention)
	require.NoError(err)
	require.Equal(uint64(0), result.Blocks)

	primary, err := s.accessor.GetBlockByHeight(ctx, tag, reorgBlock.GetHeight())
	require.NoError(err)
	s.equalProto(reorgBlock, primary)
}

func (s *blockStorageTestSuite) TestPromoteBlockConsolidationShadowsIdempotentRetry() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 1, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)
	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/first.cscb.zstd", 10, 20, 20, true, "")

	result, err := s.accessor.PromoteBlockConsolidationShadows(ctx, tag, startHeight, startHeight+1, 10, config.DefaultSingleBlockObjectRetention)
	require.NoError(err)
	require.Equal(uint64(1), result.Blocks)

	result, err = s.accessor.PromoteBlockConsolidationShadows(ctx, tag, startHeight, startHeight+1, 10, config.DefaultSingleBlockObjectRetention)
	require.NoError(err)
	require.Equal(uint64(0), result.Blocks)

	primary, err := s.accessor.GetBlockByHeight(ctx, tag, blocks[0].GetHeight())
	require.NoError(err)
	s.equalProto(expectedConsolidationShadow(blocks[0], "consolidated/first.cscb.zstd", 10, 20, 20), primary)
}

func (s *blockStorageTestSuite) TestPromoteBlockConsolidationShadowsRollsBackOnInvalidCandidate() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 2, tag)

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)
	s.insertConsolidationShadow(ctx, blocks[0], "consolidated/valid.cscb.zstd", 10, 20, 20, true, "")
	s.insertInvalidConsolidationShadow(
		ctx,
		blocks[1],
		"consolidated/invalid.cscb.zstd",
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		30,
		0,
		40,
	)

	_, err = s.accessor.PromoteBlockConsolidationShadows(ctx, tag, startHeight, startHeight+2, 10, config.DefaultSingleBlockObjectRetention)
	require.Error(err)
	require.Contains(err.Error(), "invalid consolidation shadow metadata")

	primary, err := s.accessor.GetBlockByHeight(ctx, tag, blocks[0].GetHeight())
	require.NoError(err)
	s.equalProto(blocks[0], primary)

	primary, err = s.accessor.GetBlockByHeight(ctx, tag, blocks[1].GetHeight())
	require.NoError(err)
	s.equalProto(blocks[1], primary)
}

func (s *blockStorageTestSuite) TestWatermarkVisibilityControl() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	// Create blocks
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag)

	// Persist blocks WITHOUT watermark (updateWatermark=false)
	err := s.accessor.PersistBlockMetas(ctx, false, blocks, nil)
	require.NoError(err)

	// GetLatestBlock should return ErrItemNotFound because no blocks are watermarked
	_, err = s.accessor.GetLatestBlock(ctx, tag)
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound), "Expected ErrItemNotFound when no watermark exists")

	// Now persist the same blocks WITH watermark (updateWatermark=true)
	err = s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	// GetLatestBlock should now return the highest block
	latestBlock, err := s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.NotNil(latestBlock)
	require.Equal(blocks[9].Height, latestBlock.Height)
	require.Equal(blocks[9].Hash, latestBlock.Hash)

	// Add more blocks with watermark
	moreBlocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight+10, 5, tag)
	err = s.accessor.PersistBlockMetas(ctx, true, moreBlocks, nil)
	require.NoError(err)

	// GetLatestBlock should return the new highest watermarked block
	latestBlock, err = s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.Equal(moreBlocks[4].Height, latestBlock.Height)
	require.Equal(moreBlocks[4].Hash, latestBlock.Hash)
}

func (s *blockStorageTestSuite) TestWatermarkWithReorg() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	// Create initial chain
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag)
	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	// Verify latest block
	latestBlock, err := s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.Equal(blocks[9].Height, latestBlock.Height)

	// Simulate reorg: create alternative chain from height startHeight+7
	// This represents the reorg scenario where we need to replace blocks
	reorgBlocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight+7, 3, tag)
	// Change hashes to simulate different blocks
	for i := range reorgBlocks {
		reorgBlocks[i].Hash = fmt.Sprintf("0xreorg%d", i)
		if i > 0 {
			reorgBlocks[i].ParentHash = reorgBlocks[i-1].Hash
		} else {
			reorgBlocks[i].ParentHash = blocks[6].Hash // Link to pre-reorg chain
		}
	}

	// Persist reorg blocks with watermark
	err = s.accessor.PersistBlockMetas(ctx, true, reorgBlocks, blocks[6])
	require.NoError(err)

	// GetLatestBlock should return the new reorg tip
	latestBlock, err = s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	require.Equal(reorgBlocks[2].Height, latestBlock.Height)
	require.Equal(reorgBlocks[2].Hash, latestBlock.Hash)
}

func (s *blockStorageTestSuite) TestWatermarkMultipleTags() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	tag1 := uint32(1)
	tag2 := uint32(2)

	// Create blocks for tag1
	blocks1 := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 5, tag1)
	err := s.accessor.PersistBlockMetas(ctx, true, blocks1, nil)
	require.NoError(err)

	// Create blocks for tag2
	blocks2 := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag2)
	err = s.accessor.PersistBlockMetas(ctx, true, blocks2, nil)
	require.NoError(err)

	// Verify each tag has its own latest block
	latestBlock1, err := s.accessor.GetLatestBlock(ctx, tag1)
	require.NoError(err)
	require.Equal(blocks1[4].Height, latestBlock1.Height)

	latestBlock2, err := s.accessor.GetLatestBlock(ctx, tag2)
	require.NoError(err)
	require.Equal(blocks2[9].Height, latestBlock2.Height)
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightRangeStillWorks() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight

	// Create blocks without watermark
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag)
	err := s.accessor.PersistBlockMetas(ctx, false, blocks, nil)
	require.NoError(err)

	// GetBlocksByHeightRange should still work even without watermark
	// This is important for defense-in-depth validation
	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+10)
	require.NoError(err)
	require.Len(fetchedBlocks, 10)

	// Verify the blocks are correct
	sort.Slice(fetchedBlocks, func(i, j int) bool {
		return fetchedBlocks[i].Height < fetchedBlocks[j].Height
	})
	for i := 0; i < 10; i++ {
		s.equalProto(blocks[i], fetchedBlocks[i])
	}
}

func TestIntegrationBlockStorageTestSuite(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &blockStorageTestSuite{config: cfg})
}

// ---------------------------------------------------------------------------------------------
// Set-based PersistBlockMetas (INF-1675)
// ---------------------------------------------------------------------------------------------

func (s *blockStorageTestSuite) cloneBlocks(blocks []*api.BlockMetadata) []*api.BlockMetadata {
	cloned := make([]*api.BlockMetadata, len(blocks))
	for i, block := range blocks {
		cloned[i] = proto.Clone(block).(*api.BlockMetadata)
	}
	return cloned
}

func (s *blockStorageTestSuite) countBlockMetadataAtHeight(ctx context.Context, blockTag uint32, height uint64) int {
	require := testutil.Require(s.T())
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM block_metadata WHERE tag = $1 AND height = $2`, blockTag, height).Scan(&count)
	require.NoError(err)
	return count
}

func (s *blockStorageTestSuite) canonicalIDsByHeight(ctx context.Context, blockTag uint32) map[uint64]int64 {
	require := testutil.Require(s.T())
	rows, err := s.db.QueryContext(ctx, `SELECT height, block_metadata_id FROM canonical_blocks WHERE tag = $1`, blockTag)
	require.NoError(err)
	defer rows.Close()
	ids := make(map[uint64]int64)
	for rows.Next() {
		var height uint64
		var id int64
		require.NoError(rows.Scan(&height, &id))
		ids[height] = id
	}
	require.NoError(rows.Err())
	return ids
}

func (s *blockStorageTestSuite) countBlockMetadata(ctx context.Context, blockTag uint32) int {
	require := testutil.Require(s.T())
	var count int
	require.NoError(s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM block_metadata WHERE tag = $1`, blockTag).Scan(&count))
	return count
}

func (s *blockStorageTestSuite) TestPersistBlockMetasSkippedAcrossChunkBoundaries() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	boundary := persistBlockMetasChunkSize
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, boundary+5, tag)
	// Skipped rows straddle the first chunk boundary: last row of chunk 1, first two of chunk 2.
	for _, i := range []int{boundary - 1, boundary, boundary + 1} {
		blocks[i] = &api.BlockMetadata{Tag: tag, Height: startHeight + uint64(i), Skipped: true}
	}
	blocks[boundary+2].ParentHeight = blocks[boundary-2].Height
	blocks[boundary+2].ParentHash = blocks[boundary-2].Hash
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(blocks), nil))

	fetched, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(len(blocks)))
	require.NoError(err)
	require.Len(fetched, len(blocks))
	for i := range blocks {
		s.equalProto(blocks[i], fetched[i])
	}
	latest, err := s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	s.equalProto(blocks[len(blocks)-1], latest)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasAllSkippedBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := make([]*api.BlockMetadata, 0, 3)
	for i := uint64(0); i < 3; i++ {
		blocks = append(blocks, &api.BlockMetadata{Tag: tag, Height: startHeight + i, Skipped: true})
	}
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(blocks), nil))

	fetched, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+3)
	require.NoError(err)
	require.Len(fetched, 3)
	for i := range blocks {
		s.equalProto(blocks[i], fetched[i])
	}
	latest, err := s.accessor.GetLatestBlock(ctx, tag)
	require.NoError(err)
	s.equalProto(blocks[2], latest)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasSameHeightRegularAndSkippedLastWins() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	chain := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 5, tag)
	skippedAt2 := &api.BlockMetadata{Tag: tag, Height: startHeight + 2, Skipped: true}
	skippedAt3 := &api.BlockMetadata{Tag: tag, Height: startHeight + 3, Skipped: true}
	// Caller order decides: the skipped row comes after the regular one at height 2 and before it
	// at height 3. Chain validation ignores skipped rows, so the regular chain stays continuous.
	batch := []*api.BlockMetadata{chain[0], chain[1], chain[2], skippedAt2, skippedAt3, chain[3], chain[4]}
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(batch), nil))

	at2, err := s.accessor.GetBlockByHeight(ctx, tag, startHeight+2)
	require.NoError(err)
	s.equalProto(skippedAt2, at2)
	at3, err := s.accessor.GetBlockByHeight(ctx, tag, startHeight+3)
	require.NoError(err)
	s.equalProto(chain[3], at3)
	// Both rows are kept in block_metadata and the regular ones stay retrievable by hash.
	require.Equal(2, s.countBlockMetadataAtHeight(ctx, tag, startHeight+2))
	require.Equal(2, s.countBlockMetadataAtHeight(ctx, tag, startHeight+3))
	s.equalProto(chain[2], mustGetBlockByHash(s.T(), s.accessor, chain[2]))
	s.equalProto(chain[3], mustGetBlockByHash(s.T(), s.accessor, chain[3]))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasSameHeightReorgPairLastWins() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	chain := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 10, tag)
	top := chain[9]
	altA := proto.Clone(top).(*api.BlockMetadata)
	altA.Hash = "0xalt-a"
	altA.ParentHash = ""
	altA.ParentHeight = 0
	altA.ObjectKeyMain = "alt-a"
	altB := proto.Clone(altA).(*api.BlockMetadata)
	altB.Hash = "0xalt-b"
	altB.ObjectKeyMain = "alt-b"

	batch := append(s.cloneBlocks(chain), proto.Clone(altA).(*api.BlockMetadata), proto.Clone(altB).(*api.BlockMetadata))
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, batch, nil))
	canonical, err := s.accessor.GetBlockByHeight(ctx, tag, top.Height)
	require.NoError(err)
	s.equalProto(altB, canonical)
	s.equalProto(top, mustGetBlockByHash(s.T(), s.accessor, top))
	s.equalProto(altA, mustGetBlockByHash(s.T(), s.accessor, altA))
	require.Equal(3, s.countBlockMetadataAtHeight(ctx, tag, top.Height))

	// Reversed caller order among the same-height blocks flips the winner.
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks([]*api.BlockMetadata{altB, altA}), nil))
	canonical, err = s.accessor.GetBlockByHeight(ctx, tag, top.Height)
	require.NoError(err)
	s.equalProto(altA, canonical)
	require.Equal(3, s.countBlockMetadataAtHeight(ctx, tag, top.Height))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasDuplicateEntriesInBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	chain := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 5, tag)
	duplicate := proto.Clone(chain[2]).(*api.BlockMetadata)
	duplicate.ParentHash = ""
	duplicate.ParentHeight = 0
	duplicate.ObjectKeyMain = "duplicate"
	skippedA := &api.BlockMetadata{Tag: tag, Height: startHeight + 5, Skipped: true}
	skippedB := &api.BlockMetadata{Tag: tag, Height: startHeight + 5, Skipped: true}
	// The same conflict key twice in one batch must not trip "cannot affect row a second time".
	batch := []*api.BlockMetadata{chain[0], chain[1], chain[2], duplicate, chain[3], chain[4], skippedA, skippedB}
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(batch), nil))

	require.Equal(1, s.countBlockMetadataAtHeight(ctx, tag, startHeight+2))
	at2, err := s.accessor.GetBlockByHeight(ctx, tag, startHeight+2)
	require.NoError(err)
	require.Equal("duplicate", at2.ObjectKeyMain, "the later duplicate updates the row")
	require.Equal(1, s.countBlockMetadataAtHeight(ctx, tag, startHeight+5))
	at5, err := s.accessor.GetBlockByHeight(ctx, tag, startHeight+5)
	require.NoError(err)
	require.True(at5.Skipped)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasIdempotentReplay() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	size := persistBlockMetasChunkSize + 1
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, size, tag)
	mid := size / 2
	blocks[mid] = &api.BlockMetadata{Tag: tag, Height: startHeight + uint64(mid), Skipped: true}
	blocks[mid+1].ParentHeight = blocks[mid-1].Height
	blocks[mid+1].ParentHash = blocks[mid-1].Hash
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(blocks), nil))
	ids := s.canonicalIDsByHeight(ctx, tag)
	count := s.countBlockMetadata(ctx, tag)
	require.Len(ids, size)
	require.Equal(size, count)

	// An identical replay changes nothing.
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(blocks), nil))
	require.Equal(ids, s.canonicalIDsByHeight(ctx, tag))
	require.Equal(count, s.countBlockMetadata(ctx, tag))

	// A replay with new placements updates the rows in place and keeps every id.
	replayed := s.cloneBlocks(blocks)
	for _, block := range replayed {
		if block.Skipped {
			continue
		}
		block.ObjectKeyMain += ".replayed"
		block.Timestamp = utils.ToTimestamp(block.GetTimestamp().GetSeconds() + 1)
	}
	require.NoError(s.accessor.PersistBlockMetas(ctx, false, s.cloneBlocks(replayed), nil))
	require.Equal(ids, s.canonicalIDsByHeight(ctx, tag))
	require.Equal(count, s.countBlockMetadata(ctx, tag))
	fetched, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(size))
	require.NoError(err)
	require.Len(fetched, size)
	for i := range replayed {
		s.equalProto(replayed[i], fetched[i])
	}
}

func (s *blockStorageTestSuite) TestPersistBlockMetasMultiTagBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	const otherTag = tag + 1
	// Chain validation ignores tags, so the two tags must occupy disjoint height ranges within one
	// batch; the second tag's first block has no parent hash so the check does not cross tags.
	first := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 5, tag)
	second := testutil.MakeBlockMetadatasFromStartHeight(startHeight+5, 5, otherTag)
	second[0].ParentHash = ""
	second[0].ParentHeight = 0
	batch := append(s.cloneBlocks(first), s.cloneBlocks(second)...)
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, batch, nil))

	for _, expected := range [][]*api.BlockMetadata{first, second} {
		fetched, err := s.accessor.GetBlocksByHeightRange(ctx, expected[0].Tag, expected[0].Height, expected[0].Height+5)
		require.NoError(err)
		require.Len(fetched, 5)
		for i := range expected {
			s.equalProto(expected[i], fetched[i])
		}
		// Nothing leaked into the other tag's canonical chain.
		_, err = s.accessor.GetBlockByHeight(ctx, expected[0].Tag, expected[0].Height+5)
		require.Error(err)
		require.True(xerrors.Is(err, errors.ErrItemNotFound))
	}
}

func (s *blockStorageTestSuite) TestPersistBlockMetasMixedObjectFormatsBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 40, tag)
	for i, block := range blocks {
		if i%2 == 0 {
			block.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
			continue
		}
		block.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
		block.ObjectKeyMain = fmt.Sprintf("consolidated/batch-%d.cscb.gzip", i/10)
		block.ByteOffset = uint64(i) * 100
		block.ByteLength = 100
		block.UncompressedLength = 250
		block.StorageGeneration = "v2"
	}
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(blocks), nil))

	fetched, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+40)
	require.NoError(err)
	require.Len(fetched, 40)
	for i := range blocks {
		s.equalProto(blocks[i], fetched[i])
	}
}

// fenceBlockForRetirement moves an already persisted single-block row to a CSCB placement, records
// its shadow, and prepares its retirement, which fences the row. It returns the row id and the
// consolidated object key the row must keep from then on.
func (s *blockStorageTestSuite) fenceBlockForRetirement(ctx context.Context, block *api.BlockMetadata) (int64, string) {
	require := testutil.Require(s.T())
	guardStorage, ok := s.accessor.(internal.SingleBlockUploadGuardStorage)
	require.True(ok)

	cscbKey := fmt.Sprintf("consolidated/fenced-%d.cscb.gzip", block.Height)
	var blockMetadataID int64
	err := s.db.QueryRowContext(ctx, `
		UPDATE block_metadata
		SET object_key_main = $1,
			object_format = $2,
			byte_offset = $3,
			byte_length = $4,
			uncompressed_length = $5
		WHERE tag = $6 AND hash = $7
		RETURNING id`,
		cscbKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		64,
		128,
		256,
		block.Tag,
		block.Hash,
	).Scan(&blockMetadataID)
	require.NoError(err)

	validatedAt := time.Now().UTC().Add(-96 * time.Hour)
	retireAfter := validatedAt.Add(72 * time.Hour)
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, single_block_object_key_main,
			consolidated_object_key_main, object_format, byte_offset, byte_length,
			uncompressed_length, validated_at, single_block_retention_started_at, single_block_delete_after
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12)`,
		blockMetadataID,
		block.Tag,
		block.Height,
		block.Hash,
		block.ObjectKeyMain,
		cscbKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		64,
		128,
		256,
		validatedAt,
		retireAfter,
	)
	require.NoError(err)

	repo := retirement.NewPostgresRepository(s.db)
	manifest := retirement.RetirementManifest{
		BlockMetadataID:                blockMetadataID,
		Tag:                            block.Tag,
		Height:                         block.Height,
		Hash:                           block.Hash,
		State:                          retirement.RetirementStateEligible,
		Bucket:                         "integration-bucket",
		SingleBlockObjectKey:           block.ObjectKeyMain,
		SingleBlockObjectKeySHA256:     sha256Hex(block.ObjectKeyMain),
		SingleBlockObjectVersionIDs:    []string{"single-block-v1"},
		SingleBlockObjectETag:          "single-block-etag",
		SingleBlockObjectBytes:         512,
		ConsolidatedObjectKey:          cscbKey,
		ConsolidatedObjectVersionID:    "cscb-v1",
		ConsolidatedObjectETag:         "cscb-etag",
		ConsolidatedByteOffset:         64,
		ConsolidatedByteLength:         128,
		ConsolidatedUncompressedLength: 256,
		PayloadSHA256:                  strings.Repeat("a", 64),
		PreparedAt:                     time.Now().UTC(),
	}
	require.NoError(repo.PrepareRetirement(ctx, manifest, ""))

	fencedGuard, err := guardStorage.AcquireSingleBlockUploadGuard(ctx, block.Tag, block.Height, block.Hash)
	require.NoError(err)
	require.True(fencedGuard.RetirementFenced())
	require.NoError(fencedGuard.Release())
	return blockMetadataID, cscbKey
}

func (s *blockStorageTestSuite) TestPersistBlockMetasPreservesRetirementFencedCSCBPlacementInBatch() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 3, tag)
	for _, block := range blocks {
		block.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
		block.ObjectKeyMain = fmt.Sprintf("single-block/%d.gzip", block.Height)
	}
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(blocks), nil))
	fencedID, cscbKey := s.fenceBlockForRetirement(ctx, blocks[1])
	idsBefore := s.canonicalIDsByHeight(ctx, tag)

	// Replay all three rows with new single-block placements in one set-based statement.
	replayed := s.cloneBlocks(blocks)
	for _, block := range replayed {
		block.ObjectKeyMain = fmt.Sprintf("single-block/replayed-%d.gzip", block.Height)
		block.StorageGeneration = "v2"
	}
	require.NoError(s.accessor.PersistBlockMetas(ctx, false, s.cloneBlocks(replayed), nil))

	for _, fetched := range []*api.BlockMetadata{
		mustGetBlockByHash(s.T(), s.accessor, blocks[1]),
		mustGetBlockByHeight(s.T(), s.accessor, blocks[1]),
	} {
		require.Equal(cscbKey, fetched.ObjectKeyMain)
		require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, fetched.ObjectFormat)
		require.Equal(uint64(64), fetched.ByteOffset)
		require.Equal(uint64(128), fetched.ByteLength)
		require.Equal(uint64(256), fetched.UncompressedLength)
		require.Equal("", fetched.GetStorageGeneration())
	}
	for _, i := range []int{0, 2} {
		fetched := mustGetBlockByHeight(s.T(), s.accessor, blocks[i])
		s.equalProto(replayed[i], fetched)
	}
	idsAfter := s.canonicalIDsByHeight(ctx, tag)
	require.Equal(idsBefore, idsAfter)
	require.Equal(fencedID, idsAfter[blocks[1].Height])
}

func isPostgresDeadlock(err error) bool {
	var pqErr *pq.Error
	return xerrors.As(err, &pqErr) && pqErr.Code == "40P01"
}

func (s *blockStorageTestSuite) TestPersistBlockMetasConcurrentOverlappingWriters() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	chain := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 450, tag)
	// Two writers on overlapping ranges, the shape of a poller and a backfiller sharing a tag.
	ranges := [][2]int{{0, 300}, {150, 450}}
	const rounds = 5
	var wg sync.WaitGroup
	errs := make(chan error, len(ranges)*rounds)
	deadlocks := make(chan struct{}, len(ranges)*rounds*10)
	for _, r := range ranges {
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				var err error
				for attempt := 0; attempt < 10; attempt++ {
					err = s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(chain[lo:hi]), nil)
					if err == nil || !isPostgresDeadlock(err) {
						break
					}
					deadlocks <- struct{}{}
				}
				if err != nil {
					errs <- err
				}
			}
		}(r[0], r[1])
	}
	wg.Wait()
	close(errs)
	close(deadlocks)
	for err := range errs {
		require.NoError(err)
	}
	s.T().Logf("detected deadlocks retried: %d", len(deadlocks))

	fetched, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+450)
	require.NoError(err)
	require.Len(fetched, 450)
	for i := range chain {
		s.equalProto(chain[i], fetched[i])
	}
	require.Equal(450, s.countBlockMetadata(ctx, tag))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasWaitsForSingleBlockUploadGuard() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, 3, tag)
	require.NoError(s.accessor.PersistBlockMetas(ctx, true, s.cloneBlocks(blocks), nil))

	guardStorage, ok := s.accessor.(internal.SingleBlockUploadGuardStorage)
	require.True(ok)
	guard, err := guardStorage.AcquireSingleBlockUploadGuard(ctx, blocks[1].Tag, blocks[1].Height, blocks[1].Hash)
	require.NoError(err)
	require.False(guard.RetirementFenced())
	defer func() { _ = guard.Release() }()

	replayed := s.cloneBlocks(blocks)
	for _, block := range replayed {
		block.ObjectKeyMain += ".replayed"
	}
	done := make(chan error, 1)
	go func() {
		done <- s.accessor.PersistBlockMetas(ctx, false, s.cloneBlocks(replayed), nil)
	}()
	select {
	case err := <-done:
		require.Failf("persist bypassed the single-block upload guard", "unexpected result: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(guard.Release())
	select {
	case err := <-done:
		require.NoError(err)
	case <-time.After(10 * time.Second):
		require.Fail("persist did not complete after the upload guard was released")
	}
	fetched := mustGetBlockByHeight(s.T(), s.accessor, blocks[1])
	s.equalProto(replayed[1], fetched)
}
