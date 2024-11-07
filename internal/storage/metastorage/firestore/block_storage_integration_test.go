package firestore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	storage_errors "github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	tag = 1
)

type blockStorageTestSuite struct {
	suite.Suite
	accessor internal.MetaStorage
	config   *config.Config
}

func (s *blockStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var accessor internal.MetaStorage
	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.BlockStartHeight = 10
	cfg.StorageType.MetaStorageType = config.MetaStorageType_FIRESTORE
	s.config = cfg
	app := testapp.New(
		s.T(),
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&accessor),
		fx.Populate(&cfg),
	)
	defer app.Close()
	s.accessor = accessor
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/emulator/v1/projects/%s/databases/(default)/documents", os.Getenv("FIRESTORE_EMULATOR_HOST"), cfg.GCP.Project), nil)
	require.NoError(err)
	_, err = http.DefaultClient.Do(req)
	require.NoError(err)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByMaxWriteSize() {
	tests := []struct {
		totalBlocks int
	}{
		{totalBlocks: maxBulkWriteSize},
		{totalBlocks: maxBulkWriteSize * 2},
		{totalBlocks: maxBulkWriteSize * 4},
		{totalBlocks: maxBulkWriteSize * 8},
	}
	for _, test := range tests {
		s.T().Run(fmt.Sprintf("test %d blocks", test.totalBlocks), func(t *testing.T) {
			s.runTestPersistBlockMetas(test.totalBlocks)
		})
	}
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByMaxReadSize() {
	tests := []struct {
		totalBlocks int
	}{
		{totalBlocks: maxBulkWriteSize},
		{totalBlocks: maxBulkWriteSize * 5},
	}
	for _, test := range tests {
		s.T().Run(fmt.Sprintf("test %d blocks", test.totalBlocks), func(t *testing.T) {
			s.runTestPersistBlockMetas(test.totalBlocks)
		})
	}
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByInvalidChain() {
	require := testutil.Require(s.T())
	blocks := testutil.MakeBlockMetadatas(100, tag)
	blocks[73].Hash = "0xdeadbeef"
	err := s.accessor.PersistBlockMetas(context.Background(), true, blocks, nil)
	require.Error(err)
	require.True(errors.Is(err, parser.ErrInvalidChain))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByInvalidLastBlock() {
	require := testutil.Require(s.T())
	blocks := testutil.MakeBlockMetadatasFromStartHeight(1_000_000, 100, tag)
	lastBlock := testutil.MakeBlockMetadata(999_999, tag)
	lastBlock.Hash = "0xdeadbeef"
	err := s.accessor.PersistBlockMetas(context.Background(), true, blocks, lastBlock)
	require.Error(err)
	require.True(errors.Is(err, parser.ErrInvalidChain))
}

func (s *blockStorageTestSuite) TestPersistBlockMetasNotUpdatingWatermark() {
	require := testutil.Require(s.T())
	blocks := testutil.MakeBlockMetadatasFromStartHeight(1_000_000, 100, tag)
	err := s.accessor.PersistBlockMetas(context.Background(), true, blocks[:50], nil)
	require.NoError(err)
	latestBlock, err := s.accessor.GetLatestBlock(context.Background(), tag)
	require.NoError(err)
	require.Equal(uint64(1000049), latestBlock.Height)

	// Latest should remain at the previous height when updateWatermark is set to false.
	err = s.accessor.PersistBlockMetas(context.Background(), false, blocks[50:], nil)
	require.NoError(err)
	latestBlock, err = s.accessor.GetLatestBlock(context.Background(), tag)
	require.NoError(err)
	require.Equal(uint64(1000049), latestBlock.Height)
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

func (s *blockStorageTestSuite) runTestPersistBlockMetas(totalBlocks int) {
	require := testutil.Require(s.T())
	startHeight := s.config.Chain.BlockStartHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, totalBlocks, tag)
	log := zaptest.NewLogger(s.T())
	ctx := context.TODO()

	// shuffle it to make sure it still works
	shuffleSeed := time.Now().UnixNano()
	rand.Seed(shuffleSeed)
	rand.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })
	log.Info(fmt.Sprintf("shuffled blocks with seed %d", shuffleSeed))

	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	if err != nil {
		panic(err)
	}

	expectedLatestBlock := proto.Clone(blocks[totalBlocks-1])

	// fetch range with missing item
	_, err = s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(totalBlocks+100))
	require.Error(err)
	require.True(errors.Is(err, storage_errors.ErrItemNotFound))

	// fetch valid range
	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, startHeight, startHeight+uint64(totalBlocks))
	if err != nil {
		panic(err)
	}
	sort.Slice(fetchedBlocks, func(i, j int) bool {
		return fetchedBlocks[i].Height < fetchedBlocks[j].Height
	})
	assert.Len(s.T(), fetchedBlocks, int(totalBlocks))

	for i := 0; i < len(blocks); i++ {
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
		panic(err)
	}
	assert.Len(s.T(), fetchedBlocksMeta, 4)
	s.equalProto(blocks[1], fetchedBlocksMeta[0])
	s.equalProto(blocks[totalBlocks/2], fetchedBlocksMeta[1])
	s.equalProto(blocks[0], fetchedBlocksMeta[2])
	s.equalProto(blocks[totalBlocks-1], fetchedBlocksMeta[3])

	fetchedBlockMeta, err := s.accessor.GetLatestBlock(ctx, tag)
	if err != nil {
		panic(err)
	}
	s.equalProto(expectedLatestBlock, fetchedBlockMeta)
}

func (s *blockStorageTestSuite) TestPersistBlockMetas() {
	s.runTestPersistBlockMetas(10)
}

func (s *blockStorageTestSuite) TestPersistBlockMetasNotUpdateWatermark() {
	totalBlocks := 10
	blocks := testutil.MakeBlockMetadatasFromStartHeight(s.config.Chain.BlockStartHeight, totalBlocks, tag)
	ctx := context.TODO()

	err := s.accessor.PersistBlockMetas(ctx, false, blocks, nil)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(blocks); i++ {
		// fetch block through two ways, should always return identical result
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
	}

	_, err = s.accessor.GetLatestBlock(ctx, tag)
	assert.True(s.T(), errors.Is(err, storage_errors.ErrItemNotFound))
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
	assert.True(s.T(), errors.Is(err, storage_errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestGetBlockByHeightInvalidHeight() {
	_, err := s.accessor.GetBlockByHeight(context.TODO(), tag, 0)
	assert.True(s.T(), errors.Is(err, storage_errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightsInvalidHeight() {
	_, err := s.accessor.GetBlocksByHeights(context.TODO(), tag, []uint64{0})
	assert.True(s.T(), errors.Is(err, storage_errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightsBlockNotFound() {
	_, err := s.accessor.GetBlocksByHeights(context.TODO(), tag, []uint64{15})
	assert.True(s.T(), errors.Is(err, storage_errors.ErrItemNotFound))
}

func (s *blockStorageTestSuite) TestGetBlockByHashInvalidHeight() {
	_, err := s.accessor.GetBlockByHash(context.TODO(), tag, 0, "0x0")
	assert.True(s.T(), errors.Is(err, storage_errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightRangeInvalidRange() {
	_, err := s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 100, 100)
	assert.True(s.T(), errors.Is(err, storage_errors.ErrOutOfRange))

	_, err = s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 0, s.config.Chain.BlockStartHeight)
	assert.True(s.T(), errors.Is(err, storage_errors.ErrInvalidHeight))
}

func (s *blockStorageTestSuite) equalProto(x, y any) {
	if diff := cmp.Diff(x, y, protocmp.Transform()); diff != "" {
		assert.FailNow(s.T(), diff)
	}
}

func TestIntegrationBlockStorageTestSuite(t *testing.T) {
	// TODO: speed up the tests before re-enabling TestAllEnvs.
	// testapp.TestAllEnvs(t, func(t *testing.T, cfg *config.Config) {
	// 	suite.Run(t, &blockStorageTestSuite{config: cfg})
	// })

	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &blockStorageTestSuite{config: cfg})
}
