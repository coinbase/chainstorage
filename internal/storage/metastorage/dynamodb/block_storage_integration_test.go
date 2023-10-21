package dynamodb

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
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
	var accessor internal.MetaStorage
	app := testapp.New(
		s.T(),
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&accessor),
	)
	defer app.Close()
	s.accessor = accessor
}

func (s *blockStorageTestSuite) TestPersistBlockMetasByMaxWriteSize() {
	tests := []struct {
		totalBlocks int
	}{
		{totalBlocks: maxBlocksWriteSize},
		{totalBlocks: maxBlocksWriteSize * 2},
		{totalBlocks: maxBlocksWriteSize * 4},
		{totalBlocks: maxBlocksWriteSize * 8},
		{totalBlocks: maxBlocksWriteSize * 64},
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
		{totalBlocks: maxTransactGetItemsSize},
		{totalBlocks: maxTransactGetItemsSize * 5},
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
	blocks := testutil.MakeBlockMetadatas(100, tag)
	// Mark 37th block as skipped and point the next block to the previous block.
	blocks[37] = &api.BlockMetadata{
		Tag:     tag,
		Height:  37,
		Skipped: true,
	}
	blocks[38].ParentHeight = blocks[36].Height
	blocks[38].ParentHash = blocks[36].Hash
	err := s.accessor.PersistBlockMetas(ctx, true, blocks, nil)
	require.NoError(err)

	fetchedBlocks, err := s.accessor.GetBlocksByHeightRange(ctx, tag, 0, 100)
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
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

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
	assert.True(s.T(), xerrors.Is(err, errors.ErrItemNotFound))
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
	if s.config.Chain.BlockStartHeight > 0 {
		_, err := s.accessor.GetBlockByHeight(context.TODO(), 0, tag)
		assert.True(s.T(), xerrors.Is(err, errors.ErrInvalidHeight))
	}
}

func (s *blockStorageTestSuite) TestGetBlocksByHeightRangeInvalidRange() {
	_, err := s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 100, 100)
	assert.True(s.T(), xerrors.Is(err, errors.ErrOutOfRange))
	if s.config.Chain.BlockStartHeight > 0 {
		_, err = s.accessor.GetBlocksByHeightRange(context.TODO(), tag, 0, s.config.Chain.BlockStartHeight)
		assert.True(s.T(), xerrors.Is(err, errors.ErrOutOfRange))
	}
}

func (s *blockStorageTestSuite) equalProto(x, y interface{}) {
	if diff := cmp.Diff(x, y, protocmp.Transform()); diff != "" {
		assert.FailNow(s.T(), diff)
	}
}

func TestIntegrationBlockStorageTestSuite(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &blockStorageTestSuite{config: cfg})
}
