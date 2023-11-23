package activity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity/errors"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const tag uint32 = 1

var (
	blockReorgedOption = testutil.WithBlockHashFormat("0x%s~")
	blockSkippedOption = testutil.WithBlockSkipped()
)

type SyncerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	ctrl                      *gomock.Controller
	metaStorage               *metastoragemocks.MockMetaStorage
	blobStorage               *blobstoragemocks.MockBlobStorage
	masterBlockchainClient    *clientmocks.MockClient
	slaveBlockchainClient     *clientmocks.MockClient
	validatorBlockchainClient *clientmocks.MockClient
	consensusBlockchainClient *clientmocks.MockClient
	masterEndpointProvider    endpoints.EndpointProvider
	slaveEndpointProvider     endpoints.EndpointProvider
	cfg                       *config.Config
	app                       testapp.TestApp
	syncer                    *Syncer
	logger                    *zap.Logger
	env                       *cadence.TestEnv
}

func TestSyncerTestSuite(t *testing.T) {
	suite.Run(t, new(SyncerTestSuite))
}

func (s *SyncerTestSuite) TestNoOp() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 120}, nil)
	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.NoError(err)
	require.Equal(uint64(120), result.forkHeight)
	require.Equal(uint64(100), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestNoReorg() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return uint64(102), nil
		})
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(102), response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestNoReorg_Failover() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return uint64(102), nil
		})
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)
	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
		Failover:        true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(102), response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestReorg() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 90}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(71, 5, tag)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag), nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag)...), nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag, blockReorgedOption), nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag, blockReorgedOption)...), nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 20, 0)
	require.NoError(err)
	require.Equal(uint64(75), result.forkHeight)
	require.Equal(uint64(100), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestReorgIrreversibleDistanceExceeded() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 90}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(71, 5, tag)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag), nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag)...), nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag, blockReorgedOption), nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag, blockReorgedOption)...), nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	_, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 14, 0)
	require.NoError(err)
}

func (s *SyncerTestSuite) TestReorgIrreversibleDistanceExceeded_NoReorgChain() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 90}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(81, 9, tag)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(90, 1, tag)...), nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(90, 1, tag, blockReorgedOption)...), nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	_, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 1, 0)
	require.NoError(err)
}

func (s *SyncerTestSuite) TestReorg_DisableReorgValidation() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 90}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(71, 5, tag)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag), nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag)...), nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag, blockReorgedOption), nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag, blockReorgedOption)...), nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.NoError(err)
	require.Equal(uint64(75), result.forkHeight)
	require.Equal(uint64(100), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestReorg_NumBlocksToSkip() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 90}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(71, 5, tag)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag), nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag)...), nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag, blockReorgedOption), nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag, blockReorgedOption)...), nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 20, 5)
	require.NoError(err)
	require.Equal(uint64(75), result.forkHeight)
	require.Equal(uint64(95), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestReorgInMasterNode() {
	// This test verifies the following case:
	//  * A fork block (height = 100) is found in local chain
	//  * The master node experiences a chain reorg right before the syncer activity is about to sync the list of blocks.
	//    In this example the list to sync is [101, 102].
	// The process is illustrated by the graph below:
	//                       [100'] <- [101'] <- [102'] ---- reorg happens at height = 100 right after the fork is detected
	//                      /
	// master: [98] <- [99] <- [100] <- [101] <- [102]  ---- before a fork (height = 100) is detected
	// Local:  [98] <- [99] <- [100] ----------------------- if the activity syncs blocks 101' and 102', the local block chain will
	//                                                       be broken because 101' links to 100' which is not in local DB
	//
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(102), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	masterBlocksAfterReorg := testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag)
	masterBlocksAfterReorg[0].ParentHash = "linkToNewNodeOfHeight100"
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(masterBlocksAfterReorg, nil)
	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(100), response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.Empty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestNextMasterBlockSkipped() {
	// This test verifies the following case:
	//  * A fork block (height = 100) is found in local chain.
	//  * The next two block are skipped.
	//  * nextMasterBlock (height = 103) is linked to the fork block.
	// master: [98] <- [99] <- [100] <- [101 (skipped)] <- [102 (skipped] <- [103]
	// Local:  [98] <- [99] <- [100]
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(103), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	masterBlocks := testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag, blockSkippedOption)
	nextMasterBlock := testutil.MakeBlockMetadata(103, tag)
	nextMasterBlock.ParentHash = beforeFork[len(beforeFork)-1].Hash
	masterBlocks = append(masterBlocks, nextMasterBlock)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(masterBlocks, nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(3)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(3)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(103), response.LatestSyncedHeight)
	require.Equal(uint64(3), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestLatestBlockNotSkipped() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(300), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(testutil.MakeBlockMetadata(100, tag), nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	masterBlocks := testutil.MakeBlockMetadatasFromStartHeight(101, 60, tag)
	masterBlocks = append(masterBlocks, testutil.MakeBlockMetadatasFromStartHeight(161, 40, tag, blockSkippedOption)...)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(masterBlocks, nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			if height > 160 {
				return testutil.MakeBlocksFromStartHeight(height, 1, tag, blockSkippedOption)[0], nil
			}
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(100)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(100)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(160), response.LatestSyncedHeight)
	require.Equal(uint64(200), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestFastSync() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(300), nil)

	forkBlock := testutil.MakeBlockMetadata(100, tag)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(forkBlock, nil)

	s.slaveBlockchainClient.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlock(height, tag), nil
		}).Times(100)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(100)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), forkBlock).Return(nil)

	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
		FastSync:        true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(200), response.LatestSyncedHeight)
	require.Equal(uint64(200), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestFastSync_UnexpectedReorg() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(300), nil)

	// Simulate non-continuous chain when fast sync is turned on.
	forkBlock := testutil.MakeBlockMetadata(100, tag)
	forkBlock.Hash = "0xdeadbeef"
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(forkBlock, nil)

	s.slaveBlockchainClient.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlock(height, tag), nil
		}).Times(100)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(100)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), forkBlock).Return(parser.ErrInvalidChain)

	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
		FastSync:        true,
	}
	_, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
	require.Contains(err.Error(), "failed to upload metadata for blocks from 101 to 201: invalid chain")
}

func (s *SyncerTestSuite) TestReorgWithSkippedBlocks() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(90), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 80}, nil)

	// simulated chain: 61-67 (same), 68 (fork), 69-70 (different), 71-75 (skipped), 76-80 (different)
	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(61, 8, tag)
	skippedBlocks := testutil.MakeBlockMetadatasFromStartHeight(71, 5, tag, blockSkippedOption)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(skippedBlocks, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag)...), nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(61, 70, tag), nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(skippedBlocks, testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag, blockReorgedOption)...), nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(69, 2, tag, blockReorgedOption)...), nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.NoError(err)
	require.Equal(uint64(68), result.forkHeight)
	require.Equal(uint64(90), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestReorgWithSkippedBlocks2() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(90), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 80}, nil)

	// simulated chain: 61 (same), 62 (fork), 63-68 (skipped), 69-70 (different)
	myChain := testutil.MakeBlockMetadatasFromStartHeight(61, 2, tag)
	myChain = append(myChain, testutil.MakeBlockMetadatasFromStartHeight(63, 8, tag, blockSkippedOption)...)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(myChain, nil)

	theirChain := testutil.MakeBlockMetadatasFromStartHeight(61, 2, tag)
	theirChain = append(theirChain, testutil.MakeBlockMetadatasFromStartHeight(63, 6, tag, blockSkippedOption)...)
	theirChain = append(theirChain, testutil.MakeBlockMetadatasFromStartHeight(69, 2, tag, blockReorgedOption)...)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(theirChain, nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.NoError(err)
	require.Equal(uint64(62), result.forkHeight)
	require.Equal(uint64(90), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestReorgWithSkippedBlocks3() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(90), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 80}, nil)

	// simulated chain: 61-62 (skipped), 63 (fork), 64-68 (skipped), 69-70 (different), 71-80 (skipped)
	skippedBlocks := testutil.MakeBlockMetadatasFromStartHeight(71, 10, tag, blockSkippedOption)
	myChain := testutil.MakeBlockMetadatasFromStartHeight(61, 2, tag, blockSkippedOption)
	myChain = append(myChain, testutil.MakeBlockMetadata(63, tag))
	myChain = append(myChain, testutil.MakeBlockMetadatasFromStartHeight(64, 5, tag, blockSkippedOption)...)
	myChain = append(myChain, testutil.MakeBlockMetadatasFromStartHeight(69, 2, tag)...)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(skippedBlocks, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(myChain, nil)

	theirChain := testutil.MakeBlockMetadatasFromStartHeight(61, 2, tag, blockSkippedOption)
	theirChain = append(theirChain, testutil.MakeBlockMetadata(63, tag))
	theirChain = append(theirChain, testutil.MakeBlockMetadatasFromStartHeight(64, 5, tag, blockSkippedOption)...)
	theirChain = append(theirChain, testutil.MakeBlockMetadatasFromStartHeight(69, 2, tag, blockReorgedOption)...)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(skippedBlocks, nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(theirChain, nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.NoError(err)
	require.Equal(uint64(63), result.forkHeight)
	require.Equal(uint64(90), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestReorgWithSkippedBlocks4() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(90), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 80}, nil)

	// simulated chain: 61-62 (skipped), 63 (fork), 64-70 (skipped), 71-80 (skipped)
	skippedBlocks := testutil.MakeBlockMetadatasFromStartHeight(71, 10, tag, blockSkippedOption)
	myChain := testutil.MakeBlockMetadatasFromStartHeight(61, 2, tag, blockSkippedOption)
	myChain = append(myChain, testutil.MakeBlockMetadata(63, tag))
	myChain = append(myChain, testutil.MakeBlockMetadatasFromStartHeight(64, 7, tag, blockSkippedOption)...)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(skippedBlocks, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(myChain, nil)

	theirChain := testutil.MakeBlockMetadatasFromStartHeight(61, 2, tag, blockSkippedOption)
	theirChain = append(theirChain, testutil.MakeBlockMetadata(63, tag))
	theirChain = append(theirChain, testutil.MakeBlockMetadatasFromStartHeight(64, 7, tag, blockSkippedOption)...)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(skippedBlocks, nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(theirChain, nil)

	s.metaStorage.EXPECT().
		PersistBlockMetas(gomock.Any(), true, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			return nil
		})

	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.NoError(err)
	require.Equal(uint64(63), result.forkHeight)
	require.Equal(uint64(90), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestMasterNodeGetLatestHeightFailure() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(0), xerrors.Errorf("master node GetLatestHeight failure"))
	_, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.Error(err)
	require.Contains(err.Error(), "master node GetLatestHeight failure")
}

func (s *SyncerTestSuite) TestMetastoreGetLatestBlockFailure() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(nil, xerrors.Errorf("metastore GetLatestBlock failure"))
	_, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.Error(err)
	require.Contains(err.Error(), "metastore GetLatestBlock failure")
}

func (s *SyncerTestSuite) TestMetastoreGetBlocksInRangeFailure() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(76, 5, tag), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 90}, nil)

	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("metastore GetBlocksInRange failure"))
	_, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.Error(err)
	require.Contains(err.Error(), "metastore GetBlocksInRange failure")
}

func (s *SyncerTestSuite) TestReorgWithFastSync() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(120), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)
	result, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, true, 0, 0)
	require.NoError(err)
	require.Equal(uint64(100), result.forkHeight)
	require.Equal(uint64(120), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestBatchGetBlockMetadataFailure() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 90}, nil)

	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(81, 10, tag), nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("master node BatchGetBlockMetadata failure"))

	_, err := s.syncer.handleReorg(context.TODO(), s.logger, tag, false, 0, 0)
	require.Error(err)
	require.Contains(err.Error(), "master node BatchGetBlockMetadata failure")
}

func (s *SyncerTestSuite) TestAddOrUpdateTransactionsInParallel() {
	require := testutil.Require(s.T())

	stableTag := uint32(2)
	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return uint64(102), nil
		})
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), stableTag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, stableTag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, stableTag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksWithTransactionsFromStartHeight(height, 1, tag, 3)[0], nil
		}).Times(2)

	s.metaStorage.EXPECT().AddTransactions(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                          stableTag,
		MaxBlocksToSync:              100,
		Parallelism:                  4,
		TransactionsWriteParallelism: 2,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(102), response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestAddOrUpdateTransactionsInParallel_WithBatches() {
	require := testutil.Require(s.T())

	stableTag := uint32(2)
	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return uint64(102), nil
		})
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), stableTag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, stableTag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, stableTag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksWithTransactionsFromStartHeight(height, 1, tag, 100)[0], nil
		}).Times(2)

	s.metaStorage.EXPECT().AddTransactions(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                          stableTag,
		MaxBlocksToSync:              100,
		Parallelism:                  4,
		TransactionsWriteParallelism: 2,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(102), response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestAddOrUpdateTransactionsInParallel_CheckOnTransactionLoopParams() {
	require := testutil.Require(s.T())

	stableTag := uint32(2)
	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return uint64(102), nil
		})
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), stableTag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, stableTag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 1, stableTag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksWithTransactionsFromStartHeight(height, 1, tag, 3)[0], nil
		})

	expectedTxns := []string{"transactionHash0", "transactionHash1", "transactionHash2"}
	s.metaStorage.EXPECT().
		AddTransactions(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, transactions []*model.Transaction, parallelism int) error {
			for i := range transactions {
				transactions[i].Hash = expectedTxns[i]
			}
			return nil
		})

	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                          stableTag,
		MaxBlocksToSync:              100,
		Parallelism:                  4,
		TransactionsWriteParallelism: 1,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(uint64(101), response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestAddOrUpdateTransactionsInParallel_Err() {
	require := testutil.Require(s.T())

	stableTag := uint32(2)
	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return uint64(102), nil
		})
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), stableTag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, stableTag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 1, stableTag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), stableTag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksWithTransactionsFromStartHeight(height, 1, tag, 3)[0], nil
		})
	s.masterBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), stableTag, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksWithTransactionsFromStartHeight(height, 1, tag, 3)[0], nil
		})

	s.metaStorage.EXPECT().AddTransactions(gomock.Any(), gomock.Any(), gomock.Any()).Return(xerrors.New("failed to add or update transaction")).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)

	request := &SyncerRequest{
		Tag:                          stableTag,
		MaxBlocksToSync:              100,
		Parallelism:                  4,
		TransactionsWriteParallelism: 2,
	}
	_, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
}

func (s *SyncerTestSuite) TestBlockValidationFailure() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(101), nil)

	forkBlock := testutil.MakeBlockMetadata(100, tag)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(forkBlock, nil)

	s.slaveBlockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), tag, gomock.Any()).Return(&api.Block{}, xerrors.New("block validation failed"))
	s.masterBlockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), tag, gomock.Any(), gomock.Any()).Return(&api.Block{}, xerrors.New("block validation failed"))

	request := &SyncerRequest{
		Tag:             tag,
		MaxBlocksToSync: 100,
		Parallelism:     4,
		FastSync:        true,
	}
	_, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
}

func (s *SyncerTestSuite) TestConsensusValidation_Success() {
	// This test verifies the following case:
	// master: [98] <- [99] <- [100] <- [101] <- [102]
	// local: [98] <- [99] <- [100]
	// latestFinalizedHeight = 102 - 20 + 1 = 83

	require := testutil.Require(s.T())

	const (
		latestHeight         uint64 = 102
		irreversibleDistance uint64 = 20
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)

	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	s.metaStorage.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, latestFinalizedHeight).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag)[0], nil)
	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, latestFinalizedHeight, latestFinalizedHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag), nil)

	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                  tag,
		MaxBlocksToSync:      100,
		Parallelism:          4,
		IrreversibleDistance: irreversibleDistance,
		ConsensusValidation:  true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(latestHeight, response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestConsensusValidation_ConsensusClientFailure() {
	require := testutil.Require(s.T())

	const (
		latestHeight         uint64 = 102
		irreversibleDistance uint64 = 20
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)

	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	s.metaStorage.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, latestFinalizedHeight).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag)[0], nil)
	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, latestFinalizedHeight, latestFinalizedHeight+1).
		Return(nil, xerrors.New("failed to fetch block"))

	request := &SyncerRequest{
		Tag:                  tag,
		MaxBlocksToSync:      100,
		Parallelism:          4,
		IrreversibleDistance: irreversibleDistance,
		ConsensusValidation:  true,
	}
	_, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
	require.ErrorContains(err, "failed to fetch block")
	require.ErrorContains(err, errors.ErrTypeConsensusClusterFailure)
}

func (s *SyncerTestSuite) TestConsensusValidation_Muted() {
	require := testutil.Require(s.T())

	const (
		latestHeight         uint64 = 102
		irreversibleDistance uint64 = 20
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)

	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	s.metaStorage.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, latestFinalizedHeight).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag)[0], nil)
	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, latestFinalizedHeight, latestFinalizedHeight+1).
		Return(nil, xerrors.New("failed to fetch block"))

	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                      tag,
		MaxBlocksToSync:          100,
		Parallelism:              4,
		IrreversibleDistance:     irreversibleDistance,
		ConsensusValidation:      true,
		ConsensusValidationMuted: true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(latestHeight, response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestConsensusValidation_BlockHashMismatch() {
	require := testutil.Require(s.T())

	const (
		latestHeight         uint64 = 102
		irreversibleDistance uint64 = 20
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)

	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	s.metaStorage.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, latestFinalizedHeight).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag)[0], nil)
	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, latestFinalizedHeight, latestFinalizedHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag, blockReorgedOption), nil)

	request := &SyncerRequest{
		Tag:                  tag,
		MaxBlocksToSync:      100,
		Parallelism:          4,
		IrreversibleDistance: irreversibleDistance,
		ConsensusValidation:  true,
	}
	_, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
	require.ErrorContains(err, "detected mismatch block hash")
	require.ErrorContains(err, errors.ErrTypeConsensusValidationFailure)
}

func (s *SyncerTestSuite) TestConsensusValidation_NoFinalizedBlock() {
	// This test verifies the following case:
	// master: [8] <- [9] <- [10] <- [11] <- [12]
	// local: [8] <- [9] <- [10]
	// latestFinalizedHeight = 12 - 20 + 1 = -7
	// which means there is no finalized block yet.

	require := testutil.Require(s.T())

	const (
		latestHeight         uint64 = 12
		irreversibleDistance uint64 = 20
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 10}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(8, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(11, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                  tag,
		MaxBlocksToSync:      100,
		Parallelism:          4,
		IrreversibleDistance: irreversibleDistance,
		ConsensusValidation:  true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(latestHeight, response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestConsensusValidation_FinalizedBlock_Skipped() {
	// This test verifies the following case:
	// master: [80] <- [81(skipped)] <- [82(skipped)] <- [83(skipped)] <- [84] <- ... <- [98] <- [99] <- [100] <- [101] <- [102]
	// local:  [80] <- [81(skipped)] <- [82(skipped)] <- [83(skipped)] <- [84] <- ... <-[98] <- [99] <- [100]
	// latestFinalizedHeight = 102 - 20 + 1 = 83, which is a skipped block
	// For this case, we should validate the first non-skipped block prior to that.

	require := testutil.Require(s.T())

	const (
		latestHeight         uint64 = 102
		irreversibleDistance uint64 = 20
		skippedBlockNum      int    = 3
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(98, 3, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(beforeFork, nil)

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 2, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(2)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(2)

	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	s.metaStorage.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag, blockSkippedOption)[0], nil).Times(skippedBlockNum)
	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag, blockSkippedOption), nil).Times(skippedBlockNum)

	nonSkippedHeight := latestFinalizedHeight - uint64(skippedBlockNum)
	s.metaStorage.EXPECT().
		GetBlockByHeight(gomock.Any(), tag, nonSkippedHeight).
		Return(testutil.MakeBlockMetadatasFromStartHeight(nonSkippedHeight, 1, tag)[0], nil)
	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, nonSkippedHeight, nonSkippedHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(nonSkippedHeight, 1, tag), nil)

	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                  tag,
		MaxBlocksToSync:      100,
		Parallelism:          4,
		IrreversibleDistance: irreversibleDistance,
		ConsensusValidation:  true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(latestHeight, response.LatestSyncedHeight)
	require.Equal(uint64(2), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestConsensusValidation_NoMetadataInMetaStorage() {
	// This is for the case where the latestFinalizedBlock has not been persisted to data storage
	// Local:  [100] <- [101]
	// Master: [100] <- [101'] <- [102'] <- [103'] <- [104']
	// when irreversibleDistance = 3, latestFinalizedBlock = 104 - 3 + 1 = 102, which has not been written to metaStorage

	require := testutil.Require(s.T())

	const (
		latestHeight         uint64 = 104
		irreversibleDistance uint64 = 3
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 101}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(100, 1, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(101, 1, tag)...), nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(101, 1, tag, blockReorgedOption)...), nil)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 4, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
		}).Times(4)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(4)

	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, latestFinalizedHeight, latestFinalizedHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(latestFinalizedHeight, 1, tag), nil)

	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	request := &SyncerRequest{
		Tag:                  tag,
		MaxBlocksToSync:      100,
		Parallelism:          4,
		IrreversibleDistance: irreversibleDistance,
		ConsensusValidation:  true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(latestHeight, response.LatestSyncedHeight)
	require.Equal(uint64(4), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) TestConsensusValidation_NoMetadataInMetaStorage_SkippedFinalizedBlock() {
	// This is for the case where the latestFinalizedBlock has not been persisted to data storage
	// Local:  [100] <- [101]
	// Master: [100] <- [101'] <- [102'(skipped)] <- [103'(skipped)] <- [104'(skipped)] <- [105']
	// when irreversibleDistance = 3, latestFinalizedBlock = 105 - 3 + 1 = 103, which has not been written to metaStorage and is a skipped block

	require := testutil.Require(s.T())

	const (
		latestHeight              uint64 = 105
		nonSkippedFinalizedHeight uint64 = 101
		irreversibleDistance      uint64 = 3
	)

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(latestHeight, nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 101}, nil)

	beforeFork := testutil.MakeBlockMetadatasFromStartHeight(100, 1, tag)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(101, 1, tag)...), nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(append(beforeFork, testutil.MakeBlockMetadatasFromStartHeight(101, 1, tag, blockReorgedOption)...), nil)
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Nil(lastBlock)
			require.Equal(1, len(blocks))
			require.Equal(uint64(100), blocks[0].Height)
			return nil
		})

	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		Return(testutil.MakeBlockMetadatasFromStartHeight(101, 5, tag), nil)
	s.slaveBlockchainClient.EXPECT().
		GetBlockByHash(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
			if height == nonSkippedFinalizedHeight || height == latestHeight {
				return testutil.MakeBlocksFromStartHeight(height, 1, tag)[0], nil
			}
			return testutil.MakeBlocksFromStartHeight(height, 1, tag, blockSkippedOption)[0], nil
		}).Times(5)
	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).Times(5)

	s.consensusBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
			if from == nonSkippedFinalizedHeight {
				return testutil.MakeBlockMetadatasFromStartHeight(nonSkippedFinalizedHeight, 1, tag), nil
			}
			return testutil.MakeBlockMetadatasFromStartHeight(from, 1, tag, blockSkippedOption), nil
		}).Times(3)

	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.True(updateWatermark)
			require.Equal(5, len(blocks))
			require.Equal(nonSkippedFinalizedHeight, blocks[0].Height)
			require.Equal(latestHeight, blocks[4].Height)
			require.Equal(uint64(100), lastBlock.Height)
			return nil
		})

	request := &SyncerRequest{
		Tag:                  tag,
		MaxBlocksToSync:      100,
		Parallelism:          4,
		IrreversibleDistance: irreversibleDistance,
		ConsensusValidation:  true,
	}
	response, err := s.syncer.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(latestHeight, response.LatestSyncedHeight)
	require.Equal(uint64(5), response.SyncGap)
	require.NotEmpty(response.TimeSinceLastBlock)
}

func (s *SyncerTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var deps struct {
		fx.In
		MasterEndpoints endpoints.EndpointProvider `name:"master"`
		SlaveEndpoints  endpoints.EndpointProvider `name:"slave"`
	}

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "bar",
				Weight: 1,
			},
		},
	}

	s.ctrl = gomock.NewController(s.T())
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.masterBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.slaveBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.validatorBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.consensusBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Client.Master.EndpointGroup = *endpointGroup
	cfg.Chain.Client.Slave.EndpointGroup = *endpointGroup
	s.cfg = cfg
	s.env = cadence.NewTestActivityEnv(s)
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewSyncer),
		fx.Provide(NewNopHeartbeater),
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Provide(func() blobstorage.BlobStorage {
			return s.blobStorage
		}),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Provide(fx.Annotated{
			Name: "master",
			Target: func() client.Client {
				return s.masterBlockchainClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "slave",
			Target: func() client.Client {
				return s.slaveBlockchainClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "validator",
			Target: func() client.Client {
				return s.validatorBlockchainClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "consensus",
			Target: func() client.Client {
				return s.consensusBlockchainClient
			},
		}),
		fx.Populate(&s.syncer),
		fx.Populate(&s.logger),
		fx.Populate(&deps),
	)
	s.masterEndpointProvider = deps.MasterEndpoints
	s.slaveEndpointProvider = deps.SlaveEndpoints
}

func (s *SyncerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}
