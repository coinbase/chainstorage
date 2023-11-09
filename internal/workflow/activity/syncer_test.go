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
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
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
	result, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
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

	result, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
	require.NoError(err)
	require.Equal(uint64(75), result.forkHeight)
	require.Equal(uint64(100), result.canonicalChainTipHeight)
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

	result, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
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

	result, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
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

	result, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
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

	result, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
	require.NoError(err)
	require.Equal(uint64(63), result.forkHeight)
	require.Equal(uint64(90), result.canonicalChainTipHeight)
}

func (s *SyncerTestSuite) TestMasterNodeGetLatestHeightFailure() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(0), xerrors.Errorf("master node GetLatestHeight failure"))
	_, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
	require.Error(err)
	require.Contains(err.Error(), "master node GetLatestHeight failure")
}

func (s *SyncerTestSuite) TestMetastoreGetLatestBlockFailure() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(100), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(nil, xerrors.Errorf("metastore GetLatestBlock failure"))
	_, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
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
	_, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
	require.Error(err)
	require.Contains(err.Error(), "metastore GetBlocksInRange failure")
}

func (s *SyncerTestSuite) TestReorgWithFastSync() {
	require := testutil.Require(s.T())

	s.masterBlockchainClient.EXPECT().GetLatestHeight(gomock.Any()).Return(uint64(120), nil)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(&api.BlockMetadata{Height: 100}, nil)
	result, err := s.syncer.handleReorg(context.TODO(), tag, true, s.logger)
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

	_, err := s.syncer.handleReorg(context.TODO(), tag, false, s.logger)
	require.Error(err)
	require.Contains(err.Error(), "master node BatchGetBlockMetadata failure")
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
