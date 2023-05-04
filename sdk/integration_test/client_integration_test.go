package integration_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/sdk/services"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/server"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
)

type SDKTestSuite struct {
	suite.Suite
	session sdk.Session
	app     testapp.TestApp
}

func TestIntegrationSDKTestSuite(t *testing.T) {
	suite.Run(t, new(SDKTestSuite))
}

func (s *SDKTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	manager := services.NewManager()
	defer manager.Shutdown()

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_ETHEREUM),
		config.WithNetwork(common.Network_NETWORK_ETHEREUM_MAINNET),
		config.WithEnvironment(config.EnvDevelopment),
	)
	require.NoError(err)

	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithConfig(cfg),
		sdk.Module,
		parser.Module,
		downloader.Module,
		gateway.Module,
		fx.Populate(&s.session),
	)

	require.NotNil(s.session.Client())
	require.NotNil(s.session.Parser())
}

func (s *SDKTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *SDKTestSuite) TestStreamBlocks_InitialPositionInStream() {
	require := testutil.Require(s.T())

	client := s.session.Client()

	numOfEvents := uint64(5)
	ch, err := client.StreamChainEvents(context.Background(), sdk.StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{
			InitialPositionInStream: server.InitialPositionLatest,
		},
		NumberOfEvents: numOfEvents,
	})
	require.NoError(err)

	count := uint64(0)
	for result := range ch {
		require.NotNil(result)
		s.app.Logger().Info(
			"got result from stream",
			zap.Reflect("event", result.BlockchainEvent),
			zap.Reflect("block", result.Block.Metadata),
		)

		require.NotNil(result.Block)
		require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, result.Block.Blockchain)
		require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, result.Block.Network)
		require.NoError(result.Error)
		count += 1
	}
	require.Equal(numOfEvents, count)
}

func (s *SDKTestSuite) TestStreamBlocks_Sequence() {
	require := testutil.Require(s.T())

	client := s.session.Client()

	numOfEvents := uint64(5)
	ch, err := client.StreamChainEvents(context.Background(), sdk.StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{
			Sequence: "100",
		},
		NumberOfEvents: numOfEvents,
	})
	require.NoError(err)

	count := uint64(0)
	for result := range ch {
		expectedSequence := strconv.Itoa(int(101 + count))
		require.NotNil(result)
		s.app.Logger().Info(
			"got result from stream",
			zap.Reflect("event", result.BlockchainEvent),
			zap.Reflect("block", result.Block.Metadata),
		)

		require.NotNil(result.Block)
		require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, result.Block.Blockchain)
		require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, result.Block.Network)
		require.Equal(expectedSequence, result.BlockchainEvent.Sequence)
		require.NoError(result.Error)
		count += 1
	}
	require.Equal(numOfEvents, count)
}

func (s *SDKTestSuite) TestStreamBlocks_SequenceNum() {
	s.T().Skip("Enable this test once the server-side change is deployed")
	require := testutil.Require(s.T())

	client := s.session.Client()

	numOfEvents := uint64(5)
	ch, err := client.StreamChainEvents(context.Background(), sdk.StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{
			SequenceNum: 100,
		},
		NumberOfEvents: numOfEvents,
	})
	require.NoError(err)

	count := uint64(0)
	for result := range ch {
		expectedSequencNum := int64(101 + count)
		require.NotNil(result)
		s.app.Logger().Info(
			"got result from stream",
			zap.Reflect("event", result.BlockchainEvent),
			zap.Reflect("block", result.Block.Metadata),
		)

		require.NotNil(result.Block)
		require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, result.Block.Blockchain)
		require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, result.Block.Network)
		require.Equal(expectedSequencNum, result.BlockchainEvent.SequenceNum)
		require.NoError(result.Error)
		count += 1
	}
	require.Equal(numOfEvents, count)
}

func (s *SDKTestSuite) TestStreamBlocks_EventOnly() {
	require := testutil.Require(s.T())

	client := s.session.Client()

	numOfEvents := uint64(5)
	ch, err := client.StreamChainEvents(context.Background(), sdk.StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{
			InitialPositionInStream: server.InitialPositionEarliest,
		},
		NumberOfEvents: numOfEvents,
		EventOnly:      true,
	})
	require.NoError(err)

	count := uint64(0)
	for result := range ch {
		require.NotNil(result)
		s.app.Logger().Info(
			"got result from stream",
			zap.Reflect("event", result.BlockchainEvent),
		)

		require.Nil(result.Block)
		require.NotNil(result.BlockchainEvent)
		require.NotEmpty(result.BlockchainEvent.Sequence)
		require.NotEmpty(result.BlockchainEvent.Type)
		require.NotNil(result.BlockchainEvent.Block)
		require.NoError(result.Error)
		count += 1
	}
	require.Equal(numOfEvents, count)
}

func (s *SDKTestSuite) TestGetBlock() {
	const (
		tag    = uint32(1)
		height = uint64(14_000_000)
		hash   = "0x9bff49171de27924fa958faf7b7ce605c1ff0fdee86f4c0c74239e6ae20d9446"
	)
	require := testutil.Require(s.T())

	ctx := context.Background()
	client := s.session.Client()
	parser := s.session.Parser()

	rawBlock, err := client.GetBlockWithTag(ctx, tag, height, hash)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, rawBlock.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, rawBlock.Network)
	require.Equal(tag, rawBlock.Metadata.Tag)
	require.Equal(height, rawBlock.Metadata.Height)
	require.Equal(hash, rawBlock.Metadata.Hash)

	nativeBlock, err := parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)
	ethereumBlock := nativeBlock.GetEthereum()
	require.NotNil(ethereumBlock)
	require.Equal(height, ethereumBlock.Header.Number)
	require.Equal(hash, ethereumBlock.Header.Hash)
	require.Equal(112, len(ethereumBlock.Transactions))
	require.Equal("0x3dac2080b4c423029fcc9c916bc430cde441badfe736fc6d1fe9325348af80fd", ethereumBlock.Transactions[0].Hash)
}

func (s *SDKTestSuite) TestGetBlocksByRange() {
	const (
		startHeight = uint64(13_000_000)
		endHeight   = uint64(13_000_005)
	)
	require := testutil.Require(s.T())

	ctx := context.Background()
	client := s.session.Client()
	parser := s.session.Parser()

	rawBlocks, err := client.GetBlocksByRange(ctx, startHeight, endHeight)
	require.NoError(err)
	require.Equal(int(endHeight-startHeight), len(rawBlocks))
	for i, rawBlock := range rawBlocks {
		height := startHeight + uint64(i)
		require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, rawBlock.Blockchain)
		require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, rawBlock.Network)
		require.Equal(height, rawBlock.Metadata.Height)
		require.NotEmpty(rawBlock.Metadata.Hash)

		nativeBlock, err := parser.ParseNativeBlock(context.Background(), rawBlock)
		require.NoError(err)
		ethereumBlock := nativeBlock.GetEthereum()
		require.NotNil(ethereumBlock)
		require.Equal(height, ethereumBlock.Header.Number)
		require.NotEmpty(ethereumBlock.Header.Hash)
		numTransactions := nativeBlock.NumTransactions
		require.Equal(int(numTransactions), len(rawBlock.GetEthereum().TransactionReceipts))
		require.Equal(int(numTransactions), len(rawBlock.GetEthereum().TransactionTraces))
		require.Equal(int(numTransactions), len(ethereumBlock.Transactions))
	}
}
