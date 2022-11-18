package integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	btcTag            = uint32(1)
	btcHash           = "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"
	btcHashNotFound   = "00000000c937983704a73af28acdec37b049d214adbda81d7e2a3dd146f6ed08"
	btcParentHash     = "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3"
	btcBlockTimestamp = "2021-08-18T17:00:34Z"
	btcHeight         = uint64(696_402)
	btcParentHeight   = uint64(696_401)
	btcHeightNotFound = uint64(99_999_999)
)

type bitcoinIntegrationTestSuite struct {
	suite.Suite

	app    testapp.TestApp
	client client.Client
	parser parser.Parser
}

func TestIntegrationBitcoinTestSuite(t *testing.T) {
	suite.Run(t, new(bitcoinIntegrationTestSuite))
}

func (s *bitcoinIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_BITCOIN),
		config.WithNetwork(common.Network_NETWORK_BITCOIN_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		testapp.WithConfig(cfg),
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)

	s.client = deps.Client
	s.parser = deps.Parser
	require.NotNil(s.client)
	require.NotNil(s.parser)
}

func (s *bitcoinIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *bitcoinIntegrationTestSuite) TestBitcoinGetBlock() {
	const (
		nativeFixture = "parser/bitcoin/integration/get_block_native_integration.json"
		updateFixture = false // Change to true to update the fixture.
	)

	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), btcTag, btcHeight, btcHash)
			},
		},
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), btcTag, btcHeight)
			},
		},
	}

	for _, test := range tests {
		s.app.Logger().Info("fetching block")
		require := testutil.Require(s.T())
		rawBlock, err := test.getBlock()
		require.NoError(err)

		s.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, rawBlock.Blockchain)
		s.Equal(common.Network_NETWORK_BITCOIN_MAINNET, rawBlock.Network)
		s.Equal(btcTag, rawBlock.Metadata.Tag)
		s.Equal(btcHash, rawBlock.Metadata.Hash)
		s.Equal(btcParentHash, rawBlock.Metadata.ParentHash)
		s.Equal(btcHeight, rawBlock.Metadata.Height)
		s.Equal(btcParentHeight, rawBlock.Metadata.ParentHeight)
		s.Equal(testutil.MustTimestamp(btcBlockTimestamp), rawBlock.Metadata.Timestamp)
		s.NotNil(rawBlock.Blobdata)
		s.Equal(2512, len(rawBlock.GetBitcoin().InputTransactions))

		s.Equal(0, len(rawBlock.GetBitcoin().InputTransactions[0].Data))
		s.Equal(1, len(rawBlock.GetBitcoin().InputTransactions[1].Data))
		s.Equal(4, len(rawBlock.GetBitcoin().InputTransactions[4].Data))

		nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
		require.NoError(err)
		if updateFixture {
			bitcoinBlock := nativeBlock.GetBitcoin()
			bitcoinBlock.Transactions = bitcoinBlock.Transactions[:2]
			fixtures.MustMarshalJSON(nativeFixture, bitcoinBlock)
		}

		// check native parsing logic
		var expectedNative api.BitcoinBlock
		fixtures.MustUnmarshalJSON(nativeFixture, &expectedNative)

		require.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, nativeBlock.Blockchain)
		require.Equal(common.Network_NETWORK_BITCOIN_MAINNET, nativeBlock.Network)
		require.Equal(btcTag, nativeBlock.Tag)
		require.Equal(btcHash, nativeBlock.Hash)
		require.Equal(btcParentHash, nativeBlock.ParentHash)
		require.Equal(btcHeight, nativeBlock.Height)
		require.Equal(btcParentHeight, nativeBlock.ParentHeight)
		require.Equal(testutil.MustTimestamp(btcBlockTimestamp), nativeBlock.Timestamp)
		require.Equal(uint64(2512), nativeBlock.NumTransactions)
		require.Equal(expectedNative.Header, nativeBlock.GetBitcoin().Header)
		require.Equal(expectedNative.Transactions[0], nativeBlock.GetBitcoin().Transactions[0])
		require.Equal(expectedNative.Transactions[1], nativeBlock.GetBitcoin().Transactions[1])
		require.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, nativeBlock.Blockchain)
		require.Equal(common.Network_NETWORK_BITCOIN_MAINNET, nativeBlock.Network)

		// Don't forget to reset the flag before you commit.
		require.False(updateFixture)
	}
}

func (s *bitcoinIntegrationTestSuite) TestBitcoinGetBlock_NotFound() {
	require := testutil.Require(s.T())

	_, err := s.client.GetBlockByHeight(context.Background(), btcTag, btcHeightNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())

	_, err = s.client.GetBlockByHash(context.Background(), btcTag, btcHeightNotFound, btcHashNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
}

func (s *bitcoinIntegrationTestSuite) TestBitcoinGetLatestHeight() {
	height, err := s.client.GetLatestHeight(context.Background())
	s.NoError(err)
	s.NotEqual(0, height)
}

func (s *bitcoinIntegrationTestSuite) TestBitcoinBatchGetBlockMetadata() {
	const (
		from      = 100
		to        = 105
		numBlocks = to - from
	)

	require := testutil.Require(s.T())

	blocks, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, from, to)
	require.NoError(err)
	require.Equal(numBlocks, len(blocks))
}
