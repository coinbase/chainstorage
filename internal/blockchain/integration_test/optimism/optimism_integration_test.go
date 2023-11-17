package optimism_test

import (
	"context"
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	optimismTag        uint32 = 1
	optimismHeight     uint64 = 0x666167a
	optimismHash              = "0xf05ccf1638b3d3cee9361dcb0e6023900b514a21d9a114e023adc61ecf2f7ffa"
	optimismParentHash        = "0xb1e1c950e03142f8b7ead6b8cead01544a54b8129e8ad378b00bf1a2373601c5"
)

type optimismIntegrationTestSuite struct {
	suite.Suite

	app    testapp.TestApp
	logger *zap.Logger
	client client.Client
	parser parser.Parser
}

func TestIntegrationOptimismTestSuite(t *testing.T) {
	suite.Run(t, new(optimismIntegrationTestSuite))
}

func (s *optimismIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_OPTIMISM),
		config.WithNetwork(common.Network_NETWORK_OPTIMISM_MAINNET),
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
		restapi.Module,
		client.Module,
		parser.Module,
		testapp.WithConfig(cfg),
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)

	s.logger = s.app.Logger()
	s.client = deps.Client
	s.parser = deps.Parser
	require.NotNil(s.client)
	require.NotNil(s.parser)
}

func (s *optimismIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *optimismIntegrationTestSuite) TestOptimismGetBlock() {
	const (
		nativeFixture = "parser/optimism/mainnet/native_block_107353722.json"
		updateFixture = false // Change to true to update the fixture.
	)

	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), optimismTag, optimismHeight, optimismHash)
			},
		},
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), optimismTag, optimismHeight)

			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.app.Logger().Info("fetching block")
			require := testutil.Require(s.T())
			rawBlock, err := test.getBlock()
			require.NoError(err)

			s.Equal(common.Blockchain_BLOCKCHAIN_OPTIMISM, rawBlock.Blockchain)
			s.Equal(common.Network_NETWORK_OPTIMISM_MAINNET, rawBlock.Network)
			s.Equal(optimismTag, rawBlock.Metadata.Tag)
			s.Equal(optimismHash, rawBlock.Metadata.Hash)
			s.Equal(optimismParentHash, rawBlock.Metadata.ParentHash)
			s.Equal(optimismHeight, rawBlock.Metadata.Height)

			nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
			if updateFixture {
				fixtures.MustMarshalPB(nativeFixture, nativeBlock)
			}

			// check native parsing logic
			var expectedNativeBlock api.NativeBlock
			fixtures.MustUnmarshalPB(nativeFixture, &expectedNativeBlock)

			require.NoError(err)
			require.Equal(common.Blockchain_BLOCKCHAIN_OPTIMISM, nativeBlock.Blockchain)
			require.Equal(common.Network_NETWORK_OPTIMISM_MAINNET, nativeBlock.Network)
			require.Equal(optimismTag, nativeBlock.Tag)
			require.Equal(optimismHash, nativeBlock.Hash)
			require.Equal(optimismParentHash, nativeBlock.ParentHash)
			require.Equal(optimismHeight, nativeBlock.Height)
			require.Equal(uint64(19), nativeBlock.NumTransactions)
			require.Equal(expectedNativeBlock.GetEthereum().Header, nativeBlock.GetEthereum().Header)

			for i, transaction := range expectedNativeBlock.GetEthereum().Transactions {
				require.Equal(transaction, nativeBlock.GetEthereum().Transactions[i])
			}

			err = s.parser.ValidateBlock(context.Background(), nativeBlock)
			require.NoError(err)

			// Don't forget to reset the flag before you commit.
			require.False(updateFixture)
		})
	}
}
