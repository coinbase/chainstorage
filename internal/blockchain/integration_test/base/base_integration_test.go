package base_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	baseIntegrationTestSuite struct {
		suite.Suite

		config testConfig
		app    testapp.TestApp
		logger *zap.Logger
		client client.Client
		parser parser.Parser
	}

	testConfig struct {
		blockchain     common.Blockchain
		network        common.Network
		tag            uint32
		blockNumber    uint64
		blockHash      string
		nativeFixture  string
		rosettaFixture string
		updateFixture  bool
	}
)

func (s *baseIntegrationTestSuite) SetupSuite() {
	cfg, err := config.New(
		config.WithBlockchain(s.config.blockchain),
		config.WithNetwork(s.config.network),
	)
	s.Require().NoError(err)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}

	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithConfig(cfg),
		jsonrpc.Module,
		restapi.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	s.client = deps.Client
	s.parser = deps.Parser
}

func (s *baseIntegrationTestSuite) TearDownSuite() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationBaseTestSuite_Goerli(t *testing.T) {
	suite.Run(t, &baseIntegrationTestSuite{
		config: testConfig{
			blockchain:     common.Blockchain_BLOCKCHAIN_BASE,
			network:        common.Network_NETWORK_BASE_GOERLI,
			tag:            1,
			blockNumber:    7542,
			blockHash:      "0x7edba9bf0ac05b8f5b97f31bc54c54b0891414023b2e6ec27beef93c9925c298",
			nativeFixture:  "parser/base/goerli/native_block_7542.json",
			rosettaFixture: "parser/base/goerli/rosetta_block_7542.json",
			updateFixture:  false,
		},
	})
}

func TestIntegrationBaseTestSuite_Mainnet(t *testing.T) {
	suite.Run(t, &baseIntegrationTestSuite{
		config: testConfig{
			blockchain:     common.Blockchain_BLOCKCHAIN_BASE,
			network:        common.Network_NETWORK_BASE_MAINNET,
			tag:            1,
			blockNumber:    81636,
			blockHash:      "0x44868bb5ef810d9808ca8fe3047607bbca1bf1c916e3fb4b5362b0ede8585775",
			nativeFixture:  "parser/base/mainnet/native_block_81636.json",
			rosettaFixture: "parser/base/mainnet/rosetta_block_81636.json",
			updateFixture:  false,
		},
	})

	suite.Run(t, &baseIntegrationTestSuite{
		config: testConfig{
			blockchain:     common.Blockchain_BLOCKCHAIN_BASE,
			network:        common.Network_NETWORK_BASE_MAINNET,
			tag:            1,
			blockNumber:    4051331,
			blockHash:      "0xcc9a1817391afa94b1a045400c43fb97f8b8d7c3c6989a273722b809d030768b",
			nativeFixture:  "parser/base/mainnet/native_block_4051331.json",
			rosettaFixture: "parser/base/mainnet/rosetta_block_4051331.json",
			updateFixture:  false,
		},
	})
}

func (s *baseIntegrationTestSuite) TestBaseGetBlock() {
	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), s.config.tag, s.config.blockNumber, s.config.blockHash)
			},
		},
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), s.config.tag, s.config.blockNumber)
			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.app.Logger().Info("fetching block")
			require := testutil.Require(s.T())
			rawBlock, err := test.getBlock()
			require.NoError(err)

			nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)

			rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), rawBlock)
			require.NoError(err)

			if s.config.updateFixture {
				fixtures.MustMarshalPB(s.config.nativeFixture, nativeBlock)
				fixtures.MustMarshalPB(s.config.rosettaFixture, rosettaBlock)
			}

			// check native parser
			var expectedNative api.NativeBlock
			fixtures.MustUnmarshalPB(s.config.nativeFixture, &expectedNative)
			require.Equal(expectedNative.GetEthereum().Header, nativeBlock.GetEthereum().Header)
			require.Equal(len(expectedNative.GetEthereum().Transactions), len(nativeBlock.GetEthereum().GetTransactions()))
			for i, transaction := range expectedNative.GetEthereum().Transactions {
				require.Equal(transaction, nativeBlock.GetEthereum().Transactions[i])
			}

			// check rosetta parser
			var expectedRosetta api.RosettaBlock
			fixtures.MustUnmarshalPB(s.config.rosettaFixture, &expectedRosetta)
			require.Equal(expectedRosetta.GetBlock().BlockIdentifier, rosettaBlock.GetBlock().BlockIdentifier)
			require.Equal(expectedRosetta.GetBlock().ParentBlockIdentifier, rosettaBlock.GetBlock().ParentBlockIdentifier)
			require.Equal(expectedRosetta.GetBlock().Timestamp, rosettaBlock.GetBlock().Timestamp)
			require.Equal(len(expectedRosetta.GetBlock().Transactions), len(rosettaBlock.GetBlock().Transactions))
			for i, transaction := range expectedRosetta.GetBlock().Transactions {
				require.Equal(transaction, expectedRosetta.GetBlock().Transactions[i])
			}

			err = s.parser.ValidateBlock(context.Background(), nativeBlock)
			require.NoError(err)

			// Don't forget to reset the flag before you commit.
			require.False(s.config.updateFixture)
		})
	}
}
