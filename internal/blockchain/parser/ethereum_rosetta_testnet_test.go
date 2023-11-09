package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type ethereumRosettaParserTestnetTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  Parser
}

func TestEthereumRosettaParserTestnetTestSuite(t *testing.T) {
	suite.Run(t, new(ethereumRosettaParserTestnetTestSuite))
}

func (s *ethereumRosettaParserTestnetTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_GOERLI),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *ethereumRosettaParserTestnetTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *ethereumRosettaParserTestnetTestSuite) TestBlockRewardTransaction_BeforeMerge() {
	require := testutil.Require(s.T())
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_GOERLI,
		Metadata:   ethereumMetadata,
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeaderBeforeMergeGoerli,
				TransactionReceipts: [][]byte{fixtureReceiptBeforeMergeGoerli},
				TransactionTraces:   [][]byte{fixtureTracesBeforeMergeGoerli},
			},
		},
	}

	rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	rewardTxn := rosettaBlock.Block.Transactions[0]
	require.Equal(1, len(rewardTxn.Operations))
	// miner reward op
	require.Equal("2000000000000000000", rewardTxn.Operations[0].Amount.Value)
}

func (s *ethereumRosettaParserTestnetTestSuite) TestBlockRewardTransaction_AfterMerge() {
	require := testutil.Require(s.T())
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_GOERLI,
		Metadata:   ethereumMetadata,
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeaderAfterMergeGoerli,
				TransactionReceipts: [][]byte{fixtureReceiptAfterMergeGoerli},
				TransactionTraces:   [][]byte{fixtureTracesAfterMergeGoerli},
			},
		},
	}

	rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	rewardTxn := rosettaBlock.Block.Transactions[0]
	require.Equal(1, len(rewardTxn.Operations))
	// miner reward op
	require.Equal("0", rewardTxn.Operations[0].Amount.Value)
}
