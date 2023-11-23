package ethereum

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainstorage/internal/utils/testutil"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
)

type ethereumRosettaGoerliParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestEthereumRosettaGoerliParserTestSuite(t *testing.T) {
	suite.Run(t, new(ethereumRosettaGoerliParserTestSuite))
}

func (s *ethereumRosettaGoerliParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_GOERLI),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *ethereumRosettaGoerliParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *ethereumRosettaGoerliParserTestSuite) TestParseBlock_EthereumGoerli_Genesis() {
	require := testutil.Require(s.T())

	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_goerli_0.json")
	require.NoError(err)
	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(actualRosettaBlock)

	txns := actualRosettaBlock.GetBlock().Transactions
	require.Equal(261, len(txns))
	sampleTxn := txns[2]
	require.Equal("GENESIS_0000000000000000000000000000000000000001", sampleTxn.TransactionIdentifier.Hash)
	sampleOperation := txns[2].Operations[0]
	require.Equal("0x0000000000000000000000000000000000000001", sampleOperation.Account.Address)
	require.Equal("1", sampleOperation.Amount.Value)
	require.Equal("ETH", sampleOperation.Amount.Currency.Symbol)
	require.Equal(int32(18), sampleOperation.Amount.Currency.Decimals)
}
