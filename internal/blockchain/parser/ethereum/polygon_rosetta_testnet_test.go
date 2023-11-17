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

type polygonRosettaParserTestnestTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestPolygonRosettaParserTestnetTestSuite(t *testing.T) {
	suite.Run(t, new(polygonRosettaParserTestnestTestSuite))
}

func (s *polygonRosettaParserTestnestTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_TESTNET),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *polygonRosettaParserTestnestTestSuite) TestParseBlock_PolygonTestnet_Genesis() {
	require := testutil.Require(s.T())

	block, err := testutil.LoadRawBlock("parser/polygon/polygon_testnet_raw_block_0.json")
	require.NoError(err)
	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(actualRosettaBlock)

	txns := actualRosettaBlock.GetBlock().Transactions
	require.Equal(6, len(txns))
	sampleTxn := txns[2]
	require.Equal("GENESIS_be188d6641e8b680743a4815dfa0f6208038960f", sampleTxn.TransactionIdentifier.Hash)
	sampleOperation := txns[2].Operations[0]
	require.Equal("0xbe188d6641e8b680743a4815dfa0f6208038960f", sampleOperation.Account.Address)
	require.Equal("1000000000000000000000", sampleOperation.Amount.Value)
	require.Equal("MATIC", sampleOperation.Amount.Currency.Symbol)
	require.Equal(int32(18), sampleOperation.Amount.Currency.Decimals)
}
