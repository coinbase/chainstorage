package bitcoin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type bitcoinRosettaParserTestSuite struct {
	suite.Suite
	app    testapp.TestApp
	parser internal.RosettaParser
}

func TestBitcoinRosettaParserTestSuite(t *testing.T) {
	suite.Run(t, new(bitcoinRosettaParserTestSuite))
}

func (s *bitcoinRosettaParserTestSuite) SetupTest() {
	s.app = testapp.New(s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Provide(NewBitcoinRosettaParser),
		fx.Populate(&s.parser),
	)
	s.NotNil(s.parser)
}

func (s *bitcoinRosettaParserTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *bitcoinRosettaParserTestSuite) TestParseGenesisBitcoinBlock() {
	require := testutil.Require(s.T())

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_0.json")
	require.NoError(err)

	rosettaBlock, err := s.parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	require.Equal("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
		rosettaBlock.Block.GetBlockIdentifier().GetHash())
	require.EqualValues("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b:0",
		rosettaBlock.GetBlock().Transactions[0].Operations[1].GetCoinChange().CoinIdentifier.Identifier)
	require.Nil(rosettaBlock.GetBlock().Transactions[0].Operations[0].GetCoinChange())
}

func (s *bitcoinRosettaParserTestSuite) TestSkipTransaction() {
	require := testutil.Require(s.T())

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_91880.json")
	require.NoError(err)

	rosettaBlock, err := s.parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	require.EqualValues("SKIPPED",
		rosettaBlock.GetBlock().Transactions[0].Operations[0].GetStatus())
}

func (s *bitcoinRosettaParserTestSuite) TestParseBlock() {
	require := testutil.Require(s.T())

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_200000.json")
	require.NoError(err)

	rosettaBlock, err := s.parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	require.EqualValues(388, len(rosettaBlock.GetBlock().Transactions))
}

func (s *bitcoinRosettaParserTestSuite) TestNullData() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/get_block.json")
	require.NoError(err)
	tx1, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction.json")
	require.NoError(err)
	tx2, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction_tx2.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   bitcoinMetadata,
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
				InputTransactions: []*api.RepeatedBytes{
					{
						Data: [][]byte{},
					},
					{
						Data: [][]byte{
							tx1,
							tx2,
						},
					},
				},
			},
		},
	}
	rosettaBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)

	require.Nil(rosettaBlock.GetBlock().Transactions[0].Operations[2].GetCoinChange())
}

func (s *bitcoinRosettaParserTestSuite) TestWitnessTransactions() {
	require := testutil.Require(s.T())

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)

	rosettaBlock, err := s.parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	for _, tx := range rosettaBlock.GetBlock().Transactions {
		if tx.TransactionIdentifier.GetHash() == "8ff5a34c17248112593c9c77f4085451213c5f63509710731c007dae7b75bbbf" {
			require.Equal(
				"8ff5a34c17248112593c9c77f4085451213c5f63509710731c007dae7b75bbbf:0",
				tx.Operations[1].CoinChange.CoinIdentifier.Identifier,
			)
		}
	}
}

func (s *bitcoinRosettaParserTestSuite) TestInputNullAddressTransactions() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/get_block_pubkey_input_non_coinbase_case.json")
	require.NoError(err)
	txInput, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction_tx3.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          bitcoinTag,
			Hash:         "00000000000002829417bdbe92f624d37870ac6780077c4cefa7e1261293ea42",
			ParentHash:   "000000000000092646ec9cc60b7d989a3e49f8b1d0eb41479c41a30ee4319915",
			Height:       159006,
			ParentHeight: 159005,
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
				InputTransactions: []*api.RepeatedBytes{
					{
						Data: [][]byte{},
					},
					{
						Data: [][]byte{
							txInput,
						},
					},
				},
			},
		},
	}
	rosettaBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal("6425f19b62e07961ddba1519722b21a21e550fc417b64866b06a8f580d44848f:0",
		rosettaBlock.GetBlock().Transactions[1].Operations[0].Account.Address)
}
