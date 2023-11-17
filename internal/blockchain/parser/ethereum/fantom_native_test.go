package ethereum

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	fantomTag        uint32 = 1
	fantomHash              = "0x000000010000000fe763077a20b2e1efbbcc58fe7866fb33d32c4d42834e91b6"
	fantomParentHash        = "0x00000001000000027bcad26d4ef227709ac6ad024fb2505d78da77b1d7c13d37"
	fantomHeight     uint64 = 0x2
)

type fantomParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestFantomParserTestSuite(t *testing.T) {
	suite.Run(t, new(fantomParserTestSuite))
}

func (s *fantomParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_FANTOM, common.Network_NETWORK_FANTOM_MAINNET),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *fantomParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *fantomParserTestSuite) TestParseFantomBlock() {
	require := testutil.Require(s.T())

	traces := s.fixtureTracesParsingHelper("parser/fantom/fantom_paritytrace_basic.json")
	fixtureHeader, err := fixtures.ReadFile("parser/fantom/fantom_block.json")
	require.NoError(err)
	fixtureReceipt, err := fixtures.ReadFile("parser/fantom/fantom_transaction_receit.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_FANTOM,
		Network:    common.Network_NETWORK_FANTOM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        fantomTag,
			Hash:       fantomHash,
			ParentHash: fantomParentHash,
			Height:     fantomHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt},
				TransactionTraces:   traces,
			},
		},
	}

	var expected api.EthereumBlock
	fixtures.MustUnmarshalJSON("parser/fantom/fantom_getnativeblock.json", &expected)
	expected.Transactions[0].Receipt.OptionalStatus = &api.EthereumTransactionReceipt_Status{
		Status: 1,
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_FANTOM, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_FANTOM_MAINNET, nativeBlock.Network)
	actual := nativeBlock.GetEthereum()
	require.NotNil(actual)
	require.Equal(expected.Header, actual.Header)
	require.Equal(expected.Transactions, actual.Transactions)
}

func (s *fantomParserTestSuite) fixtureTracesParsingHelper(filePath string) [][]byte {
	require := testutil.Require(s.T())

	fixtureParityTrace, err := fixtures.ReadFile(filePath)
	require.NoError(err)

	var tmpTraces []json.RawMessage
	err = json.Unmarshal(fixtureParityTrace, &tmpTraces)
	require.NoError(err)

	traces := make([][]byte, len(tmpTraces))
	for i, trace := range tmpTraces {
		traces[i] = trace
	}

	return traces
}
