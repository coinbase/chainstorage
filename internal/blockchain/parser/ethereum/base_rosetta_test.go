package ethereum

import (
	"context"
	"math/big"
	"testing"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

type baseRosettaParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestBaseRosettaParserTestSuite(t *testing.T) {
	suite.Run(t, new(baseRosettaParserTestSuite))
}

func (s *baseRosettaParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BASE, common.Network_NETWORK_BASE_GOERLI),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *baseRosettaParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *baseRosettaParserTestSuite) TestWithdrawTransaction() {
	require := testutil.Require(s.T())

	fixtureHeader := fixtures.MustReadFile("parser/base/goerli/base_withdrawal_getblockheaderresponse.json")
	fixtureReceipt := fixtures.MustReadFile("parser/base/goerli/base_withdrawal_gettransactionreceipt.json")
	fixtureTraces := fixtures.MustReadFile("parser/base/goerli/base_withdrawal_gettraceblockbyhashresponse.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BASE,
		Network:    common.Network_NETWORK_BASE_GOERLI,
		Metadata: &api.BlockMetadata{
			Tag:        baseTag,
			Hash:       "0xd135ea33756574d5dcd7818d78df713cc265679683ba5b9ce002e0f2d4233471",
			ParentHash: "0x6f98a06dde560a62bb2cb9d4a005bbd4475a4be3d60c75464323d75e870372fd",
			Height:     1041847,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt},
				TransactionTraces:   [][]byte{fixtureTraces},
			},
		},
	}

	expected := &rosetta.Transaction{
		TransactionIdentifier: &rosetta.TransactionIdentifier{
			Hash: "0xdfbb544fe4288dd73c323bcc546f1354f2241a899213708cf64cfad475d3de39",
		},
		Operations: []*rosetta.Operation{
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 0,
				},
				RelatedOperations: nil,
				Type:              "FEE",
				Status:            "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x86c98d7a1e8ea72fb3affcab17aa15e4422d575b",
				},
				Amount: &rosetta.Amount{
					Value:    "-221869999423780",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 1,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 0,
					},
				},
				Type:   "FEE",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x4200000000000000000000000000000000000011",
				},
				Amount: &rosetta.Amount{
					Value:    "118575996857736",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 2,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 0,
					},
				},
				Type:   "FEE",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x4200000000000000000000000000000000000019",
				},
				Amount: &rosetta.Amount{
					Value:    "3142264",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 3,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 0,
					},
				},
				Type:   "FEE",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x420000000000000000000000000000000000001a",
				},
				Amount: &rosetta.Amount{
					Value:    "103293999423780",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 4,
				},
				Type:   "CALL",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x86c98d7a1e8ea72fb3affcab17aa15e4422d575b",
				},
				Amount: &rosetta.Amount{
					Value:    "-100000000000000",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 5,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 4,
					},
				},
				Type:   "CALL",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x4200000000000000000000000000000000000016",
				},
				Amount: &rosetta.Amount{
					Value:    "100000000000000",
					Currency: &ethereumRosettaCurrency,
				},
			},
		},
	}

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)

	withdrawalTransaction := actual.Block.Transactions[0]
	require.NotNil(withdrawalTransaction)

	sum := sumValues(withdrawalTransaction.Operations)
	require.Equal(int64(0), sum.Int64())

	require.Equal(expected.TransactionIdentifier, withdrawalTransaction.TransactionIdentifier)
	require.Equal(expected.Operations, withdrawalTransaction.Operations)
}

func (s *baseRosettaParserTestSuite) TestEip1559Transaction() {
	require := testutil.Require(s.T())

	fixtureHeader := fixtures.MustReadFile("parser/base/goerli/base_eip1559_getblockheaderresponse.json")
	fixtureReceipt := fixtures.MustReadFile("parser/base/goerli/base_eip1559_gettransactionreceipt.json")
	fixtureTraces := fixtures.MustReadFile("parser/base/goerli/base_eip1559_gettraceblockbyhashresponse.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BASE,
		Network:    common.Network_NETWORK_BASE_GOERLI,
		Metadata: &api.BlockMetadata{
			Tag:        baseTag,
			Hash:       "0xd135ea33756574d5dcd7818d78df713cc265679683ba5b9ce002e0f2d4233471",
			ParentHash: "0x6f98a06dde560a62bb2cb9d4a005bbd4475a4be3d60c75464323d75e870372fd",
			Height:     1041847,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt},
				TransactionTraces:   [][]byte{fixtureTraces},
			},
		},
	}

	expected := &rosetta.Transaction{
		TransactionIdentifier: &rosetta.TransactionIdentifier{
			Hash: "0x56654d70e6aa4efd47224f9c3e97a1b70fa9a473ff23e7d457311d363d18f391",
		},
		Operations: []*rosetta.Operation{
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 0,
				},
				RelatedOperations: nil,
				Type:              "FEE",
				Status:            "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x69155e7ca2e688ccdc247f6c4ddf374b3ae77bd6",
				},
				Amount: &rosetta.Amount{
					Value:    "-133495375815540",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 1,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 0,
					},
				},
				Type:   "FEE",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x4200000000000000000000000000000000000011",
				},
				Amount: &rosetta.Amount{
					Value:    "39517798992000",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 2,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 0,
					},
				},
				Type:   "FEE",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x4200000000000000000000000000000000000019",
				},
				Amount: &rosetta.Amount{
					Value:    "1113000",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 3,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 0,
					},
				},
				Type:   "FEE",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x420000000000000000000000000000000000001a",
				},
				Amount: &rosetta.Amount{
					Value:    "93977575710540",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 4,
				},
				Type:   "CALL",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0x69155e7ca2e688ccdc247f6c4ddf374b3ae77bd6",
				},
				Amount: &rosetta.Amount{
					Value:    "-100000000000000000",
					Currency: &ethereumRosettaCurrency,
				},
			},
			{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: 5,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: 4,
					},
				},
				Type:   "CALL",
				Status: "SUCCESS",
				Account: &rosetta.AccountIdentifier{
					Address: "0xc861f3e7e578485c0e0761d77a8be466e4317945",
				},
				Amount: &rosetta.Amount{
					Value:    "100000000000000000",
					Currency: &ethereumRosettaCurrency,
				},
			},
		},
	}

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)

	eip1559Txn := actual.Block.Transactions[0]
	require.NotNil(eip1559Txn)

	sum := sumValues(eip1559Txn.Operations)
	require.Equal(int64(0), sum.Int64())

	require.Equal(expected.TransactionIdentifier, eip1559Txn.TransactionIdentifier)
	require.Equal(expected.Operations, eip1559Txn.Operations)
}

func sumValues(ops []*rosetta.Operation) *big.Int {
	sum := big.NewInt(0)
	for _, op := range ops {
		value, _ := internal.BigInt(op.Amount.Value)
		sum = sum.Add(sum, value)
	}

	return sum
}
