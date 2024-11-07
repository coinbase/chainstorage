package ethereum

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

const (
	polygonOldTag       uint32 = 1
	polygonNewTag       uint32 = 2
	polygonHeight       uint64 = 0x2092dbd
	polygonParentHeight uint64 = 0x2092dbc
	polygonHash                = "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3"
	polygonParentHash          = "0x3ce76c407965ae36104d49e146fdbfc2306eea3bffb1f1a0f9ea14e8d4d53f3d"
)

type polygonRosettaParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestPolygonRosettaParserTestSuite(t *testing.T) {
	suite.Run(t, new(polygonRosettaParserTestSuite))
}

func (s *polygonRosettaParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *polygonRosettaParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *polygonRosettaParserTestSuite) TestPolygonRosettaParser() {
	require := testutil.Require(s.T())

	fixturePolygonHeader := fixtures.MustReadFile("parser/polygon/polygon_fixture_header_34155965.json")
	fixturePolygonReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_receipt_34155965.json")
	fixturePolygonBorReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_bor_receipt_34155965.json")
	fixturePolygonTraces := fixtures.MustReadFile("parser/polygon/polygon_fixture_traces_34155965.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_POLYGON,
		Network:    common.Network_NETWORK_POLYGON_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          polygonNewTag,
			Hash:         polygonHash,
			Height:       polygonHeight,
			ParentHash:   polygonParentHash,
			ParentHeight: polygonParentHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixturePolygonHeader,
				TransactionReceipts: [][]byte{fixturePolygonReceipt, fixturePolygonBorReceipt},
				TransactionTraces:   [][]byte{fixturePolygonTraces},
				ExtraData: &api.EthereumBlobdata_Polygon{
					Polygon: &api.PolygonExtraData{
						Author: []byte(`"0x160cdef60e786295728a6ea334c091238e474e01"`),
					},
				},
			},
		},
	}

	metadata, _ := rosetta.FromSDKMetadata(map[string]any{
		"gas_limit": "0xfce7",
		"gas_price": "0xba43b7400",
		"receipt": map[string]any{
			"blockHash":         "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
			"blockNumber":       "0x2092dbd",
			"cumulativeGasUsed": "0xd15f",
			"effectiveGasPrice": "0xba43b7400",
			"from":              "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
			"gasUsed":           "0xd15f",
			"contractAddress":   any(nil),
			"logs": []any{
				map[string]any{
					"address":     "0x3376c61c450359d402f07909bda979a4c0e6c32f",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x0000000000000000000000000000000000000000000000000000000000000001",
					"logIndex":    "0x0",
					"removed":     false,
					"topics": []any{
						"0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x00000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e97",
					},
					"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					"transactionIndex": "0x0",
				},
				map[string]any{
					"address":     "0x0000000000000000000000000000000000001010",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x000000000000000000000000000000000000000000000000000985667bb7c76f0000000000000000000000000000000000000000000000000bac25454dd1b400000000000000000000000000000000000000000000000329622b11d5bbd78baa0000000000000000000000000000000000000000000000000ba29fded219ec910000000000000000000000000000000000000000000003296234973c378f5319",
					"logIndex":    "0x1",
					"removed":     false,
					"topics": []any{
						"0x4dfe1bbbcf077ddc3e01291eea2d5c70c2b422b415d95645b9adcfd678cb1d63",
						"0x0000000000000000000000000000000000000000000000000000000000001010",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x000000000000000000000000160cdef60e786295728a6ea334c091238e474e01",
					},
					"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					"transactionIndex": "0x0",
				},
			},
			"logsBloom":        "0x00000000000000000000000000000001000000000040000000000040300000000000000200000000000000000000000000008000000000000000000000000001000000020000000000000000000000800000000000000000000100000000000002000000000000000000000000000000000000000000000080000000000000000000000400000000000000000000000000000000020000000001000000000000200000000000000000000000000000000000000000000000000000000000004000000000000000000001000000000000000000000000000000100000000000000000200800000000000000000000200000000000000000000000000000100000",
			"status":           "0x1",
			"to":               "0x3376c61c450359d402f07909bda979a4c0e6c32f",
			"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
			"transactionIndex": "0x0",
			"type":             "0x0",
		},
		"trace": map[string]any{
			"calls": []any{
				map[string]any{
					"from":    "0x3376c61c450359d402f07909bda979a4c0e6c32f",
					"to":      "0xe2dd4e4b32d785dad138181d0b045497f0f1bbb0",
					"gas":     "0x8a0e",
					"gasUsed": "0x6090",
					"input":   "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
					"output":  "0x",
					"type":    "DELEGATECALL",
				},
			},
			"from":    "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
			"gas":     "0xa8af",
			"gasUsed": "0x7d27",
			"input":   "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
			"output":  "0x",
			"to":      "0x3376c61c450359d402f07909bda979a4c0e6c32f",
			"type":    "CALL",
			"value":   "0x0",
		},
	})

	unknownMetadata, _ := rosetta.FromSDKMetadata(map[string]any{
		"token_address": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
	})

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	expected := &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Index: 34155965,
				Hash:  "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
			},
			ParentBlockIdentifier: &rosetta.BlockIdentifier{
				Index: 34155964,
				Hash:  "0x3ce76c407965ae36104d49e146fdbfc2306eea3bffb1f1a0f9ea14e8d4d53f3d",
			},
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
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
								Address: "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
							},
							Amount: &rosetta.Amount{
								Value:    "-2679949999196015",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
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
								Address: "0x160cdef60e786295728a6ea334c091238e474e01",
							},
							Amount: &rosetta.Amount{
								Value:    "2679949999196015",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 2,
							},
							RelatedOperations: nil,
							Type:              "FEE",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
							},
							Amount: &rosetta.Amount{
								Value:    "-803985",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 3,
							},
							RelatedOperations: []*rosetta.OperationIdentifier{
								{
									Index: 2,
								},
							},
							Type:   "FEE",
							Status: "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x70bca57f4579f58670ab2d18ef16e02c17553c38",
							},
							Amount: &rosetta.Amount{
								Value:    "803985",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
					},
					Metadata: metadata,
				},
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xd9273904d2913833c1740831c41f583cac249ef62de2c3c880dd43ca1f8c33fc",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 0,
							},
							RelatedOperations: nil,
							Type:              "PAYMENT",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x0000000000000000000000000000000000000000",
							},
							Amount: &rosetta.Amount{
								Value: "-20750000000000000",
								Currency: &rosetta.Currency{
									Symbol:   "UNKNOWN",
									Metadata: unknownMetadata,
								},
							},
							CoinChange: nil,
							Metadata:   nil,
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
							Type:   "PAYMENT",
							Status: "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xbc850334dd4a52a8d4306e78da404271290561de",
							},
							Amount: &rosetta.Amount{
								Value: "20750000000000000",
								Currency: &rosetta.Currency{
									Symbol:   "UNKNOWN",
									Metadata: unknownMetadata,
								},
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 2,
							},
							RelatedOperations: nil,
							Type:              "CALL",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x0000000000000000000000000000000000001010",
							},
							Amount: &rosetta.Amount{
								Value:    "-1772043931424358792936",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 3,
							},
							RelatedOperations: nil,
							Type:              "CALL",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xdc7da6884ff2c90c713724e2baa6ab134983927d",
							},
							Amount: &rosetta.Amount{
								Value:    "1772043931424358792936",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
					},
				},
			},
		},
	}
	s.testapp.Logger().Debug("comparing blocks", zap.Reflect("expected", expected), zap.Reflect("actual", actual))

	require.Equal(expected.Block.BlockIdentifier, actual.Block.BlockIdentifier)
	require.Equal(expected.Block.ParentBlockIdentifier, actual.Block.ParentBlockIdentifier)
	require.Equal(len(expected.Block.Transactions), len(actual.Block.Transactions))
	require.Equal(expected.Block.Transactions[0].TransactionIdentifier, actual.Block.Transactions[0].TransactionIdentifier)
	require.Equal(expected.Block.Transactions[0].RelatedTransactions, actual.Block.Transactions[0].RelatedTransactions)
	require.Equal(expected.Block.Transactions[0].Operations, actual.Block.Transactions[0].Operations)
	expectedTx2SDKMetadata, _ := rosetta.ToSDKMetadata(expected.Block.Transactions[0].Metadata)
	actualTx2SDKMetadata, _ := rosetta.ToSDKMetadata(actual.Block.Transactions[0].Metadata)
	require.Equal(expectedTx2SDKMetadata, actualTx2SDKMetadata)

	require.Equal(expected.Block.Transactions[1].TransactionIdentifier, actual.Block.Transactions[1].TransactionIdentifier)
	require.Equal(expected.Block.Transactions[1].RelatedTransactions, actual.Block.Transactions[1].RelatedTransactions)
	require.Equal(expected.Block.Transactions[1].Operations, actual.Block.Transactions[1].Operations)
}

func (s *polygonRosettaParserTestSuite) TestPolygonRosettaParser_HopProtocol() {
	require := testutil.Require(s.T())

	fixturePolygonHeader := fixtures.MustReadFile("parser/polygon/polygon_fixture_header_38656992.json")
	fixturePolygonHopProtocolReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_hop_protocol_receipt_38656992.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_POLYGON,
		Network:    common.Network_NETWORK_POLYGON_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          polygonNewTag,
			Hash:         "0x9c6be2c781c039968a6b6e635d822343cd46966cdef025f62da3800b1b8099af",
			Height:       38656992,
			ParentHash:   polygonParentHash,
			ParentHeight: 38656991,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixturePolygonHeader,
				TransactionReceipts: [][]byte{fixturePolygonHopProtocolReceipt},
				TransactionTraces:   [][]byte{},
				ExtraData: &api.EthereumBlobdata_Polygon{
					Polygon: &api.PolygonExtraData{
						Author: []byte(`"0x160cdef60e786295728a6ea334c091238e474e01"`),
					},
				},
			},
		},
	}

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(len(actual.Block.Transactions), 1)
	require.Equal(len(actual.Block.Transactions[0].Operations), 16)

	op := actual.Block.Transactions[0].Operations[15]
	require.Equal(op.Account.Address, "0x953c64f3fec00b489b94bbcc4b55f431c68c8ceb")
	require.Equal(op.Amount.Value, "13143845791708658009")
	require.Equal(op.Amount.Currency.Symbol, "MATIC")
}

func (s *polygonRosettaParserTestSuite) TestPolygonRosettaParser_MPL() {
	require := testutil.Require(s.T())

	fixturePolygonHeader := fixtures.MustReadFile("parser/polygon/polygon_fixture_header_31992640.json")
	fixturePolygonMPLReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_receipt_31992640.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_POLYGON,
		Network:    common.Network_NETWORK_POLYGON_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          polygonNewTag,
			Hash:         "0x9c6be2c781c039968a6b6e635d822343cd46966cdef025f62da3800b1b8099af",
			Height:       31992640,
			ParentHash:   polygonParentHash,
			ParentHeight: 31992639,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixturePolygonHeader,
				TransactionReceipts: [][]byte{fixturePolygonMPLReceipt},
				TransactionTraces:   [][]byte{},
				ExtraData: &api.EthereumBlobdata_Polygon{
					Polygon: &api.PolygonExtraData{
						Author: []byte(`"0x160cdef60e786295728a6ea334c091238e474e01"`),
					},
				},
			},
		},
	}

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(len(actual.Block.Transactions), 1)
	require.Equal(len(actual.Block.Transactions[0].Operations), 2)

	op := actual.Block.Transactions[0].Operations[1]
	require.Equal(op.Account.Address, "0x52d77a8187160e41d08f892f46d6cf8ada1f6771")
	require.Equal(op.Amount.Value, "100000000000000000")
	require.Equal(op.Amount.Currency.Symbol, "MATIC")
}

func (s *polygonRosettaParserTestSuite) TestRosettaPolygonResponseParity() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/polygon/raw_block_38102393.json")
	require.NoError(err)

	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	expectedRosettaBlock, err := testutil.LoadRosettaBlock("parser/polygon/rosetta_polygon_block_response_38102393.json")
	require.NoError(err)

	expectedRosettaProtoTxns, err := rosetta.FromSDKTransactions(expectedRosettaBlock.Block.Transactions)
	require.NoError(err)
	require.Equal(len(expectedRosettaProtoTxns), len(actualRosettaBlock.Block.Transactions))
	for i := 0; i < len(expectedRosettaProtoTxns); i++ {
		err := s.normalizeTransaction(expectedRosettaProtoTxns[i], actualRosettaBlock.Block.Transactions[i])
		require.NoError(err)
		require.Equal(expectedRosettaProtoTxns[i], actualRosettaBlock.Block.Transactions[i])
	}
}

func (s *polygonRosettaParserTestSuite) normalizeTransaction(expected *rosetta.Transaction, actual *rosetta.Transaction) error {
	if len(expected.Operations) != len(actual.Operations) {
		return fmt.Errorf("operation size is different for transaction:%v", expected.TransactionIdentifier.Hash)
	}

	for i := range expected.Operations {
		// lowercase all rosetta account addresses to match ChainStorage
		expected.Operations[i].Account.Address = strings.ToLower(expected.Operations[i].Account.Address)

		// skip validation for unknown erc20 currency
		if actual.Operations[i].GetAmount().GetCurrency().GetSymbol() == defaultERC20Symbol {
			actual.Operations[i].Amount.Currency = expected.Operations[i].Amount.Currency
		}
	}

	// skip metadata(receipt, trace, gasPrice, gasLimit) validation
	expected.Metadata = actual.Metadata

	return nil
}

func (s *polygonRosettaParserTestSuite) TestParseBlock_PolygonMainnet_Genesis() {
	require := testutil.Require(s.T())

	block, err := testutil.LoadRawBlock("parser/polygon/raw_block_0.json")
	require.NoError(err)
	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(actualRosettaBlock)

	txns := actualRosettaBlock.GetBlock().Transactions
	require.Equal(8, len(txns))
	sampleTxn := txns[2]
	require.Equal("GENESIS_b8bb158b93c94ed35c1970d610d1e2b34e26652c", sampleTxn.TransactionIdentifier.Hash)
	sampleOperation := txns[2].Operations[0]
	require.Equal("0xb8bb158b93c94ed35c1970d610d1e2b34e26652c", sampleOperation.Account.Address)
	require.Equal("1000000000000000000000", sampleOperation.Amount.Value)
	require.Equal("MATIC", sampleOperation.Amount.Currency.Symbol)
	require.Equal(int32(18), sampleOperation.Amount.Currency.Decimals)
}

func (s *polygonRosettaParserTestSuite) TestPolygonRosettaParser_WithoutAuthor() {
	require := testutil.Require(s.T())

	fixturePolygonHeader := fixtures.MustReadFile("parser/polygon/polygon_fixture_header_34155965.json")
	fixturePolygonReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_receipt_34155965.json")
	fixturePolygonBorReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_bor_receipt_34155965.json")
	fixturePolygonTraces := fixtures.MustReadFile("parser/polygon/polygon_fixture_traces_34155965.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_POLYGON,
		Network:    common.Network_NETWORK_POLYGON_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          polygonOldTag,
			Hash:         polygonHash,
			Height:       polygonHeight,
			ParentHash:   polygonParentHash,
			ParentHeight: polygonParentHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixturePolygonHeader,
				TransactionReceipts: [][]byte{fixturePolygonReceipt, fixturePolygonBorReceipt},
				TransactionTraces:   [][]byte{fixturePolygonTraces},
			},
		},
	}

	metadata, _ := rosetta.FromSDKMetadata(map[string]any{
		"gas_limit": "0xfce7",
		"gas_price": "0xba43b7400",
		"receipt": map[string]any{
			"blockHash":         "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
			"blockNumber":       "0x2092dbd",
			"cumulativeGasUsed": "0xd15f",
			"effectiveGasPrice": "0xba43b7400",
			"from":              "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
			"gasUsed":           "0xd15f",
			"contractAddress":   any(nil),
			"logs": []any{
				map[string]any{
					"address":     "0x3376c61c450359d402f07909bda979a4c0e6c32f",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x0000000000000000000000000000000000000000000000000000000000000001",
					"logIndex":    "0x0",
					"removed":     false,
					"topics": []any{
						"0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x00000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e97",
					},
					"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					"transactionIndex": "0x0",
				},
				map[string]any{
					"address":     "0x0000000000000000000000000000000000001010",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x000000000000000000000000000000000000000000000000000985667bb7c76f0000000000000000000000000000000000000000000000000bac25454dd1b400000000000000000000000000000000000000000000000329622b11d5bbd78baa0000000000000000000000000000000000000000000000000ba29fded219ec910000000000000000000000000000000000000000000003296234973c378f5319",
					"logIndex":    "0x1",
					"removed":     false,
					"topics": []any{
						"0x4dfe1bbbcf077ddc3e01291eea2d5c70c2b422b415d95645b9adcfd678cb1d63",
						"0x0000000000000000000000000000000000000000000000000000000000001010",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x000000000000000000000000160cdef60e786295728a6ea334c091238e474e01",
					},
					"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					"transactionIndex": "0x0",
				},
			},
			"logsBloom":        "0x00000000000000000000000000000001000000000040000000000040300000000000000200000000000000000000000000008000000000000000000000000001000000020000000000000000000000800000000000000000000100000000000002000000000000000000000000000000000000000000000080000000000000000000000400000000000000000000000000000000020000000001000000000000200000000000000000000000000000000000000000000000000000000000004000000000000000000001000000000000000000000000000000100000000000000000200800000000000000000000200000000000000000000000000000100000",
			"status":           "0x1",
			"to":               "0x3376c61c450359d402f07909bda979a4c0e6c32f",
			"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
			"transactionIndex": "0x0",
			"type":             "0x0",
		},
		"trace": map[string]any{
			"calls": []any{
				map[string]any{
					"from":    "0x3376c61c450359d402f07909bda979a4c0e6c32f",
					"to":      "0xe2dd4e4b32d785dad138181d0b045497f0f1bbb0",
					"gas":     "0x8a0e",
					"gasUsed": "0x6090",
					"input":   "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
					"output":  "0x",
					"type":    "DELEGATECALL",
				},
			},
			"from":    "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
			"gas":     "0xa8af",
			"gasUsed": "0x7d27",
			"input":   "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
			"output":  "0x",
			"to":      "0x3376c61c450359d402f07909bda979a4c0e6c32f",
			"type":    "CALL",
			"value":   "0x0",
		},
	})

	unknownMetadata, _ := rosetta.FromSDKMetadata(map[string]any{
		"token_address": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
	})

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	expected := &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Index: 34155965,
				Hash:  "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
			},
			ParentBlockIdentifier: &rosetta.BlockIdentifier{
				Index: 34155964,
				Hash:  "0x3ce76c407965ae36104d49e146fdbfc2306eea3bffb1f1a0f9ea14e8d4d53f3d",
			},
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					},
					Operations: nil,
					Metadata:   metadata,
				},
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xd9273904d2913833c1740831c41f583cac249ef62de2c3c880dd43ca1f8c33fc",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 0,
							},
							RelatedOperations: nil,
							Type:              "PAYMENT",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x0000000000000000000000000000000000000000",
							},
							Amount: &rosetta.Amount{
								Value: "-20750000000000000",
								Currency: &rosetta.Currency{
									Symbol:   "UNKNOWN",
									Metadata: unknownMetadata,
								},
							},
							CoinChange: nil,
							Metadata:   nil,
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
							Type:   "PAYMENT",
							Status: "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xbc850334dd4a52a8d4306e78da404271290561de",
							},
							Amount: &rosetta.Amount{
								Value: "20750000000000000",
								Currency: &rosetta.Currency{
									Symbol:   "UNKNOWN",
									Metadata: unknownMetadata,
								},
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 2,
							},
							RelatedOperations: nil,
							Type:              "CALL",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x0000000000000000000000000000000000001010",
							},
							Amount: &rosetta.Amount{
								Value:    "-1772043931424358792936",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 3,
							},
							RelatedOperations: nil,
							Type:              "CALL",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xdc7da6884ff2c90c713724e2baa6ab134983927d",
							},
							Amount: &rosetta.Amount{
								Value:    "1772043931424358792936",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
					},
				},
			},
		},
	}
	s.testapp.Logger().Debug("comparing blocks", zap.Reflect("expected", expected), zap.Reflect("actual", actual))

	require.Equal(expected.Block.BlockIdentifier, actual.Block.BlockIdentifier)
	require.Equal(expected.Block.ParentBlockIdentifier, actual.Block.ParentBlockIdentifier)
	require.Equal(len(expected.Block.Transactions), len(actual.Block.Transactions))
	require.Equal(expected.Block.Transactions[0].TransactionIdentifier, actual.Block.Transactions[0].TransactionIdentifier)
	require.Equal(expected.Block.Transactions[0].RelatedTransactions, actual.Block.Transactions[0].RelatedTransactions)
	require.Equal(expected.Block.Transactions[0].Operations, actual.Block.Transactions[0].Operations)
	expectedTx2SDKMetadata, _ := rosetta.ToSDKMetadata(expected.Block.Transactions[0].Metadata)
	actualTx2SDKMetadata, _ := rosetta.ToSDKMetadata(actual.Block.Transactions[0].Metadata)
	require.Equal(expectedTx2SDKMetadata, actualTx2SDKMetadata)

	require.Equal(expected.Block.Transactions[1].TransactionIdentifier, actual.Block.Transactions[1].TransactionIdentifier)
	require.Equal(expected.Block.Transactions[1].RelatedTransactions, actual.Block.Transactions[1].RelatedTransactions)
	require.Equal(expected.Block.Transactions[1].Operations, actual.Block.Transactions[1].Operations)
}

func (s *polygonRosettaParserTestSuite) TestPolygonRosettaParser_PolygonTestnet() {
	require := testutil.Require(s.T())

	fixturePolygonHeader := fixtures.MustReadFile("parser/polygon/polygon_fixture_header_34155965.json")
	fixturePolygonReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_receipt_34155965.json")
	fixturePolygonBorReceipt := fixtures.MustReadFile("parser/polygon/polygon_fixture_bor_receipt_34155965.json")
	fixturePolygonTraces := fixtures.MustReadFile("parser/polygon/polygon_fixture_traces_34155965.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_POLYGON,
		Network:    common.Network_NETWORK_POLYGON_TESTNET,
		Metadata: &api.BlockMetadata{
			Tag:          polygonNewTag,
			Hash:         polygonHash,
			Height:       polygonHeight,
			ParentHash:   polygonParentHash,
			ParentHeight: polygonParentHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixturePolygonHeader,
				TransactionReceipts: [][]byte{fixturePolygonReceipt, fixturePolygonBorReceipt},
				TransactionTraces:   [][]byte{fixturePolygonTraces},
				ExtraData: &api.EthereumBlobdata_Polygon{
					Polygon: &api.PolygonExtraData{
						Author: []byte(`"0x160cdef60e786295728a6ea334c091238e474e01"`),
					},
				},
			},
		},
	}

	metadata, _ := rosetta.FromSDKMetadata(map[string]any{
		"gas_limit": "0xfce7",
		"gas_price": "0xba43b7400",
		"receipt": map[string]any{
			"blockHash":         "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
			"blockNumber":       "0x2092dbd",
			"cumulativeGasUsed": "0xd15f",
			"effectiveGasPrice": "0xba43b7400",
			"from":              "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
			"gasUsed":           "0xd15f",
			"contractAddress":   any(nil),
			"logs": []any{
				map[string]any{
					"address":     "0x3376c61c450359d402f07909bda979a4c0e6c32f",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x0000000000000000000000000000000000000000000000000000000000000001",
					"logIndex":    "0x0",
					"removed":     false,
					"topics": []any{
						"0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x00000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e97",
					},
					"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					"transactionIndex": "0x0",
				},
				map[string]any{
					"address":     "0x0000000000000000000000000000000000001010",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x000000000000000000000000000000000000000000000000000985667bb7c76f0000000000000000000000000000000000000000000000000bac25454dd1b400000000000000000000000000000000000000000000000329622b11d5bbd78baa0000000000000000000000000000000000000000000000000ba29fded219ec910000000000000000000000000000000000000000000003296234973c378f5319",
					"logIndex":    "0x1",
					"removed":     false,
					"topics": []any{
						"0x4dfe1bbbcf077ddc3e01291eea2d5c70c2b422b415d95645b9adcfd678cb1d63",
						"0x0000000000000000000000000000000000000000000000000000000000001010",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x000000000000000000000000160cdef60e786295728a6ea334c091238e474e01",
					},
					"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					"transactionIndex": "0x0",
				},
			},
			"logsBloom":        "0x00000000000000000000000000000001000000000040000000000040300000000000000200000000000000000000000000008000000000000000000000000001000000020000000000000000000000800000000000000000000100000000000002000000000000000000000000000000000000000000000080000000000000000000000400000000000000000000000000000000020000000001000000000000200000000000000000000000000000000000000000000000000000000000004000000000000000000001000000000000000000000000000000100000000000000000200800000000000000000000200000000000000000000000000000100000",
			"status":           "0x1",
			"to":               "0x3376c61c450359d402f07909bda979a4c0e6c32f",
			"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
			"transactionIndex": "0x0",
			"type":             "0x0",
		},
		"trace": map[string]any{
			"calls": []any{
				map[string]any{
					"from":    "0x3376c61c450359d402f07909bda979a4c0e6c32f",
					"to":      "0xe2dd4e4b32d785dad138181d0b045497f0f1bbb0",
					"gas":     "0x8a0e",
					"gasUsed": "0x6090",
					"input":   "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
					"output":  "0x",
					"type":    "DELEGATECALL",
				},
			},
			"from":    "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
			"gas":     "0xa8af",
			"gasUsed": "0x7d27",
			"input":   "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
			"output":  "0x",
			"to":      "0x3376c61c450359d402f07909bda979a4c0e6c32f",
			"type":    "CALL",
			"value":   "0x0",
		},
	})

	unknownMetadata, _ := rosetta.FromSDKMetadata(map[string]any{
		"token_address": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
	})

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	expected := &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Index: 34155965,
				Hash:  "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
			},
			ParentBlockIdentifier: &rosetta.BlockIdentifier{
				Index: 34155964,
				Hash:  "0x3ce76c407965ae36104d49e146fdbfc2306eea3bffb1f1a0f9ea14e8d4d53f3d",
			},
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
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
								Address: "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
							},
							Amount: &rosetta.Amount{
								Value:    "-2679949999196015",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
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
								Address: "0x160cdef60e786295728a6ea334c091238e474e01",
							},
							Amount: &rosetta.Amount{
								Value:    "2679949999196015",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 2,
							},
							RelatedOperations: nil,
							Type:              "FEE",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
							},
							Amount: &rosetta.Amount{
								Value:    "-803985",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 3,
							},
							RelatedOperations: []*rosetta.OperationIdentifier{
								{
									Index: 2,
								},
							},
							Type:   "FEE",
							Status: "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x70bca57f4579f58670ab2d18ef16e02c17553c38",
							},
							Amount: &rosetta.Amount{
								Value:    "803985",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
					},
					Metadata: metadata,
				},
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xd9273904d2913833c1740831c41f583cac249ef62de2c3c880dd43ca1f8c33fc",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 0,
							},
							RelatedOperations: nil,
							Type:              "PAYMENT",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x0000000000000000000000000000000000000000",
							},
							Amount: &rosetta.Amount{
								Value: "-20750000000000000",
								Currency: &rosetta.Currency{
									Symbol:   "UNKNOWN",
									Metadata: unknownMetadata,
								},
							},
							CoinChange: nil,
							Metadata:   nil,
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
							Type:   "PAYMENT",
							Status: "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xbc850334dd4a52a8d4306e78da404271290561de",
							},
							Amount: &rosetta.Amount{
								Value: "20750000000000000",
								Currency: &rosetta.Currency{
									Symbol:   "UNKNOWN",
									Metadata: unknownMetadata,
								},
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 2,
							},
							RelatedOperations: nil,
							Type:              "CALL",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x0000000000000000000000000000000000001010",
							},
							Amount: &rosetta.Amount{
								Value:    "-1772043931424358792936",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 3,
							},
							RelatedOperations: nil,
							Type:              "CALL",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xdc7da6884ff2c90c713724e2baa6ab134983927d",
							},
							Amount: &rosetta.Amount{
								Value:    "1772043931424358792936",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
					},
				},
			},
		},
	}
	s.testapp.Logger().Debug("comparing blocks", zap.Reflect("expected", expected), zap.Reflect("actual", actual))

	require.Equal(expected.Block.BlockIdentifier, actual.Block.BlockIdentifier)
	require.Equal(expected.Block.ParentBlockIdentifier, actual.Block.ParentBlockIdentifier)
	require.Equal(len(expected.Block.Transactions), len(actual.Block.Transactions))
	require.Equal(expected.Block.Transactions[0].TransactionIdentifier, actual.Block.Transactions[0].TransactionIdentifier)
	require.Equal(expected.Block.Transactions[0].RelatedTransactions, actual.Block.Transactions[0].RelatedTransactions)
	require.Equal(expected.Block.Transactions[0].Operations, actual.Block.Transactions[0].Operations)
	expectedTx2SDKMetadata, _ := rosetta.ToSDKMetadata(expected.Block.Transactions[0].Metadata)
	actualTx2SDKMetadata, _ := rosetta.ToSDKMetadata(actual.Block.Transactions[0].Metadata)
	require.Equal(expectedTx2SDKMetadata, actualTx2SDKMetadata)

	require.Equal(expected.Block.Transactions[1].TransactionIdentifier, actual.Block.Transactions[1].TransactionIdentifier)
	require.Equal(expected.Block.Transactions[1].RelatedTransactions, actual.Block.Transactions[1].RelatedTransactions)
	require.Equal(expected.Block.Transactions[1].Operations, actual.Block.Transactions[1].Operations)
}
