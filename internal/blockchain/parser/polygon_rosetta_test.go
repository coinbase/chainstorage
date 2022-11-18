package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

const (
	polygonTag          uint32 = 1
	polygonHeight       uint64 = 0x2092dbd
	polygonParentHeight uint64 = 0x2092dbc
	polygonHash                = "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3"
	polygonParentHash          = "0x3ce76c407965ae36104d49e146fdbfc2306eea3bffb1f1a0f9ea14e8d4d53f3d"
)

var (
	fixturePolygonHeader = []byte(`{
		"baseFeePerGas": "0xf",
    "difficulty": "0x12",
    "extraData": "0xd682021083626f7288676f312e31382e33856c696e7578000000000000000000944d005cf8f5d2da03e13c4e0dfbd720caa3a4d781e17edb5ac38a5efdb5827a59a50aa77f9cac2b3a638013fd0a9ea68d0d2134822aa299907cd5c3df86f2c701",
    "gasLimit": "0x1c9c380",
    "gasUsed": "0x5575db",
    "hash": "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
    "logsBloom": "0x0928248408800a908c0820700066140303490102085813000b020540301e28040340300a000448062070781100c4540428018000494724980c004041102cef79220c24c33022480080800019303e24e0141385608d84810175411434800d23476200094d222320008460ca818422896a002694040800c281b10000b0b8042200201500048c8250890490a04060041056000005b3520500040109280201084051a6d00840d13804430020403ea098000050440258000609048820d020051021468e03490e00406420c7118528cb0a0540305024c0000884080818a306c451e00080186898034080cd0000211001006019c11092214580804187026840a8110d8b",
    "miner": "0x0000000000000000000000000000000000000000",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "nonce": "0x0000000000000000",
    "number": "0x2092dbd",
    "parentHash": "0x3ce76c407965ae36104d49e146fdbfc2306eea3bffb1f1a0f9ea14e8d4d53f3d",
    "receiptsRoot": "0xf1d048cb00fdda23a550ad1aa55fc56c01865fbd86b07609961b51095278b58b",
    "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
    "size": "0x6ee1",
    "stateRoot": "0x4b45448f1eae50ec34af5e7f0f7cba20ffe5710c61433eb4af84e7aeb907de49",
    "timestamp": "0x6343ad59",
    "totalDifficulty": "0x1f110693",
		"transactions": [
			{
				"blockHash": "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
				"blockNumber": "0x2092dbd",
				"from": "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
				"gas": "0xfce7",
				"gasPrice": "0xba43b7400",
				"hash": "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
				"input": "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
				"nonce": "0x1",
				"r": "0x95dbd815d93318ac09ba300efdc5a98d60a1e7a6274f027dffcc3873332df9f1",
				"s": "0x6f723c2f4d8e2017e8b6935d848f187ac2b8ef0ceccd5e42cc37e46c77dab1d3",
				"to": "0x3376c61c450359d402f07909bda979a4c0e6c32f",
				"transactionIndex": "0x0",
				"type": "0x0",
				"v": "0x135",
				"value": "0x0"
			}
		],
		"transactionsRoot": "0x31ea11c6796e140a3eef51e2ca9e6fcf9ba448786b863c31549dd4325eb8233c"
	}`)

	fixturePolygonReceipt = []byte(`{
    "blockHash": "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
    "blockNumber": "0x2092dbd",
    "contractAddress": null,
    "cumulativeGasUsed": "0xd15f",
    "effectiveGasPrice": "0xba43b7400",
    "from": "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
    "gasUsed": "0xd15f",
    "logs": [
			{
				"address": "0x3376c61c450359d402f07909bda979a4c0e6c32f",
				"blockHash": "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
				"blockNumber": "0x2092dbd",
				"data": "0x0000000000000000000000000000000000000000000000000000000000000001",
				"logIndex": "0x0",
				"removed": false,
				"topics": [
						"0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x00000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e97"
				],
				"transactionHash": "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
				"transactionIndex": "0x0"
			},
			{
				"address": "0x0000000000000000000000000000000000001010",
				"blockHash": "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
				"blockNumber": "0x2092dbd",
				"data": "0x000000000000000000000000000000000000000000000000000985667bb7c76f0000000000000000000000000000000000000000000000000bac25454dd1b400000000000000000000000000000000000000000000000329622b11d5bbd78baa0000000000000000000000000000000000000000000000000ba29fded219ec910000000000000000000000000000000000000000000003296234973c378f5319",
				"logIndex": "0x1",
				"removed": false,
				"topics": [
						"0x4dfe1bbbcf077ddc3e01291eea2d5c70c2b422b415d95645b9adcfd678cb1d63",
						"0x0000000000000000000000000000000000000000000000000000000000001010",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x000000000000000000000000160cdef60e786295728a6ea334c091238e474e01"
				],
				"transactionHash": "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
				"transactionIndex": "0x0"
			}
    ],
    "logsBloom": "0x00000000000000000000000000000001000000000040000000000040300000000000000200000000000000000000000000008000000000000000000000000001000000020000000000000000000000800000000000000000000100000000000002000000000000000000000000000000000000000000000080000000000000000000000400000000000000000000000000000000020000000001000000000000200000000000000000000000000000000000000000000000000000000000004000000000000000000001000000000000000000000000000000100000000000000000200800000000000000000000200000000000000000000000000000100000",
    "status": "0x1",
    "to": "0x3376c61c450359d402f07909bda979a4c0e6c32f",
    "transactionHash": "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
    "transactionIndex": "0x0",
    "type": "0x0"
	}`)

	fixturePolygonTraces = []byte(`{
		"type": "CALL",
    "from": "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
    "to": "0x3376c61c450359d402f07909bda979a4c0e6c32f",
    "value": "0x0",
    "gas": "0xa8af",
    "gasUsed": "0x7d27",
    "input": "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
    "output": "0x",
    "calls": [
        {
            "type": "DELEGATECALL",
            "from": "0x3376c61c450359d402f07909bda979a4c0e6c32f",
            "to": "0xe2dd4e4b32d785dad138181d0b045497f0f1bbb0",
            "gas": "0x8a0e",
            "gasUsed": "0x6090",
            "input": "0xa22cb46500000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e970000000000000000000000000000000000000000000000000000000000000001",
            "output": "0x"
        }
    ]
	}`)
)

type polygonRosettaParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  Parser
}

func TestPolygonRosettaParserTestSuite(t *testing.T) {
	suite.Run(t, new(polygonRosettaParserTestSuite))
}

func (s *polygonRosettaParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
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

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_POLYGON,
		Network:    common.Network_NETWORK_POLYGON_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          polygonTag,
			Hash:         polygonHash,
			Height:       polygonHeight,
			ParentHash:   polygonParentHash,
			ParentHeight: polygonParentHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixturePolygonHeader,
				TransactionReceipts: [][]byte{fixturePolygonReceipt},
				TransactionTraces:   [][]byte{fixturePolygonTraces},
				ExtraData: &api.EthereumBlobdata_Polygon{
					Polygon: &api.PolygonBlobdata{
						Author: []byte(`"0x160cdef60e786295728a6ea334c091238e474e01"`),
					},
				},
			},
		},
	}

	metadata, _ := rosetta.FromSDKMetadata(map[string]interface{}{
		"gas_limit": "0xfce7",
		"gas_price": "0xba43b7400",
		"receipt": map[string]interface{}{
			"blockHash":         "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
			"blockNumber":       "0x2092dbd",
			"cumulativeGasUsed": "0xd15f",
			"effectiveGasPrice": "0xba43b7400",
			"from":              "0x5f23c6e5db2d14f70e7b97393ee4dcc96855a815",
			"gasUsed":           "0xd15f",
			"contractAddress":   interface{}(nil),
			"logs": []interface{}{
				map[string]interface{}{
					"address":     "0x3376c61c450359d402f07909bda979a4c0e6c32f",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x0000000000000000000000000000000000000000000000000000000000000001",
					"logIndex":    "0x0",
					"removed":     false,
					"topics": []interface{}{
						"0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31",
						"0x0000000000000000000000005f23c6e5db2d14f70e7b97393ee4dcc96855a815",
						"0x00000000000000000000000021b1493fcf71b200d5f2ede6ea01b9a57ee00e97",
					},
					"transactionHash":  "0xdd0075ea30920528e234304494961d7f264461bc10e68f9d8f187f9dd23b6b17",
					"transactionIndex": "0x0",
				},
				map[string]interface{}{
					"address":     "0x0000000000000000000000000000000000001010",
					"blockHash":   "0xff1380994a9aa720150c0ed5b44e3b6c1d61c957c81a463bc1d8768d9aec87d3",
					"blockNumber": "0x2092dbd",
					"data":        "0x000000000000000000000000000000000000000000000000000985667bb7c76f0000000000000000000000000000000000000000000000000bac25454dd1b400000000000000000000000000000000000000000000000329622b11d5bbd78baa0000000000000000000000000000000000000000000000000ba29fded219ec910000000000000000000000000000000000000000000003296234973c378f5319",
					"logIndex":    "0x1",
					"removed":     false,
					"topics": []interface{}{
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
		"trace": map[string]interface{}{
			"calls": []interface{}{
				map[string]interface{}{
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
								Value:    "-2679950000000000",
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
								Value:    "2679950000000000",
								Currency: &polygonRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
					},
					Metadata: metadata,
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
}

func (s *polygonRosettaParserTestSuite) TestRosettaPolygonResponseParity() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/polygon/raw_block_34155965.json")
	require.NoError(err)
	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	expectedRosettaBlock, err := testutil.LoadRosettablock("parser/polygon/rosetta_polygon_block_response_34155965.json")
	require.NoError(err)

	expectedRosettaProtoTxns, err := rosetta.FromSDKTransactions(expectedRosettaBlock.Block.Transactions)
	require.NoError(err)

	require.Equal(len(expectedRosettaProtoTxns), len(actualRosettaBlock.Block.Transactions))
	for i := 0; i < len(expectedRosettaProtoTxns); i++ {
		err := s.normalizeTransactions(expectedRosettaProtoTxns[i], actualRosettaBlock.Block.Transactions[i])
		require.Nil(err)
		require.Equal(expectedRosettaProtoTxns[i].TransactionIdentifier, actualRosettaBlock.Block.Transactions[i].TransactionIdentifier)
		require.Equal(expectedRosettaProtoTxns[i].Operations, actualRosettaBlock.Block.Transactions[i].Operations)
		expectedTx2SDKMetadata, _ := rosetta.ToSDKMetadata(expectedRosettaProtoTxns[i].Metadata)
		actualTx2SDKMetadata, _ := rosetta.ToSDKMetadata(actualRosettaBlock.Block.Transactions[i].Metadata)
		require.Equal(expectedTx2SDKMetadata, actualTx2SDKMetadata)
	}
}

func (s *polygonRosettaParserTestSuite) normalizeTransactions(expected *rosetta.Transaction, actual *rosetta.Transaction) error {
	// lowercase all rosetta_ethereum addresses to match ChainStorage
	for i := range expected.Operations {
		expected.Operations[i].Account.Address = strings.ToLower(expected.Operations[i].Account.Address)
	}

	if len(actual.Metadata) != 0 {
		actualMetadata, err := rosetta.ToSDKMetadata(actual.Metadata)
		if err != nil {
			return err
		}
		rawReceipt := actualMetadata["receipt"]
		if rawReceipt != nil {
			receipt := rawReceipt.(map[string]interface{})
			// ChainStorage is missing the status field for blocks pre-Byzantium fork, while rosetta includes it
			if _, ok := receipt["status"]; !ok {
				receipt["status"] = "0x0"
			}
		}

		rawTrace := actualMetadata["trace"]
		if rawTrace != nil {
			trace := rawTrace.(map[string]interface{})
			// remove timing information since this is inconsistent across nodes
			delete(trace, "time")
		}

		modifiedActualMetadata, err := rosetta.FromSDKMetadata(actualMetadata)
		if err != nil {
			return err
		}

		actual.Metadata = modifiedActualMetadata
	}

	// remove time to be consistent with the chainstorage-modified transaction trace
	if len(expected.Metadata) != 0 {
		expectedMetadata := map[string]interface{}{}
		for key, meta := range expected.Metadata {
			dst, err := types.UnmarshalToInterface(meta)
			if err != nil {
				return err
			}
			expectedMetadata[key] = dst.(map[string]interface{})["value"]
		}

		rawTrace := expectedMetadata["trace"]
		if rawTrace != nil {
			trace := rawTrace.(map[string]interface{})
			// remove timing information since this is inconsistent across nodes
			delete(trace, "time")
		}

		modifiedExpectedMetadata, err := rosetta.FromSDKMetadata(expectedMetadata)
		if err != nil {
			return err
		}

		expected.Metadata = modifiedExpectedMetadata
	}

	return nil
}
