package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

type ethereumRosettaParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  Parser
}

func TestEthereumRosettaParserTestSuite(t *testing.T) {
	suite.Run(t, new(ethereumRosettaParserTestSuite))
}

func (s *ethereumRosettaParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *ethereumRosettaParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *ethereumRosettaParserTestSuite) TestEthereumRosettaParser() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   ethereumMetadata,
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt},
				TransactionTraces:   [][]byte{fixtureTraces},
			},
		},
	}

	secondTransactionMetadata, _ := rosetta.FromSDKMetadata(map[string]interface{}{
		"gas_limit": "0x15f90",
		"gas_price": "0x22cee4f700",
		"receipt": map[string]interface{}{
			"blockHash":         "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
			"blockNumber":       "0xacc290",
			"from":              "0x98265d92b016df8758f361fb8d2f9a813c82494a",
			"gasUsed":           "0x1b889",
			"contractAddress":   interface{}(nil),
			"cumulativeGasUsed": "0xbca58c",
			"logs": []interface{}{
				map[string]interface{}{
					"address":     "0xe5caef4af8780e59df925470b050fb23c43ca68c",
					"blockHash":   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
					"blockNumber": "0xacc290",
					"data":        "0x0000000000000000000000000000000000000000000000000000000715d435c0",
					"logIndex":    "0x119",
					"removed":     false,
					"topics": []interface{}{
						"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
						"0x00000000000000000000000098265d92b016df8758f361fb8d2f9a813c82494a",
						"0x00000000000000000000000092330d8818e8a3b50f027c819fa46031ffba2c8c",
					},
					"transactionHash":  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
					"transactionIndex": "0x0",
				},
				map[string]interface{}{
					"address":     "0xe5caef4af8780e59df925470b050fb23c43ca68c",
					"blockHash":   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
					"blockNumber": "0xacc290",
					"data":        "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000e029ae811464737200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000033b2e3cbfa3d80192847e1a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
					"logIndex":    "0x120",
					"removed":     false,
					"topics": []interface{}{
						"0x29ae811400000000000000000000000000000000000000000000000000000000",
						"0x000000000000000000000000be8e3e3618f7474f8cb1d074a26affef007e98fb",
						"0x6473720000000000000000000000000000000000000000000000000000000000",
						"0x0000000000000000000000000000000000000000033b2e3cbfa3d80192847e1a",
					},
					"transactionHash":  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
					"transactionIndex": "0x0",
				},
				map[string]interface{}{
					"address":     "0xad72c532d9fe5c51292d950dd0a160c76ff3fa30",
					"blockHash":   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
					"blockNumber": "0xacc290",
					"data":        "0x00000000000000000000000000000000000000000000000000000000000000c8",
					"logIndex":    "0x121",
					"removed":     false,
					"topics": []interface{}{
						"0xc1405953cccdad6b442e266c84d66ad671e2534c6584f8e6ef92802f7ad294d5",
						"0x000000000000000000000000be8e3e3618f7474f8cb1d074a26affef007e98fb",
						"0x0000000000000000000000000000000000000000033b2e3cbfa3d80192847e1a",
						"0x0000000000000000000000001fe16de955718cfab7a44605458ab023838c2793",
					},
					"transactionHash":  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
					"transactionIndex": "0x0",
				},
				map[string]interface{}{
					"address":     "0x518ba36f1ca6dfe3bb1b098b8dd0444030e79d9f",
					"blockHash":   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
					"blockNumber": "0xacc290",
					"data":        "0x",
					"logIndex":    "0x122",
					"removed":     false,
					"topics": []interface{}{
						"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
						"0x0000000000000000000000000000000000000000",
						"0x05379b307e6ae02e522fb134fad1254a4e7fbac1",
						"0x0000000000000000000000000000000000000000000000000000000000001950",
					},
					"transactionHash":  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
					"transactionIndex": "0x0",
				},
			},
			"logsBloom":        "0x00200000000000000000000080000000000080000200000000010000000000000000000000000000000000000000000002000000080000000000000000200001000000000000000010000008000000200000000000400000000000000000000000000000000000000000000000000000000000002000040000000010000000000000000000000000004000000000000000002000000000088000004000000000020000000000000000000000000000000400000000000000000000000000800000000002000001000000000000000000000000000000001000000002000020000010200000000000000000000000000000000000000000000000008004000000",
			"status":           "0x1",
			"to":               "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
			"transactionHash":  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
			"transactionIndex": "0x0",
			"type":             "0x0",
		},
		"trace": map[string]interface{}{
			"calls": []interface{}{
				map[string]interface{}{
					"from":    "0x61c86828fd30ca479c51413abc03f0f8dcec2120",
					"gas":     "0x7207e",
					"gasUsed": "0x2fd4",
					"input":   "0x70a0823100000000000000000000000061c86828fd30ca479c51413abc03f0f8dcec2120",
					"output":  "0x00000000000000000000000000000000000000000000000b94174e1c4d4a9288",
					"to":      "0xba630d3ba20502ba07975b15c719beecc8e4ebb0",
					"type":    "CALL",
					"value":   "0x1",
				},
			},
			"from":    "0x4823cc90c145fd6a16ab7668043dbba5ce79cdfc",
			"gas":     "0x10b1c",
			"gasUsed": "0x4c91",
			"input":   "0xa9059cbb00000000000000000000000022852cdfdda5eb9b0e25d6581bdb82a156ac4c400000000000000000000000000000000000000000000000000000000162598040",
			"output":  "0x",
			"time":    "41.567024ms",
			"to":      "0xdac17f958d2ee523a2206206994597c13d831ec7",
			"type":    "CALL",
			"value":   "0x0",
		},
	})

	expected := &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Index: 11322000,
				Hash:  "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
			},
			ParentBlockIdentifier: &rosetta.BlockIdentifier{
				Index: 11321999,
				Hash:  "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
			},
			Timestamp: &timestamp.Timestamp{
				Seconds: 1606234041,
			},
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 0,
							},
							RelatedOperations: nil,
							Type:              "MINER_REWARD",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xd224ca0c819e8e97ba0136b3b95ceff503b79f53",
							},
							Amount: &rosetta.Amount{
								Value:    "2000000000000000000",
								Currency: &ethereumRosettaCurrency,
							},
							CoinChange: nil,
							Metadata:   nil,
						},
					},
					Metadata: nil,
				},
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
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
								Address: "0x4823cc90c145fd6a16ab7668043dbba5ce79cdfc",
							},
							Amount: &rosetta.Amount{
								Value:    "-16860161500000000",
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
								Address: "0xd224ca0c819e8e97ba0136b3b95ceff503b79f53",
							},
							Amount: &rosetta.Amount{
								Value:    "16860161500000000",
								Currency: &ethereumRosettaCurrency,
							},
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 2,
							},
							RelatedOperations: nil,
							Type:              "CALL",
							Status:            "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0x61c86828fd30ca479c51413abc03f0f8dcec2120",
							},
							Amount: &rosetta.Amount{
								Value:    "-1",
								Currency: &ethereumRosettaCurrency,
							},
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
							Type:   "CALL",
							Status: "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address: "0xba630d3ba20502ba07975b15c719beecc8e4ebb0",
							},
							Amount: &rosetta.Amount{
								Value:    "1",
								Currency: &ethereumRosettaCurrency,
							},
						},
					},
					Metadata: secondTransactionMetadata,
				},
			},
			Metadata: nil,
		},
	}

	actual, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	s.testapp.Logger().Debug("comparing blocks", zap.Reflect("expected", expected), zap.Reflect("actual", actual))

	require.Equal(expected.Block.BlockIdentifier, actual.Block.BlockIdentifier)
	require.Equal(expected.Block.ParentBlockIdentifier, actual.Block.ParentBlockIdentifier)
	require.Equal(len(expected.Block.Transactions), len(actual.Block.Transactions))
	require.Equal(expected.Block.Transactions[0], actual.Block.Transactions[0])
	require.Equal(expected.Block.Transactions[1].TransactionIdentifier, actual.Block.Transactions[1].TransactionIdentifier)
	require.Equal(expected.Block.Transactions[1].RelatedTransactions, actual.Block.Transactions[1].RelatedTransactions)
	require.Equal(expected.Block.Transactions[1].Operations, actual.Block.Transactions[1].Operations)
	expectedTx2SDKMetadata, _ := rosetta.ToSDKMetadata(expected.Block.Transactions[1].Metadata)
	actualTx2SDKMetadata, _ := rosetta.ToSDKMetadata(actual.Block.Transactions[1].Metadata)
	require.Equal(expectedTx2SDKMetadata, actualTx2SDKMetadata)
}

func (s *ethereumRosettaParserTestSuite) TestRosettaEthereumResponseParity() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_468179.json")
	require.NoError(err)
	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	expectedRosettaBlock, err := testutil.LoadRosettablock("parser/ethereum/rosetta_ethereum_block_response_468179.json")
	require.NoError(err)

	expectedRosettaProtoTxns, err := rosetta.FromSDKTransactions(expectedRosettaBlock.Block.Transactions)
	require.NoError(err)
	require.Equal(len(expectedRosettaProtoTxns), len(actualRosettaBlock.Block.Transactions))
	for i := 0; i < len(expectedRosettaProtoTxns); i++ {
		err := s.normalizeTransactions(expectedRosettaProtoTxns[i], actualRosettaBlock.Block.Transactions[i])
		require.Nil(err)
		require.Equal(expectedRosettaProtoTxns[i], actualRosettaBlock.Block.Transactions[i])
	}
}

func (s *ethereumRosettaParserTestSuite) TestBlockRewardTransaction_Frontier() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_468179.json")
	require.NoError(err)
	rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	rewardTxn := rosettaBlock.Block.Transactions[0]
	require.Equal(1, len(rewardTxn.Operations))
	// miner reward op
	require.Equal("5000000000000000000", rewardTxn.Operations[0].Amount.Value)
}

func (s *ethereumRosettaParserTestSuite) TestBlockRewardTransaction_Byzantium() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_4370006.json")
	require.NoError(err)
	rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	rewardTxn := rosettaBlock.Block.Transactions[0]
	require.Equal(2, len(rewardTxn.Operations))
	// miner reward op
	require.Equal("3093750000000000000", rewardTxn.Operations[0].Amount.Value)
	// uncle reward op
	require.Equal("2625000000000000000", rewardTxn.Operations[1].Amount.Value)
}

func (s *ethereumRosettaParserTestSuite) TestBlockRewardTransaction_Constantinople() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_7280016.json")
	require.NoError(err)
	rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	rewardTxn := rosettaBlock.Block.Transactions[0]
	require.Equal(2, len(rewardTxn.Operations))
	// miner reward op
	require.Equal("2062500000000000000", rewardTxn.Operations[0].Amount.Value)
	// uncle reward op
	require.Equal("1750000000000000000", rewardTxn.Operations[1].Amount.Value)
}

func (s *ethereumRosettaParserTestSuite) TestBlockRewardTransaction_Paris() {
	require := testutil.Require(s.T())
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   ethereumMetadata,
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeaderAfterMerge,
				TransactionReceipts: [][]byte{fixtureReceiptAfterMerge},
				TransactionTraces:   [][]byte{fixtureTracesAfterMerge},
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

func (s *ethereumRosettaParserTestSuite) TestTraceOps_FailureZeroValue() {
	// flattenedTraces for failedTxnHash
	//
	// "flattenedTraces": [
	//  {
	//    "error": "invalid jump destination",
	//    "type": "CALL",
	//    "value": "0",
	//    "traceType": "CALL",
	//    "callType": "CALL",
	//  }
	// ]
	require := testutil.Require(s.T())
	failedTxnHash := "0xa0a5e34b9b19b398c5a073513ecb461899ceb45246f51e6d470ae0cf23b39075"
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_4404763.json")
	require.NoError(err)
	rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(96, len(rosettaBlock.Block.Transactions))
	var failedTxn *rosetta.Transaction
	for _, txn := range rosettaBlock.Block.Transactions {
		if txn.TransactionIdentifier.Hash == failedTxnHash {
			failedTxn = txn
		}
	}
	// assert that only fee operations were included
	require.Equal(2, len(failedTxn.Operations))
	for _, op := range failedTxn.Operations {
		require.Equal("FEE", op.Type)
	}
}
func (s *ethereumRosettaParserTestSuite) TestTraceOps_FailureNonZeroValue() {
	// flattenedTraces for failedTxnHash
	//
	// "flattenedTraces": [
	//  {
	//    "error": "execution reverted",
	//    "type": "CALL",
	//    "value": "391575292818086729",
	//    "traceType": "CALL",
	//    "callType": "CALL",
	//  }
	// ]
	require := testutil.Require(s.T())
	failedTxnHash := "0x4bea168ff5ff8bf2434e1d0cd679a4e758476ab1c0ab1e2c3cc9b4914ef7a49f"
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_11098450.json")
	require.NoError(err)
	rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(186, len(rosettaBlock.Block.Transactions))
	var failedTxn *rosetta.Transaction
	for _, txn := range rosettaBlock.Block.Transactions {
		if txn.TransactionIdentifier.Hash == failedTxnHash {
			failedTxn = txn
		}
	}
	require.Equal(4, len(failedTxn.Operations))
	require.Equal(opTypeFee, failedTxn.Operations[0].Type)
	require.Equal(opTypeFee, failedTxn.Operations[1].Type)
	require.Equal(traceTypeCall, failedTxn.Operations[2].Type)
	require.Equal("-391575292818086729", failedTxn.Operations[2].Amount.Value)
	require.Equal(opStatusFailure, failedTxn.Operations[2].Status)
	require.Equal("0x078ad2aa3b4527e4996d087906b2a3da51bba122", failedTxn.Operations[2].Account.Address)
	require.Equal(traceTypeCall, failedTxn.Operations[3].Type)
	require.Equal(opStatusFailure, failedTxn.Operations[3].Status)
	require.Equal("391575292818086729", failedTxn.Operations[3].Amount.Value)
	require.Equal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", failedTxn.Operations[3].Account.Address)
}

func (s *ethereumRosettaParserTestSuite) normalizeTransactions(expected *rosetta.Transaction, actual *rosetta.Transaction) error {
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
			// ChainStorage receipts include extra fields that need to be removed
			delete(receipt, "to")
			delete(receipt, "from")
			delete(receipt, "type")
			// ChainStorage's contractAddress can be nil vs. zero-address
			if receipt["contractAddress"] == nil {
				receipt["contractAddress"] = "0x0000000000000000000000000000000000000000"
			}
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
		expectedMetadata, err := rosetta.ToSDKMetadata(expected.Metadata)
		if err != nil {
			return err
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

// Block with EIP-1559 base fee & txs. This block taken from mainnet:
//   https://etherscan.io/block/0x68985b6b06bb5c6012393145729babb983fc16c50ec5207972ddda02de02f7e2
// This block has 7 transactions, all EIP-1559 type except the last.
func (s *ethereumRosettaParserTestSuite) TestBlock_13998626() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_13998626.json")
	require.NoError(err)
	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	expectedRosettaBlock, err := testutil.LoadRosettablock("parser/ethereum/rosetta_ethereum_block_response_13998626.json")
	require.NoError(err)

	expectedRosettaProtoTxns, err := rosetta.FromSDKTransactions(expectedRosettaBlock.Block.Transactions)
	require.NoError(err)
	require.Equal(len(expectedRosettaProtoTxns), len(actualRosettaBlock.Block.Transactions))
	for i := 0; i < len(expectedRosettaProtoTxns); i++ {
		err := s.normalizeTransactions(expectedRosettaProtoTxns[i], actualRosettaBlock.Block.Transactions[i])
		require.Nil(err)
		require.Equal(expectedRosettaProtoTxns[i].Operations, actualRosettaBlock.Block.Transactions[i].Operations)
		require.Equal(expectedRosettaProtoTxns[i].TransactionIdentifier, actualRosettaBlock.Block.Transactions[i].TransactionIdentifier)
	}
}

// Block with non-zero self-destruct
func (s *ethereumRosettaParserTestSuite) TestBlock_SelfDestruct() {
	require := testutil.Require(s.T())
	block, err := testutil.LoadRawBlock("parser/ethereum/raw_block_15753009.json")
	require.NoError(err)
	actualRosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(actualRosettaBlock)
	require.Equal(211, len(actualRosettaBlock.Block.Transactions))

	txn := actualRosettaBlock.Block.Transactions[74]
	lastOp := txn.Operations[len(txn.Operations)-1]
	require.Equal(opTypeDestruct, lastOp.Type)
}
