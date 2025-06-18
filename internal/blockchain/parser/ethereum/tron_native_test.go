package ethereum

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type tronParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestTronParserTestSuite(t *testing.T) {
	// t.Skip()
	suite.Run(t, new(tronParserTestSuite))
}

func (s *tronParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_TRON, common.Network_NETWORK_TRON_MAINNET),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *tronParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *tronParserTestSuite) TestParseTronBlock() {
	require := testutil.Require(s.T())

	fixtureHeader := fixtures.MustReadFile("parser/tron/raw_block_header.json")

	rawReceipts, err := s.fixtureParsingHelper("parser/tron/raw_block_tx_receipt.json")
	require.NoError(err)

	rawTraces, err := s.fixtureParsingHelper("parser/tron/raw_block_trace_tx_info.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_TRON,
		Network:    common.Network_NETWORK_TRON_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        2,
			Hash:       "0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			ParentHash: "0x0000000004034f5b43c5934257b3d1f1a313bba4af0a4dd2f778fda9e641b615",
			Height:     0x4034f5c,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: rawReceipts,
				TransactionTraces:   rawTraces,
			},
		},
	}

	expectedHeader := &api.EthereumHeader{
		Hash:       "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
		ParentHash: "0000000004034f5b43c5934257b3d1f1a313bba4af0a4dd2f778fda9e641b615",
		Number:     0x4034F5C,
		Timestamp:  &timestamppb.Timestamp{Seconds: 1732627338},
		Transactions: []string{
			"d581afa9158fbed69fb10d6a2245ad45d912a3da03ff24d59f3d2f6df6fd9529",
			"e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
		},
		Nonce:            "0x0000000000000000",
		Sha3Uncles:       "0x0000000000000000000000000000000000000000000000000000000000000000",
		LogsBloom:        "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
		TransactionsRoot: "d270690faa58558c2b03ae600334f71f9d5a0ad42d7313852fb3742e8576eec9",
		StateRoot:        "0x",
		ReceiptsRoot:     "0x0000000000000000000000000000000000000000000000000000000000000000",
		Miner:            "TNeEwWHXLLUgEtfzTnYN8wtVenGxuMzZCE",
		TotalDifficulty:  "0",
		ExtraData:        "0x",
		Size:             0x1a366,
		GasLimit:         0x2b3b43dc6,
		GasUsed:          0xb1006d,
		MixHash:          "0x0000000000000000000000000000000000000000000000000000000000000000",
		OptionalBaseFeePerGas: &api.EthereumHeader_BaseFeePerGas{
			BaseFeePerGas: uint64(0),
		},
	}
	expectedFlattenedTraces := []*api.EthereumTransactionFlattenedTrace{
		{
			Type:             "CALL",
			From:             "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			To:               "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			Value:            "200",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "499bdbdfaae021dd510c70b433bc48d88d8ca6e0b7aee13ce6d726114e365aaf",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{
					CallValue: 100,
				},
				{
					CallValue: 100,
				},
			},
		},
		{
			Type:             "CALL",
			From:             "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			To:               "TXA2WjFc5f86deJcZZCdbdpkpUTKTA3VDM",
			Value:            "1000",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "997225b56440a9bd172f05f44a663830b72093a12502551cda99b0bc7c60cbc1",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{
					TokenId:   "1004777",
					CallValue: 1000000000000000,
				},
				{
					CallValue: 1000,
				},
			},
		},
		{
			Type:             "CALL",
			From:             "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			To:               "TXA2WjFc5f86deJcZZCdbdpkpUTKTA3VDM",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "7ac8dd16dede5c512330f5033c8fd6f5390d742aa51b805f805098109eb54fe9",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{
					TokenId:   "1004777",
					CallValue: 1000,
				},
				{
					TokenId:   "1004777",
					CallValue: 100,
				},
			},
		},
		{
			Type:             "CALL",
			From:             "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			To:               "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5",
			Value:            "100000",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "cf6f699d9bdae8aa25fae310a06bb60a29a7812548cf3c1d83c737fd1a22c0ee",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{
					TokenId:   "1004777",
					CallValue: 100,
				},
				{
					CallValue: 100000,
				},
			},
		},
		{
			Type:             "CALL",
			From:             "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5",
			To:               "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "95787b9a6558c7b6b624d0c1bece9723a7f4c3d414010b6ac105ae5f5aebffbc",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{},
			},
		},
		{
			Type:             "CALL",
			From:             "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5",
			To:               "TGzjkw66CtL49eKiQFDwJDuXG9HSQd69p2",
			Value:            "822996311610",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "14526162e31d969ef0dca9b902d51ecc0ffab87dc936dce62022f368119043af",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{
					CallValue: 822994311610,
				},
				{
					CallValue: 2000000,
				},
			},
		},
		{
			Type:             "CALL",
			From:             "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			To:               "TXA2WjFc5f86deJcZZCdbdpkpUTKTA3VDM",
			Value:            "0",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "8e088220a26ca8d794786e78096e71259cf8744cccdc4f07a8129aa8ee29bb98",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{},
			},
		},
		{
			Type:             "CALL",
			From:             "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			To:               "TNXC2YCSxhdxsVqhqu3gYZYme6n4i6T1C1",
			Value:            "1424255258",
			TraceType:        "CALL",
			CallType:         "CALL",
			TraceId:          "83b1d41ba953aab4da6e474147f647599ea53bb3213306897127b57e85ddd1ca",
			Status:           1,
			BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
			BlockNumber:      0x4034F5C,
			TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			TransactionIndex: 1,
			TronCallValueInfo: []*api.TronCallValueInfo{
				{
					CallValue: 1424255258,
				},
			},
		},
	}

	expectedTransactions := []*api.EthereumTransaction{
		{
			Hash:  "d581afa9158fbed69fb10d6a2245ad45d912a3da03ff24d59f3d2f6df6fd9529",
			From:  "TDQFomPihdhP8Jzr2LMpdcXgg9qxKfZZmD",
			To:    "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
			Index: 0,
			Receipt: &api.EthereumTransactionReceipt{
				TransactionHash:   "d581afa9158fbed69fb10d6a2245ad45d912a3da03ff24d59f3d2f6df6fd9529",
				BlockHash:         "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
				BlockNumber:       67325788,
				From:              "TDQFomPihdhP8Jzr2LMpdcXgg9qxKfZZmD",
				To:                "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
				CumulativeGasUsed: 130285,
				GasUsed:           130285,
				LogsBloom:         "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
				EffectiveGasPrice: 210,
				OptionalStatus:    &api.EthereumTransactionReceipt_Status{Status: 1},
				TransactionIndex:  0,
				OptionalNetUsage: &api.EthereumTransactionReceipt_NetUsage{
					NetUsage: 345,
				},
				OptionalEnergyUsage: &api.EthereumTransactionReceipt_EnergyUsage{
					EnergyUsage: 130285,
				},
				OptionalEnergyUsageTotal: &api.EthereumTransactionReceipt_EnergyUsageTotal{
					EnergyUsageTotal: 130285,
				},
				OptionalEnergyPenaltyTotal: &api.EthereumTransactionReceipt_EnergyPenaltyTotal{
					EnergyPenaltyTotal: 100635,
				},
				Logs: []*api.EthereumEventLog{
					{
						TransactionHash:  "d581afa9158fbed69fb10d6a2245ad45d912a3da03ff24d59f3d2f6df6fd9529",
						BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
						BlockNumber:      67325788,
						Address:          "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
						Data:             "0x0000000000000000000000000000000000000000000000000000000000027165",
						TransactionIndex: 0,
						LogIndex:         0,
						Topics: []string{
							"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
							"0x00000000000000000000000025a51e3e65287539b8d4eb559cbca4488a08bb00",
							"0x0000000000000000000000009dc5da2b3c502661c8448ba88bacf7f0b22272ad",
						},
					},
				},
			},
		},
		{
			Hash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
			From:  "TNXC2YCSxhdxsVqhqu3gYZYme6n4i6T1C1",
			To:    "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
			Index: 69,
			Receipt: &api.EthereumTransactionReceipt{
				TransactionHash:   "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
				BlockHash:         "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
				BlockNumber:       67325788,
				From:              "TNXC2YCSxhdxsVqhqu3gYZYme6n4i6T1C1",
				To:                "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
				CumulativeGasUsed: 1432695,
				GasUsed:           74135,
				LogsBloom:         "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
				EffectiveGasPrice: 210,
				OptionalStatus:    &api.EthereumTransactionReceipt_Status{Status: 1},
				TransactionIndex:  69,
				OptionalFee: &api.EthereumTransactionReceipt_Fee{
					Fee: 379,
				},
				OptionalNetFee: &api.EthereumTransactionReceipt_NetFee{
					NetFee: 379,
				},
				OptionalEnergyUsage: &api.EthereumTransactionReceipt_EnergyUsage{
					EnergyUsage: 68976,
				},
				OptionalOriginEnergyUsage: &api.EthereumTransactionReceipt_OriginEnergyUsage{
					OriginEnergyUsage: 5159,
				},
				OptionalEnergyUsageTotal: &api.EthereumTransactionReceipt_EnergyUsageTotal{
					EnergyUsageTotal: 74135,
				},
				Logs: []*api.EthereumEventLog{
					{
						LogIndex:         16,
						TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
						TransactionIndex: 69,
						BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
						BlockNumber:      67325788,
						Address:          "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
						Data:             "0x00000000000000000000000000000000000000000000000000000001f9873bc7000000000000000000000000000000000000000000000000093732ae413feb69000000000000000000000000000000000000000000000000093732b42dd59ebe0000000000000000000000000000000000000000000000000000801f33d9f651000000000000000000000000000000000000000000000000000000000036b158",
						Topics: []string{
							"0xda6e3523d5765dedff9534b488c7e508318178571c144293451989755e9379e7",
							"0x0000000000000000000000000000000000000000000000000000000000000001",
						},
					},
					{
						LogIndex:         17,
						TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
						TransactionIndex: 69,
						BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
						BlockNumber:      67325788,
						Address:          "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
						Data:             "0x000000000000000000000000000000000000000000000000093732a856669e8f000000000000000000000000000000000000000000000000093732b42dd59ebe000000000000000000000000000000000000000000000000000000bf9e4899ba000000000000000000000000000000000000000000000000000000000000a3810000000000000000000000000000000000000000000000000000000000000000",
						Topics: []string{
							"0x74fed619850adf4ba83cfb92b9566b424e3de6de4d9a7adc3b1909ea58421a55",
							"0x00000000000000000000000089ae01b878dffc8088222adf1fb08ebadfeea53a",
							"0x0000000000000000000000004d12f87c18a914dddbc2b27f378ad126a79b76b6",
							"0x0000000000000000000000000000000000000000000000000000000000000001",
						},
					},
					{
						LogIndex:         18,
						TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
						TransactionIndex: 69,
						BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
						BlockNumber:      67325788,
						// Address:          "0xc60a6f5c81431c97ed01b61698b6853557f3afd4",
						Address: "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
						Data:    "0x000000000000000000000000000000000000000000000000000000bf9e4899ba",
						Topics: []string{
							"0xf2def54ec5eba61fd8f18d019c7beaf6a47df317fb798b3263ad69ec227c9261",
							"0x00000000000000000000000089ae01b878dffc8088222adf1fb08ebadfeea53a",
							"0x0000000000000000000000004d12f87c18a914dddbc2b27f378ad126a79b76b6",
							"0x0000000000000000000000000000000000000000000000000000000000000001",
						},
					},
					{
						LogIndex:         19,
						TransactionHash:  "e14935e6144007163609bb49292897ba81bf7ee93bf28ba4cc5ebd0d6b95f4b9",
						TransactionIndex: 69,
						BlockHash:        "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87",
						BlockNumber:      67325788,
						Address:          "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd",
						Data:             "0x000000000000000000000000000000000000000000000000000000bf9e4899ba0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000c032ffd0000000000000000000000000000000000000000000000000000000054e4691a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000093732b42dd59ebe",
						Topics: []string{
							"0xf7e21d5bf17851f93ab7bda7e390841620f59dfbe9d86add32824f33bd40d3f5",
							"0x00000000000000000000000089ae01b878dffc8088222adf1fb08ebadfeea53a",
							"0x0000000000000000000000004d12f87c18a914dddbc2b27f378ad126a79b76b6",
						},
					},
				},
			},
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_TRON, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_TRON_MAINNET, nativeBlock.Network)
	actualBlock := nativeBlock.GetEthereum()
	require.NotNil(actualBlock)
	require.Equal(expectedHeader, actualBlock.Header)

	require.Equal(2, len(actualBlock.Transactions))

	require.Equal(8, len(actualBlock.Transactions[1].FlattenedTraces))

	tx := actualBlock.Transactions[1]
	for i, trace := range tx.FlattenedTraces {
		trace_i := expectedFlattenedTraces[i]
		require.Equal(trace_i.Type, trace.Type)
		require.Equal(trace_i.From, trace.From)
		require.Equal(trace_i.To, trace.To)
		require.Equal(trace_i.Value, trace.Value)
		require.Equal(trace_i.TraceType, trace.TraceType)
		require.Equal(trace_i.CallType, trace.CallType)
		require.Equal(trace_i.TraceId, trace.TraceId)
		require.Equal(trace_i.Status, trace.Status)
		require.Equal(trace_i.BlockHash, trace.BlockHash)
		require.Equal(trace_i.BlockNumber, trace.BlockNumber)
		require.Equal(trace_i.TransactionHash, trace.TransactionHash)
		require.Equal(trace_i.TransactionIndex, trace.TransactionIndex)
	}
	require.Equal(tx.FlattenedTraces, expectedFlattenedTraces)

	for i, tx := range actualBlock.Transactions {
		expected_tx := expectedTransactions[i]
		require.Equal(expected_tx.Hash, tx.Hash)
		require.Equal(expected_tx.From, tx.From)
		require.Equal(expected_tx.To, tx.To)
		require.Equal(expected_tx.Index, tx.Index)

		require.Equal(expected_tx.Receipt.From, tx.Receipt.From)
		require.Equal(expected_tx.Receipt.To, tx.Receipt.To)
		require.Equal(expected_tx.Receipt.TransactionHash, tx.Receipt.TransactionHash)
		require.Equal(expected_tx.Receipt.TransactionIndex, tx.Receipt.TransactionIndex)
		require.Equal(expected_tx.Receipt.BlockHash, tx.Receipt.BlockHash)
		require.Equal(expected_tx.Receipt.BlockNumber, tx.Receipt.BlockNumber)
		require.Equal(expected_tx.Receipt.CumulativeGasUsed, tx.Receipt.CumulativeGasUsed)
		require.Equal(expected_tx.Receipt.GasUsed, tx.Receipt.GasUsed)
		require.Equal(expected_tx.Receipt.LogsBloom, tx.Receipt.LogsBloom)
		require.Equal(expected_tx.Receipt.EffectiveGasPrice, tx.Receipt.EffectiveGasPrice)
		require.Equal(expected_tx.Receipt.Logs, tx.Receipt.Logs)

		if expected_tx.Receipt.GetOptionalFee() != nil {
			require.NotNil(tx.Receipt.GetOptionalFee())
			require.Equal(expected_tx.Receipt.GetFee(), tx.Receipt.GetFee())
		} else {
			require.Nil(tx.Receipt.GetOptionalFee())
		}
		if expected_tx.Receipt.GetOptionalNetFee() != nil {
			require.NotNil(tx.Receipt.GetOptionalNetFee())
			require.Equal(expected_tx.Receipt.GetNetFee(), tx.Receipt.GetNetFee())
		} else {
			require.Nil(tx.Receipt.GetOptionalNetFee())
		}
		if expected_tx.Receipt.GetOptionalNetUsage() != nil {
			require.NotNil(tx.Receipt.GetOptionalNetUsage())
			require.Equal(expected_tx.Receipt.GetNetUsage(), tx.Receipt.GetNetUsage())
		} else {
			require.Nil(tx.Receipt.GetOptionalNetUsage())
		}
		if expected_tx.Receipt.GetOptionalEnergyUsage() != nil {
			require.NotNil(tx.Receipt.GetOptionalEnergyUsage())
			require.Equal(expected_tx.Receipt.GetEnergyUsage(), tx.Receipt.GetEnergyUsage())
		} else {
			require.Nil(tx.Receipt.GetOptionalEnergyUsage())
		}
		if expected_tx.Receipt.GetOptionalEnergyUsageTotal() != nil {
			require.NotNil(tx.Receipt.GetOptionalEnergyUsageTotal())
			require.Equal(expected_tx.Receipt.GetEnergyUsageTotal(), tx.Receipt.GetEnergyUsageTotal())
		} else {
			require.Nil(tx.Receipt.GetOptionalEnergyUsageTotal())
		}
		if expected_tx.Receipt.GetOptionalEnergyPenaltyTotal() != nil {
			require.NotNil(tx.Receipt.GetOptionalEnergyPenaltyTotal())
			require.Equal(expected_tx.Receipt.GetEnergyPenaltyTotal(), tx.Receipt.GetEnergyPenaltyTotal())
		} else {
			require.Nil(tx.Receipt.GetOptionalEnergyPenaltyTotal())
		}
		if expected_tx.Receipt.GetOptionalOriginEnergyUsage() != nil {
			require.NotNil(tx.Receipt.GetOptionalOriginEnergyUsage())
			require.Equal(expected_tx.Receipt.GetOriginEnergyUsage(), tx.Receipt.GetOriginEnergyUsage())
		} else {
			require.Nil(tx.Receipt.GetOptionalOriginEnergyUsage())
		}
		if expected_tx.Receipt.GetOptionalNetUsage() != nil {
			require.NotNil(tx.Receipt.GetOptionalNetUsage())
			require.Equal(expected_tx.Receipt.GetNetUsage(), tx.Receipt.GetNetUsage())
		} else {
			require.Nil(tx.Receipt.GetOptionalNetUsage())
		}
	}
}

func (s *tronParserTestSuite) fixtureParsingHelper(filePath string) ([][]byte, error) {

	fixtureParityTrace, _ := fixtures.ReadFile(filePath)

	var tmpItems []json.RawMessage
	err := json.Unmarshal(fixtureParityTrace, &tmpItems)

	items := make([][]byte, len(tmpItems))
	for i, item := range tmpItems {
		items[i] = item
	}
	return items, err
}

func (s *tronParserTestSuite) TestToTronHash() {
	require := testutil.Require(s.T())

	testCases := []struct {
		input    string
		expected string
		comment  string
	}{
		{"0x0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87", "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87", "with 0x prefix"},
		{"0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87", "0000000004034f5cd8946001c721db6457608ad887b3734c825d55826c3c3c87", "without 0x prefix"},
		{"0xABCDEF1234567890", "ABCDEF1234567890", "uppercase hex"},
		{"", "", "empth string"},
		{"0x", "", "only 0x prefix"},
	}

	for _, tc := range testCases {
		result := toTronHash(tc.input)
		require.Equal(tc.expected, result, tc.comment)
	}
}

func (s *tronParserTestSuite) TestHexToTronAddress() {
	require := testutil.Require(s.T())
	testCases := []struct {
		input    string
		expected string
		comment  string
	}{
		{"0x8b0359acac03bac62cbf89c4b787cb10b3c3f513", "TNeEwWHXLLUgEtfzTnYN8wtVenGxuMzZCE", "with 0x prefix"},
		{"0xc60a6f5c81431c97ed01b61698b6853557f3afd4", "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd", "with 0x prefix"},
		{"0x4d12f87c18a914dddbc2b27f378ad126a79b76b6", "TGzjkw66CtL49eKiQFDwJDuXG9HSQd69p2", "with 0x prefix"},
		{"0xe8667633c747066c70672c58207cc745a9860527", "TXA2WjFc5f86deJcZZCdbdpkpUTKTA3VDM", "with 0x prefix"},
		{"0x89ae01b878dffc8088222adf1fb08ebadfeea53a", "TNXC2YCSxhdxsVqhqu3gYZYme6n4i6T1C1", "with 0x prefix"},

		{"418b0359acac03bac62cbf89c4b787cb10b3c3f513", "TNeEwWHXLLUgEtfzTnYN8wtVenGxuMzZCE", "without 0x but have 41 prefix"},
		{"41c60a6f5c81431c97ed01b61698b6853557f3afd4", "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd", "without 0x but have 41 prefix"},
		{"414d12f87c18a914dddbc2b27f378ad126a79b76b6", "TGzjkw66CtL49eKiQFDwJDuXG9HSQd69p2", "without 0x but have 41 prefix"},
		{"41e8667633c747066c70672c58207cc745a9860527", "TXA2WjFc5f86deJcZZCdbdpkpUTKTA3VDM", "without 0x but have 41 prefix"},
		{"4189ae01b878dffc8088222adf1fb08ebadfeea53a", "TNXC2YCSxhdxsVqhqu3gYZYme6n4i6T1C1", "without 0x but have 41 prefix"},

		{"c64e69acde1c7b16c2a3efcdbbdaa96c3644c2b3", "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5", "without 0x and 41 prefix"},
		{"a614f803b6fd780986a42c78ec9c7f77e6ded13c", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "without 0x and 41 prefix"},

		{"", "", "empty string"},
	}

	for _, tc := range testCases {
		result := hexToTronAddress(tc.input)
		require.Equal(tc.expected, result, tc.comment)
	}
}
