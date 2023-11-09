package parser

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	arbitrumTag        uint32 = 1
	arbitrumHash              = "0xf54ec2c280ff82691162f4c2d6d1c53e7fdd4b889e103dc8af2ebbca28139f5c"
	arbitrumParentHash        = "0x892b3cd6f96434f2abc51cf6a610f4b2d64e031b49eb3b835c408abb28a2ce89"
	arbitrumHeight     uint64 = 0x121EB6E
)

type arbitrumParserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  Parser
}

func TestArbitrumParserTestSuite(t *testing.T) {
	suite.Run(t, new(arbitrumParserTestSuite))
}

func (s *arbitrumParserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ARBITRUM, common.Network_NETWORK_ARBITRUM_MAINNET),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *arbitrumParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *arbitrumParserTestSuite) TestParseArbitrumBlock() {
	require := testutil.Require(s.T())

	fixtureReceipt := fixtures.MustReadFile("parser/ethereum/raw_block_receipt.json")
	traces := s.fixtureTracesParsingHelper("parser/arbitrum/arb_paritytrace_basic.json")
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ARBITRUM,
		Network:    common.Network_NETWORK_ARBITRUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        ethereumTag,
			Hash:       ethereumHash,
			ParentHash: ethereumParentHash,
			Height:     ethereumHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt},
				TransactionTraces:   traces,
			},
		},
	}

	expected := &api.EthereumBlock{
		Header: &api.EthereumHeader{
			Hash:             "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
			ParentHash:       "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
			Number:           0xacc290,
			Timestamp:        &timestamppb.Timestamp{Seconds: int64(1606234041)},
			Transactions:     []string{"0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8"},
			Nonce:            "0xc83f6d8ab7e58888",
			Sha3Uncles:       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
			LogsBloom:        "0xfdf668a334802d0164a3e3cab8f79d6bab99b9800f565fa9dea9138b2192371768c4d8f83681bb0647e07d000807499cca14bcd05d1202c180e0e2a0f2777225f4990de285aa8086d82acd2c5653c46fe8943c0e50e6521a879a5144c14f57125c064c122e730c959509992ac09588e9c648da88a6eac64805fe9132d280772abd048d16428227c6c0d8c57a460c8281e0203f8791e402cdba21c5ea0430a282a2b3a7e593a2392a2523b7961b2fd0a06752631744001311b4a9ad111d20ec7d4c2d4e02892ed5023b12126442a219ac16400e40051900d250a3b7e6adc2e13053393130810561402181040301ca492fdb24320064c43a50c42c31ea259f4820",
			TransactionsRoot: "0x8fcfe4d75266508496020675b9c3acfdf0074bf2d177c6366b40f669306310db",
			StateRoot:        "0xf7135b656a6513846894dad825c7a2403ee2f93ea9e3fe0e8cd846ba0df2fd7d",
			ReceiptsRoot:     "0x18b4e30527b17d9e1e8f0dc129c828a28a2a32b43a651b4f9302a2686f7a5963",
			Miner:            "0xd224ca0c819e8e97ba0136b3b95ceff503b79f53",
			Difficulty:       0xc7ad271a33ba1,
			TotalDifficulty:  "18930033225567982479580",
			ExtraData:        "0x7575706f6f6c2e636e2d3333",
			Size:             0xb8ec,
			GasLimit:         0xbe2d22,
			GasUsed:          0xbe252d,
			Uncles:           []string{},
		},
		Transactions: []*api.EthereumTransaction{
			{
				BlockHash:      "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				BlockNumber:    0xacc290,
				From:           "0x4823cc90c145fd6a16ab7668043dbba5ce79cdfc",
				Gas:            0x15f90,
				GasPrice:       0x22cee4f700,
				Hash:           "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
				Input:          "0xa9059cbb00000000000000000000000022852cdfdda5eb9b0e25d6581bdb82a156ac4c400000000000000000000000000000000000000000000000000000000162598040",
				Nonce:          0x1f0,
				To:             "0xdac17f958d2ee523a2206206994597c13d831ec7",
				Index:          0,
				Value:          "10",
				Type:           uint64(0),
				BlockTimestamp: &timestamppb.Timestamp{Seconds: int64(1606234041)},
				OptionalTransactionAccessList: &api.EthereumTransaction_TransactionAccessList{
					TransactionAccessList: &api.EthereumTransactionAccessList{
						AccessList: []*api.EthereumTransactionAccess{
							{
								Address: "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
								StorageKeys: []string{
									"0x0000000000000000000000000000000000000000000000000000000000000003",
									"0x0000000000000000000000000000000000000000000000000000000000000007",
								},
							},
							{
								Address:     "0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
								StorageKeys: []string{},
							},
						},
					},
				},
				Receipt: &api.EthereumTransactionReceipt{
					TransactionHash:   "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
					TransactionIndex:  0x0,
					BlockHash:         "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
					BlockNumber:       0xacc290,
					From:              "0x98265d92b016df8758f361fb8d2f9a813c82494a",
					To:                "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
					CumulativeGasUsed: 0xbca58c,
					GasUsed:           0x1b889,
					EffectiveGasPrice: 0x22cee4f700,
					ContractAddress:   "",
					LogsBloom:         "0x00200000000000000000000080000000000080000200000000010000000000000000000000000000000000000000000002000000080000000000000000200001000000000000000010000008000000200000000000400000000000000000000000000000000000000000000000000000000000002000040000000010000000000000000000000000004000000000000000002000000000088000004000000000020000000000000000000000000000000400000000000000000000000000800000000002000001000000000000000000000000000000001000000002000020000010200000000000000000000000000000000000000000000000008004000000",
					Root:              "",
					OptionalStatus: &api.EthereumTransactionReceipt_Status{
						Status: 1,
					},
					Type: uint64(0),
					Logs: []*api.EthereumEventLog{
						{
							Removed:          false,
							LogIndex:         0x119,
							TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
							TransactionIndex: 0x0,
							BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
							BlockNumber:      0xacc290,
							Address:          "0xe5caef4af8780e59df925470b050fb23c43ca68c",
							Data:             "0x0000000000000000000000000000000000000000000000000000000715d435c0",
							Topics: []string{
								"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
								"0x00000000000000000000000098265d92b016df8758f361fb8d2f9a813c82494a",
								"0x00000000000000000000000092330d8818e8a3b50f027c819fa46031ffba2c8c",
							},
						},
						{
							Removed:          false,
							LogIndex:         0x120,
							TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
							TransactionIndex: 0x0,
							BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
							BlockNumber:      0xacc290,
							Address:          "0xe5caef4af8780e59df925470b050fb23c43ca68c",
							Data:             "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000e029ae811464737200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000033b2e3cbfa3d80192847e1a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
							Topics: []string{
								"0x29ae811400000000000000000000000000000000000000000000000000000000",
								"0x000000000000000000000000be8e3e3618f7474f8cb1d074a26affef007e98fb",
								"0x6473720000000000000000000000000000000000000000000000000000000000",
								"0x0000000000000000000000000000000000000000033b2e3cbfa3d80192847e1a",
							},
						},
						{
							Removed:          false,
							LogIndex:         0x121,
							TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
							TransactionIndex: 0x0,
							BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
							BlockNumber:      0xacc290,
							Address:          "0xad72c532d9fe5c51292d950dd0a160c76ff3fa30",
							Data:             "0x00000000000000000000000000000000000000000000000000000000000000c8",
							Topics: []string{
								"0xc1405953cccdad6b442e266c84d66ad671e2534c6584f8e6ef92802f7ad294d5",
								"0x000000000000000000000000be8e3e3618f7474f8cb1d074a26affef007e98fb",
								"0x0000000000000000000000000000000000000000033b2e3cbfa3d80192847e1a",
								"0x0000000000000000000000001fe16de955718cfab7a44605458ab023838c2793",
							},
						},
						{
							Removed:          false,
							LogIndex:         0x122,
							TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
							TransactionIndex: 0x0,
							BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
							BlockNumber:      0xacc290,
							Address:          "0x518ba36f1ca6dfe3bb1b098b8dd0444030e79d9f",
							Data:             "0x",
							Topics: []string{
								"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
								"0x0000000000000000000000000000000000000000",
								"0x05379b307e6ae02e522fb134fad1254a4e7fbac1",
								"0x0000000000000000000000000000000000000000000000000000000000001950",
							},
						},
					},
				},
				FlattenedTraces: []*api.EthereumTransactionFlattenedTrace{
					{
						BlockNumber:      0xacc290,
						BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
						TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
						TransactionIndex: 0,
						Type:             "call",
						TraceType:        "call",
						CallType:         "call",
						TraceId:          "call_0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
						From:             "0x4823cc90c145fd6a16ab7668043dbba5ce79cdfc",
						To:               "0xdac17f958d2ee523a2206206994597c13d831ec7",
						Value:            "0",
						Gas:              0x3c7a8,
						GasUsed:          0,
						Input:            "0xa9059cbb00000000000000000000000022852cdfdda5eb9b0e25d6581bdb82a156ac4c400000000000000000000000000000000000000000000000000000000162598040",
						Output:           "",
						Subtraces:        uint64(1),
						TraceAddress:     []uint64{},
						Status:           0,
						Error:            "Revert",
					},
					{
						BlockNumber:      0xacc290,
						BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
						TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
						TransactionIndex: 0,
						Type:             "call",
						TraceType:        "call",
						CallType:         "call",
						TraceId:          "call_0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8_0",
						From:             "0xdac17f958d2ee523a2206206994597c13d831ec7",
						To:               "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
						Value:            "0",
						Gas:              0x2f3dd,
						GasUsed:          0x776,
						Input:            "0x23b872dd000000000000000000000000ba1bfd85432905ff4a2e7f516b56b7485dbdc5f6000000000000000000000000960ea3e3c7fb317332d990873d354e18d764559000000000000000000000000000000000000000000000000000000001d4aa9788",
						Output:           "0x0000000000000000000000000000000000000000000000000000000000000001",
						Subtraces:        uint64(0),
						TraceAddress:     []uint64{0},
						Status:           0,
						Error:            "Revert",
					},
				},
				TokenTransfers: []*api.EthereumTokenTransfer{
					{
						TokenAddress:     "0xe5caef4af8780e59df925470b050fb23c43ca68c",
						FromAddress:      "0x98265d92b016df8758f361fb8d2f9a813c82494a",
						ToAddress:        "0x92330d8818e8a3b50f027c819fa46031ffba2c8c",
						Value:            "30431000000",
						TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
						TransactionIndex: 0x0,
						LogIndex:         0x119,
						BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
						BlockNumber:      11322000,
						TokenTransfer: &api.EthereumTokenTransfer_Erc20{
							Erc20: &api.ERC20TokenTransfer{
								FromAddress: "0x98265d92b016df8758f361fb8d2f9a813c82494a",
								ToAddress:   "0x92330d8818e8a3b50f027c819fa46031ffba2c8c",
								Value:       "30431000000",
							},
						},
					},
					{
						TokenAddress:     "0x518ba36f1ca6dfe3bb1b098b8dd0444030e79d9f",
						FromAddress:      "0x0000000000000000000000000000000000000000",
						ToAddress:        "0x05379b307e6ae02e522fb134fad1254a4e7fbac1",
						Value:            "",
						TransactionHash:  "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
						TransactionIndex: 0x0,
						LogIndex:         0x122,
						BlockHash:        "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
						BlockNumber:      11322000,
						TokenTransfer: &api.EthereumTokenTransfer_Erc721{
							Erc721: &api.ERC721TokenTransfer{
								FromAddress: "0x0000000000000000000000000000000000000000",
								ToAddress:   "0x05379b307e6ae02e522fb134fad1254a4e7fbac1",
								TokenId:     "6480",
							},
						},
					},
				},
			},
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_ARBITRUM, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_ARBITRUM_MAINNET, nativeBlock.Network)

	actual := nativeBlock.GetEthereum()
	require.NotNil(actual)
	require.Equal(expected.Header, actual.Header)
	require.Equal(expected.Transactions, actual.Transactions)
}

// test transaction traces error populating logic with this order:
// trace_3
//
//	|- trace_4
//	|- trace_5_error
//	  |- trace_6(no error in raw trace, but need to assign error)
//	|- trace_7
func (s *arbitrumParserTestSuite) TestParseArbitrumNativeBlock_ErrorFlattenedTraces() {
	require := testutil.Require(s.T())

	fixtureReceipt := fixtures.MustReadFile("parser/ethereum/raw_block_receipt.json")
	traces := s.fixtureTracesParsingHelper("parser/arbitrum/arb_paritytrace_errorflattenedtrace.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ARBITRUM,
		Network:    common.Network_NETWORK_ARBITRUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        ethereumTag,
			Hash:       ethereumHash,
			ParentHash: ethereumParentHash,
			Height:     ethereumHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt},
				TransactionTraces:   traces,
			},
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)

	actual := nativeBlock.GetEthereum()
	require.NotNil(actual)
	transaction := actual.Transactions[0]
	flattenedTraces := transaction.FlattenedTraces
	require.Empty(flattenedTraces[0].Error)
	require.Empty(flattenedTraces[1].Error)
	require.Equal(flattenedTraces[2].Error, "Revert")
	require.Equal(flattenedTraces[3].Error, "Revert")
	require.Empty(flattenedTraces[4].Error)
}

func (s *arbitrumParserTestSuite) TestParseArbitrumNativeBlock_ErrorFlattenedTraces_TraceOutOfOrder() {
	require := testutil.Require(s.T())

	fixtureReceipt := fixtures.MustReadFile("parser/ethereum/raw_block_receipt.json")
	traces := s.fixtureTracesParsingHelper("parser/arbitrum/arb_paritytrace_traceoutoforder.json")
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ARBITRUM,
		Network:    common.Network_NETWORK_ARBITRUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        ethereumTag,
			Hash:       ethereumHash,
			ParentHash: ethereumParentHash,
			Height:     ethereumHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt},
				TransactionTraces:   traces,
			},
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)

	actual := nativeBlock.GetEthereum()
	require.NotNil(actual)
	transaction := actual.Transactions[0]
	flattenedTraces := transaction.FlattenedTraces
	s.flattenTraceVerifyHelper(flattenedTraces, 0, 5, false)
	s.flattenTraceVerifyHelper(flattenedTraces, 6, 8, true)
	s.flattenTraceVerifyHelper(flattenedTraces, 9, 9, false)
	s.flattenTraceVerifyHelper(flattenedTraces, 10, 14, true)
	s.flattenTraceVerifyHelper(flattenedTraces, 15, 16, false)
	s.flattenTraceVerifyHelper(flattenedTraces, 17, 18, true)
	s.flattenTraceVerifyHelper(flattenedTraces, 19, 34, false)
}

func (s *arbitrumParserTestSuite) TestParseArbitrumNativeBlock_FlattenedTracesForMultipleTxns() {
	require := testutil.Require(s.T())

	fixtureHeader, err := fixtures.ReadFile("parser/arbitrum/arb_fixtureheader.json")
	require.NoError(err)
	fixtureReceipt_1, err := fixtures.ReadFile("parser/arbitrum/arb_fixturereceipt_1.json")
	require.NoError(err)
	fixtureReceipt_2, err := fixtures.ReadFile("parser/arbitrum/arb_fixturereceipt_2.json")
	require.NoError(err)
	traces := s.fixtureTracesParsingHelper("parser/arbitrum/arb_paritytrace_twotxns.json")

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ARBITRUM,
		Network:    common.Network_NETWORK_ARBITRUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:        arbitrumTag,
			Hash:       arbitrumHash,
			ParentHash: arbitrumParentHash,
			Height:     arbitrumHeight,
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              fixtureHeader,
				TransactionReceipts: [][]byte{fixtureReceipt_2, fixtureReceipt_1},
				TransactionTraces:   traces,
			},
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)

	actual := nativeBlock.GetEthereum()
	require.NotNil(actual)
	transaction1 := actual.Transactions[0]
	transaction2 := actual.Transactions[1]
	flattenedTraces1 := transaction1.FlattenedTraces
	flattenedTraces2 := transaction2.FlattenedTraces

	for _, trace := range flattenedTraces1 {
		require.Equal(trace.TransactionHash, "0xaa9a084287700be4bde21b4562cb1946f49922530c4dd1c9f86f879dd6b7cb10")
		require.Equal(trace.TransactionIndex, uint64(0))
	}
	for _, trace := range flattenedTraces2 {
		require.Equal(trace.TransactionHash, "0x4f002f77a7d632e122c5203262c226ae4e0c6d884e697bf56febc9e9360bc47d")
		require.Equal(trace.TransactionIndex, uint64(1))
	}
	s.flattenTraceVerifyHelper(flattenedTraces1, 0, len(flattenedTraces1)-1, true)
	s.flattenTraceVerifyHelper(flattenedTraces2, 0, len(flattenedTraces2)-1, true)
}

func (s *arbitrumParserTestSuite) fixtureTracesParsingHelper(filePath string) [][]byte {
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

func (s *arbitrumParserTestSuite) flattenTraceVerifyHelper(flattenedTraces []*api.EthereumTransactionFlattenedTrace, fromIndex int, toIndex int, isError bool) {
	require := testutil.Require(s.T())

	for i := fromIndex; i <= toIndex; i++ {
		if isError {
			require.Equal("Revert", flattenedTraces[i].Error)
			require.Equal(uint64(0), flattenedTraces[i].Status)

		} else {
			require.Empty(flattenedTraces[i].Error)
			require.Equal(uint64(1), flattenedTraces[i].Status)
		}
	}
}
