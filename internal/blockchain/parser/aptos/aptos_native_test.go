package aptos

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type aptosNativeParserTestSuite struct {
	suite.Suite
	app         testapp.TestApp
	parser      internal.NativeParser
	aptosParser *aptosNativeParserImpl
}

const (
	// A block for BlockMetadata and StateCheckpoint transactions.
	aptosTag          = uint32(2)
	aptosHeight       = uint64(10000)
	aptosParentHeight = uint64(9999)
	aptosHash         = "0x7eee0512cef0754f58890802a6c3ba1e1032bb1820f45b825adbbee4f10d9d71"
	aptosParentHash   = ""
	aptosTimestamp    = "2022-10-12T22:48:48Z"

	// A block for UserTransaction.
	aptosHeight1       = uint64(44991655)
	aptosParentHeight1 = uint64(44991654)
	aptosHash1         = "0xa6ee6266348bc84793388aa376453a4c076821b479b97f5faba68ec03cf7b253"
	aptosParentHash1   = ""
	aptosTimestamp1    = "2023-04-07T04:57:52Z"

	// A block for GenesisTransaction.
	aptosHeight2       = uint64(0)
	aptosParentHeight2 = uint64(0)
	aptosHash2         = "0x0000000000000000000000000000000000000000000000000000000000000000"
	aptosParentHash2   = ""
	aptosTimestamp2    = "1970-01-01T00:00:00Z"

	aptosHeight3       = uint64(1798814)
	aptosParentHeight3 = uint64(1798813)
	aptosHash3         = "0x2464566c37f67e8b4042676e38ec9179cae5251582984279abd9d6823f9d7b85"
	aptosParentHash3   = ""
	aptosTimestamp3    = "2022-10-21T01:02:18Z"

	aptosHeight4       = uint64(84219770)
	aptosParentHeight4 = uint64(1798813)
	aptosHash4         = "0xb2ed70351815757017fd2d8a17b3e70fd9fd57d0d1ad6ade7ddecdf8b6c535b5"
	aptosParentHash4   = ""
	aptosTimestamp4    = "2023-08-23T18:02:26Z"
)

var (
	writeResource1 = `{
              "epoch_interval": "7200000000",
              "height": "10000",
              "new_block_events": {
                "counter": "10001",
                "guid": {
                  "id": {
                    "addr": "0x1",
                    "creation_num": "3"
                  }
                }
              },
              "update_epoch_interval_events": {
                "counter": "0",
                "guid": {
                  "id": {
                    "addr": "0x1",
                    "creation_num": "4"
                  }
                }
              }
            }`

	writeResource2 = `{
              "validators": [
                {
                  "failed_proposals": "0",
                  "successful_proposals": "62"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "47"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "32"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "27"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "28"
                },
                {
                  "failed_proposals": "3",
                  "successful_proposals": "256"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "120"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "60"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "29"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "87"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "171"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "173"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "163"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "179"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "337"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "44"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "39"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "35"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "31"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "39"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "899"
                },
                {
                  "failed_proposals": "1",
                  "successful_proposals": "818"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "844"
                },
                {
                  "failed_proposals": "6",
                  "successful_proposals": "0"
                },
                {
                  "failed_proposals": "4",
                  "successful_proposals": "769"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "890"
                },
                {
                  "failed_proposals": "1",
                  "successful_proposals": "836"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "862"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "771"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "722"
                },
                {
                  "failed_proposals": "0",
                  "successful_proposals": "629"
                }
              ]
            }`

	writeResource3 = `{
              "microseconds": "1665614928907827"
            }`

	eventData1 = `{
            "epoch": "2",
            "failed_proposer_indices": [],
            "hash": "0x7eee0512cef0754f58890802a6c3ba1e1032bb1820f45b825adbbee4f10d9d71",
            "height": "10000",
            "previous_block_votes_bitvec": "0xffff0e36",
            "proposer": "0x324df1e27c4129a58d73851ae0e9366064dc666a73e747051e203694a4cb257",
            "round": "10014",
            "time_microseconds": "1665614928907827"
          }`

	writeResource4 = `{
              "coin": {
                "value": "141100965309177"
              }
            }`

	eventData2 = `{
            "amount": "100000000"
          }`

	eventData3 = `{
                "hosting_account": "0x2",
                "proposal_type_info": {
                  "account_address": "0x2",
                  "module_name": "0x676f7665726e616e63655f70726f706f73616c",
                  "struct_name": "0x476f7665726e616e636550726f706f73616c"
                }
              }`

	eventData4 = `{
            "hosting_account": "0x1",
            "proposal_type_info": {
              "account_address": "0x1",
              "module_name": "0x676f7665726e616e63655f70726f706f73616c",
              "struct_name": "0x476f7665726e616e636550726f706f73616c"
            }
          }`

	arguments1 = `[
        "Flour #35253"
      ]`
)

func TestAptosNativeParserTestSuite(t *testing.T) {
	suite.Run(t, new(aptosNativeParserTestSuite))
}

func (s *aptosNativeParserTestSuite) SetupTest() {
	s.app = testapp.New(s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_APTOS, common.Network_NETWORK_APTOS_MAINNET),
		fx.Provide(NewAptosNativeParser),
		fx.Populate(&s.parser),
	)
	s.NotNil(s.parser)

	// This is used to test many internal structure parsing.
	s.aptosParser = &aptosNativeParserImpl{
		logger:   s.app.Logger(),
		validate: validator.New(),
		config:   s.app.Config(),
	}
}

func (s *aptosNativeParserTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *aptosNativeParserTestSuite) TestMarshalAptosQuantity() {
	require := testutil.Require(s.T())
	type TestStruct struct {
		BlockHeight AptosQuantity
	}

	testStruct := TestStruct{
		BlockHeight: 123,
	}
	data, err := json.Marshal(testStruct)
	require.NoError(err)
	require.Equal(`{"BlockHeight":"123"}`, string(data))
}

func (s *aptosNativeParserTestSuite) TestUnmarshalAptosQuantity() {
	type TestStruct struct {
		BlockHeight AptosQuantity
	}

	tests := []struct {
		name     string
		expected uint64
		input    string
	}{
		{
			name:     "normal",
			expected: 1234,
			input:    "1234",
		},
		{
			name:     "empty",
			expected: 0,
			input:    "",
		},
		{
			name:     "zero",
			expected: 0,
			input:    "0",
		},
	}
	for _, test := range tests {
		s.T().Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"BlockHeight": "%v"}`, test.input)
			var testStruct TestStruct
			err := json.Unmarshal([]byte(data), &testStruct)
			require.NoError(err)
			require.Equal(test.expected, testStruct.BlockHeight.Value())
		})
	}
}

func (s *aptosNativeParserTestSuite) TestUnmarshalAptosQuantity_Failure() {
	type TestStruct struct {
		BlockHeight AptosQuantity
	}

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "with 0x",
			input: `"0x1234"`,
		},
		{
			name:  "negative",
			input: `"-1234"`,
		},
		{
			name:  "overflow",
			input: `"123456789123456789123456789"`,
		},
	}
	for _, test := range tests {
		s.T().Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"BlockHeight": %v}`, test.input)
			var testStruct TestStruct
			err := json.Unmarshal([]byte(data), &testStruct)
			require.Contains(err.Error(), "failed to decode AptosQuantity")
		})
	}
}

func (s *aptosNativeParserTestSuite) TestParseBlock() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_APTOS,
		Network:    common.Network_NETWORK_APTOS_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          aptosTag,
			Hash:         aptosHash,
			ParentHash:   aptosParentHash,
			Height:       aptosHeight,
			ParentHeight: aptosParentHeight,
		},
		Blobdata: &api.Block_Aptos{
			Aptos: &api.AptosBlobdata{
				Block: fixtures.MustReadFile("parser/aptos/block.json"),
			},
		},
	}
	numTransactions := 2

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	expectedTimestamp := testutil.MustTimestamp(aptosTimestamp)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, nativeBlock.Network)
	require.Equal(aptosTag, nativeBlock.Tag)
	require.Equal(aptosHeight, nativeBlock.Height)
	require.Equal(aptosParentHeight, nativeBlock.ParentHeight)
	require.Equal(aptosHash, nativeBlock.Hash)
	require.Equal(aptosParentHash, nativeBlock.ParentHash)
	require.Equal(expectedTimestamp, nativeBlock.Timestamp)
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))

	aptosBlock := nativeBlock.GetAptos()
	require.NotNil(aptosBlock)
	require.Equal(&api.AptosHeader{
		BlockHeight: aptosHeight,
		BlockHash:   aptosHash,
		BlockTime:   expectedTimestamp,
	}, aptosBlock.GetHeader())
	require.Equal(numTransactions, len(aptosBlock.GetTransactions()))

	// The first transaction: block metadata transaction
	transaction := aptosBlock.GetTransactions()[0]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     20083,
		BlockHeight: 10000,
		Info: &api.AptosTransactionInfo{
			Hash:                "0x0a0ea9f2c10639794ad183308f33ae01722675a6b13115aaf9af284d1f64fad2",
			StateChangeHash:     "0x69dfd32387cdf3cecfd957812bb25f83586b0b5c1a3e1eabdf116f128bc9503c",
			EventRootHash:       "0xcdb57d4606ea74b47b6f7b304376d18e667dfbcce271ee7cbd4f0cd69b5c2736",
			GasUsed:             0,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0x55488600270a86152286cbc9b38bcde1fe1ac42cbc8135335d05b6e33a4090cd",
			Changes: []*api.AptosWriteSetChange{
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x5ddf404c60e96e9485beafcabb95609fed8e38e941a725cae4dcec8296fb32d7",
							TypeStr:      "0x1::block::BlockResource",
							Data:         writeResource1,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x8048c954221814b04533a9f0a9946c3a8d472ac62df5accb9f47c097e256e8b6",
							TypeStr:      "0x1::stake::ValidatorPerformance",
							Data:         writeResource2,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x7b1615bf012d3c94223f3f76287ee2f7bdf31d364071128b256aeff0841b626d",
							TypeStr:      "0x1::timestamp::CurrentTimeMicroseconds",
							Data:         writeResource3,
						},
					},
				},
			},
		},
		Type: api.AptosTransaction_BLOCK_METADATA,
		TxnData: &api.AptosTransaction_BlockMetadata{
			BlockMetadata: &api.AptosBlockMetadataTransaction{
				Id:    "0x7eee0512cef0754f58890802a6c3ba1e1032bb1820f45b825adbbee4f10d9d71",
				Epoch: 2,
				Round: 10014,
				Events: []*api.AptosEvent{
					{
						Key: &api.AptosEventKey{
							CreationNumber: 3,
							AccountAddress: "0x1",
						},
						SequenceNumber: 10000,
						Type:           "0x1::block::NewBlockEvent",
						Data:           string(eventData1),
					},
				},
				PreviousBlockVotesBitvec: []byte{
					255,
					255,
					14,
					54,
				},
				Proposer:              "0x324df1e27c4129a58d73851ae0e9366064dc666a73e747051e203694a4cb257",
				FailedProposerIndices: []uint32{},
			},
		},
	}, transaction)

	// The second transaction: state checkpoint transaction
	transaction = aptosBlock.GetTransactions()[1]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     20084,
		BlockHeight: 10000,
		Info: &api.AptosTransactionInfo{
			Hash:            "0x67131d11785ca4d027c136f4f4a80e14b30e4acc830f2dc4890e82244f9aad31",
			StateChangeHash: "0xafb6e14fe47d850fd0a7395bcfb997ffacf4715e0f895cc162c218e4a7564bc6",
			EventRootHash:   "0x414343554d554c41544f525f504c414345484f4c4445525f4841534800000000",
			OptionalStateCheckpointHash: &api.AptosTransactionInfo_StateCheckpointHash{
				StateCheckpointHash: "0x32b73b0478aae08821a350d9e28ec4bec9d55fa93a8a04f55e0fc7912fc0ce78",
			},
			GasUsed:             0,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0x8a662ed7ff3c2aa743494c86e763440362b3f409a46124342015a910ff51acb8",
			Changes:             []*api.AptosWriteSetChange{},
		},
		Type: api.AptosTransaction_STATE_CHECKPOINT,
		TxnData: &api.AptosTransaction_StateCheckpoint{
			StateCheckpoint: &api.AptosStateCheckpointTransaction{},
		},
	}, transaction)
}

// Test the parsing of all 6 types of write set changes.
func (s *aptosNativeParserTestSuite) TestParseChanges() {
	require := testutil.Require(s.T())

	input := fixtures.MustReadFile("parser/aptos/write_set_changes.json")
	var changes []json.RawMessage
	err := json.Unmarshal(input, &changes)
	require.NoError(err)
	require.Equal(6, len(changes))

	results, err := s.aptosParser.parseChanges(changes)
	require.NoError(err)
	require.Equal(6, len(results))

	// DeleteModule
	require.Equal(&api.AptosWriteSetChange{
		Type: api.AptosWriteSetChange_DELETE_MODULE,
		Change: &api.AptosWriteSetChange_DeleteModule{
			DeleteModule: &api.AptosDeleteModule{
				Address:      "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
				StateKeyHash: "0x5ddf404c60e96e9485beafcabb95609fed8e38e941a725cae4dcec8296fb32d7",
				Module: &api.AptosMoveModuleId{
					Address: "0x1",
					Name:    "aptos_coin",
				},
			},
		},
	}, results[0])

	// DeleteResource
	require.Equal(&api.AptosWriteSetChange{
		Type: api.AptosWriteSetChange_DELETE_RESOURCE,
		Change: &api.AptosWriteSetChange_DeleteResource{
			DeleteResource: &api.AptosDeleteResource{
				Address:      "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
				StateKeyHash: "0x5ddf404c60e96e9485beafcabb95609fed8e38e941a725cae4dcec8296fb32d7",
				Resource:     "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>",
			},
		},
	}, results[1])

	// DeleteTableItem
	require.Equal(&api.AptosWriteSetChange{
		Type: api.AptosWriteSetChange_DELETE_TABLE_ITEM,
		Change: &api.AptosWriteSetChange_DeleteTableItem{
			DeleteTableItem: &api.AptosDeleteTableItem{
				StateKeyHash: "0x5ddf404c60e96e9485beafcabb95609fed8e38e941a725cae4dcec8296fb32d7",
				Handle:       "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
				Key:          "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
				Data: &api.AptosDeleteTableData{
					Key:     "dummy_key",
					KeyType: "dummy_value",
				},
			},
		},
	}, results[2])

	// WriteResource
	require.Equal(&api.AptosWriteSetChange{
		Type: api.AptosWriteSetChange_WRITE_RESOURCE,
		Change: &api.AptosWriteSetChange_WriteResource{
			WriteResource: &api.AptosWriteResource{
				Address:      "0x1",
				StateKeyHash: "0x7b1615bf012d3c94223f3f76287ee2f7bdf31d364071128b256aeff0841b626d",
				TypeStr:      "0x1::timestamp::CurrentTimeMicroseconds",
				Data:         `{"microseconds": "1665614928907827"}`,
			},
		},
	}, results[3])

	// WriteTableItem
	require.Equal(&api.AptosWriteSetChange{
		Type: api.AptosWriteSetChange_WRITE_TABLE_ITEM,
		Change: &api.AptosWriteSetChange_WriteTableItem{
			WriteTableItem: &api.AptosWriteTableItem{
				StateKeyHash: "0x6e4b28d40f98a106a65163530924c0dcb40c1349d3aa915d108b4d6cfc1ddb19",
				Handle:       "0x1b854694ae746cdbd8d44186ca4929b2b337df21d1c74633be19b2710552fdca",
				Key:          "0x0619dc29a0aac8fa146714058e8dd6d2d0f3bdf5f6331907bf91f3acd81e6935",
				Value:        "0x60e2aeae10436d010000000000000000",
				Data: &api.AptosWriteTableItemData{
					Key:       "dummy_key",
					KeyType:   "dummy_key_type",
					Value:     "dummy_value",
					ValueType: "dummy_value_type",
				},
			},
		},
	}, results[4])

	// WriteModule
	require.Equal(&api.AptosWriteSetChange{
		Type: api.AptosWriteSetChange_WRITE_MODULE,
		Change: &api.AptosWriteSetChange_WriteModule{
			WriteModule: &api.AptosWriteModule{
				Address:      "0x1",
				StateKeyHash: "0x14341421783c9456a4611af5623c6b783452ca8be626a933998270eaafa1aeb1",
				Data: &api.AptosMoveModuleBytecode{
					Bytecode: "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
					Abi: &api.AptosMoveModule{
						Address: "0x1",
						Name:    "acl",
						Friends: []*api.AptosMoveModuleId{
							{
								Address: "0x1",
								Name:    "code",
							},
							{
								Address: "0x1",
								Name:    "gas_schedule",
							},
						},
						ExposedFunctions: []*api.AptosMoveFunction{
							{
								Name:              "add",
								Visibility:        api.AptosMoveFunction_PUBLIC,
								IsEntry:           false,
								GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{},
								Params: []string{
									"&mut 0x1::acl::ACL",
									"address",
								},
								Return: []string{},
							},
							{
								Name:       "assert_contains",
								Visibility: api.AptosMoveFunction_PRIVATE,
								IsEntry:    false,
								GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{
									{
										Constraints: []string{
											"param1",
											"param2",
										},
									},
								},
								Params: []string{
									"&0x1::acl::ACL",
									"address",
								},
								Return: []string{
									"bool",
								},
							},
						},
						Structs: []*api.AptosMoveStruct{
							{
								Name:     "ACL",
								IsNative: false,
								Abilities: []string{
									"copy",
									"drop",
									"store",
								},
								GenericTypeParams: []*api.AptosMoveStructGenericTypeParam{
									{
										Constraints: []string{
											"dummy_a",
											"dummy_b",
										},
									},
								},
								Fields: []*api.AptosMoveStructField{
									{
										Name: "list",
										Type: "vector",
									},
								},
							},
						},
					},
				},
			},
		},
	}, results[5])
}

// Test the parsing of all 5 types of transaction payloads.
func (s *aptosNativeParserTestSuite) TestParseTransactionPayloads() {
	require := testutil.Require(s.T())

	input := fixtures.MustReadFile("parser/aptos/transaction_payloads.json")
	var payloads []json.RawMessage
	err := json.Unmarshal(input, &payloads)
	require.NoError(err)
	require.Equal(5, len(payloads))

	results := make([]*api.AptosTransactionPayload, len(payloads))
	for i, p := range payloads {
		result, err := s.aptosParser.parseTransactionPayload(p)
		require.NoError(err)
		results[i] = result
	}

	// EntryFunctionPayload
	require.Equal(&api.AptosTransactionPayload{
		Type: api.AptosTransactionPayload_ENTRY_FUNCTION_PAYLOAD,
		Payload: &api.AptosTransactionPayload_EntryFunctionPayload{
			EntryFunctionPayload: &api.AptosEntryFunctionPayload{
				Function: &api.AptosEntryFunctionId{
					Module: &api.AptosMoveModuleId{
						Address: "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa",
						Name:    "coin_bridge",
					},
					FunctionName: "lz_receive",
				},
				TypeArguments: []string{
					"0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC",
				},
				Arguments: [][]byte{
					[]byte(`"110"`),
					[]byte(`"0x1bacc2205312534375c8d1801c27d28370656cff"`),
				},
			},
		},
	}, results[0])

	// ScriptPayload
	require.Equal(&api.AptosTransactionPayload{
		Type: api.AptosTransactionPayload_SCRIPT_PAYLOAD,
		Payload: &api.AptosTransactionPayload_ScriptPayload{
			ScriptPayload: &api.AptosScriptPayload{
				Code: &api.AptosMoveScriptBytecode{
					Bytecode: "0xa11ceb0b050000000501000203020505070b0712170829200000b020b03110002",
					Abi: &api.AptosMoveFunction{
						Name:       "main",
						Visibility: api.AptosMoveFunction_PUBLIC,
						IsEntry:    true,
						GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{
							{
								Constraints: []string{},
							},
						},
						Params: []string{
							"signer",
							"signer",
							"address",
							"u64",
						},
						Return: []string{},
					},
				},
				TypeArguments: []string{
					"0x1::aptos_coin::AptosCoin",
				},
				Arguments: [][]byte{
					[]byte(`"0xce024fa293bc67be13dda22dee72b3fe49b1e86e23dcb03eae8b754670b0539d"`),
					[]byte(`877500`),
					[]byte(arguments1),
				},
			},
		},
	}, results[1])

	// ModuleBundlePayload
	require.Equal(&api.AptosTransactionPayload{
		Type: api.AptosTransactionPayload_MODULE_BUNDLE_PAYLOAD,
		Payload: &api.AptosTransactionPayload_ModuleBundlePayload{
			ModuleBundlePayload: &api.AptosModuleBundlePayload{
				Modules: []*api.AptosMoveModuleBytecode{
					{
						Bytecode: "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
						Abi: &api.AptosMoveModule{
							Address: "0x1",
							Name:    "acl",
							Friends: []*api.AptosMoveModuleId{
								{
									Address: "0x1",
									Name:    "code",
								},
								{
									Address: "0x1",
									Name:    "gas_schedule",
								},
							},
							ExposedFunctions: []*api.AptosMoveFunction{
								{
									Name:              "add",
									Visibility:        api.AptosMoveFunction_PUBLIC,
									IsEntry:           false,
									GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{},
									Params: []string{
										"&mut 0x1::acl::ACL",
										"address",
									},
									Return: []string{},
								},
							},
							Structs: []*api.AptosMoveStruct{
								{
									Name:     "ACL",
									IsNative: false,
									Abilities: []string{
										"copy",
										"drop",
										"store",
									},
									GenericTypeParams: []*api.AptosMoveStructGenericTypeParam{
										{
											Constraints: []string{
												"dummy_a",
												"dummy_b",
											},
										},
									},
									Fields: []*api.AptosMoveStructField{
										{
											Name: "list",
											Type: "vector",
										},
									},
								},
							},
						},
					},
					{
						Bytecode: "0xa11ceb0b0500000006010002030206050807070f0d081c200c3c04000000010001010001060900",
						Abi: &api.AptosMoveModule{
							Address: "0x1",
							Name:    "bcs",
							Friends: []*api.AptosMoveModuleId{},
							ExposedFunctions: []*api.AptosMoveFunction{
								{
									Name:       "to_bytes",
									Visibility: api.AptosMoveFunction_PUBLIC,
									IsEntry:    false,
									GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{
										{
											Constraints: []string{},
										},
									},
									Params: []string{
										"&T0",
									},
									Return: []string{
										"vector<u8>",
									},
								},
							},
							Structs: []*api.AptosMoveStruct{},
						},
					},
				},
			},
		},
	}, results[2])

	// MultisigPayload
	require.Equal(&api.AptosTransactionPayload{
		Type: api.AptosTransactionPayload_MULTISIG_PAYLOAD,
		Payload: &api.AptosTransactionPayload_MultisigPayload{
			MultisigPayload: &api.AptosMultisigPayload{
				MultisigAddress: "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
				OptionalTransactionPayload: &api.AptosMultisigPayload_TransactionPayload{
					TransactionPayload: &api.AptosMultisigTransactionPayload{
						Type: api.AptosMultisigTransactionPayload_ENTRY_FUNCTION_PAYLOAD,
						Payload: &api.AptosMultisigTransactionPayload_EntryFunctionPayload{
							EntryFunctionPayload: &api.AptosEntryFunctionPayload{
								Function: &api.AptosEntryFunctionId{
									Module: &api.AptosMoveModuleId{
										Address: "0x1",
										Name:    "aptos_coin",
									},
									FunctionName: "transfer",
								},
								TypeArguments: []string{
									"0x1::aptos_coin::AptosCoin",
								},
								Arguments: [][]byte{
									[]byte(`"877500"`),
								},
							},
						},
					},
				},
			},
		},
	}, results[3])

	//WriteSetPayload
	require.Equal(&api.AptosTransactionPayload{
		Type: api.AptosTransactionPayload_WRITE_SET_PAYLOAD,
		Payload: &api.AptosTransactionPayload_WriteSetPayload{
			WriteSetPayload: &api.AptosWriteSetPayload{
				WriteSet: &api.AptosWriteSet{
					WriteSetType: api.AptosWriteSet_SCRIPT_WRITE_SET,
					WriteSet: &api.AptosWriteSet_ScriptWriteSet{
						ScriptWriteSet: &api.AptosScriptWriteSet{
							ExecuteAs: "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
							Script: &api.AptosScriptPayload{
								Code: &api.AptosMoveScriptBytecode{
									Bytecode: "0xa11ceb0b050000000501000203020505070b0712170829200000b020b03110002",
									Abi: &api.AptosMoveFunction{
										Name:       "main",
										Visibility: api.AptosMoveFunction_PUBLIC,
										IsEntry:    true,
										GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{
											{
												Constraints: []string{},
											},
										},
										Params: []string{
											"signer",
											"signer",
											"address",
											"u64",
										},
										Return: []string{},
									},
								},
								TypeArguments: []string{
									"0x1::aptos_coin::AptosCoin",
								},
								Arguments: [][]byte{
									[]byte(`"0xce024fa293bc67be13dda22dee72b3fe49b1e86e23dcb03eae8b754670b0539d"`),
									[]byte(`"877500"`),
								},
							},
						},
					},
				},
			},
		},
	}, results[4])
}

// Test the parsing of all 3 types of transaction signatures.
func (s *aptosNativeParserTestSuite) TestParseTransactionSignatures() {
	require := testutil.Require(s.T())

	input := fixtures.MustReadFile("parser/aptos/transaction_signatures.json")
	var payloads []json.RawMessage
	err := json.Unmarshal(input, &payloads)
	require.NoError(err)
	require.Equal(3, len(payloads))

	results := make([]*api.AptosSignature, len(payloads))
	for i, p := range payloads {
		result, err := s.aptosParser.parseTransactionSignature(p)
		require.NoError(err)
		results[i] = result
	}

	//Ed25519Signature
	require.Equal(&api.AptosSignature{
		Type: api.AptosSignature_ED25519,
		Signature: &api.AptosSignature_Ed25519{
			Ed25519: &api.AptosEd25519Signature{
				PublicKey: "0xa4359868f5c115b86aee128b9e74600506e502121a2a6648eaeb022183fc32e3",
				Signature: "0x35bf510b3a7d53f2ee948a48b8f184028c1da3dba30d44e3737270f01e17",
			},
		},
	}, results[0])

	//MultiEd25519Signature
	require.Equal(&api.AptosSignature{
		Type: api.AptosSignature_MULTI_ED25519,
		Signature: &api.AptosSignature_MultiEd25519{
			MultiEd25519: &api.AptosMultiEd25519Signature{
				PublicKeys: []string{
					"0xa4359868f5c115b86aee128b9e74600506e502121a2a6648eaeb022183fc32e3",
					"0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
				},
				Signatures: []string{
					"0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
					"0x35bf510b3a7d53f2ee948a48b8f184028c1da3dba30d4163f5afc3719d26c1c97ed8e4e3240394912c1",
				},
				Threshold:        0,
				PublicKeyIndices: "0xa11ceb0b050000000501000203020505070b0712170829200000b020b03110002",
			},
		},
	}, results[1])

	//MultiAgentSignature
	require.Equal(&api.AptosSignature{
		Type: api.AptosSignature_MULTI_AGENT,
		Signature: &api.AptosSignature_MultiAgent{
			MultiAgent: &api.AptosMultiAgentSignature{
				Sender: &api.AptosAccountSignature{
					Type: api.AptosAccountSignature_ED25519,
					Signature: &api.AptosAccountSignature_Ed25519{
						Ed25519: &api.AptosEd25519Signature{
							PublicKey: "0xa4359868f5c115b86aee128b9e74600506e502121a2a6648eaeb022183fc32e3",
							Signature: "0x35bf510b3a7d53f2ee948a48b8f184028c1da3dba30d4163f5afc3719d26c1c97ed8e4e3737270f01e17",
						},
					},
				},
				SecondarySignerAddresses: []string{
					"0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
				},
				SecondarySigners: []*api.AptosAccountSignature{
					{
						Type: api.AptosAccountSignature_ED25519,
						Signature: &api.AptosAccountSignature_Ed25519{
							Ed25519: &api.AptosEd25519Signature{
								PublicKey: "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
								Signature: "0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
							},
						},
					},
					{
						Type: api.AptosAccountSignature_MULTI_ED25519,
						Signature: &api.AptosAccountSignature_MultiEd25519{
							MultiEd25519: &api.AptosMultiEd25519Signature{
								PublicKeys: []string{
									"0xa4359868f5c115b86aee128b9e74600506e502121a2a6648eaeb022183fc32e3",
									"0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
								},
								Signatures: []string{
									"0x88fbd33f54e1126269769780feb24480428179f552e2313fbe571b72e62a1ca1",
									"0x35bf510b3a7d53f2ee948a48b8f184028c1da3dba30d4163f5afc3719d26c1c97ed8e4e3240394912c1",
								},
								Threshold:        0,
								PublicKeyIndices: "0xa11ceb0b050000000501000203020505070b0712170829200000b020b03110002",
							},
						},
					},
				},
			},
		},
	}, results[2])
}

func (s *aptosNativeParserTestSuite) TestParseBlock_UserTransaction() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_APTOS,
		Network:    common.Network_NETWORK_APTOS_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          aptosTag,
			Hash:         aptosHash1,
			ParentHash:   aptosParentHash1,
			Height:       aptosHeight1,
			ParentHeight: aptosParentHeight1,
		},
		Blobdata: &api.Block_Aptos{
			Aptos: &api.AptosBlobdata{
				Block: fixtures.MustReadFile("parser/aptos/simplified_user_block.json"),
			},
		},
	}
	numTransactions := 1

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	expectedTimestamp := testutil.MustTimestamp(aptosTimestamp1)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, nativeBlock.Network)
	require.Equal(aptosTag, nativeBlock.Tag)
	require.Equal(aptosHeight1, nativeBlock.Height)
	require.Equal(aptosParentHeight1, nativeBlock.ParentHeight)
	require.Equal(aptosHash1, nativeBlock.Hash)
	require.Equal(aptosParentHash1, nativeBlock.ParentHash)
	require.Equal(expectedTimestamp, nativeBlock.Timestamp)
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))

	aptosBlock := nativeBlock.GetAptos()
	require.NotNil(aptosBlock)
	require.Equal(&api.AptosHeader{
		BlockHeight: aptosHeight1,
		BlockHash:   aptosHash1,
		BlockTime:   expectedTimestamp,
	}, aptosBlock.GetHeader())
	require.Equal(numTransactions, len(aptosBlock.GetTransactions()))

	// Verify the only user transaction in the block.
	transaction := aptosBlock.GetTransactions()[0]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     115805296,
		BlockHeight: aptosHeight1,
		Info: &api.AptosTransactionInfo{
			Hash:                "0x9e3bc0297f3eb7a96267ac277fea8a3731ede0ed71387eed0ade0910a272c2bd",
			StateChangeHash:     "0x81fa1390a90afc3966d2fd76347dbffdb18e701d7b24231fa2dee0f3d427d799",
			EventRootHash:       "0x464cf21a3864b0a1f7681a22a9fcef9589db80b67c239ecbecb03149070a4566",
			GasUsed:             590,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0xcf6a0291afb04e70ea91532354ed4688633c3892c407c55657d7b9e1319e4450",
			Changes: []*api.AptosWriteSetChange{
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0",
							StateKeyHash: "0x3af446f0f4f0065f2f8427159f93a96fbebcea04bdb409d0ac9bf6a599c9e7cf",
							TypeStr:      "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>",
							Data:         writeResource4,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_TABLE_ITEM,
					Change: &api.AptosWriteSetChange_WriteTableItem{
						WriteTableItem: &api.AptosWriteTableItem{
							StateKeyHash: "0x6e4b28d40f98a106a65163530924c0dcb40c1349d3aa915d108b4d6cfc1ddb19",
							Handle:       "0x1b854694ae746cdbd8d44186ca4929b2b337df21d1c74633be19b2710552fdca",
							Key:          "0x0619dc29a0aac8fa146714058e8dd6d2d0f3bdf5f6331907bf91f3acd81e6935",
							Value:        "0x057f217efa476d010000000000000000",
							Data:         &api.AptosWriteTableItemData{},
						},
					},
				},
			},
		},
		Type: api.AptosTransaction_USER,
		TxnData: &api.AptosTransaction_User{
			User: &api.AptosUserTransaction{
				Request: &api.AptosUserTransactionRequest{
					Sender:                  "0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0",
					SequenceNumber:          52014,
					MaxGasAmount:            4000,
					GasUnitPrice:            144,
					ExpirationTimestampSecs: s.aptosParser.parseTimestamp(int64(1680886669 * 1000000)),
					Payload: &api.AptosTransactionPayload{
						Type: api.AptosTransactionPayload_ENTRY_FUNCTION_PAYLOAD,
						Payload: &api.AptosTransactionPayload_EntryFunctionPayload{
							EntryFunctionPayload: &api.AptosEntryFunctionPayload{
								Function: &api.AptosEntryFunctionId{
									Module: &api.AptosMoveModuleId{
										Address: "0x1",
										Name:    "aptos_account",
									},
									FunctionName: "transfer",
								},
								TypeArguments: []string{},
								Arguments: [][]byte{
									[]byte(`"0x49de4df35975d3e3563d276f9c54524d2588197c6dd6906e9f5f083570351c44"`),
									[]byte(`"100000000"`),
								},
							},
						},
					},
					Signature: &api.AptosSignature{
						Type: api.AptosSignature_ED25519,
						Signature: &api.AptosSignature_Ed25519{
							Ed25519: &api.AptosEd25519Signature{
								PublicKey: "0x5e9a2968688a26663d59693541591947b9b00f37c0ea2bed1cb7bcc90040a58e",
								Signature: "0xd1e592cbbc0fb4434522c392b0925ec71a4bc24bdfff6877fc8d5aa67e3cb93e6e28c8678615b7fbbcd1cad1e3053ae8cd5be287448e0d76e9669aa87c2bf700",
							},
						},
					},
				},
				Events: []*api.AptosEvent{
					{
						Key: &api.AptosEventKey{
							CreationNumber: 3,
							AccountAddress: "0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0",
						},
						SequenceNumber: 52014,
						Type:           "0x1::coin::WithdrawEvent",
						Data:           string(eventData2),
					},
				},
			},
		},
	}, transaction)
}

func (s *aptosNativeParserTestSuite) TestParseBlock_GenesisTransaction() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_APTOS,
		Network:    common.Network_NETWORK_APTOS_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          aptosTag,
			Hash:         aptosHash2,
			ParentHash:   aptosParentHash2,
			Height:       aptosHeight2,
			ParentHeight: aptosParentHeight2,
		},
		Blobdata: &api.Block_Aptos{
			Aptos: &api.AptosBlobdata{
				Block: fixtures.MustReadFile("parser/aptos/simplified_genesis_block.json"),
			},
		},
	}
	numTransactions := 1

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	expectedTimestamp := testutil.MustTimestamp(aptosTimestamp2)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, nativeBlock.Network)
	require.Equal(aptosTag, nativeBlock.Tag)
	require.Equal(aptosHeight2, nativeBlock.Height)
	require.Equal(aptosParentHeight2, nativeBlock.ParentHeight)
	require.Equal(aptosHash2, nativeBlock.Hash)
	require.Equal(aptosParentHash2, nativeBlock.ParentHash)
	require.Equal(expectedTimestamp, nativeBlock.Timestamp)
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))

	aptosBlock := nativeBlock.GetAptos()
	require.NotNil(aptosBlock)
	require.Equal(&api.AptosHeader{
		BlockHeight: aptosHeight2,
		BlockHash:   aptosHash2,
		BlockTime:   expectedTimestamp,
	}, aptosBlock.GetHeader())
	require.Equal(numTransactions, len(aptosBlock.GetTransactions()))

	// Verify the only genesis transaction in the block.
	transaction := aptosBlock.GetTransactions()[0]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     0,
		BlockHeight: aptosHeight2,
		Info: &api.AptosTransactionInfo{
			Hash:            "0xf650d76ea0a3176f0412b7e6bea5eb6fbf1d7adb0c39ab18e378adefb9247309",
			StateChangeHash: "0x69a83a9acd09f9ad76261d526eb1747141514dbb2c1a627cfcfe7cb558e33379",
			EventRootHash:   "0x0a21a7c56574c90e9fe6ce18fb68fb0fc27026f0d6064b97d0a7ba1c1d1a6007",
			OptionalStateCheckpointHash: &api.AptosTransactionInfo_StateCheckpointHash{
				StateCheckpointHash: "0x0d08659868360de4cf1d395d1e87c9e0bed31c5f3aab9636eec3ce30aaae6d47",
			},
			GasUsed:             0,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0xc326ceae74abbecb19b24a208425083603d2d5fbfebec776aa5338a237cc53b0",
			Changes: []*api.AptosWriteSetChange{
				{
					Type: api.AptosWriteSetChange_WRITE_MODULE,
					Change: &api.AptosWriteSetChange_WriteModule{
						WriteModule: &api.AptosWriteModule{
							Address:      "0x1",
							StateKeyHash: "0x3d50c646a20116b68fce1c7c74b943289115ef2ca92b1a55187307e0b04a2acb",
							Data: &api.AptosMoveModuleBytecode{
								Bytecode: "0xa11ceb0b0500000006010002030206050807070f0d081c200c3c04000000010001010001060900010a020362637308746f5f627974657300000000000000000000000000000000000000000000000000000000000000010001020000",
								Abi: &api.AptosMoveModule{
									Address: "0x1",
									Name:    "bcs",
									Friends: []*api.AptosMoveModuleId{},
									ExposedFunctions: []*api.AptosMoveFunction{
										{
											Name:       "to_bytes",
											Visibility: api.AptosMoveFunction_PUBLIC,
											IsEntry:    false,
											GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{
												{
													Constraints: []string{},
												},
											},
											Params: []string{
												"&T0",
											},
											Return: []string{
												"vector<u8>",
											},
										},
									},
									Structs: []*api.AptosMoveStruct{},
								},
							},
						},
					},
				},
			},
		},
		Type: api.AptosTransaction_GENESIS,
		TxnData: &api.AptosTransaction_Genesis{
			Genesis: &api.AptosGenesisTransaction{
				Payload: &api.AptosWriteSet{
					WriteSetType: api.AptosWriteSet_DIRECT_WRITE_SET,
					WriteSet: &api.AptosWriteSet_DirectWriteSet{
						DirectWriteSet: &api.AptosDirectWriteSet{
							WriteSetChange: []*api.AptosWriteSetChange{
								{
									Type: api.AptosWriteSetChange_WRITE_MODULE,
									Change: &api.AptosWriteSetChange_WriteModule{
										WriteModule: &api.AptosWriteModule{
											Address:      "0x1",
											StateKeyHash: "0x3d50c646a20116b68fce1c7c74b943289115ef2ca92b1a55187307e0b04a2acb",
											Data: &api.AptosMoveModuleBytecode{
												Bytecode: "0xa11ceb0b0500000006010002030206050807070f0d081c200c3c04000000010001010001060900010a020362637308746f5f627974657300000000000000000000000000000000000000000000000000000000000000010001020000",
												Abi: &api.AptosMoveModule{
													Address: "0x1",
													Name:    "bcs",
													Friends: []*api.AptosMoveModuleId{},
													ExposedFunctions: []*api.AptosMoveFunction{
														{
															Name:       "to_bytes",
															Visibility: api.AptosMoveFunction_PUBLIC,
															IsEntry:    false,
															GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{
																{
																	Constraints: []string{},
																},
															},
															Params: []string{
																"&T0",
															},
															Return: []string{
																"vector<u8>",
															},
														},
													},
													Structs: []*api.AptosMoveStruct{},
												},
											},
										},
									},
								},
							},
							Events: []*api.AptosEvent{
								{
									Key: &api.AptosEventKey{
										CreationNumber: 7,
										AccountAddress: "0x2",
									},
									SequenceNumber: 0,
									Type:           "0x1::voting::RegisterForumEvent",
									Data:           string(eventData3),
								},
							},
						},
					},
				},
				Events: []*api.AptosEvent{
					{
						Key: &api.AptosEventKey{
							CreationNumber: 6,
							AccountAddress: "0x1",
						},
						SequenceNumber: 0,
						Type:           "0x1::voting::RegisterForumEvent",
						Data:           string(eventData4),
					},
				},
			},
		},
	}, transaction)
}
func (s *aptosNativeParserTestSuite) TestParseBlock_NullAbiTransaction() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_APTOS,
		Network:    common.Network_NETWORK_APTOS_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          aptosTag,
			Hash:         aptosHash3,
			ParentHash:   aptosParentHash3,
			Height:       aptosHeight3,
			ParentHeight: aptosParentHeight3,
		},
		Blobdata: &api.Block_Aptos{
			Aptos: &api.AptosBlobdata{
				Block: fixtures.MustReadFile("parser/aptos/null_abi_block.json"),
			},
		},
	}
	numTransactions := 3

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	expectedTimestamp := testutil.MustTimestamp(aptosTimestamp3)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, nativeBlock.Network)
	require.Equal(aptosTag, nativeBlock.Tag)
	require.Equal(aptosHeight3, nativeBlock.Height)
	require.Equal(aptosParentHeight3, nativeBlock.ParentHeight)
	require.Equal(aptosHash3, nativeBlock.Hash)
	require.Equal(aptosParentHash3, nativeBlock.ParentHash)
	require.Equal(expectedTimestamp, nativeBlock.Timestamp)
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))

	aptosBlock := nativeBlock.GetAptos()
	require.NotNil(aptosBlock)
	require.Equal(&api.AptosHeader{
		BlockHeight: aptosHeight3,
		BlockHash:   aptosHash3,
		BlockTime:   expectedTimestamp,
	}, aptosBlock.GetHeader())
	require.Equal(numTransactions, len(aptosBlock.GetTransactions()))
	// Verify the problematic transaction with the nil ABI.
	transaction := aptosBlock.GetTransactions()[1]
	require.Equal(&api.AptosTransaction_User{
		User: &api.AptosUserTransaction{
			Request: &api.AptosUserTransactionRequest{
				Sender:                  "0xd1f2a75f141524b8b9d3168ac90de1b82c7ab5698d1863eeeadb75cebac15308",
				SequenceNumber:          64,
				MaxGasAmount:            10000,
				GasUnitPrice:            100,
				ExpirationTimestampSecs: &timestamp.Timestamp{Seconds: 1666314147},
				Payload: &api.AptosTransactionPayload{
					Type: api.AptosTransactionPayload_SCRIPT_PAYLOAD,
					Payload: &api.AptosTransactionPayload_ScriptPayload{
						ScriptPayload: &api.AptosScriptPayload{
							Code: &api.AptosMoveScriptBytecode{
								Bytecode: "0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12",
								Abi: &api.AptosMoveFunction{
									GenericTypeParams: []*api.AptosMoveFunctionGenericTypeParam{},
								},
							},
							TypeArguments: []string{
								"0x1::aptos_coin::AptosCoin",
								"0xc51ce2cd42b3eaec2841fd87f43040e7ee6106a9fadcdb21522302b4b4c2a13b::PRT__OXAHVP::protonzckp",
								"0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::curves::Uncorrelated",
							},
							Arguments: [][]byte{
								[]byte(`"10000"`),
								[]byte(`"19332081"`),
							},
						},
					},
				},
				Signature: &api.AptosSignature{
					Type: api.AptosSignature_ED25519,
					Signature: &api.AptosSignature_Ed25519{
						Ed25519: &api.AptosEd25519Signature{
							PublicKey: "0xf283d0e6386333a88ed41c5672f5bbe93e562a4f8a7cfbcb974a7bd794b997bf",
							Signature: "0x970b17baa632f4d66fb11637f53a4f05093716a82c022a676a09b3c59b69de8e5e86ae4825b5f0e8e5998c849ecd365ce9ce93e68fd3583b579fd611b92ec006",
						},
					},
				},
			},
			Events: []*api.AptosEvent{},
		},
	}, transaction.GetTxnData())
}

func (s *aptosNativeParserTestSuite) TestParseBlock_FeePayerTransaction() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_APTOS,
		Network:    common.Network_NETWORK_APTOS_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          aptosTag,
			Hash:         aptosHash4,
			ParentHash:   aptosParentHash4,
			Height:       aptosHeight4,
			ParentHeight: aptosParentHeight4,
		},
		Blobdata: &api.Block_Aptos{
			Aptos: &api.AptosBlobdata{
				Block: fixtures.MustReadFile("parser/aptos/fee_payer_block.json"),
			},
		},
	}
	numTransactions := 1

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	expectedTimestamp := testutil.MustTimestamp(aptosTimestamp4)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, nativeBlock.Network)
	require.Equal(aptosTag, nativeBlock.Tag)
	require.Equal(aptosHeight4, nativeBlock.Height)
	require.Equal(aptosParentHeight4, nativeBlock.ParentHeight)
	require.Equal(aptosHash4, nativeBlock.Hash)
	require.Equal(aptosParentHash4, nativeBlock.ParentHash)
	require.Equal(expectedTimestamp, nativeBlock.Timestamp)
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))

	aptosBlock := nativeBlock.GetAptos()
	require.NotNil(aptosBlock)
	require.Equal(&api.AptosHeader{
		BlockHeight: aptosHeight4,
		BlockHash:   aptosHash4,
		BlockTime:   expectedTimestamp,
	}, aptosBlock.GetHeader())
	require.Equal(numTransactions, len(aptosBlock.GetTransactions()))
	transaction := aptosBlock.GetTransactions()[0]

	require.Equal(&api.AptosTransaction_User{
		User: &api.AptosUserTransaction{
			Request: &api.AptosUserTransactionRequest{
				Sender:                  "0xca79adb0e3c2dc08728cb6baca0b55c21e9543860af5e4944cb15c65f6743e22",
				SequenceNumber:          0,
				MaxGasAmount:            10000,
				GasUnitPrice:            100,
				ExpirationTimestampSecs: &timestamp.Timestamp{Seconds: 1692813765},
				Payload: &api.AptosTransactionPayload{
					Type: api.AptosTransactionPayload_ENTRY_FUNCTION_PAYLOAD,
					Payload: &api.AptosTransactionPayload_EntryFunctionPayload{
						EntryFunctionPayload: &api.AptosEntryFunctionPayload{
							TypeArguments: []string{},
							Arguments: [][]byte{
								[]byte(`"Test Collection"`),
								[]byte(`"Test Points Holder"`),
								[]byte(`"10"`),
							},
							Function: &api.AptosEntryFunctionId{
								Module: &api.AptosMoveModuleId{
									Address: "0x120845781465940bd1d09aa73a55352594f7e7c4942a9550cd23919bdc3f6efc",
									Name:    "token_manager",
								},
								FunctionName: "add_points_to_unique_soulbound_token",
							},
						},
					},
				},
				Signature: &api.AptosSignature{
					Type: api.AptosSignature_FEE_PAYER,
					Signature: &api.AptosSignature_FeePayer{
						FeePayer: &api.AptosFeePayerSignature{
							Sender: &api.AptosAccountSignature{
								Type: api.AptosAccountSignature_ED25519,
								Signature: &api.AptosAccountSignature_Ed25519{
									Ed25519: &api.AptosEd25519Signature{
										PublicKey: "0x563c8512c5750416602e6565df9d4be44aa8f45e21b69518a7a3e44ae04da020",
										Signature: "0x230b1b8477a69cb008ebd302da11fb618647a547b80982ed264e46e59e5f6b051c892b531fe1e8f94c335437616f404533d0c8fbdf821ba59dba24835053380f",
									},
								},
							},
							SecondarySignerAddresses: []string{
								"0xb04cb8bf6a6eb607bd65a5d528335bf58bcb612ebc5f2b9c83a8a24a7f30e7bc",
							},
							SecondarySigners: []*api.AptosAccountSignature{
								{
									Type: api.AptosAccountSignature_ED25519,
									Signature: &api.AptosAccountSignature_Ed25519{
										Ed25519: &api.AptosEd25519Signature{
											PublicKey: "0x2147e3a6279124defe78b74fddc717fb01535e2c8e30d7c97909c9012d6d8f77",
											Signature: "0x6c266bd5fc75f060fecc90f10494687d6226ee9cf48df5cd6b54225ae459ad1313b935d6495a74c96448d87e39da990d932041206a0a8e5b5618b72223712103",
										},
									},
								},
							},
							FeePayerSigner: &api.AptosAccountSignature{
								Type: api.AptosAccountSignature_ED25519,
								Signature: &api.AptosAccountSignature_Ed25519{
									Ed25519: &api.AptosEd25519Signature{
										PublicKey: "0xc57822e5be5b3d06556bd5d0da9a36dff3f17444523bcd070556b39aa13f67ed",
										Signature: "0xa9bbdbaa4880bd45d4a4a668e7aa080082d5a3f7cb94a8871b8f3e6269a6d760a781147ce49a98bb69bb3d968d5e9b677fd18aa0182710433ef4ce1734580504",
									},
								},
							},
							FeePayerAddress: "0xf5feedac06f7fa0d8a0de62ce00ec66bd0aaf3fb56c2ac9f986eda2efac65062",
						},
					},
				},
			},
			Events: []*api.AptosEvent{},
		},
	}, transaction.GetTxnData())
}
