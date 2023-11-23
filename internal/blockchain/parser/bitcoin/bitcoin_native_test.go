package bitcoin

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	bitcoinTag          uint32 = 1
	bitcoinHeight       uint64 = 696402
	bitcoinParentHeight uint64 = 696401
	bitcoinHash                = "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"
	bitcoinParentHash          = "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3"
)

var (
	bitcoinMetadata = &api.BlockMetadata{
		Tag:          bitcoinTag,
		Hash:         bitcoinHash,
		ParentHash:   bitcoinParentHash,
		Height:       bitcoinHeight,
		ParentHeight: bitcoinParentHeight,
	}
)

type bitcoinNativeParserTestSuite struct {
	suite.Suite
	app    testapp.TestApp
	parser internal.NativeParser
}

func TestBitcoinNativeParserTestSuite(t *testing.T) {
	suite.Run(t, new(bitcoinNativeParserTestSuite))
}

func (s *bitcoinNativeParserTestSuite) SetupTest() {
	s.app = testapp.New(s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&s.parser),
	)
	s.NotNil(s.parser)
}

func (s *bitcoinNativeParserTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlock() {
	const (
		fixture       = "parser/bitcoin/get_block_native.json"
		updateFixture = false // Change to true to update the fixture.
	)

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

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_BITCOIN_MAINNET, nativeBlock.Network)
	require.Equal(bitcoinTag, nativeBlock.Tag)
	require.Equal(bitcoinHash, nativeBlock.Hash)
	require.Equal(bitcoinParentHash, nativeBlock.ParentHash)
	require.Equal(bitcoinHeight, nativeBlock.Height)
	require.Equal(bitcoinParentHeight, nativeBlock.ParentHeight)
	require.Equal(testutil.MustTimestamp("2021-08-18T17:00:34Z"), nativeBlock.Timestamp)
	require.Equal(uint64(2), nativeBlock.NumTransactions)

	actual := nativeBlock.GetBitcoin()
	require.NotNil(actual)
	if updateFixture {
		fixtures.MustMarshalJSON(fixture, actual)
	}

	var expected api.BitcoinBlock
	fixtures.MustUnmarshalJSON(fixture, &expected)
	require.Equal(expected.Header, actual.Header)
	require.EqualValues(expected.Transactions, actual.Transactions)

	// Don't forget to reset the flag before you commit.
	require.False(updateFixture)
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlock_old_addresses_schema_backcompat() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/oldschema/get_block_oldschemaaddresses.json")
	require.NoError(err)
	tx1, err := fixtures.ReadFile("parser/bitcoin/oldschema/get_raw_transaction_oldschemaaddresses.json")
	require.NoError(err)
	tx2, err := fixtures.ReadFile("parser/bitcoin/oldschema/get_raw_transaction_tx2_oldschemaaddresses.json")
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

	var expected api.BitcoinBlock
	fixtures.MustUnmarshalJSON("parser/bitcoin/get_block_native.json", &expected)

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_BITCOIN_MAINNET, nativeBlock.Network)

	actual := nativeBlock.GetBitcoin()
	require.NotNil(actual)
	require.Equal(expected.Header, actual.Header)
	require.EqualValues(expected.Transactions, actual.Transactions)
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlockPubKey() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/get_block_pubkey_case.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   bitcoinMetadata,
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
			},
		},
	}

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(bitcoinScriptTypePubKey, nativeBlock.GetBitcoin().Transactions[0].Outputs[0].ScriptPublicKey.Type)
	require.Equal(bitcoinScriptTypePubKeyHash, nativeBlock.GetBitcoin().Transactions[0].Outputs[1].ScriptPublicKey.Type)
	require.Equal("1LVnx7af5JPzW41vVURfbmxs3ENEzPcEqF", nativeBlock.GetBitcoin().Transactions[0].Outputs[1].ScriptPublicKey.Address)
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlockPubKeyInputNonCoinbase() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/get_block_pubkey_input_non_coinbase_case.json")
	require.NoError(err)
	txInput, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction_tx_input.json")
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

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(bitcoinScriptTypePubKey, nativeBlock.GetBitcoin().Transactions[1].Inputs[0].FromOutput.ScriptPublicKey.Type)
	require.Equal("12dES7ZSWHFPnocFErthMALCXEXg3o6oqz", nativeBlock.GetBitcoin().Transactions[1].Inputs[0].FromOutput.ScriptPublicKey.Address)
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlockPubKeyOutputNonCoinbase() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/get_block_pubkey_output_non_coinbase_case.json")
	require.NoError(err)
	txOutput, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction_tx_output.json")
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
							txOutput,
						},
					},
				},
			},
		},
	}

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(bitcoinScriptTypePubKey, nativeBlock.GetBitcoin().Transactions[1].Outputs[0].ScriptPublicKey.Type)
	require.Equal(bitcoinScriptTypePubKeyHash, nativeBlock.GetBitcoin().Transactions[1].Outputs[1].ScriptPublicKey.Type)
	require.Equal("1VayNert3x1KzbpzMGt2qdqrAThiRovi8", nativeBlock.GetBitcoin().Transactions[1].Outputs[0].ScriptPublicKey.Address)
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlockMultiSig() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/oldschema/get_block_multisig_case.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   bitcoinMetadata,
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
			},
		},
	}

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(bitcoinScriptTypePubKeyHash, nativeBlock.GetBitcoin().Transactions[0].Outputs[2].ScriptPublicKey.Type)
	require.Equal("1EKKwLBZHx9wCy3R8NE5FCLU1iZacxnbRS", nativeBlock.GetBitcoin().Transactions[0].Outputs[2].ScriptPublicKey.Address)
	require.Equal(bitcoinScriptTypeMultisig, nativeBlock.GetBitcoin().Transactions[0].Outputs[3].ScriptPublicKey.Type)
	require.Empty(nativeBlock.GetBitcoin().Transactions[0].Outputs[3].ScriptPublicKey.Address)
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlock_ErrValidateBitcoinBlock() {
	tests := []struct {
		name     string
		file     string
		messages []string
	}{
		{
			name: "BitcoinBlock_requires_hash",
			file: "parser/bitcoin/errcases/get_block_err_nohash.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Hash'",
			},
		},
		{
			name: "BitcoinBlock_requires_previous_block_hash",
			file: "parser/bitcoin/errcases/get_block_err_noprevhash.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.PreviousBlockHash'",
			},
		},
		{
			name: "BitcoinBlock_requires_time",
			file: "parser/bitcoin/errcases/get_block_err_notime.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Time'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_non-empty",
			file: "parser/bitcoin/errcases/get_block_err_notxempty.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_non-nil",
			file: "parser/bitcoin/errcases/get_block_err_notxnil.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_content_non-empty_tx",
			file: "parser/bitcoin/errcases/get_block_err_txemptytx.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0].TxId'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_content_non-nil_tx",
			file: "parser/bitcoin/errcases/get_block_err_txniltx.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0]'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_hash",
			file: "parser/bitcoin/errcases/get_block_err_txnohash.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0].Hash'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_id",
			file: "parser/bitcoin/errcases/get_block_err_txnoid.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0].TxId'",
			},
		},
		{
			name: "BitcoinBlock__requires_transaction_vin_non-empty",
			file: "parser/bitcoin/errcases/get_block_err_txnovinempty.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0].Vin'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_vin_non-nil",
			file: "parser/bitcoin/errcases/get_block_err_txnovinnil.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0].Vin'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_vout_non-empty",
			file: "parser/bitcoin/errcases/get_block_err_txnovoutempty.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0].Vout'",
			},
		},
		{
			name: "BitcoinBlock_requires_transaction_vout_non-nil",
			file: "parser/bitcoin/errcases/get_block_err_txnovoutnil.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Tx[0].Vout'",
			},
		},
	}

	for _, test := range tests {
		require := testutil.Require(s.T())
		// ensure that test name does not contains spaces for easier find/search replaced with _
		require.NotContains(test.name, " ")
		s.T().Run(test.name, func(t *testing.T) {
			header, err := fixtures.ReadFile(test.file)
			require.NoError(err)

			block := &api.Block{
				Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
				Network:    common.Network_NETWORK_BITCOIN_MAINNET,
				Metadata:   bitcoinMetadata,
				Blobdata: &api.Block_Bitcoin{
					Bitcoin: &api.BitcoinBlobdata{
						Header: header,
					},
				},
			}

			_, err = s.parser.ParseBlock(context.Background(), block)
			require.Error(err)
			for _, msg := range test.messages {
				require.Contains(err.Error(), msg)
			}
		})
	}
}

func (s *bitcoinNativeParserTestSuite) TestParseBitcoinBlock_ErrValidateInputTx() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("parser/bitcoin/get_block.json")
	require.NoError(err)

	tests := []struct {
		name     string
		file     string
		messages []string
	}{
		{
			name: "BitcoinBlock_requires_input_transaction_address_not_empty",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_addressempty.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout[0].ScriptPubKey.Address[main]'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_address_not_nil",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_addressnil.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout[0].ScriptPubKey.Address[main]'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_pubkey_not_empty",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_pubkeyempty.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout[0].ScriptPubKey.Address[unsupported]'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_pubkey_not_nil",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_pubkeynil.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout[0].ScriptPubKey'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_pubkey_not_unsupported",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_pubkeytypeunsupported.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout[0].ScriptPubKey.Address[unsupported]'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_not_empty",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_empty.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_not_nil",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_nil.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_id",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_noid.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.TxId'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_vout_non-empty",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_novoutempty.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_vout_non-nil",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_novoutnil.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_vout_content_non-nil",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_voutnil.json",
			messages: []string{
				"parseTransactions failed",
				"failed to validate bitcoin input transaction",
				"Key: 'BitcoinInputTransactionLit.Vout[0]'",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_not_found",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_parsevinnotfound.json",
			messages: []string{
				"parseTransactions failed",
				"parseVin at not found on metadataMap for [0] with tx",
			},
		},
		{
			name: "BitcoinBlock_requires_input_transaction_not_out_of_bound",
			file: "parser/bitcoin/errcases/get_raw_transaction_err_parsevinoutofbound.json",
			messages: []string{
				"parseTransactions failed",
				"output index not found in input transaction's vout",
			},
		},
	}

	for _, test := range tests {
		// ensure that test name does not contains spaces for easier find/search replaced with _
		require.NotContains(test.name, " ")
		s.T().Run(test.name, func(t *testing.T) {
			transaction, err := fixtures.ReadFile(test.file)
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
									transaction,
								},
							},
						},
					},
				},
			}

			_, err = s.parser.ParseBlock(context.Background(), block)
			require.Error(err)
			for _, msg := range test.messages {
				require.Contains(err.Error(), msg)
			}
		})
	}
}

func TestParse_BitcoinDecimalQuantity(t *testing.T) {
	maxBtcPlusOneSatoshi := (btcutil.MaxSatoshi + 1) / btcutil.SatoshiPerBitcoin
	someJson := []byte(fmt.Sprintf(`{
		"difficulty":21659344833264.85,
		"value": %v
	}`, maxBtcPlusOneSatoshi))
	var d struct {
		Difficulty BitcoinDecimalQuantity `json:"difficulty"`
		Value      BitcoinDecimalQuantity `json:"value"`
	}
	err := json.Unmarshal(someJson, &d)
	require.NoError(t, err)

	require.Equal(t, "21659344833264.85", d.Difficulty.String())
	require.Equal(t, "21000000.00", d.Value.String())
	require.Equal(t, "21000000.00000001", fmt.Sprintf("%.8f", d.Value.Value()))
}

func TestParseBitcoinTransactionLit(t *testing.T) {
	tests := []struct {
		name            string
		hasTransactions bool
		input           string
	}{
		{
			name:            "transactionStruct",
			hasTransactions: true,
			input: `{
				"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				"tx": [
					{
						"txid": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
						"vin": [{ "txid": "some_id_1", "vout": 1 }]
					},
					{
						"txid": "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
						"vin": [{ "txid": "some_id_2", "vout": 2 }]
					}
				]
			}`,
		},
		{
			name:            "transactionHash",
			hasTransactions: true,
			input: `	{
				"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				"tx": [
					"0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
					"0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991"
				]
			}`,
		},
		{
			name:            "noTransaction",
			hasTransactions: false,
			input: `{
				"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				"timestamp": "0x5fbd2fb9"
			}`,
		},
		{
			name:            "emptyTransaction",
			hasTransactions: false,
			input: `{
				"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				"tx": []
			}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			var block BitcoinBlockLit
			err := json.Unmarshal([]byte(test.input), &block)
			require.NoError(err)

			require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", block.Hash.Value())
			if test.hasTransactions {
				require.Equal(2, len(block.Transactions))
				require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", block.Transactions[0].Identifier.Value())
				require.Equal("0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991", block.Transactions[1].Identifier.Value())
			} else {
				require.Equal(0, len(block.Transactions))
			}
		})
	}
}
