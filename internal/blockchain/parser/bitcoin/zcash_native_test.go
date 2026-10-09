package bitcoin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestZcashNativeParser_FiltersPrivacyTransactionsAndPreservesOriginalIndex(t *testing.T) {
	require := require.New(t)
	parser, app := newZcashNativeParser(t)
	defer app.Close()

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ZCASH,
		Network:    common.Network_NETWORK_ZCASH_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          1,
			Hash:         "blockhash",
			ParentHash:   "parenthash",
			Height:       1,
			ParentHeight: 0,
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{
					"hash":"blockhash",
					"height":1,
					"time":1,
					"nTx":3,
					"previousblockhash":"parenthash",
					"tx":[
						{
							"txid":"coinbase-tx",
							"hash":"coinbase-tx",
							"vin":[{"coinbase":"01","sequence":4294967295}],
							"vout":[
								{
									"value":12.5,
									"n":0,
									"scriptPubKey":{
										"asm":"OP_DUP OP_HASH160 1111111111111111111111111111111111111111 OP_EQUALVERIFY OP_CHECKSIG",
										"hex":"76a914111111111111111111111111111111111111111188ac",
										"type":"pubkeyhash",
										"address":"t1coinbase"
									}
								}
							]
						},
						{
							"txid":"privacy-tx",
							"hash":"privacy-tx",
							"vin":[],
							"vout":[],
							"vShieldedSpend":[{"dummy":"value"}]
						},
						{
							"txid":"transparent-tx",
							"hash":"transparent-tx",
							"vin":[
								{
									"txid":"prevtx",
									"vout":0,
									"scriptSig":{"asm":"","hex":""},
									"sequence":4294967295
								}
							],
							"vout":[
								{
									"value":0.999,
									"n":0,
									"scriptPubKey":{
										"asm":"OP_DUP OP_HASH160 2222222222222222222222222222222222222222 OP_EQUALVERIFY OP_CHECKSIG",
										"hex":"76a914222222222222222222222222222222222222222288ac",
										"type":"pubkeyhash",
										"address":"t1transparent"
									}
								}
							]
						}
					]
				}`),
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{}},
					{Data: [][]byte{}},
					{
						Data: [][]byte{
							[]byte(`{
								"txid":"prevtx",
								"vout":[
									{
										"value":1.0,
										"n":0,
										"scriptPubKey":{
											"asm":"OP_DUP OP_HASH160 3333333333333333333333333333333333333333 OP_EQUALVERIFY OP_CHECKSIG",
											"hex":"76a914333333333333333333333333333333333333333388ac",
											"type":"pubkeyhash",
											"address":"t1prevout"
										}
									}
								]
							}`),
						},
					},
				},
			},
		},
	}

	nativeBlock, err := parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(uint64(2), nativeBlock.NumTransactions)

	bitcoinBlock := nativeBlock.GetBitcoin()
	require.NotNil(bitcoinBlock)
	require.Len(bitcoinBlock.Transactions, 2)
	require.Equal(uint64(3), bitcoinBlock.Header.NumberOfTransactions)
	require.Equal(uint64(0), bitcoinBlock.Transactions[0].Index)
	require.Equal(uint64(2), bitcoinBlock.Transactions[1].Index)
}

func TestZcashNativeParser_FiltersTransactionsMissingEitherVinOrVout(t *testing.T) {
	require := require.New(t)
	parser, app := newZcashNativeParser(t)
	defer app.Close()

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ZCASH,
		Network:    common.Network_NETWORK_ZCASH_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          1,
			Hash:         "blockhash",
			ParentHash:   "parenthash",
			Height:       1,
			ParentHeight: 0,
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{
					"hash":"blockhash",
					"height":1,
					"time":1,
					"nTx":2,
					"previousblockhash":"parenthash",
					"tx":[
						{
							"txid":"coinbase-tx",
							"hash":"coinbase-tx",
							"vin":[{"coinbase":"01","sequence":4294967295}],
							"vout":[
								{
									"value":12.5,
									"n":0,
									"scriptPubKey":{
										"asm":"OP_DUP OP_HASH160 1111111111111111111111111111111111111111 OP_EQUALVERIFY OP_CHECKSIG",
										"hex":"76a914111111111111111111111111111111111111111188ac",
										"type":"pubkeyhash",
										"address":"t1coinbase"
									}
								}
							]
						},
						{
							"txid":"privacy-tx",
							"hash":"privacy-tx",
							"vin":[
								{
									"txid":"prevtx",
									"vout":0,
									"scriptSig":{"asm":"","hex":""},
									"sequence":4294967295
								}
							],
							"vout":[],
							"valueBalance":"0.0"
						}
					]
				}`),
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{}},
					{
						Data: [][]byte{
							[]byte(`{
								"txid":"prevtx",
								"vout":[
									{
										"value":1.0,
										"n":0,
										"scriptPubKey":{
											"asm":"OP_DUP OP_HASH160 3333333333333333333333333333333333333333 OP_EQUALVERIFY OP_CHECKSIG",
											"hex":"76a914333333333333333333333333333333333333333388ac",
											"type":"pubkeyhash",
											"address":"t1prevout"
										}
									}
								]
							}`),
						},
					},
				},
			},
		},
	}

	nativeBlock, err := parser.ParseBlock(context.Background(), block)
	require.NoError(err)

	bitcoinBlock := nativeBlock.GetBitcoin()
	require.NotNil(bitcoinBlock)
	require.Equal(uint64(1), nativeBlock.NumTransactions)
	require.Len(bitcoinBlock.Transactions, 1)
	require.Equal("coinbase-tx", bitcoinBlock.Transactions[0].TransactionId)
}

func TestZcashNativeParser_ParseOptionsPassthrough(t *testing.T) {
	require := require.New(t)
	parser, app := newZcashNativeParser(t)
	defer app.Close()

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ZCASH,
		Network:    common.Network_NETWORK_ZCASH_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag: 1, Hash: "blockhash", ParentHash: "parenthash",
			Height: 1, ParentHeight: 0,
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{
					"hash":"blockhash","height":1,"time":1,"nTx":1,
					"previousblockhash":"parenthash",
					"tx":[{
						"txid":"coinbase-tx","hash":"coinbase-tx",
						"vin":[{"coinbase":"01","sequence":4294967295}],
						"vout":[{"value":12.5,"n":0,"scriptPubKey":{
							"asm":"OP_DUP OP_HASH160 1111111111111111111111111111111111111111 OP_EQUALVERIFY OP_CHECKSIG",
							"hex":"76a914111111111111111111111111111111111111111188ac",
							"type":"pubkeyhash","address":"t1coinbase"
						}}]
					}],
					"vShieldedSpend":[{"dummy":"value"}]
				}`),
				InputTransactions: []*api.RepeatedBytes{{Data: [][]byte{}}},
			},
		},
	}

	// All three options applied; should not break zcash parse.
	nb, err := parser.ParseBlock(context.Background(),
		block,
		internal.WithSkipScripts(),
		internal.WithSkipWitnesses(),
		internal.WithSkipShielded(),
	)
	require.NoError(err)
	require.NotNil(nb.GetBitcoin())
	require.Equal(uint64(1), nb.NumTransactions)

	tx := nb.GetBitcoin().Transactions[0]
	require.Empty(tx.Hex)
	for _, out := range tx.Outputs {
		require.Empty(out.ScriptPublicKey.Assembly)
		require.Empty(out.ScriptPublicKey.Hex)
	}
}

func newZcashNativeParser(t *testing.T) (internal.NativeParser, testapp.TestApp) {
	t.Helper()

	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ZCASH, common.Network_NETWORK_ZCASH_MAINNET),
		fx.Provide(NewZcashNativeParser),
		fx.Populate(&parser),
	)

	return parser, app
}
