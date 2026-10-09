package bitcoin

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// TestBitcoinFamilyStreamParseParity runs ParseBlock and StreamBlockIter
// against the same fixture for every bitcoin-family chain and asserts
// their transaction lists match exactly. This is the load-bearing test
// that guarantees streaming honors chain-specific hooks (preprocessTx,
// txFilter) — without it, overrides like Dash's hash backfill or
// Zcash's shielded-tx filter can silently diverge from ParseBlock.
func TestBitcoinFamilyStreamParseParity(t *testing.T) {
	cases := []struct {
		name       string
		blockchain common.Blockchain
		network    common.Network
		newParser  any // fx.Provide constructor
		rawBlock   func(t *testing.T) *api.Block
	}{
		{
			name:       "Bitcoin",
			blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
			network:    common.Network_NETWORK_BITCOIN_MAINNET,
			newParser:  NewBitcoinNativeParser,
			rawBlock: func(t *testing.T) *api.Block {
				rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
				require.NoError(t, err)
				return rawBlock
			},
		},
		{
			name:       "Dash",
			blockchain: common.Blockchain_BLOCKCHAIN_DASH,
			network:    common.Network_NETWORK_DASH_MAINNET,
			newParser:  NewDashNativeParser,
			rawBlock: func(t *testing.T) *api.Block {
				header, err := fixtures.ReadFile("parser/bitcoin/dash_get_block_pubkey_case.json")
				require.NoError(t, err)
				return &api.Block{
					Blockchain: common.Blockchain_BLOCKCHAIN_DASH,
					Network:    common.Network_NETWORK_DASH_MAINNET,
					Metadata:   bitcoinMetadata,
					Blobdata: &api.Block_Bitcoin{
						Bitcoin: &api.BitcoinBlobdata{Header: header},
					},
				}
			},
		},
		{
			name:       "Zcash",
			blockchain: common.Blockchain_BLOCKCHAIN_ZCASH,
			network:    common.Network_NETWORK_ZCASH_MAINNET,
			newParser:  NewZcashNativeParser,
			rawBlock: func(t *testing.T) *api.Block {
				// Coinbase + shielded-only (no transparent txs with
				// external inputs, avoids needing InputTransactions
				// fixture plumbing). Exercises isTransparentZcashTx
				// filter: coinbase kept, shielded dropped → 1 tx in
				// output. Also exercises Dash/Zcash-style hash
				// backfill since we omit the "hash" field.
				return &api.Block{
					Blockchain: common.Blockchain_BLOCKCHAIN_ZCASH,
					Network:    common.Network_NETWORK_ZCASH_MAINNET,
					Metadata: &api.BlockMetadata{
						Tag: 1, Hash: "blockhash", ParentHash: "parenthash",
						Height: 1, ParentHeight: 0,
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
										"vin":[{"coinbase":"01","sequence":4294967295}],
										"vout":[{"value":12.5,"n":0,"scriptPubKey":{"asm":"OP_DUP OP_HASH160 1111111111111111111111111111111111111111 OP_EQUALVERIFY OP_CHECKSIG","hex":"76a914111111111111111111111111111111111111111188ac","type":"pubkeyhash","address":"t1coinbase"}}]
									},
									{
										"txid":"privacy-tx",
										"vin":[],
										"vout":[],
										"vShieldedSpend":[{"dummy":"value"}]
									}
								]
							}`),
						},
					},
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			var parser internal.NativeParser
			app := testapp.New(t,
				testapp.WithBlockchainNetwork(tc.blockchain, tc.network),
				fx.Provide(tc.newParser),
				fx.Populate(&parser),
			)
			defer app.Close()

			rawBlock := tc.rawBlock(t)

			ctx := context.Background()
			baseline, err := parser.ParseBlock(ctx, rawBlock)
			require.NoError(err, "ParseBlock must succeed on fixture")
			baseTxs := baseline.GetBitcoin().GetTransactions()

			streamer, ok := parser.(internal.BitcoinStreamer)
			require.True(ok, "parser %T must implement BitcoinStreamer", parser)

			headerBytes := rawBlock.GetBitcoin().GetHeader()
			opener := func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(headerBytes)), nil
			}
			loadGroup := NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions())
			stream := streamer.StreamBlockIter(ctx, opener, loadGroup)

			var streamed []*api.BitcoinTransaction
			for tx, iterErr := range stream.Transactions() {
				require.NoError(iterErr)
				streamed = append(streamed, tx)
			}

			require.Equal(len(baseTxs), len(streamed), "tx count must match between ParseBlock and StreamBlockIter")
			for i, baseTx := range baseTxs {
				s := streamed[i]
				require.Equal(baseTx.TransactionId, s.TransactionId, "%s: tx[%d] id mismatch", tc.name, i)
				require.Equal(baseTx.Hash, s.Hash, "%s: tx[%d] hash mismatch (catches Dash/Zcash backfill gap)", tc.name, i)
				require.Equal(baseTx.InputCount, s.InputCount, "%s: tx[%d] InputCount mismatch", tc.name, i)
				require.Equal(baseTx.OutputCount, s.OutputCount, "%s: tx[%d] OutputCount mismatch", tc.name, i)
			}
		})
	}
}
