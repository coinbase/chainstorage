package bitcoin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func decoderAt(s string) *json.Decoder {
	return json.NewDecoder(strings.NewReader(s))
}

// newStreamIterParser wires a bitcoin native parser and returns it as the
// concrete impl so tests can reach StreamBlockIter.
func newStreamIterParser(t *testing.T) (*bitcoinNativeParserImpl, testapp.TestApp) {
	t.Helper()
	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&parser),
	)
	return parser.(*bitcoinNativeParserImpl), app
}

// openerFor returns an openReader closure over the given bytes plus a
// counter that records how many times the opener was invoked.
func openerFor(data []byte) (func() (io.ReadCloser, error), *int32) {
	var calls int32
	return func() (io.ReadCloser, error) {
		atomic.AddInt32(&calls, 1)
		return io.NopCloser(bytes.NewReader(data)), nil
	}, &calls
}

// TestBlockStreamIter_IterateThenHeader exercises the free path: Header()
// is called after iteration, so no extra reader open.
func TestBlockStreamIter_IterateThenHeader(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)
	headerJSON := rawBlock.GetBitcoin().GetHeader()

	baseline, err := parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	opener, calls := openerFor(headerJSON)
	stream := parser.StreamBlockIter(context.Background(), opener, NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions()))

	var count int
	for tx, err := range stream.Transactions() {
		require.NoError(err)
		require.NotNil(tx)
		count++
	}
	require.Equal(len(baseline.GetBitcoin().GetTransactions()), count)

	// Header is free now: no new opener call.
	before := atomic.LoadInt32(calls)
	header, err := stream.Header()
	require.NoError(err)
	require.Equal(atomic.LoadInt32(calls), before, "Header after iteration must not open the reader again")

	require.Equal(baseline.GetBitcoin().GetHeader().Hash, header.Hash)
	require.Equal(baseline.GetBitcoin().GetHeader().Height, header.Height)
	require.Equal(baseline.GetBitcoin().GetHeader().MerkleRoot, header.MerkleRoot)

	// Calling Header() again hits the cache — still no new opens.
	header2, err := stream.Header()
	require.NoError(err)
	require.Equal(atomic.LoadInt32(calls), before)
	require.Equal(header.Hash, header2.Hash)
}

// TestBlockStreamIter_HeaderBeforeIter exercises the slow path: Header()
// is called first, paying for a dedicated header-only scan.
func TestBlockStreamIter_HeaderBeforeIter(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)
	headerJSON := rawBlock.GetBitcoin().GetHeader()

	baseline, err := parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	opener, calls := openerFor(headerJSON)
	stream := parser.StreamBlockIter(context.Background(), opener, NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions()))

	header, err := stream.Header()
	require.NoError(err)
	require.Equal(int32(1), atomic.LoadInt32(calls), "Header-first path opens the reader once for the scan")
	require.Equal(baseline.GetBitcoin().GetHeader().Hash, header.Hash)
	require.Equal(baseline.GetBitcoin().GetHeader().Height, header.Height)
	require.Equal(baseline.GetBitcoin().GetHeader().MerkleRoot, header.MerkleRoot)

	// A second Header() call hits the cache; no extra open.
	header2, err := stream.Header()
	require.NoError(err)
	require.Equal(int32(1), atomic.LoadInt32(calls))
	require.Equal(header.Hash, header2.Hash)

	// Iteration opens the reader a second time (full pass), yields all txs.
	var count int
	for tx, iterErr := range stream.Transactions() {
		require.NoError(iterErr)
		require.NotNil(tx)
		count++
	}
	require.Equal(int32(2), atomic.LoadInt32(calls), "Iteration opens reader again after header-only scan")
	require.Equal(len(baseline.GetBitcoin().GetTransactions()), count)
}

// TestBlockStreamIter_HeaderOnly exercises pure header retrieval with
// no iteration.
func TestBlockStreamIter_HeaderOnly(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)
	headerJSON := rawBlock.GetBitcoin().GetHeader()

	baseline, err := parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	opener, calls := openerFor(headerJSON)
	stream := parser.StreamBlockIter(context.Background(), opener, NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions()))

	header, err := stream.Header()
	require.NoError(err)
	require.Equal(int32(1), atomic.LoadInt32(calls))
	require.Equal(baseline.GetBitcoin().GetHeader().Hash, header.Hash)
	require.Equal(baseline.GetBitcoin().GetHeader().Height, header.Height)
}

// TestBlockStreamIter_BreakEarly confirms that early termination does not
// cache a header (iteration was partial, so we re-scan).
func TestBlockStreamIter_BreakEarly(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)
	headerJSON := rawBlock.GetBitcoin().GetHeader()

	opener, calls := openerFor(headerJSON)
	stream := parser.StreamBlockIter(context.Background(), opener, NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions()))

	var count int
	for tx, iterErr := range stream.Transactions() {
		require.NoError(iterErr)
		require.NotNil(tx)
		count++
		if count == 5 {
			break
		}
	}
	require.Equal(5, count)
	require.Equal(int32(1), atomic.LoadInt32(calls))

	// Header was NOT captured (iteration was partial); slow path runs.
	header, err := stream.Header()
	require.NoError(err)
	require.NotNil(header)
	require.Equal(int32(2), atomic.LoadInt32(calls), "break-early iteration does not cache header; Header() pays for a scan")
}

// TestBlockStreamIter_ParityWithParseBlock checks field-by-field equivalence
// on all transactions between the iter path and ParseBlock.
func TestBlockStreamIter_ParityWithParseBlock(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)
	headerJSON := rawBlock.GetBitcoin().GetHeader()

	baseline, err := parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)

	opener, _ := openerFor(headerJSON)
	stream := parser.StreamBlockIter(context.Background(), opener, NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions()))

	var streamed []*api.BitcoinTransaction
	for tx, iterErr := range stream.Transactions() {
		require.NoError(iterErr)
		streamed = append(streamed, tx)
	}

	baseTxs := baseline.GetBitcoin().GetTransactions()
	require.Equal(len(baseTxs), len(streamed))
	for i := range baseTxs {
		require.Equal(baseTxs[i].TransactionId, streamed[i].TransactionId)
		require.Equal(baseTxs[i].Hash, streamed[i].Hash)
		require.Equal(baseTxs[i].InputCount, streamed[i].InputCount)
		require.Equal(baseTxs[i].OutputCount, streamed[i].OutputCount)
		require.Equal(baseTxs[i].Fee, streamed[i].Fee)
	}
}

// TestBlockStreamIter_OptionsPassthrough confirms that ParseOption args
// are forwarded into the underlying StreamBlock.
func TestBlockStreamIter_OptionsPassthrough(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)
	headerJSON := rawBlock.GetBitcoin().GetHeader()

	opener, _ := openerFor(headerJSON)
	stream := parser.StreamBlockIter(context.Background(), opener, NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions()),
		internal.WithSkipScripts(), internal.WithSkipWitnesses())

	for tx, iterErr := range stream.Transactions() {
		require.NoError(iterErr)
		require.Empty(tx.Hex, "WithSkipScripts should empty tx.Hex")
		for _, in := range tx.Inputs {
			if in.ScriptSignature != nil {
				require.Empty(in.ScriptSignature.Assembly)
				require.Empty(in.ScriptSignature.Hex)
			}
			require.Empty(in.TransactionInputWitnesses, "WithSkipWitnesses")
		}
		for _, out := range tx.Outputs {
			if out.ScriptPublicKey != nil {
				require.Empty(out.ScriptPublicKey.Assembly)
				require.Empty(out.ScriptPublicKey.Hex)
			}
		}
	}

	header, err := stream.Header()
	require.NoError(err)
	require.NotEmpty(header.Hash)
}

// TestSkipJSONValue covers the JSON-depth skipper used by
// parseBitcoinHeaderSkippingTx.
func TestSkipJSONValue(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"scalar_string", `"foo"`},
		{"scalar_number", `42`},
		{"scalar_bool", `true`},
		{"scalar_null", `null`},
		{"empty_array", `[]`},
		{"empty_object", `{}`},
		{"flat_array", `[1, 2, 3]`},
		{"nested_object", `{"a": 1, "b": {"c": 2}, "d": [3, 4, {"e": 5}]}`},
		{"deeply_nested", `[[[[1, 2, 3]]]]`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Wrap the value in an envelope so we can verify the decoder
			// is positioned correctly after skipJSONValue.
			in := `{"skip":` + c.in + `,"keep":"sentinel"}`
			dec := decoderAt(in)

			// Consume '{' and "skip".
			_, err := dec.Token()
			require.NoError(t, err)
			_, err = dec.Token()
			require.NoError(t, err)

			require.NoError(t, skipJSONValue(dec))

			// The next key should be "keep".
			tok, err := dec.Token()
			require.NoError(t, err)
			require.Equal(t, "keep", tok)
		})
	}
}
