package solana

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// The Vote program appears in the account keys of every vote
// transaction, which is the bulk of any mainnet slot — a realistic
// allowlist probe for the filter tests.
const voteProgramID = "Vote111111111111111111111111111111111111111"

// solanaStreamFixtures are the v2 (jsonParsed, tag 2) fixtures the
// whole-block parser is already tested against. Slot values only feed
// header.Slot and are applied identically to both paths.
var solanaStreamFixtures = []struct {
	name string
	path string
	slot uint64
}{
	{name: "241043141", path: "parser/solana/block_241043141_v2.json", slot: 241043141},
	{name: "195545749", path: "parser/solana/block_195545749_v2.json", slot: 195545750},
	{name: "217003034", path: "parser/solana/block_217003034_v2.json", slot: 217003034},
	{name: "220114808", path: "parser/solana/block_220114808_v2.json", slot: 220114808},
}

// newStreamIterParser wires a solana native parser and returns it as
// the concrete impl so tests can reach StreamBlockIter.
func newStreamIterParser(t testing.TB) (*solanaNativeParserImpl, testapp.TestApp) {
	t.Helper()
	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(NewSolanaNativeParser),
		fx.Populate(&parser),
	)
	return parser.(*solanaNativeParserImpl), app
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

func rawSolanaBlock(header []byte, slot uint64) *api.Block {
	return &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 2, Height: slot},
		Blobdata:   &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: header}},
	}
}

func collectStream(t *testing.T, stream BlockStream) []*api.SolanaTransactionV2 {
	t.Helper()
	var txs []*api.SolanaTransactionV2
	for tx, err := range stream.Transactions() {
		require.NoError(t, err)
		txs = append(txs, tx)
	}
	return txs
}

// TestSolanaStreamParseParity runs ParseBlock and StreamBlockIter
// against every v2 fixture and asserts the transaction list, header and
// rewards match exactly. This is the load-bearing test: the streaming
// path must never diverge from the whole-block parser.
func TestSolanaStreamParseParity(t *testing.T) {
	parser, app := newStreamIterParser(t)
	defer app.Close()
	ctx := context.Background()

	for _, fx := range solanaStreamFixtures {
		t.Run(fx.name, func(t *testing.T) {
			require := require.New(t)
			header := fixtures.MustReadFile(fx.path)

			baseline, err := parser.ParseBlock(ctx, rawSolanaBlock(header, fx.slot))
			require.NoError(err, "ParseBlock must succeed on fixture")
			base := baseline.GetSolanaV2()
			require.NotNil(base)

			opener, calls := openerFor(header)
			stream := parser.StreamBlockIter(ctx, opener, fx.slot)
			got := collectStream(t, stream)

			require.Equal(len(base.GetTransactions()), len(got), "transaction count")
			for i := range got {
				require.Truef(proto.Equal(base.GetTransactions()[i], got[i]), "transaction[%d] diverged from ParseBlock", i)
			}

			gotHeader, err := stream.Header()
			require.NoError(err)
			require.True(proto.Equal(base.GetHeader(), gotHeader), "header diverged from ParseBlock")

			gotRewards, err := stream.Rewards()
			require.NoError(err)
			require.Equal(len(base.GetRewards()), len(gotRewards), "rewards count")
			for i := range gotRewards {
				require.Truef(proto.Equal(base.GetRewards()[i], gotRewards[i]), "reward[%d] diverged from ParseBlock", i)
			}

			// Header() and Rewards() after a full iteration are free.
			require.Equal(int32(1), atomic.LoadInt32(calls), "no extra reader open after full iteration")
		})
	}
}

// TestSolanaStream_RawTransactionsVerbatim verifies RawTransactions()
// yields each element byte-for-byte as it appears in the block JSON, in
// order, with the same count as the native path, and still caches the
// tail. This is the contract the bridge readers rely on: they consume
// the getBlock element through find_transaction, not the native proto.
func TestSolanaStream_RawTransactionsVerbatim(t *testing.T) {
	parser, app := newStreamIterParser(t)
	defer app.Close()

	for _, fx := range solanaStreamFixtures {
		t.Run(fx.name, func(t *testing.T) {
			require := require.New(t)
			header := fixtures.MustReadFile(fx.path)
			var rawBlock struct {
				Transactions []json.RawMessage `json:"transactions"`
			}
			require.NoError(json.Unmarshal(header, &rawBlock))

			opener, calls := openerFor(header)
			stream := parser.StreamBlockIter(context.Background(), opener, fx.slot)
			var got []json.RawMessage
			for raw, err := range stream.RawTransactions() {
				require.NoError(err)
				got = append(got, raw)
			}
			require.Equal(len(rawBlock.Transactions), len(got))
			for i := range got {
				require.Truef(bytes.Equal(rawBlock.Transactions[i], got[i]), "transaction[%d] not verbatim", i)
			}
			hdr, err := stream.Header()
			require.NoError(err)
			require.NotEmpty(hdr.GetBlockHash())
			require.Equal(int32(1), atomic.LoadInt32(calls), "tail cached by the raw pass")
		})
	}
}

// TestSolanaStream_RawTransactionsFiltered: the filter applies to the
// raw path and kept elements decode to the same transactions the native
// path yields for the same filter.
func TestSolanaStream_RawTransactionsFiltered(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()
	ctx := context.Background()

	header := fixtures.MustReadFile("parser/solana/block_195545749_v2.json")
	filter := internal.WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
		keep, err := touchesAccount(raw, voteProgramID)
		return !keep, err // non-vote transactions only
	})

	opener, _ := openerFor(header)
	native := collectStream(t, parser.StreamBlockIter(ctx, opener, 195545750, filter))
	require.NotEmpty(native)

	var raws []json.RawMessage
	for raw, err := range parser.StreamBlockIter(ctx, opener, 195545750, filter).RawTransactions() {
		require.NoError(err)
		raws = append(raws, raw)
	}
	require.Equal(len(native), len(raws))
	for i, raw := range raws {
		var tx SolanaTransactionV2
		require.NoError(json.Unmarshal(raw, &tx))
		decoded, err := parser.parseTransactionV2(&tx)
		require.NoError(err)
		require.Truef(proto.Equal(native[i], decoded), "raw[%d] decodes to a different transaction", i)
	}
}

// TestSolanaStream_HeaderBeforeIter exercises the slow path: Header()
// before iteration runs one dedicated pass; the following iteration
// opens the reader again but must not re-derive the tail.
func TestSolanaStream_HeaderBeforeIter(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()
	ctx := context.Background()

	header := fixtures.MustReadFile("parser/solana/block_241043141_v2.json")
	baseline, err := parser.ParseBlock(ctx, rawSolanaBlock(header, 241043141))
	require.NoError(err)

	opener, calls := openerFor(header)
	stream := parser.StreamBlockIter(ctx, opener, 241043141)

	gotHeader, err := stream.Header()
	require.NoError(err)
	require.True(proto.Equal(baseline.GetSolanaV2().GetHeader(), gotHeader))
	require.Equal(int32(1), atomic.LoadInt32(calls))

	rewards, err := stream.Rewards()
	require.NoError(err)
	require.Equal(len(baseline.GetSolanaV2().GetRewards()), len(rewards))
	require.Equal(int32(1), atomic.LoadInt32(calls), "Rewards() shares the tail scan with Header()")

	got := collectStream(t, stream)
	require.Equal(len(baseline.GetSolanaV2().GetTransactions()), len(got))
	require.Equal(int32(2), atomic.LoadInt32(calls), "iteration opens the reader once more")

	again, err := stream.Header()
	require.NoError(err)
	require.True(proto.Equal(gotHeader, again))
	require.Equal(int32(2), atomic.LoadInt32(calls), "cached tail is reused")
}

// TestSolanaStream_BreakEarly verifies that a consumer breaking out of
// the range loop stops the decode without an error and leaves the
// tail unpopulated (so a later Header() pays the dedicated scan).
func TestSolanaStream_BreakEarly(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	// 1,446 transactions: enough to break out of.
	header := fixtures.MustReadFile("parser/solana/block_195545749_v2.json")
	opener, calls := openerFor(header)
	stream := parser.StreamBlockIter(context.Background(), opener, 195545750)

	seen := 0
	for _, err := range stream.Transactions() {
		require.NoError(err)
		seen++
		if seen == 2 {
			break
		}
	}
	require.Equal(2, seen)
	require.Equal(int32(1), atomic.LoadInt32(calls))

	hdr, err := stream.Header()
	require.NoError(err)
	require.NotEmpty(hdr.GetBlockHash())
	require.Equal(int32(2), atomic.LoadInt32(calls), "partial iteration does not cache the tail")
}

// TestSolanaStream_ContextCancel verifies a cancelled context surfaces
// as an iteration error instead of a silent stop.
func TestSolanaStream_ContextCancel(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	header := fixtures.MustReadFile("parser/solana/block_241043141_v2.json")
	opener, _ := openerFor(header)
	stream := parser.StreamBlockIter(ctx, opener, 241043141)

	var gotErr error
	for _, err := range stream.Transactions() {
		if err != nil {
			gotErr = err
			break
		}
	}
	require.ErrorIs(gotErr, context.Canceled)
}

// accountKeysProbe is the minimal shape a filter needs to inspect a raw
// transaction's account keys.
type accountKeysProbe struct {
	Transaction struct {
		Message struct {
			AccountKeys []struct {
				Pubkey string `json:"pubkey"`
			} `json:"accountKeys"`
		} `json:"message"`
	} `json:"transaction"`
}

func touchesAccount(raw json.RawMessage, pubkey string) (bool, error) {
	var probe accountKeysProbe
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false, err
	}
	for _, key := range probe.Transaction.Message.AccountKeys {
		if key.Pubkey == pubkey {
			return true, nil
		}
	}
	return false, nil
}

// TestSolanaStream_TransactionFilter verifies that WithTransactionFilter
// drops rejected transactions before decoding, keeps source order for
// the rest, and leaves the tail intact.
func TestSolanaStream_TransactionFilter(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()
	ctx := context.Background()

	// 1,446 transactions, most of them votes: a realistic mix for the
	// allowlist filter.
	header := fixtures.MustReadFile("parser/solana/block_195545749_v2.json")
	baseline, err := parser.ParseBlock(ctx, rawSolanaBlock(header, 195545750))
	require.NoError(err)
	baseTxs := baseline.GetSolanaV2().GetTransactions()

	// Expected: the ids of the baseline transactions whose raw JSON
	// touches the Vote program, computed from the raw side so the
	// check does not depend on how the parser encodes pubkeys.
	var rawBlock struct {
		Transactions []json.RawMessage `json:"transactions"`
	}
	require.NoError(json.Unmarshal(header, &rawBlock))
	require.Equal(len(baseTxs), len(rawBlock.Transactions))
	var expected []string
	for i, raw := range rawBlock.Transactions {
		keep, err := touchesAccount(raw, voteProgramID)
		require.NoError(err)
		if keep {
			expected = append(expected, baseTxs[i].GetTransactionId())
		}
	}
	require.NotEmpty(expected, "fixture must contain vote transactions")
	require.Less(len(expected), len(baseTxs), "fixture must contain non-vote transactions")

	filterCalls := 0
	stream := parser.StreamBlockIter(ctx, func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(header)), nil
	}, 195545750, internal.WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
		filterCalls++
		return touchesAccount(raw, voteProgramID)
	}))
	got := collectStream(t, stream)

	require.Equal(len(baseTxs), filterCalls, "filter sees every transaction")
	var gotIDs []string
	for _, tx := range got {
		gotIDs = append(gotIDs, tx.GetTransactionId())
	}
	require.Equal(expected, gotIDs, "kept transactions in source order")

	hdr, err := stream.Header()
	require.NoError(err)
	require.True(proto.Equal(baseline.GetSolanaV2().GetHeader(), hdr))
}

// TestSolanaStream_TransactionFilterDropAll: a filter that rejects
// everything yields no transactions and still produces the tail.
func TestSolanaStream_TransactionFilterDropAll(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	header := fixtures.MustReadFile("parser/solana/block_241043141_v2.json")
	opener, calls := openerFor(header)
	stream := parser.StreamBlockIter(context.Background(), opener, 241043141,
		internal.WithTransactionFilter(func(json.RawMessage) (bool, error) { return false, nil }))
	require.Empty(collectStream(t, stream))
	hdr, err := stream.Header()
	require.NoError(err)
	require.NotEmpty(hdr.GetBlockHash())
	require.Equal(int32(1), atomic.LoadInt32(calls))
}

// TestSolanaStream_TransactionFilterError: a filter error aborts the
// stream and names the transaction position.
func TestSolanaStream_TransactionFilterError(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	header := fixtures.MustReadFile("parser/solana/block_241043141_v2.json")
	opener, _ := openerFor(header)
	boom := xerrors.New("boom")
	stream := parser.StreamBlockIter(context.Background(), opener, 241043141,
		internal.WithTransactionFilter(func(json.RawMessage) (bool, error) { return false, boom }))

	var gotErr error
	for _, err := range stream.Transactions() {
		gotErr = err
		break
	}
	require.ErrorIs(gotErr, boom)
	require.Contains(gotErr.Error(), "transaction filter failed at [0]")
}

// TestSolanaStream_MalformedBlock covers the structural error paths.
func TestSolanaStream_MalformedBlock(t *testing.T) {
	parser, app := newStreamIterParser(t)
	defer app.Close()

	cases := []struct {
		name   string
		header string
		errMsg string
	}{
		{name: "not an object", header: `[]`, errMsg: "block start"},
		{name: "transactions not an array", header: `{"blockhash":"x","transactions":{}}`, errMsg: "transactions start"},
		{name: "bad transaction", header: `{"blockhash":"x","transactions":[42]}`, errMsg: "transaction[0]"},
		{name: "missing blockhash", header: `{"blockHeight":1,"transactions":[]}`, errMsg: "block hash is empty"},
		{name: "truncated", header: `{"blockhash":"x","transactions":[`, errMsg: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			opener, _ := openerFor([]byte(tc.header))
			stream := parser.StreamBlockIter(context.Background(), opener, 1)
			var gotErr error
			for _, err := range stream.Transactions() {
				if err != nil {
					gotErr = err
					break
				}
			}
			require.Error(gotErr)
			if tc.errMsg != "" {
				require.Contains(gotErr.Error(), tc.errMsg)
			}
		})
	}
}

// TestSolanaStream_HeaderOnlyIgnoresTransactions: the tail scan must not
// decode transaction elements, so a block with an undecodable
// transaction still yields its header.
func TestSolanaStream_HeaderOnlyIgnoresTransactions(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	header := []byte(`{"blockhash":"abc","blockHeight":7,"parentSlot":6,"previousBlockhash":"prev","blockTime":1700000000,"transactions":[42,{"transaction":null}],"rewards":[{"pubkey":"11111111111111111111111111111111","lamports":5,"postBalance":10,"rewardType":"Fee"}]}`)
	opener, calls := openerFor(header)
	stream := parser.StreamBlockIter(context.Background(), opener, 7)

	hdr, err := stream.Header()
	require.NoError(err)
	require.Equal("abc", hdr.GetBlockHash())
	require.Equal(uint64(7), hdr.GetSlot())
	require.Equal(uint64(6), hdr.GetParentSlot())
	require.Equal(uint64(7), hdr.GetBlockHeight())

	rewards, err := stream.Rewards()
	require.NoError(err)
	require.Len(rewards, 1)
	require.Equal(int64(5), rewards[0].GetLamports())
	require.Equal(int32(1), atomic.LoadInt32(calls))
}

func TestSkipJSONValue_Solana(t *testing.T) {
	require := require.New(t)
	for _, in := range []string{`[1,[2,{"a":[3]}],4]`, `{"a":{"b":[1,2,{"c":null}]}}`, `"s"`, `42`, `null`} {
		dec := json.NewDecoder(bytes.NewReader([]byte(in + ` "tail"`)))
		require.NoError(skipJSONValue(dec), in)
		tok, err := dec.Token()
		require.NoError(err, in)
		require.Equal("tail", tok, in)
	}
}

// Benchmarks: the whole-block parser vs the streaming walker, and the
// walker with a pre-decode filter that keeps only transactions touching
// one program — the shape of a bridge-candidate scan. Run with
//
//	go test ./internal/blockchain/parser/solana/ -run '^$' -bench 'Solana' -benchmem
//
// bytes/op is the allocation volume, not peak RSS; the prod-parity
// harness measures peak heap on real blocks.
func benchHeader(b *testing.B) []byte {
	b.Helper()
	return fixtures.MustReadFile("parser/solana/block_220114808_v2.json")
}

func BenchmarkSolanaParseBlock(b *testing.B) {
	parser, app := newStreamIterParser(b)
	defer app.Close()
	header := benchHeader(b)
	block := rawSolanaBlock(header, 220114808)
	b.SetBytes(int64(len(header)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := parser.ParseBlock(context.Background(), block); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSolanaStreamIter(b *testing.B) {
	parser, app := newStreamIterParser(b)
	defer app.Close()
	header := benchHeader(b)
	opener, _ := openerFor(header)
	b.SetBytes(int64(len(header)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := parser.StreamBlockIter(context.Background(), opener, 220114808)
		for _, err := range stream.Transactions() {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSolanaStreamIterFilterProgram(b *testing.B) {
	parser, app := newStreamIterParser(b)
	defer app.Close()
	header := benchHeader(b)
	opener, _ := openerFor(header)
	// SPL Token program: present in a minority of transactions, the way
	// a bridge program id is. bytes.Contains is the cheapest possible
	// pre-filter; a real allowlist would decode account keys.
	needle := []byte("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
	filter := internal.WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
		return bytes.Contains(raw, needle), nil
	})
	b.SetBytes(int64(len(header)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := parser.StreamBlockIter(context.Background(), opener, 220114808, filter)
		for _, err := range stream.Transactions() {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkSolanaStreamRawFilterProgram is the bridge-indexer shape:
// allowlist filter, kept elements yielded as raw bytes, no native
// decoding at all.
func BenchmarkSolanaStreamRawFilterProgram(b *testing.B) {
	parser, app := newStreamIterParser(b)
	defer app.Close()
	header := benchHeader(b)
	opener, _ := openerFor(header)
	needle := []byte("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
	filter := internal.WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
		return bytes.Contains(raw, needle), nil
	})
	b.SetBytes(int64(len(header)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := parser.StreamBlockIter(context.Background(), opener, 220114808, filter)
		for _, err := range stream.RawTransactions() {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSolanaStreamIterFilterNone(b *testing.B) {
	parser, app := newStreamIterParser(b)
	defer app.Close()
	header := benchHeader(b)
	opener, _ := openerFor(header)
	filter := internal.WithTransactionFilter(func(json.RawMessage) (bool, error) { return false, nil })
	b.SetBytes(int64(len(header)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := parser.StreamBlockIter(context.Background(), opener, 220114808, filter)
		for _, err := range stream.Transactions() {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
