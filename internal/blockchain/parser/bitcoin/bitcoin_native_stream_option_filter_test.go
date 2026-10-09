package bitcoin

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

// TestBlockStreamIter_TransactionFilterOption verifies the caller-level
// WithTransactionFilter is honored on bitcoin-family chains rather than
// silently ignored: keeping only the coinbase transaction (the one raw
// element containing a "coinbase" vin key) yields exactly one tx, and
// the header is still produced.
func TestBlockStreamIter_TransactionFilterOption(t *testing.T) {
	require := require.New(t)
	parser, app := newStreamIterParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)
	headerJSON := rawBlock.GetBitcoin().GetHeader()
	loadGroup := NewInMemoryInputTxGroupLoader(rawBlock.GetBitcoin().GetInputTransactions())

	baseline, err := parser.ParseBlock(context.Background(), rawBlock)
	require.NoError(err)
	require.Greater(len(baseline.GetBitcoin().GetTransactions()), 1)

	seen := 0
	opener, _ := openerFor(headerJSON)
	stream := parser.StreamBlockIter(context.Background(), opener, loadGroup,
		internal.WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
			seen++
			return bytes.Contains(raw, []byte(`"coinbase"`)), nil
		}))
	var kept int
	for _, iterErr := range stream.Transactions() {
		require.NoError(iterErr)
		kept++
	}
	require.Equal(len(baseline.GetBitcoin().GetTransactions()), seen, "filter sees every transaction")
	require.Equal(1, kept, "only the coinbase transaction is kept")

	header, err := stream.Header()
	require.NoError(err)
	require.NotEmpty(header.Hash)
}
