package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func NewMegaethNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Reuse the Ethereum native parser since its an EVM chain, but tolerate MegaETH's
	// duplicate-tx-hash quirk in receipt/tx index validation (see below).
	opts = append(opts, WithReceiptIndexMismatchTolerator(tolerateMegaethDuplicateReceiptIndex))
	return NewEthereumNativeParser(params, opts...)
}

// tolerateMegaethDuplicateReceiptIndex reports whether a receipt/transaction TransactionIndex
// mismatch is explained by the same tx hash appearing multiple times in a MegaETH block (e.g.
// mainnet block 10004831). A MegaETH block can list one tx hash at several indices;
// eth_getTransactionReceipt then returns the single canonical receipt (one index) for every
// occurrence, so positional receipt/tx pairing disagrees on index even though the receipt is
// correct. The caller has already confirmed hash/blockHash/blockNumber match.
//
// The receipt is intentionally NOT mutated: the node only has one receipt for the duplicated
// hash, so the canonical index is the honest value. As a result, for the duplicated tx the
// flattened transaction's TransactionIndex (its block position) may differ from the
// receipt/event-log/token-transfer TransactionIndex (the node's canonical index).
func tolerateMegaethDuplicateReceiptIndex(
	transaction *api.EthereumTransaction,
	receipt *api.EthereumTransactionReceipt,
	transactions []*api.EthereumTransaction,
	txHashCounts map[string]int,
) bool {
	// (a) The hash must genuinely be duplicated in this block (position-independent).
	if txHashCounts[transaction.Hash] <= 1 {
		return false
	}
	// (b) The receipt's index must point at a real slot that holds the SAME hash —
	//     i.e. the canonical occurrence — not an arbitrary/garbage index. Compare in
	//     uint64 BEFORE converting to int to avoid a 64->int overflow producing a
	//     negative/wrapped index.
	if receipt.TransactionIndex >= uint64(len(transactions)) {
		return false
	}
	idx := int(receipt.TransactionIndex)
	return transactions[idx].Hash == transaction.Hash
}
