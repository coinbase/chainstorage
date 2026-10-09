package sdk

import (
	"context"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// ParseOption is re-exported from the parser package so SDK consumers
// can skip fields they will not consume (e.g. bitcoin scripts,
// witnesses, zcash shielded data). See WithSkip* helpers.
type ParseOption = parser.ParseOption

// NativeStreamedBlock is the chain-agnostic view returned by
// Client.StreamNativeBlock. It exposes block metadata plus a Close
// method that releases backing resources (disk spool). Chain-specific
// iteration is available via nil-checkable accessors that mirror
// *api.NativeBlock's oneof shape.
//
// Consumer pattern parallels GetBlock + ParseNativeBlock:
//
//	native, err := client.StreamNativeBlock(ctx, tag, height, hash)
//	if err != nil { return err }
//	defer native.Close()
//
//	if bs := native.GetBitcoin(); bs != nil {
//	    for tx, err := range bs.Transactions() { ... }
//	    header, _ := bs.Header()
//	}
type NativeStreamedBlock = parser.NativeStreamedBlock

// BitcoinNativeStream is the bitcoin-family iterator view. Obtained
// from NativeStreamedBlock.GetBitcoin() when the configured chain is
// bitcoin-family (bitcoin, bitcoincash, litecoin, dash, zcash).
type BitcoinNativeStream = parser.BitcoinNativeStream

// EthereumNativeStream is the ethereum iterator view. Stub interface
// until the ethereum streaming walker lands; GetEthereum() always
// returns nil today.
type EthereumNativeStream = parser.EthereumNativeStream

// SolanaNativeStream is the Solana iterator view. Obtained from
// NativeStreamedBlock.GetSolana() when the configured chain is Solana.
// Pair it with WithTransactionFilter to drop transactions before they
// are decoded, and use RawTransactions() when the consumer wants the
// getBlock JSON element itself rather than the native proto — kept
// elements are then yielded verbatim with no decoding at all:
//
//	native, err := client.StreamNativeBlock(ctx, tag, slot, hash,
//	    sdk.WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
//	        return bytes.Contains(raw, programID), nil
//	    }))
//	if err != nil { return err }
//	defer native.Close()
//	if ss := native.GetSolana(); ss != nil {
//	    for raw, err := range ss.RawTransactions() { ... }   // or ss.Transactions()
//	    header, _ := ss.Header()
//	}
type SolanaNativeStream = parser.SolanaNativeStream

// TransactionFilter is the pre-decode keep/skip hook consumed by
// WithTransactionFilter.
type TransactionFilter = parser.TransactionFilter

var (
	WithSkipScripts       = parser.WithSkipScripts
	WithSkipWitnesses     = parser.WithSkipWitnesses
	WithSkipShielded      = parser.WithSkipShielded
	WithTransactionFilter = parser.WithTransactionFilter
)

// Parser is the chain-agnostic parsing surface for SDK consumers.
// Streaming entry points live on Client (which owns the download
// lifecycle); this interface handles already-materialized *api.Block
// values.
type Parser interface {
	ParseNativeBlock(ctx context.Context, rawBlock *api.Block, opts ...ParseOption) (*api.NativeBlock, error)
	GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error)
	ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
	ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error
}
