package internal

import (
	"context"
	"encoding/json"
	"io"
	"iter"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// NativeStreamedBlock is the chain-agnostic interface returned by
// Parser.ParseStreamNative. It exposes block metadata plus a Close
// method that releases backing resources (most notably the
// decompressed spool file in the downloader layer), and nil-checkable
// per-chain accessors that mirror *api.NativeBlock's oneof shape.
//
// Callers pick the accessor matching the chain their code handles. If
// the configured chain does not yet support streaming (or if the
// block's chain does not match), all Get<Chain>() accessors return
// nil and callers should fall back to GetBlock + ParseNativeBlock.
type NativeStreamedBlock interface {
	// GetMetadata returns the block metadata from the downloaded envelope.
	GetMetadata() *api.BlockMetadata
	// Close releases backing resources. Safe to call multiple times.
	Close() error
	// GetBitcoin returns a bitcoin-family stream view, or nil when the
	// configured chain is not bitcoin-family.
	GetBitcoin() BitcoinNativeStream
	// GetEthereum returns an ethereum stream view. Always nil today;
	// will be populated once the ethereum streaming walker lands.
	GetEthereum() EthereumNativeStream
	// GetSolana returns a Solana stream view, or nil when the
	// configured chain is not Solana.
	GetSolana() SolanaNativeStream
}

// BitcoinNativeStream is the bitcoin-family iterator view. Yielded
// transactions match the output of Parser.ParseNativeBlock's
// Block.Bitcoin.Transactions exactly (parity enforced by
// bitcoin_native_stream_parity_test).
//
// See the bitcoin parser package for the "free after iteration"
// Header() ordering contract.
type BitcoinNativeStream interface {
	// Transactions yields each decoded transaction in the block.
	Transactions() iter.Seq2[*api.BitcoinTransaction, error]
	// Header returns the block header, lazily populated.
	Header() (*api.BitcoinHeader, error)
}

// EthereumNativeStream is the ethereum iterator view. Stub interface
// until the ethereum streaming walker is implemented; today
// NativeStreamedBlock.GetEthereum() always returns nil.
type EthereumNativeStream interface {
	Transactions() iter.Seq2[*api.EthereumTransaction, error]
	Header() (*api.EthereumHeader, error)
}

// SolanaNativeStream is the Solana iterator view over one slot's
// getBlock JSON. Yielded transactions match the output of
// Parser.ParseNativeBlock's Block.SolanaV2.Transactions exactly, in
// source order (parity enforced by solana_native_stream_test); with a
// WithTransactionFilter option, rejected transactions are dropped
// before native decoding.
//
// See the solana parser package for the "free after iteration"
// Header() / Rewards() ordering contract.
type SolanaNativeStream interface {
	// Transactions yields each decoded transaction in the block.
	Transactions() iter.Seq2[*api.SolanaTransactionV2, error]
	// RawTransactions yields each transaction's getBlock JSON element
	// verbatim — after WithTransactionFilter, before any native decoding.
	// A kept transaction costs one copy and no conversion, and the bytes
	// are exactly what a getBlock consumer (find_transaction on the block
	// JSON) expects, so an indexer can persist them without a proto
	// round-trip. Shares the tail-caching contract with Transactions().
	RawTransactions() iter.Seq2[json.RawMessage, error]
	// Header returns the block header, lazily populated.
	Header() (*api.SolanaHeader, error)
	// Rewards returns the block-level rewards, lazily populated.
	Rewards() ([]*api.SolanaReward, error)
}

// BitcoinInputTxGroupLoader returns the prev-output transactions for
// the i-th block transaction. See bitcoin.InputTxGroupLoader for
// details.
type BitcoinInputTxGroupLoader func(i int) (*api.RepeatedBytes, error)

// BitcoinStreamer identifies bitcoin-family NativeParser implementations
// that support streaming via StreamBlockIter. The generic Parser impl
// type-asserts its underlying NativeParser to BitcoinStreamer; non-
// bitcoin chains don't implement this and fall back to leaving
// NativeStreamedBlock.GetBitcoin() nil.
type BitcoinStreamer interface {
	StreamBlockIter(
		ctx context.Context,
		openReader func() (io.ReadCloser, error),
		loadGroup BitcoinInputTxGroupLoader,
		opts ...ParseOption,
	) BitcoinNativeStream
}

// SolanaStreamer identifies the Solana NativeParser implementation that
// supports streaming via StreamBlockIter. The generic Parser impl
// type-asserts its underlying NativeParser to SolanaStreamer; other
// chains leave NativeStreamedBlock.GetSolana() nil.
//
// slot is the block height from the envelope metadata. It is the one
// header field the getBlock JSON does not carry, so the streamer takes
// it explicitly (ParseBlock reads it from Block.Metadata the same way).
type SolanaStreamer interface {
	StreamBlockIter(
		ctx context.Context,
		openReader func() (io.ReadCloser, error),
		slot uint64,
		opts ...ParseOption,
	) SolanaNativeStream
}
