package parser

import (
	"go.uber.org/fx"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/aptos"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/bitcoin"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/rosetta"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/solana"
)

type (
	Parser = internal.Parser

	// NativeParser is re-exported so callers outside this package can
	// instantiate a chain-specific native parser directly without importing
	// the internal subpackage.
	NativeParser = internal.NativeParser

	ParityCheckFailedError = internal.ParityCheckFailedError

	// ParseOption is re-exported so callers outside this package (including
	// the SDK) can construct options without importing the internal
	// subpackage directly.
	ParseOption = internal.ParseOption

	// NativeStreamedBlock is the chain-agnostic container returned
	// by Parser.ParseStreamNative. Re-exported so SDK consumers can
	// reference it without importing internal.
	NativeStreamedBlock = internal.NativeStreamedBlock

	// BitcoinNativeStream is the bitcoin-family iterator view.
	// Exposed via NativeStreamedBlock.GetBitcoin().
	BitcoinNativeStream = internal.BitcoinNativeStream

	// EthereumNativeStream is the ethereum iterator view. Stub
	// interface until the ethereum streaming walker lands.
	EthereumNativeStream = internal.EthereumNativeStream

	// SolanaNativeStream is the Solana iterator view. Exposed via
	// NativeStreamedBlock.GetSolana().
	SolanaNativeStream = internal.SolanaNativeStream

	// TransactionFilter is the pre-decode keep/skip hook consumed by
	// WithTransactionFilter.
	TransactionFilter = internal.TransactionFilter

	// BitcoinInputTxGroupLoader is re-exported so SDK callers can
	// type their consumer lambdas without importing internal.
	BitcoinInputTxGroupLoader = internal.BitcoinInputTxGroupLoader
)

var (
	ErrInvalidChain      = internal.ErrInvalidChain
	ErrNotImplemented    = internal.ErrNotImplemented
	ErrNotFound          = internal.ErrNotFound
	ErrInvalidParameters = internal.ErrInvalidParameters

	// Parse options (see internal/options.go for behavior).
	WithSkipScripts       = internal.WithSkipScripts
	WithSkipWitnesses     = internal.WithSkipWitnesses
	WithSkipShielded      = internal.WithSkipShielded
	WithTransactionFilter = internal.WithTransactionFilter
)

var Module = fx.Options(
	internal.Module,
	aptos.Module,
	bitcoin.Module,
	ethereum.Module,
	rosetta.Module,
	solana.Module,
)

func ValidateChain(blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	return internal.ValidateChain(blocks, lastBlock)
}

func NewNop() Parser {
	return internal.NewNop()
}
