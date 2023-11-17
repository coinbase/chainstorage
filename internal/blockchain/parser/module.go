package parser

import (
	"go.uber.org/fx"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/bitcoin"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/rosetta"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/solana"
)

type (
	Parser = internal.Parser

	ParityCheckFailedError = internal.ParityCheckFailedError
)

var (
	ErrInvalidChain      = internal.ErrInvalidChain
	ErrNotImplemented    = internal.ErrNotImplemented
	ErrNotFound          = internal.ErrNotFound
	ErrInvalidParameters = internal.ErrInvalidParameters
)

var Module = fx.Options(
	internal.Module,
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
