package client

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/client/aptos"
	"github.com/coinbase/chainstorage/internal/blockchain/client/bitcoin"
	"github.com/coinbase/chainstorage/internal/blockchain/client/ethereum"
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/client/rosetta"
	"github.com/coinbase/chainstorage/internal/blockchain/client/solana"
)

type (
	Client       = internal.Client
	ClientParams = internal.ClientParams
	ClientOption = internal.ClientOption
)

var Module = fx.Options(
	internal.Module,
	aptos.Module,
	bitcoin.Module,
	ethereum.Module,
	rosetta.Module,
	solana.Module,
)

var (
	ErrBlockNotFound  = internal.ErrBlockNotFound
	ErrNotImplemented = internal.ErrNotImplemented
)

func WithBestEffort() ClientOption {
	return internal.WithBestEffort()
}
