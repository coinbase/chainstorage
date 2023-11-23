package sdk

import (
	"context"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type Parser interface {
	ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error)
	GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error)
	ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
	ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error
}
