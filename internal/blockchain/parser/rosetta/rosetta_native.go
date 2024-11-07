package rosetta

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	rosettaNativeParserImpl struct {
		logger *zap.Logger
	}
)

func NewRosettaNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	return &rosettaNativeParserImpl{
		logger: log.WithPackage(params.Logger),
	}, nil
}

func (p *rosettaNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, errors.New("metadata not found")
	}

	blobdata := rawBlock.GetRosetta()
	if blobdata == nil {
		return nil, errors.New("blobdata not found for rosetta")
	}

	rosettaBlock, err := ParseRosettaBlock(blobdata.Header)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rosetta block: %w", err)
	}

	otherTransactions, err := ParseOtherTransactions(blobdata.OtherTransactions)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rosetta other transactions: %w", err)
	}

	if len(otherTransactions) > 0 {
		if rosettaBlock == nil {
			return nil, fmt.Errorf("rosetta block is nil while having other transactions")
		}
		rosettaBlock.Transactions = append(rosettaBlock.Transactions, otherTransactions...)
	}

	return &api.NativeBlock{
		Blockchain:      rawBlock.Blockchain,
		Network:         rawBlock.Network,
		Tag:             metadata.Tag,
		Hash:            metadata.Hash,
		ParentHash:      metadata.ParentHash,
		Height:          metadata.Height,
		ParentHeight:    metadata.ParentHeight,
		Timestamp:       rosettaBlock.GetTimestamp(),
		NumTransactions: uint64(len(rosettaBlock.GetTransactions())),
		Block: &api.NativeBlock_Rosetta{
			Rosetta: rosettaBlock,
		},
	}, nil
}

func (p *rosettaNativeParserImpl) GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return nil, internal.ErrNotImplemented
}
