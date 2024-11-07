package activity

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Reader struct {
		baseActivity
		metaStorage metastorage.MetaStorage
	}

	ReaderParams struct {
		fx.In
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
	}

	ReaderRequest struct {
		Tag         uint32 `validate:"required"`
		Height      uint64
		LatestBlock bool
	}

	ReaderResponse struct {
		// Metadata is set to nil if the block does not exist in meta storage.
		Metadata *api.BlockMetadata
	}
)

func NewReader(params ReaderParams) *Reader {
	a := &Reader{
		baseActivity: newBaseActivity(ActivityReader, params.Runtime),
		metaStorage:  params.MetaStorage,
	}
	a.register(a.execute)
	return a
}

func (a *Reader) Execute(ctx workflow.Context, request *ReaderRequest) (*ReaderResponse, error) {
	var response ReaderResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Reader) execute(ctx context.Context, request *ReaderRequest) (*ReaderResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	metadata, err := a.readBlock(ctx, request)
	if err != nil {
		if errors.Is(err, storage.ErrItemNotFound) {
			// Convert not-found error into an empty response.
			// If the block has not been processed previously, we will skip the continuous-chain validation.
			return &ReaderResponse{}, nil
		}

		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	return &ReaderResponse{
		Metadata: metadata,
	}, nil
}

func (a *Reader) readBlock(ctx context.Context, request *ReaderRequest) (*api.BlockMetadata, error) {
	if request.LatestBlock {
		return a.metaStorage.GetLatestBlock(ctx, request.Tag)
	}

	return a.metaStorage.GetBlockByHeight(ctx, request.Tag, request.Height)
}
