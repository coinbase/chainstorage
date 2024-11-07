package activity

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Loader struct {
		baseActivity
		metaStorage metastorage.MetaStorage
	}

	LoaderParams struct {
		fx.In
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
	}

	LoaderRequest struct {
		Metadata        []*api.BlockMetadata `validate:"required"`
		LastBlock       *api.BlockMetadata
		UpdateWatermark bool
	}

	LoaderResponse struct {
	}
)

func NewLoader(params LoaderParams) *Loader {
	a := &Loader{
		baseActivity: newBaseActivity(ActivityLoader, params.Runtime),
		metaStorage:  params.MetaStorage,
	}
	a.register(a.execute)
	return a
}

func (a *Loader) Execute(ctx workflow.Context, request *LoaderRequest) (*LoaderResponse, error) {
	var response LoaderResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Loader) execute(ctx context.Context, request *LoaderRequest) (*LoaderResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx)

	if err := a.metaStorage.PersistBlockMetas(ctx, request.UpdateWatermark, request.Metadata, request.LastBlock); err != nil {
		return nil, fmt.Errorf("failed to persist blockMetadata: %w", err)
	}

	response := &LoaderResponse{}
	logger.Info(
		"loaded block into storage",
		zap.Int("num_blocks", len(request.Metadata)),
		zap.Reflect("response", response),
	)
	return response, nil
}
