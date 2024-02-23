package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	UpdateWatermark struct {
		baseActivity
		config      *config.Config
		metaStorage metastorage.MetaStorage
	}

	UpdateWatermarkParams struct {
		fx.In
		fxparams.Params
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
	}

	UpdateWatermarkRequest struct {
		Tag         uint32
		BlockHeight uint64
	}

	UpdateWatermarkResponse struct {
		BlockHeight uint64
	}
)

func NewUpdateWatermark(params UpdateWatermarkParams) *UpdateWatermark {
	a := &UpdateWatermark{
		baseActivity: newBaseActivity(ActivityUpdateWatermark, params.Runtime),
		config:       params.Config,
		metaStorage:  params.MetaStorage,
	}
	a.register(a.execute)
	return a
}

func (a *UpdateWatermark) Execute(ctx workflow.Context, request *UpdateWatermarkRequest) (*UpdateWatermarkResponse, error) {
	var response UpdateWatermarkResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *UpdateWatermark) execute(ctx context.Context, request *UpdateWatermarkRequest) (*UpdateWatermarkResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}
	logger := a.getLogger(ctx).With(zap.Reflect("request", request))
	tag := a.config.GetEffectiveBlockTag(request.Tag)
	logger.Info("Updating watermark",
		zap.Uint32("tag", tag),
		zap.Uint64("height", request.BlockHeight))

	block, err := a.metaStorage.GetBlockByHeight(ctx, tag, request.BlockHeight)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block by tag %d height %d: %w", tag, request.BlockHeight, err)
	}
	err = a.metaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block}, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to set watermark: %w", err)
	}

	return &UpdateWatermarkResponse{BlockHeight: request.BlockHeight}, nil
}
