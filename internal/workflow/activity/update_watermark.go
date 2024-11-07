package activity

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
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
		// validate the chain starting from this block (inclusive)
		ValidateStart uint64
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
		zap.Uint64("validate_since", request.ValidateStart),
		zap.Uint64("height", request.BlockHeight))

	validateStart := request.BlockHeight - 1
	if request.ValidateStart > 0 {
		if request.ValidateStart >= request.BlockHeight {
			return nil, fmt.Errorf("ValidateSince %d should be smaller than BlockHeight %d",
				request.ValidateStart, request.BlockHeight)
		}
		validateStart = request.ValidateStart
	}
	if validateStart <= 0 {
		validateStart = 1
	}
	blocks, err := a.metaStorage.GetBlocksByHeightRange(ctx, tag, validateStart, request.BlockHeight+1)
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks by tag %d: %w", tag, err)
	}
	if len(blocks) > 1 {
		if err := parser.ValidateChain(blocks[1:], blocks[0]); err != nil {
			return nil, fmt.Errorf("failed to validate chain: %w", err)
		}
	}
	err = a.metaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{blocks[len(blocks)-1]}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to set watermark: %w", err)
	}

	return &UpdateWatermarkResponse{BlockHeight: request.BlockHeight}, nil
}
