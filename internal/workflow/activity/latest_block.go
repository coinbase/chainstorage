package activity

import (
	"context"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

type (
	LatestBlock struct {
		baseActivity
		config *config.Config
		logger *zap.Logger
		client gateway.Client
	}

	LatestBlockParams struct {
		fx.In
		fxparams.Params
		Runtime cadence.Runtime
		Client  gateway.Client
	}

	LatestBlockRequest struct {
	}

	LatestBlockResponse struct {
		Height uint64
	}
)

func NewLatestBlock(params LatestBlockParams) *LatestBlock {
	r := &LatestBlock{
		baseActivity: newBaseActivity(ActivityLatestBlock, params.Runtime),
		config:       params.Config,
		client:       params.Client,
	}
	r.register(r.execute)
	return r
}

func (r *LatestBlock) Execute(ctx workflow.Context, request *LatestBlockRequest) (*LatestBlockResponse, error) {
	var response LatestBlockResponse
	err := r.executeActivity(ctx, request, &response)
	return &response, err
}

func (r *LatestBlock) execute(ctx context.Context, request *LatestBlockRequest) (*LatestBlockResponse, error) {
	if err := r.validateRequest(request); err != nil {
		return nil, err
	}

	latestBlock, err := r.client.GetLatestBlock(ctx, &api.GetLatestBlockRequest{})
	if err != nil {
		return nil, xerrors.Errorf("failed to get chainstorage latest block: %w", err)
	}

	var cfg config.ChainConfig
	return &LatestBlockResponse{
		Height: latestBlock.GetHeight() - cfg.IrreversibleDistance,
	}, nil
}
