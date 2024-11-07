package activity

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/utils"
)

type (
	LivenessCheck struct {
		baseActivity
		masterBlockchainClient client.Client // Used as the source-of-truth for resolving the canonical chain. It should be connected to a single-node cluster.
		failoverManager        endpoints.FailoverManager
	}

	LivenessCheckParams struct {
		fx.In
		fxparams.Params
		Runtime                cadence.Runtime
		MasterBlockchainClient client.Client `name:"master"`
		FailoverManager        endpoints.FailoverManager
	}

	LivenessCheckRequest struct {
		Tag                    uint32        `validate:"required"`
		Failover               bool          // Failover will switch master to failover clusters.
		LivenessCheckThreshold time.Duration // LivenessCheckThreshold is the threshold to check if the last block is too old in nodes.
	}

	LivenessCheckResponse struct {
		LivenessCheckViolation bool
	}
)

func NewLivenessCheck(params LivenessCheckParams) *LivenessCheck {
	a := &LivenessCheck{
		baseActivity:           newBaseActivity(ActivityLivenessCheck, params.Runtime),
		masterBlockchainClient: params.MasterBlockchainClient,
		failoverManager:        params.FailoverManager,
	}
	a.register(a.execute)
	return a
}

func (a *LivenessCheck) Execute(ctx workflow.Context, request *LivenessCheckRequest) (*LivenessCheckResponse, error) {
	var response LivenessCheckResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *LivenessCheck) execute(ctx context.Context, request *LivenessCheckRequest) (*LivenessCheckResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}
	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

	if request.Failover {
		failoverCtx, err := a.failoverManager.WithFailoverContext(ctx, endpoints.MasterCluster)
		if err != nil {
			return nil, fmt.Errorf("failed to create failover context for master client: %w", err)
		}
		ctx = failoverCtx
	}

	violation, err := a.checkLiveness(ctx, logger, request.Tag, request.LivenessCheckThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to check node liveness: %w", err)
	}

	return &LivenessCheckResponse{
		LivenessCheckViolation: violation,
	}, nil
}

func (a *LivenessCheck) checkLiveness(
	ctx context.Context,
	logger *zap.Logger,
	tag uint32,
	livenessCheckThreshold time.Duration,
) (bool, error) {
	canonicalChainTipHeight, err := a.masterBlockchainClient.GetLatestHeight(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get canonical chain tip height: %w", err)
	}

	blocks, err := a.masterBlockchainClient.BatchGetBlockMetadata(ctx, tag, canonicalChainTipHeight, canonicalChainTipHeight+1)
	if err != nil {
		return false, fmt.Errorf("failed to get latest block metadata for block=%d: %w", canonicalChainTipHeight, err)
	}

	if len(blocks) != 1 {
		return false, fmt.Errorf("unexpected block metadata length, actual=%d: %w", len(blocks), err)
	}

	violation := false
	nodeTimeSinceLastBlock := utils.SinceTimestamp(blocks[0].GetTimestamp())
	if nodeTimeSinceLastBlock > livenessCheckThreshold {
		violation = true
	}

	logLevel := zap.InfoLevel
	if violation {
		logLevel = zap.WarnLevel
	}

	logger.Log(
		logLevel,
		"node liveness check result",
		zap.Uint64("canonical_chain_tip_height", canonicalChainTipHeight),
		zap.Reflect("block", blocks[0]),
		zap.Duration("time_since_last_block", nodeTimeSinceLastBlock),
		zap.Bool("violate_liveness_check", violation),
	)

	return violation, nil
}
