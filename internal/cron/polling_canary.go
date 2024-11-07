package cron

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	PollingCanaryTaskParams struct {
		fx.In
		fxparams.Params
		Client gateway.Client
		Config *config.Config
	}

	pollingCanaryTask struct {
		enabled bool
		client  gateway.Client
		logger  *zap.Logger
		config  *config.Config

		blockRangeSize uint64
		blockchain     common.Blockchain
	}
)

func NewPollingCanary(params PollingCanaryTaskParams) (Task, error) {
	return &pollingCanaryTask{
		enabled:        !params.Config.Cron.DisablePollingCanary,
		client:         params.Client,
		logger:         log.WithPackage(params.Logger),
		config:         params.Config,
		blockRangeSize: params.Config.Cron.BlockRangeSize,
		blockchain:     params.Config.Blockchain(),
	}, nil
}

func (t *pollingCanaryTask) Name() string {
	return "polling_canary"
}

func (t *pollingCanaryTask) Spec() string {
	return "@every 10s"
}

func (t *pollingCanaryTask) Parallelism() int64 {
	return 1
}

func (t *pollingCanaryTask) Enabled() bool {
	return t.enabled
}

func (t *pollingCanaryTask) DelayStartDuration() time.Duration {
	// delay for 10 minutes in case there is new API being added
	return 10 * time.Minute
}

func (t *pollingCanaryTask) Run(ctx context.Context) error {
	const parallelism = 4

	latestBlock, err := t.client.GetLatestBlock(ctx, &api.GetLatestBlockRequest{})
	if err != nil {
		// Skip the task if it is a not-found error.
		var grpcerr gateway.GrpcError
		if errors.As(err, &grpcerr) && grpcerr.GRPCStatus().Code() == codes.NotFound {
			return nil
		}

		return fmt.Errorf("failed to call GetLatestBlock %w", err)
	}

	t.logger.Info(
		"running polling canary task",
		zap.Reflect("latest", latestBlock),
		zap.Uint64("blockRangeSize", t.blockRangeSize),
		zap.Int("parallelism", parallelism),
	)

	tag := latestBlock.GetTag()
	height := latestBlock.GetHeight()
	hash := latestBlock.GetHash()
	endHeight := height + 1
	startHeight := endHeight - t.blockRangeSize
	if endHeight < t.blockRangeSize {
		startHeight = 0
	}

	group, ctx := syncgroup.New(
		ctx,
		syncgroup.WithThrottling(parallelism),
		syncgroup.WithFilter(t.filterError),
	)

	group.Go(func() error {
		if _, err := t.client.GetBlockFile(ctx, &api.GetBlockFileRequest{
			Tag:    tag,
			Height: height,
			Hash:   hash,
		}); err != nil {
			return fmt.Errorf("failed to call GetBlockFile (height=%v, hash=%v): %w", height, hash, err)
		}

		return nil
	})

	group.Go(func() error {
		if _, err := t.client.GetBlockFilesByRange(ctx, &api.GetBlockFilesByRangeRequest{
			Tag:         tag,
			StartHeight: startHeight,
			EndHeight:   endHeight,
		}); err != nil {
			return fmt.Errorf("failed to call GetBlockFilesByRange for blocks [%v, %v): %w", startHeight, endHeight, err)
		}

		return nil
	})

	group.Go(func() error {
		if _, err := t.client.GetRawBlock(ctx, &api.GetRawBlockRequest{
			Tag:    tag,
			Height: height,
			Hash:   hash,
		}); err != nil {
			return fmt.Errorf("failed to call GetRawBlock (height=%v, hash=%v): %w", height, hash, err)
		}

		return nil
	})

	group.Go(func() error {
		if _, err := t.client.GetRawBlocksByRange(ctx, &api.GetRawBlocksByRangeRequest{
			Tag:         tag,
			StartHeight: startHeight,
			EndHeight:   endHeight,
		}); err != nil {
			return fmt.Errorf("failed to call GetRawBlocksByRange for blocks [%v, %v): %w", startHeight, endHeight, err)
		}

		return nil
	})

	group.Go(func() error {
		if _, err := t.client.GetNativeBlock(ctx, &api.GetNativeBlockRequest{
			Tag:    tag,
			Height: height,
			Hash:   hash,
		}); err != nil {
			return fmt.Errorf("failed to call GetNativeBlock (height=%v, hash=%v): %w", height, hash, err)
		}

		return nil
	})

	group.Go(func() error {
		if _, err := t.client.GetNativeBlocksByRange(ctx, &api.GetNativeBlocksByRangeRequest{
			Tag:         tag,
			StartHeight: startHeight,
			EndHeight:   endHeight,
		}); err != nil {
			return fmt.Errorf("failed to call GetNativeBlocksByRange for blocks [%v, %v): %w", startHeight, endHeight, err)
		}

		return nil
	})

	if t.config.Chain.Feature.RosettaParser {
		group.Go(func() error {
			if _, err := t.client.GetRosettaBlock(ctx, &api.GetRosettaBlockRequest{
				Tag:    tag,
				Height: height,
				Hash:   hash,
			}); err != nil {
				return fmt.Errorf("failed to call GetRosettaBlock (height=%v, hash=%v): %w", height, hash, err)
			}

			return nil
		})
	}

	if t.config.Chain.Feature.RosettaParser {
		group.Go(func() error {
			if _, err := t.client.GetRosettaBlocksByRange(ctx, &api.GetRosettaBlocksByRangeRequest{
				Tag:         tag,
				StartHeight: startHeight,
				EndHeight:   endHeight,
			}); err != nil {
				return fmt.Errorf("failed to call GetRosettaBlocksByRange for blocks [%v, %v): %w", startHeight, endHeight, err)
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("failed to finish canary task: %w", err)
	}

	return nil
}

func (t *pollingCanaryTask) filterError(err error) error {
	var grpcErr gateway.GrpcError
	if errors.As(err, &grpcErr) {
		code := grpcErr.GRPCStatus().Code()
		if code == codes.Unimplemented || code == codes.FailedPrecondition {
			return nil
		}
	}

	return err
}
