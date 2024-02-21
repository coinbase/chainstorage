package workflow

import (
	"context"
	"strconv"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Replicator struct {
		baseWorkflow
		replicator *activity.Replicator
	}

	ReplicatorParams struct {
		fx.In
		fxparams.Params
		Runtime    cadence.Runtime
		Replicator *activity.Replicator
	}

	ReplicatorRequest struct {
		Tag             uint32
		StartHeight     uint64
		EndHeight       uint64 `validate:"gt=0,gtfield=StartHeight"`
		UpdateWatermark bool
		DataCompression string // Optional. If not specified, it is read from the workflow config.
		BatchSize       uint64 // Optional. If not specified, it is read from the workflow config.
		Parallelism     int    // Optional. If not specified, it is read from the workflow config.
	}
)

// GetTags implements InstrumentedRequest.
func (r *ReplicatorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}

var (
	_ InstrumentedRequest = (*ReplicatorRequest)(nil)
)

func NewReplicator(params ReplicatorParams) *Replicator {
	w := &Replicator{
		baseWorkflow: newBaseWorkflow(&params.Config.Workflows.Replicator, params.Runtime),
		replicator:   params.Replicator,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Replicator) Execute(ctx context.Context, request *ReplicatorRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (w *Replicator) execute(ctx workflow.Context, request *ReplicatorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}

		var cfg config.ReplicatorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		parallelism := cfg.Parallelism
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		var dataCompression api.Compression
		var err error
		dataCompression = cfg.Storage.DataCompression
		if request.DataCompression != "" {
			dataCompression, err = utils.ParseCompression(request.DataCompression)
			if err != nil {
				return xerrors.Errorf("failed to parse data compression: %w", err)
			}
		}

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		logger.Info("workflow started", zap.Uint64("batchSize", batchSize))
		ctx = w.withActivityOptions(ctx)

		for startHeight, endHeight := request.StartHeight, request.StartHeight+batchSize; startHeight < request.EndHeight; startHeight, endHeight = startHeight+batchSize, endHeight+batchSize {
			endHeight := endHeight
			if endHeight > request.EndHeight {
				endHeight = request.EndHeight
			}
			_, err := w.replicator.Execute(ctx, &activity.ReplicatorRequest{
				StartBlockHeight: startHeight,
				EndBlockHeight:   endHeight,
				Parallelism:      parallelism,
				Tag:              tag,
				UpdateWatermark:  request.UpdateWatermark,
				Compression:      dataCompression,
			})
			if err != nil {
				return xerrors.Errorf("failed to replicate blocks from %v-%v: %w", startHeight, endHeight, err)
			}
		}

		logger.Info("workflow finished")

		return nil
	})
}
