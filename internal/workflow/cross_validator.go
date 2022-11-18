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
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	CrossValidator struct {
		baseWorkflow
		crossValidator *activity.CrossValidator
	}

	CrossValidatorParams struct {
		fx.In
		fxparams.Params
		Runtime        cadence.Runtime
		CrossValidator *activity.CrossValidator
	}

	CrossValidatorRequest struct {
		StartHeight             uint64
		Tag                     uint32
		ValidationHeightPadding uint64 // Optional. If not specified, it is read from the workflow config.
		BatchSize               uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize          uint64 // Optional. If not specified, it is read from the workflow config.
		Parallelism             int    // Optional. If not specified, it is read from the workflow config.
	}
)

var (
	_ InstrumentedRequest = (*CrossValidatorRequest)(nil)
)

func NewCrossValidator(params CrossValidatorParams) *CrossValidator {
	w := &CrossValidator{
		baseWorkflow:   newBaseWorkflow(&params.Config.Workflows.CrossValidator, params.Runtime),
		crossValidator: params.CrossValidator,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *CrossValidator) Execute(ctx context.Context, request *CrossValidatorRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (w *CrossValidator) execute(ctx workflow.Context, request *CrossValidatorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		var cfg config.CrossValidatorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		startHeight := request.StartHeight
		if startHeight < cfg.ValidationStartHeight {
			return xerrors.Errorf("invalid startHeight=%v, ValidationStartHeight=%v", startHeight, cfg.ValidationStartHeight)
		}

		validationHeightPadding := cfg.IrreversibleDistance * validationHeightPaddingMultiplier
		if request.ValidationHeightPadding > 0 {
			validationHeightPadding = request.ValidationHeightPadding
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		parallelism := cfg.Parallelism
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getScope(ctx).Tagged(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
		})

		for i := 0; i < int(checkpointSize); i++ {
			backoff := workflow.NewTimer(ctx, cfg.BackoffInterval)

			crossValidatorRequest := &activity.CrossValidatorRequest{
				Tag:                     tag,
				StartHeight:             startHeight,
				ValidationHeightPadding: validationHeightPadding,
				MaxHeightsToValidate:    batchSize,
				Parallelism:             parallelism,
				MaxReorgDistance:        cfg.IrreversibleDistance,
			}
			crossValidatorResponse, err := w.crossValidator.Execute(ctx, crossValidatorRequest)
			if err != nil {
				return xerrors.Errorf("failed to execute cross validator: %w", err)
			}

			logger.Info("validated blocks", zap.Reflect("response", crossValidatorResponse))

			startHeight = crossValidatorResponse.EndHeight
			metrics.Gauge("height").Update(float64(startHeight - 1))
			metrics.Gauge("block_gap").Update(float64(crossValidatorResponse.BlockGap))
			if err := backoff.Get(ctx, nil); err != nil {
				return xerrors.Errorf("failed to sleep: %w", err)
			}
		}

		request.StartHeight = startHeight
		return workflow.NewContinueAsNewError(ctx, w.name, request)
	})
}

func (r *CrossValidatorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}
