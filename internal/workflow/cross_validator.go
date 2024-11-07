package workflow

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

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
		BackoffInterval         string // Optional. If not specified, it is read from the workflow config.
	}
)

var (
	_ InstrumentedRequest = (*CrossValidatorRequest)(nil)
)

const (
	// crossValidator metrics. need to have `workflow.cross_validator` as prefix
	crossValidatorHeightGauge   = "workflow.cross_validator.height"
	crossValidatorBlockGapGauge = "workflow.cross_validator.block_gap"
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
			return fmt.Errorf("failed to read config: %w", err)
		}

		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		startHeight := request.StartHeight
		if startHeight < cfg.ValidationStartHeight {
			return fmt.Errorf("invalid startHeight=%v, ValidationStartHeight=%v", startHeight, cfg.ValidationStartHeight)
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

		var err error
		backoffInterval := cfg.BackoffInterval
		if request.BackoffInterval != "" {
			backoffInterval, err = time.ParseDuration(request.BackoffInterval)
			if err != nil {
				return fmt.Errorf("failed to parse BackoffInterval=%v: %w", request.BackoffInterval, err)
			}
		}
		zeroBackoff := backoffInterval == 0

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
		})

		var backoff workflow.Future
		for i := 0; i < int(checkpointSize); i++ {
			if !zeroBackoff {
				backoff = workflow.NewTimer(ctx, backoffInterval)
			}

			crossValidatorRequest := &activity.CrossValidatorRequest{
				Tag:                     tag,
				StartHeight:             startHeight,
				ValidationHeightPadding: validationHeightPadding,
				MaxHeightsToValidate:    batchSize,
				Parallelism:             parallelism,
			}
			crossValidatorResponse, err := w.crossValidator.Execute(ctx, crossValidatorRequest)
			if err != nil {
				return fmt.Errorf("failed to execute cross validator: %w", err)
			}

			logger.Info("validated blocks", zap.Reflect("response", crossValidatorResponse))
			startHeight = crossValidatorResponse.EndHeight
			metrics.Gauge(crossValidatorHeightGauge).Update(float64(startHeight - 1))
			metrics.Gauge(crossValidatorBlockGapGauge).Update(float64(crossValidatorResponse.BlockGap))

			if !zeroBackoff {
				if err := backoff.Get(ctx, nil); err != nil {
					return fmt.Errorf("failed to sleep: %w", err)
				}
			}
		}

		request.StartHeight = startHeight
		return w.continueAsNew(ctx, request)
	})
}

func (r *CrossValidatorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}
