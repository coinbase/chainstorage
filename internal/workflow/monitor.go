package workflow

import (
	"context"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	Monitor struct {
		baseWorkflow
		validator *activity.Validator
	}

	MonitorParams struct {
		fx.In
		fxparams.Params
		Runtime   cadence.Runtime
		Validator *activity.Validator
	}

	MonitorRequest struct {
		StartHeight               uint64
		Tag                       uint32
		StartEventId              int64  // Optional. If not specified or less than metastorage.EventIdStartValue, it will be set as metastorage.EventIdStartValue.
		ValidationHeightPadding   uint64 // Optional. If not specified, it is read from the workflow config.
		BatchSize                 uint64 // Optional. If not specified, it is read from the workflow config.
		EventBatchSize            uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize            uint64 // Optional. If not specified, it is read from the workflow config.
		BackoffIntervalSecs       uint64 // Optional. If not specified, it is read from the workflow config.
		Parallelism               int    // Optional. If not specified, it is read from the workflow config.
		ValidatorMaxReorgDistance uint64 // Optional. If not specified, it is read from the workflow config.
		EventTag                  uint32 // Optional.
		Failover                  bool   // Optional. If not specified, it is set as false.
	}
)

const (
	validationHeightPaddingMultiplier = 2
)

var (
	_ InstrumentedRequest = (*MonitorRequest)(nil)
)

func NewMonitor(params MonitorParams) *Monitor {
	w := &Monitor{
		baseWorkflow: newBaseWorkflow(&params.Config.Workflows.Monitor, params.Runtime),
		validator:    params.Validator,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Monitor) Execute(ctx context.Context, request *MonitorRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (w *Monitor) execute(ctx workflow.Context, request *MonitorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		var cfg config.MonitorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		startEventId := request.StartEventId
		if startEventId == 0 || startEventId < metastorage.EventIdStartValue {
			startEventId = metastorage.EventIdStartValue
		}

		validationHeightPadding := cfg.IrreversibleDistance * validationHeightPaddingMultiplier
		if request.ValidationHeightPadding > 0 {
			validationHeightPadding = request.ValidationHeightPadding
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		eventBatchSize := cfg.BatchSize
		if request.EventBatchSize > 0 {
			eventBatchSize = request.EventBatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		backoffInterval := cfg.BackoffInterval
		if request.BackoffIntervalSecs > 0 {
			backoffInterval = time.Duration(request.BackoffIntervalSecs) * time.Second
		}

		parallelism := cfg.Parallelism
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		validatorMaxReorgDistance := cfg.IrreversibleDistance
		if request.ValidatorMaxReorgDistance > 0 {
			validatorMaxReorgDistance = request.ValidatorMaxReorgDistance
		}

		eventTag := cfg.GetEffectiveEventTag(request.EventTag)

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getScope(ctx).Tagged(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
			tagEventTag: strconv.Itoa(int(eventTag)),
		})

		failover := request.Failover

		startHeight := request.StartHeight
		for i := 0; i < int(checkpointSize); i++ {
			backoff := workflow.NewTimer(ctx, backoffInterval)

			if failover {
				metrics.Gauge(failoverMetricName).Update(1)
			} else {
				metrics.Gauge(failoverMetricName).Update(0)
			}

			validatorRequest := &activity.ValidatorRequest{
				Tag:                       tag,
				MaxHeightsToValidate:      batchSize,
				StartHeight:               startHeight,
				ValidationHeightPadding:   validationHeightPadding,
				StartEventId:              startEventId,
				MaxEventsToValidate:       eventBatchSize,
				Parallelism:               parallelism,
				ValidatorMaxReorgDistance: validatorMaxReorgDistance,
				EventTag:                  eventTag,
				Failover:                  failover,
			}
			validatorResponse, err := w.validator.Execute(ctx, validatorRequest)
			if err != nil {
				// There are three conditions to enable failover:
				// 1. Failover enabled in config file and chain needs to have failover endpoint configured in config service
				// 2. request.failover = false
				// 3. The error has to be coming from node provider, specifically from two calls(masterClient.GetLatestHeight, slaveClient.BatchGetBlockMetadata)
				// in validator, since monitor could error out with various reasons and we don't want to failover in those situations.
				if cfg.FailoverEnabled && !failover && IsNodeProviderFailed(err) {
					request.Failover = true
					return workflow.NewContinueAsNewError(ctx, w.name, request)
				}

				return xerrors.Errorf("failed to execute validator: %w", err)
			}
			logger.Info("validated blocks", zap.Reflect("response", validatorResponse))
			startHeight = validatorResponse.LastValidatedHeight
			metrics.Gauge("height").Update(float64(validatorResponse.LastValidatedHeight))
			metrics.Gauge("block_gap").Update(float64(validatorResponse.BlockGap))
			if validatorResponse.LastValidatedEventId != 0 {
				// only need to update when we did validate events
				startEventId = validatorResponse.LastValidatedEventId
				metrics.Gauge("event_height").Update(float64(validatorResponse.LastValidatedEventHeight))
				metrics.Gauge("event_id").Update(float64(validatorResponse.LastValidatedEventId))
				metrics.Gauge("event_gap").Update(float64(validatorResponse.EventGap))
			}
			if err := backoff.Get(ctx, nil); err != nil {
				return xerrors.Errorf("failed to sleep: %w", err)
			}
		}

		request.StartHeight = startHeight
		request.StartEventId = startEventId
		return workflow.NewContinueAsNewError(ctx, w.name, request)
	})
}

func (r *MonitorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
		tagEventTag: strconv.Itoa(int(r.EventTag)),
	}
}
