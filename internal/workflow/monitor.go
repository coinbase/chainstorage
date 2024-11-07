package workflow

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

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
		StartHeight             uint64
		Tag                     uint32
		StartEventId            int64  // Optional. If not specified or less than metastorage.EventIdStartValue, it will be set as metastorage.EventIdStartValue.
		ValidationHeightPadding uint64 // Optional. If not specified, it is read from the workflow config.
		BatchSize               uint64 // Optional. If not specified, it is read from the workflow config.
		EventBatchSize          uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize          uint64 // Optional. If not specified, it is read from the workflow config.
		BackoffInterval         string // Optional. If not specified, it is read from the workflow config.
		Parallelism             int    // Optional. If not specified, it is read from the workflow config.
		EventTag                uint32 // Optional.
		Failover                bool   // Optional. If not specified, it is set as false.
	}
)

const (
	validationHeightPaddingMultiplier = 4

	// monitor metrics. need to have `workflow.monitor` as prefix
	monitorBlockGapHealthCounter = "workflow.monitor.block_gap_health"
	monitorEventGapHealthCounter = "workflow.monitor.event_gap_health"
	monitorEventHeightGauge      = "workflow.monitor.event_height"
	monitorEventIdGauge          = "workflow.monitor.event_id"
	monitorFailoverGauge         = "workflow.monitor.failover"
	monitorHeightGauge           = "workflow.monitor.height"
	monitorBlockGapGauge         = "workflow.monitor.block_gap"
	monitorEventGapGauge         = "workflow.monitor.event_gap"
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
	workflowID := w.name
	if request.Tag != 0 {
		workflowID = fmt.Sprintf("%s/block_tag=%d", w.name, request.Tag)
	}
	return w.startWorkflow(ctx, workflowID, request)
}

func (w *Monitor) execute(ctx workflow.Context, request *MonitorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		var cfg config.MonitorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return fmt.Errorf("failed to read config: %w", err)
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

		var err error
		backoffInterval := cfg.BackoffInterval
		if request.BackoffInterval != "" {
			backoffInterval, err = time.ParseDuration(request.BackoffInterval)
			if err != nil {
				return fmt.Errorf("failed to parse BackoffInterval=%v: %w", request.BackoffInterval, err)
			}
		}
		zeroBackoff := backoffInterval == 0

		parallelism := cfg.Parallelism
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		eventTag := cfg.GetEffectiveEventTag(request.EventTag)

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
			tagEventTag: strconv.Itoa(int(eventTag)),
		})

		failover := request.Failover

		var backoff workflow.Future
		startHeight := request.StartHeight
		for i := 0; i < int(checkpointSize); i++ {
			if !zeroBackoff {
				backoff = workflow.NewTimer(ctx, backoffInterval)
			}

			if failover {
				metrics.Gauge(monitorFailoverGauge).Update(1)
			} else {
				metrics.Gauge(monitorFailoverGauge).Update(0)
			}

			validatorRequest := &activity.ValidatorRequest{
				Tag:                     tag,
				MaxHeightsToValidate:    batchSize,
				StartHeight:             startHeight,
				ValidationHeightPadding: validationHeightPadding,
				StartEventId:            startEventId,
				MaxEventsToValidate:     eventBatchSize,
				Parallelism:             parallelism,
				EventTag:                eventTag,
				Failover:                failover,
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
					return w.continueAsNew(ctx, request)
				}

				return fmt.Errorf("failed to execute validator: %w", err)
			}
			logger.Info("validated blocks", zap.Reflect("response", validatorResponse))
			lastValidatedHeight := validatorResponse.LastValidatedHeight
			startHeight = lastValidatedHeight
			metrics.Gauge(monitorHeightGauge).Update(float64(lastValidatedHeight))
			blockGap := validatorResponse.BlockGap
			metrics.Gauge(monitorBlockGapGauge).Update(float64(blockGap))

			newGapCounter := func(counterName string, resultType string) tally.Counter {
				return metrics.WithTags(map[string]string{
					resultTypeTag: resultType,
				}).Counter(counterName)
			}
			if blockGap > cfg.BlockGapLimit {
				newGapCounter(monitorBlockGapHealthCounter, resultTypeError).Inc(1)
			} else {
				newGapCounter(monitorBlockGapHealthCounter, resultTypeSuccess).Inc(1)
			}

			if validatorResponse.LastValidatedEventId != 0 {
				// only need to update when we did validate events
				startEventId = validatorResponse.LastValidatedEventId
				metrics.Gauge(monitorEventHeightGauge).Update(float64(validatorResponse.LastValidatedEventHeight))
				metrics.Gauge(monitorEventIdGauge).Update(float64(validatorResponse.LastValidatedEventId))

				eventGap := validatorResponse.EventGap
				metrics.Gauge(monitorEventGapGauge).Update(float64(eventGap))
				if eventGap > cfg.EventGapLimit {
					newGapCounter(monitorEventGapHealthCounter, resultTypeError).Inc(1)
				} else {
					newGapCounter(monitorEventGapHealthCounter, resultTypeSuccess).Inc(1)
				}
			}

			if !zeroBackoff {
				if err := backoff.Get(ctx, nil); err != nil {
					return fmt.Errorf("failed to sleep: %w", err)
				}
			}
		}

		request.StartHeight = startHeight
		request.StartEventId = startEventId
		return w.continueAsNew(ctx, request)
	})
}

func (r *MonitorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
		tagEventTag: strconv.Itoa(int(r.EventTag)),
	}
}
