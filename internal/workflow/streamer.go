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
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	Streamer struct {
		baseWorkflow
		streamer *activity.Streamer
		tag      uint32
	}

	StreamerParams struct {
		fx.In
		fxparams.Params
		Runtime  cadence.Runtime
		Streamer *activity.Streamer
	}

	StreamerRequest struct {
		BatchSize             uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize        uint64 // Optional. If not specified, it is read from the workflow config.
		BackoffInterval       string // Optional. If not specified, it is read from the workflow config.
		MaxAllowedReorgHeight uint64 // Optional. If not specified, it is read from the workflow config.
		EventTag              uint32 // Optional.
		Tag                   uint32 // Optional.
	}
)

const (
	defaultStreamerEventTag = 0

	// metrics for streamer. need to have "workflow.streamer" as prefix
	streamerHeightGauge             = "workflow.streamer.height"
	streamerGapGauge                = "workflow.streamer.gap"
	streamerTimeSinceLastBlockGauge = "workflow.streamer.time_since_last_block"
)

var (
	_ InstrumentedRequest = (*StreamerRequest)(nil)
)

func NewStreamer(params StreamerParams) *Streamer {
	w := &Streamer{
		baseWorkflow: newBaseWorkflow(&params.Config.Workflows.Streamer, params.Runtime),
		streamer:     params.Streamer,
	}
	w.registerWorkflow(w.execute)

	return w
}

func (w *Streamer) Execute(ctx context.Context, request *StreamerRequest) (client.WorkflowRun, error) {
	workflowID := w.name
	if request.EventTag != defaultStreamerEventTag {
		workflowID = fmt.Sprintf("%s/event_tag=%d", workflowID, request.EventTag)
	}
	return w.startWorkflow(ctx, workflowID, request)
}

func (w *Streamer) execute(ctx workflow.Context, request *StreamerRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		var cfg config.StreamerWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
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
				return xerrors.Errorf("failed to parse BackoffInterval=%v: %w", request.BackoffInterval, err)
			}
		}
		zeroBackoff := backoffInterval == 0

		maxAllowedReorgHeight := cfg.IrreversibleDistance
		if request.MaxAllowedReorgHeight > 0 {
			maxAllowedReorgHeight = request.MaxAllowedReorgHeight
		}

		eventTag := cfg.GetEffectiveEventTag(request.EventTag)
		tag := cfg.GetEffectiveBlockTag(request.Tag)

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)

		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagEventTag: strconv.Itoa(int(eventTag)),
		})
		var backoff workflow.Future
		for i := 0; i < int(checkpointSize); i++ {
			if !zeroBackoff {
				backoff = workflow.NewTimer(ctx, backoffInterval)
			}

			streamerRequest := &activity.StreamerRequest{
				BatchSize:             batchSize,
				Tag:                   tag,
				MaxAllowedReorgHeight: maxAllowedReorgHeight,
				EventTag:              eventTag,
			}
			response, err := w.streamer.Execute(ctx, streamerRequest)
			if err != nil {
				return xerrors.Errorf("failed to execute streamer activity: %w", err)
			}
			lastStreamedHeight := response.LatestStreamedHeight
			metrics.Gauge(streamerHeightGauge).Update(float64(lastStreamedHeight))
			metrics.Gauge(streamerGapGauge).Update(float64(response.Gap))
			if response.TimeSinceLastBlock > 0 {
				metrics.Gauge(streamerTimeSinceLastBlockGauge).Update(response.TimeSinceLastBlock.Seconds())
			}

			logger.Info("streamer", zap.Int("iteration", i), zap.Reflect("response", response))

			if !zeroBackoff {
				if err := backoff.Get(ctx, nil); err != nil {
					return xerrors.Errorf("failed to sleep: %w", err)
				}
			}
		}
		return w.continueAsNew(ctx, request)
	})
}

func (r *StreamerRequest) GetTags() map[string]string {
	return map[string]string{
		tagEventTag: strconv.Itoa(int(r.EventTag)),
	}
}
