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
		BackoffIntervalSecs   uint64 // Optional. If not specified, it is read from the workflow config.
		MaxAllowedReorgHeight uint64 // Optional. If not specified, it is read from the workflow config.
		EventTag              uint32 // Optional.
	}
)

const (
	defaultStreamerEventTag = 0
)

var (
	_ InstrumentedRequest = (*StreamerRequest)(nil)
)

func NewStreamer(params StreamerParams) *Streamer {
	w := &Streamer{
		baseWorkflow: newBaseWorkflow(&params.Config.Workflows.Streamer, params.Runtime),
		streamer:     params.Streamer,
		tag:          params.Config.GetStableBlockTag(),
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

		backoffInterval := cfg.BackoffInterval
		if request.BackoffIntervalSecs > 0 {
			backoffInterval = time.Duration(request.BackoffIntervalSecs) * time.Second
		}

		maxAllowedReorgHeight := cfg.IrreversibleDistance
		if request.MaxAllowedReorgHeight > 0 {
			maxAllowedReorgHeight = request.MaxAllowedReorgHeight
		}

		eventTag := request.EventTag

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)

		metrics := w.getScope(ctx).Tagged(map[string]string{
			tagEventTag: strconv.Itoa(int(eventTag)),
		})
		for i := 0; i < int(checkpointSize); i++ {
			backoff := workflow.NewTimer(ctx, backoffInterval)
			streamerRequest := &activity.StreamerRequest{
				BatchSize:             batchSize,
				Tag:                   w.tag,
				MaxAllowedReorgHeight: maxAllowedReorgHeight,
				EventTag:              eventTag,
			}
			response, err := w.streamer.Execute(ctx, streamerRequest)
			if err != nil {
				return xerrors.Errorf("failed to execute streamer activity: %w", err)
			}
			lastStreamedHeight := response.LatestStreamedHeight
			metrics.Gauge("height").Update(float64(lastStreamedHeight))
			metrics.Gauge("gap").Update(float64(response.Gap))
			if response.TimeSinceLastBlock > 0 {
				metrics.Gauge("time_since_last_block").Update(response.TimeSinceLastBlock.Seconds())
			}

			logger.Info("streamer", zap.Int("iteration", i), zap.Reflect("response", response))
			if err := backoff.Get(ctx, nil); err != nil {
				return xerrors.Errorf("failed to sleep: %w", err)
			}
		}
		return workflow.NewContinueAsNewError(ctx, w.name, request)
	})
}

func (r *StreamerRequest) GetTags() map[string]string {
	return map[string]string{
		tagEventTag: strconv.Itoa(int(r.EventTag)),
	}
}
