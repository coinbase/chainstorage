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
	Poller struct {
		baseWorkflow
		syncer *activity.Syncer
	}

	PollerParams struct {
		fx.In
		fxparams.Params
		Runtime cadence.Runtime
		Syncer  *activity.Syncer
	}

	PollerRequest struct {
		Tag                 uint32
		MinStartHeight      uint64
		MaxBlocksToSync     uint64
		Parallelism         int
		CheckpointSize      uint64
		DataCompression     string
		RetryableErrorCount int
		Failover            bool
		FastSync            bool
	}
)

const (
	RetryableErrorLimit = 10

	errSessionFailed          = "session_failed"
	errScheduleToStartTimeout = "schedule_to_start_timeout"
	errUnknown                = "unknown"

	failoverMetricName = "failover"
)

var (
	_ InstrumentedRequest = (*PollerRequest)(nil)
)

func NewPoller(params PollerParams) *Poller {
	w := &Poller{
		baseWorkflow: newBaseWorkflow(&params.Config.Workflows.Poller, params.Runtime),
		syncer:       params.Syncer,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Poller) Execute(ctx context.Context, request *PollerRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (w *Poller) execute(ctx workflow.Context, request *PollerRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		var cfg config.PollerWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)
		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getScope(ctx).Tagged(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
		})

		minStartHeight := request.MinStartHeight

		maxBlocksToSync := cfg.MaxBlocksToSyncPerCycle
		if request.MaxBlocksToSync > 0 {
			maxBlocksToSync = request.MaxBlocksToSync
		}

		parallelism := cfg.Parallelism
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
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

		failover := request.Failover

		fastSync := cfg.FastSync
		if request.FastSync {
			fastSync = true
		}

		// When session is enabled, sessionCtx will be used to create a session
		// so that all syncer activities will be executed by the same worker.
		// https://github.com/temporalio/sdk-go/blob/6580cbe0aa41a8b515791f95c2c15bb37db1dab1/workflow/session.go#L102
		sessionCtx := ctx
		if cfg.SessionEnabled {
			so := &workflow.SessionOptions{
				CreationTimeout:  cfg.SessionCreationTimeout,
				ExecutionTimeout: cfg.WorkflowExecutionTimeout,
				HeartbeatTimeout: cfg.ActivityHeartbeatTimeout,
			}
			sessionCtx, err = workflow.CreateSession(ctx, so)
			if err != nil {
				return xerrors.Errorf("failed to create workflow session: %w", err)
			}
			defer workflow.CompleteSession(sessionCtx)
		}

		for i := 0; i < int(checkpointSize); i++ {
			backoff := workflow.NewTimer(ctx, cfg.BackoffInterval)

			if failover {
				metrics.Gauge(failoverMetricName).Update(1)
			} else {
				metrics.Gauge(failoverMetricName).Update(0)
			}

			syncerRequest := &activity.SyncerRequest{
				Tag:             tag,
				MinStartHeight:  minStartHeight,
				MaxBlocksToSync: maxBlocksToSync,
				Parallelism:     parallelism,
				DataCompression: dataCompression,
				Failover:        failover,
				FastSync:        fastSync,
			}

			syncerResponse, err := w.syncer.Execute(sessionCtx, syncerRequest)
			if err != nil {
				// There are several errors need to be handled for workflows enabling session
				// 1. ErrSessionFailed
				// https://github.com/temporalio/sdk-go/blob/6580cbe0aa41a8b515791f95c2c15bb37db1dab1/workflow/session.go#L52
				// 2. ScheduleToStartTimeout
				// This timeout error is not retryable by default, and could happen when there is a new deployment
				if IsErrSessionFailed(err) || IsScheduleToStartTimeout(err) {
					request.RetryableErrorCount += 1

					errMetricName := w.getRetryableErrorMetricName(err)
					metrics.Counter(errMetricName).Inc(1)

					// Fail the workflow if getting too retryable errors too many times
					if request.RetryableErrorCount <= RetryableErrorLimit {
						return workflow.NewContinueAsNewError(ctx, w.name, request)
					}
					return xerrors.Errorf("retryable errors exceeded threshold: %w", err)
				}

				// Switch over to failover cluster if feature is enabled
				if cfg.FailoverEnabled && !failover {
					request.Failover = true
					return workflow.NewContinueAsNewError(ctx, w.name, request)
				}
				return xerrors.Errorf("failed to execute syncer: %w", err)
			}

			metrics.Gauge("height").Update(float64(syncerResponse.LatestSyncedHeight))
			metrics.Gauge("gap").Update(float64(syncerResponse.SyncGap))
			if syncerResponse.TimeSinceLastBlock > 0 {
				metrics.Gauge("time_since_last_block").Update(syncerResponse.TimeSinceLastBlock.Seconds())
			}
			if err := backoff.Get(ctx, nil); err != nil {
				return xerrors.Errorf("failed to sleep: %w", err)
			}
		}

		request.RetryableErrorCount = 0
		return workflow.NewContinueAsNewError(ctx, w.name, request)
	})
}

func (w *Poller) getRetryableErrorMetricName(err error) string {
	if IsErrSessionFailed(err) {
		return errSessionFailed
	} else if IsScheduleToStartTimeout(err) {
		return errScheduleToStartTimeout
	}
	return errUnknown
}

func (r *PollerRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}
