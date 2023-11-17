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
	"github.com/coinbase/chainstorage/internal/utils/pointer"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Poller struct {
		baseWorkflow
		syncer        *activity.Syncer
		livenessCheck *activity.LivenessCheck
	}

	PollerParams struct {
		fx.In
		fxparams.Params
		Runtime       cadence.Runtime
		Syncer        *activity.Syncer
		LivenessCheck *activity.LivenessCheck
	}

	PollerRequest struct {
		Tag                          uint32
		MinStartHeight               uint64
		MaxBlocksToSync              uint64
		BackoffInterval              string
		Parallelism                  int
		CheckpointSize               uint64
		DataCompression              string
		RetryableErrorCount          int
		Failover                     bool
		ConsensusFailover            bool
		FastSync                     bool
		NumBlocksToSkip              uint64
		TransactionsWriteParallelism int
		ConsensusValidation          *bool
		ConsensusValidationMuted     *bool
		State                        *PollerState
	}

	PollerState struct {
		LastLivenessCheckTimestamp  int64
		LivenessCheckViolationCount uint64
	}
)

const (
	RetryableErrorLimit = 10

	// poller metrics. need to have `workflow.poller` as prefix
	pollerHeightGauge                      = "workflow.poller.height"
	pollerGapGauge                         = "workflow.poller.gap"
	pollerTimeSinceLastBlockGauge          = "workflow.poller.time_since_last_block"
	pollerErrSessionFailedCounter          = "workflow.poller.session_failed"
	pollerErrScheduleToStartTimeoutCounter = "workflow.poller.schedule_to_start_timeout"
	pollerErrUnknownCounter                = "workflow.poller.unknown"
	pollerFailoverGauge                    = "workflow.poller.failover"
	pollerConsensusFailoverGauge           = "workflow.poller.consensus_failover"
	pollerConsensusValidationMutedGauge    = "workflow.poller.consensus_validation_muted"
	pollerLivenessCheckViolationGauge      = "workflow.poller.liveness_check_violation"
)

var (
	_ InstrumentedRequest = (*PollerRequest)(nil)
)

func NewPoller(params PollerParams) *Poller {
	w := &Poller{
		baseWorkflow:  newBaseWorkflow(&params.Config.Workflows.Poller, params.Runtime),
		syncer:        params.Syncer,
		livenessCheck: params.LivenessCheck,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Poller) Execute(ctx context.Context, request *PollerRequest) (client.WorkflowRun, error) {
	workflowID := w.name
	if request.Tag != 0 {
		workflowID = fmt.Sprintf("%s/block_tag=%d", w.name, request.Tag)
	}
	return w.startWorkflow(ctx, workflowID, request)
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
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
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

		transactionsWriteParallelism := cfg.TransactionsWriteParallelism
		if request.TransactionsWriteParallelism > 0 {
			transactionsWriteParallelism = request.TransactionsWriteParallelism
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
		consensusFailover := request.ConsensusFailover

		fastSync := cfg.FastSync
		if request.FastSync {
			fastSync = true
		}

		backoffInterval := cfg.BackoffInterval
		if request.BackoffInterval != "" {
			backoffInterval, err = time.ParseDuration(request.BackoffInterval)
			if err != nil {
				return xerrors.Errorf("failed to parse BackoffInterval=%v: %w", request.BackoffInterval, err)
			}
		}
		zeroBackoff := backoffInterval == 0

		irreversibleDistance := cfg.IrreversibleDistance

		numBlocksToSkip := cfg.NumBlocksToSkip
		if request.NumBlocksToSkip > 0 {
			numBlocksToSkip = request.NumBlocksToSkip
		}

		consensusValidation := cfg.ConsensusValidation
		if request.ConsensusValidation != nil {
			consensusValidation = *request.ConsensusValidation
		}

		consensusValidationMuted := cfg.ConsensusValidationMuted
		if request.ConsensusValidationMuted != nil {
			consensusValidationMuted = *request.ConsensusValidationMuted
		}

		if request.State == nil {
			request.State = &PollerState{}
		}

		livenessCheckThreshold := cfg.SLA.TimeSinceLastBlock
		livenessCheckViolationLimit := cfg.LivenessCheckViolationLimit
		livenessCheckInterval := cfg.LivenessCheckInterval

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

		var backoff workflow.Future
		for i := 0; i < int(checkpointSize); i++ {
			if !zeroBackoff {
				backoff = workflow.NewTimer(ctx, backoffInterval)
			}

			if failover {
				metrics.Gauge(pollerFailoverGauge).Update(1)
			} else {
				metrics.Gauge(pollerFailoverGauge).Update(0)
			}

			if consensusFailover {
				metrics.Gauge(pollerConsensusFailoverGauge).Update(1)
			} else {
				metrics.Gauge(pollerConsensusFailoverGauge).Update(0)
			}

			if !consensusValidationMuted {
				metrics.Gauge(pollerConsensusValidationMutedGauge).Update(0)
			}

			syncerRequest := &activity.SyncerRequest{
				Tag:                          tag,
				MinStartHeight:               minStartHeight,
				MaxBlocksToSync:              maxBlocksToSync,
				Parallelism:                  parallelism,
				DataCompression:              dataCompression,
				Failover:                     failover,
				ConsensusFailover:            consensusFailover,
				FastSync:                     fastSync,
				IrreversibleDistance:         irreversibleDistance,
				NumBlocksToSkip:              numBlocksToSkip,
				TransactionsWriteParallelism: transactionsWriteParallelism,
				ConsensusValidation:          consensusValidation,
				ConsensusValidationMuted:     consensusValidationMuted,
			}

			syncerResponse, err := w.syncer.Execute(sessionCtx, syncerRequest)
			if err != nil {
				// There are several errors need to be handled for workflows enabling session
				// 1. ErrSessionFailed
				// https://github.com/temporalio/sdk-go/blob/6580cbe0aa41a8b515791f95c2c15bb37db1dab1/workflow/session.go#L52
				// 2. ScheduleToStartTimeout
				// This timeout error is not retryable by default, and could happen when there is a new deployment
				if IsErrSessionFailed(sessionCtx, err) || IsScheduleToStartTimeout(err) {
					request.RetryableErrorCount += 1

					errMetricName := w.getRetryableErrorMetricName(sessionCtx, err)
					metrics.Counter(errMetricName).Inc(1)

					// Fail the workflow if getting too retryable errors too many times
					if request.RetryableErrorCount <= RetryableErrorLimit {
						return workflow.NewContinueAsNewError(ctx, w.name, request)
					}
					return xerrors.Errorf("retryable errors exceeded threshold: %w", err)
				}

				if IsConsensusClusterFailure(err) {
					// Switch over to consensus failover cluster when meet following conditions:
					// 1. error is ErrTypeConsensusClusterFailure type
					// 2. have enabled consensus failover feature in config
					// 3. using primary endpoints of consensus client right now
					if cfg.ConsensusFailoverEnabled && !consensusFailover {
						request.ConsensusFailover = true
						return workflow.NewContinueAsNewError(ctx, w.name, request)
					}

					// If the error is caused by consensus clients, we do not want to pause the poller workflow
					// For this case, we will mute consensus validation failures
					request.ConsensusValidationMuted = pointer.Ref(true)
					metrics.Gauge(pollerConsensusValidationMutedGauge).Update(1)
					return workflow.NewContinueAsNewError(ctx, w.name, request)
				}

				// Switch over to failover cluster when
				// 1. failover feature is enabled
				// 2. using primary endpoints of master/slave clusters right now
				// 3. the error is not caused by ErrTypeConsensusValidationFailure
				if cfg.FailoverEnabled && !failover && !IsConsensusValidationFailure(err) {
					return w.triggerFailover(ctx, request)
				}
				return xerrors.Errorf("failed to execute syncer: %w", err)
			}

			metrics.Gauge(pollerHeightGauge).Update(float64(syncerResponse.LatestSyncedHeight))
			metrics.Gauge(pollerGapGauge).Update(float64(syncerResponse.SyncGap))
			if syncerResponse.TimeSinceLastBlock > 0 {
				metrics.Gauge(pollerTimeSinceLastBlockGauge).Update(syncerResponse.TimeSinceLastBlock.Seconds())
			}

			if cfg.LivenessCheckEnabled {
				currentTimestamp := workflow.Now(ctx).Unix()
				if w.shouldCheckLiveness(request.State.LastLivenessCheckTimestamp, currentTimestamp, livenessCheckInterval) {
					livenessCheckRequest := &activity.LivenessCheckRequest{
						Tag:                    tag,
						Failover:               failover,
						LivenessCheckThreshold: livenessCheckThreshold,
					}
					livenessCheckResponse, err := w.livenessCheck.Execute(sessionCtx, livenessCheckRequest)
					if err != nil {
						logger.Warn("failed to execute liveness check", zap.Error(err))
						continue
					}

					count := w.calculateLivenessCheckViolation(livenessCheckResponse.LivenessCheckViolation, request.State.LivenessCheckViolationCount, livenessCheckViolationLimit)
					request.State.LastLivenessCheckTimestamp = currentTimestamp
					request.State.LivenessCheckViolationCount = count

					metrics.Gauge(pollerLivenessCheckViolationGauge).Update(float64(count))

					// Failover to the secondary cluster if the node liveness check violation count exceeds the limit
					// time to trigger failover = livenessCheckInterval * livenessCheckViolationLimit = 5min * 4 = 20min
					if count >= livenessCheckViolationLimit && cfg.FailoverEnabled && !failover {
						return w.triggerFailover(ctx, request)
					}
				}
			}

			if !zeroBackoff {
				if err := backoff.Get(ctx, nil); err != nil {
					return xerrors.Errorf("failed to sleep: %w", err)
				}
			}
		}

		request.RetryableErrorCount = 0
		return workflow.NewContinueAsNewError(ctx, w.name, request)
	})
}

func (w *Poller) getRetryableErrorMetricName(sessionCtx workflow.Context, err error) string {
	if IsErrSessionFailed(sessionCtx, err) {
		return pollerErrSessionFailedCounter
	} else if IsScheduleToStartTimeout(err) {
		return pollerErrScheduleToStartTimeoutCounter
	}
	return pollerErrUnknownCounter
}

func (r *PollerRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}

func (w *Poller) shouldCheckLiveness(lastLivenessCheckTimestamp int64, currentTimestamp int64, interval time.Duration) bool {
	if lastLivenessCheckTimestamp != 0 && currentTimestamp-lastLivenessCheckTimestamp < int64(interval.Seconds()) {
		return false
	}
	return true
}

func (w *Poller) calculateLivenessCheckViolation(violation bool, violationCount uint64, violationMax uint64) uint64 {
	if violation {
		violationCount += 1
		// Set upper bound for violation count to avoid it increases to a very large number
		if violationCount > violationMax {
			violationCount = violationMax
		}
	} else if violationCount > 0 {
		violationCount -= 1
	}
	return violationCount
}

func (w *Poller) triggerFailover(ctx workflow.Context, request *PollerRequest) error {
	request.Failover = true
	request.State = &PollerState{}
	return workflow.NewContinueAsNewError(ctx, w.name, request)
}
