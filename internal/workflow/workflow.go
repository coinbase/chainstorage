package workflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	a "github.com/coinbase/chainstorage/internal/workflow/activity/errors"
	"github.com/coinbase/chainstorage/internal/workflow/instrument"
)

type (
	Manager struct {
		config          *config.Config
		logger          *zap.Logger
		runtime         cadence.Runtime
		backfiller      *Backfiller
		poller          *Poller
		benchmarker     *Benchmarker
		monitor         *Monitor
		streamer        *Streamer
		crossValidator  *CrossValidator
		eventBackfiller *EventBackfiller
		replicator      *Replicator
	}

	ManagerParams struct {
		fx.In
		fxparams.Params
		Lifecycle       fx.Lifecycle
		Runtime         cadence.Runtime
		Backfiller      *Backfiller
		Poller          *Poller
		Benchmarker     *Benchmarker
		Monitor         *Monitor
		Streamer        *Streamer
		CrossValidator  *CrossValidator
		EventBackfiller *EventBackfiller
		Replicator      *Replicator
	}

	InstrumentedRequest interface {
		GetTags() map[string]string
	}

	MetricOption func(scope tally.Scope) tally.Scope

	baseWorkflow struct {
		name       string
		config     config.BaseWorkflowConfig
		runtime    cadence.Runtime
		validate   *validator.Validate
		instrument *instrument.Instrument
	}
)

const (
	loggerMsg = "workflow.request"

	tagBlockTag = "tag"
	tagEventTag = "event_tag"
)

func NewManager(params ManagerParams) *Manager {
	mgr := &Manager{
		config:          params.Config,
		logger:          log.WithPackage(params.Logger),
		runtime:         params.Runtime,
		backfiller:      params.Backfiller,
		poller:          params.Poller,
		benchmarker:     params.Benchmarker,
		monitor:         params.Monitor,
		streamer:        params.Streamer,
		crossValidator:  params.CrossValidator,
		eventBackfiller: params.EventBackfiller,
		replicator:      params.Replicator,
	}

	params.Lifecycle.Append(fx.Hook{
		OnStart: mgr.onStart,
		OnStop:  mgr.onStop,
	})

	return mgr
}

func (m *Manager) onStart(ctx context.Context) error {
	m.logger.Info(
		"starting workflow manager",
		zap.String("namespace", m.config.Namespace()),
		zap.String("env", string(m.config.Env())),
		zap.String("blockchain", m.config.Blockchain().GetName()),
		zap.String("network", m.config.Network().GetName()),
		zap.String("sidechain", m.config.Sidechain().GetName()),
	)

	if err := m.runtime.OnStart(ctx); err != nil {
		return xerrors.Errorf("failed to start runtime: %w", err)
	}

	return nil
}

func (m *Manager) onStop(ctx context.Context) error {
	m.logger.Info("stopping workflow manager")

	if err := m.runtime.OnStop(ctx); err != nil {
		return xerrors.Errorf("failed to stop runtime: %w", err)
	}

	return nil
}

func newBaseWorkflow(config config.BaseWorkflowConfig, runtime cadence.Runtime) baseWorkflow {
	name := config.Base().WorkflowIdentity
	metricName := fmt.Sprintf("%v.process_checkpoint", name)
	return baseWorkflow{
		name:       name,
		config:     config,
		runtime:    runtime,
		validate:   validator.New(),
		instrument: instrument.New(runtime, metricName, loggerMsg),
	}
}

func (w *baseWorkflow) registerWorkflow(workflowFn any) {
	w.runtime.RegisterWorkflow(workflowFn, workflow.RegisterOptions{
		Name: w.name,
	})
}

func (w *baseWorkflow) validateRequest(request any) error {
	return w.validateRequestCtx(context.Background(), request)
}

func (w *baseWorkflow) validateRequestCtx(ctx context.Context, request any) error {
	if err := w.validate.StructCtx(ctx, request); err != nil {
		return xerrors.Errorf("invalid workflow request (name=%v, request=%+v): %w", w.name, request, err)
	}

	return nil
}

func (w *baseWorkflow) startWorkflow(ctx context.Context, workflowID string, request any) (client.WorkflowRun, error) {
	if err := w.validateRequestCtx(ctx, request); err != nil {
		return nil, err
	}

	cfg := w.config.Base()
	workflowOptions := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                cfg.TaskList,
		WorkflowRunTimeout:                       cfg.WorkflowRunTimeout,
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		RetryPolicy:                              w.getRetryPolicy(cfg.WorkflowRetry),
	}

	execution, err := w.runtime.ExecuteWorkflow(ctx, workflowOptions, w.name, request)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute workflow: %w", err)
	}

	return execution, nil
}

func (w *baseWorkflow) executeWorkflow(ctx workflow.Context, request any, fn instrument.Fn, opts ...instrument.Option) error {
	workflowInfo := workflow.GetInfo(ctx)

	// Check if this is the last attempt.
	// This tag is used to determine if an alert should be sent for a failed workflow.
	lastAttempt := true
	if workflowInfo.RetryPolicy != nil && workflowInfo.Attempt < workflowInfo.RetryPolicy.MaximumAttempts {
		lastAttempt = false
	}

	opts = append(
		opts,
		instrument.WithLoggerField(zap.String("workflow", w.name)),
		instrument.WithLoggerField(zap.Reflect("request", request)),
		instrument.WithLoggerField(zap.Bool("last_attempt", lastAttempt)),
		instrument.WithScopeTag("last_attempt", strconv.FormatBool(lastAttempt)),
		instrument.WithFilter(IsContinueAsNewError),
	)
	if ir, ok := request.(InstrumentedRequest); ok {
		for k, v := range ir.GetTags() {
			opts = append(opts, instrument.WithScopeTag(k, v), instrument.WithLoggerField(zap.String(k, v)))
		}
	}

	return w.instrument.Instrument(ctx, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}

		if err := fn(); err != nil {
			return xerrors.Errorf("failed to execute workflow (name=%v): %w", w.name, err)
		}

		return nil
	}, opts...)
}

func (w *baseWorkflow) StopWorkflow(ctx context.Context, workflowID string, reason string) error {
	if err := w.runtime.TerminateWorkflow(ctx, workflowID, "", reason); err != nil {
		return xerrors.Errorf("failed to terminate workflowID=%s: %w", workflowID, err)
	}

	return nil
}

func (w *baseWorkflow) readConfig(ctx workflow.Context, output any) error {
	// Read config as a SideEffect to guarantee deterministic workflow execution.
	// As a result, config changes only take effect after finishing the current checkpoint.
	val := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return w.config
	})

	if err := val.Get(output); err != nil {
		return xerrors.Errorf("failed to retrieve config: %w", err)
	}

	return nil
}

func (w *baseWorkflow) getLogger(ctx workflow.Context) *zap.Logger {
	return log.WithPackage(w.runtime.GetLogger(ctx))
}

func (w *baseWorkflow) getMetricsHandler(ctx workflow.Context) client.MetricsHandler {
	return w.runtime.GetMetricsHandler(ctx)
}

func (w *baseWorkflow) withActivityOptions(ctx workflow.Context) workflow.Context {
	cfg := w.config.Base()
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              cfg.TaskList,
		StartToCloseTimeout:    cfg.ActivityStartToCloseTimeout,
		ScheduleToCloseTimeout: cfg.ActivityScheduleToCloseTimeout,
		HeartbeatTimeout:       cfg.ActivityHeartbeatTimeout,
		RetryPolicy:            w.getRetryPolicy(cfg.ActivityRetry),
	})
}

func (w *baseWorkflow) getRetryPolicy(cfg *config.RetryPolicy) *temporal.RetryPolicy {
	if cfg == nil || cfg.MaximumAttempts <= 0 {
		return nil
	}

	return &temporal.RetryPolicy{
		// Maximum number of attempts. 1 means no retry.
		MaximumAttempts: cfg.MaximumAttempts,
		// Coefficient used to calculate the next retry backoff interval.
		BackoffCoefficient: cfg.BackoffCoefficient,
		// Backoff interval for the first retry.
		InitialInterval: cfg.InitialInterval,
		// This value is the cap of the interval.
		MaximumInterval: cfg.MaximumInterval,
	}
}

// continueAsNew overrides the workflow options before the workflow is restarted;
// otherwise, the workflow needs to be manually restarted whenever there is a change in StartWorkflowOptions.
func (w *baseWorkflow) continueAsNew(ctx workflow.Context, request any) error {
	cfg := w.config.Base()

	// Override the workflow options to be carried over to the next run.
	ctx = workflow.WithWorkflowRunTimeout(ctx, cfg.WorkflowRunTimeout)
	options := workflow.ContinueAsNewErrorOptions{
		RetryPolicy: w.getRetryPolicy(cfg.WorkflowRetry),
	}

	return workflow.NewContinueAsNewErrorWithOptions(ctx, options, w.name, request)
}

func IsContinueAsNewError(err error) bool {
	return workflow.IsContinueAsNewError(err)
}

func IsErrSessionFailed(sessionCtx workflow.Context, err error) bool {
	if strings.Contains(err.Error(), workflow.ErrSessionFailed.Error()) {
		return true
	}

	if sessionInfo := workflow.GetSessionInfo(sessionCtx); sessionInfo != nil {
		return sessionInfo.SessionState == workflow.SessionStateFailed
	}

	return false
}

func IsScheduleToStartTimeout(err error) bool {
	var timeoutError *temporal.TimeoutError
	if errors.As(err, &timeoutError) {
		return timeoutError.TimeoutType() == enums.TIMEOUT_TYPE_SCHEDULE_TO_START
	}

	return false
}

func IsNodeProviderFailed(err error) bool {
	return strings.Contains(err.Error(), a.ErrTypeNodeProvider)
}

func IsConsensusClusterFailure(err error) bool {
	return strings.Contains(err.Error(), a.ErrTypeConsensusClusterFailure)
}

func IsConsensusValidationFailure(err error) bool {
	return strings.Contains(err.Error(), a.ErrTypeConsensusValidationFailure)
}
