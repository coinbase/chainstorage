package cron

import (
	"context"
	"strings"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

type (
	WorkflowStatusTaskParams struct {
		fx.In
		fxparams.Params
		Config  *config.Config
		Runtime cadence.Runtime
	}

	workflowStatusTask struct {
		config  *config.Config
		logger  *zap.Logger
		runtime cadence.Runtime
		metrics *workflowStatusMetrics
	}

	workflowStatusMetrics struct {
		countersSuccess map[string]tally.Counter
		countersError   map[string]tally.Counter
	}
)

const (
	workflowPrefix = "workflow."
	monitor        = "monitor"
	poller         = "poller"
	streamer       = "streamer"
	crossValidator = "cross_validator"
	backfiller     = "backfiller"

	maxPageSize = 20
)

var (
	workflowSeverityMap = map[string]string{
		monitor:        sev2,
		poller:         sev1,
		streamer:       sev1,
		crossValidator: sev2,
		backfiller:     sev2,
	}
)

func NewWorkflowStatus(params WorkflowStatusTaskParams) (Task, error) {
	logger := log.WithPackage(params.Logger)
	return &workflowStatusTask{
		config:  params.Config,
		logger:  logger,
		runtime: params.Runtime,
		metrics: newWorkflowStatusMetrics(params.Config.SLA.ExpectedWorkflows, params.Metrics),
	}, nil
}

func newWorkflowStatusMetrics(workflows []string, rootScope tally.Scope) *workflowStatusMetrics {
	countersSuccess := make(map[string]tally.Counter)
	countersError := make(map[string]tally.Counter)
	for _, wf := range workflows {
		severity := getWorkflowSeverity(strings.Split(wf, "/")[0])
		countersSuccess[workflowPrefix+wf] = rootScope.Tagged(map[string]string{
			slaTypeTag:    workflowPrefix + wf,
			severityTag:   severity,
			resultTypeTag: resultTypeSuccess,
		}).Counter(slaMetric)

		countersError[workflowPrefix+wf] = rootScope.Tagged(map[string]string{
			slaTypeTag:    workflowPrefix + wf,
			severityTag:   severity,
			resultTypeTag: resultTypeError,
		}).Counter(slaMetric)
	}

	return &workflowStatusMetrics{
		countersSuccess: countersSuccess,
		countersError:   countersError,
	}
}

func getWorkflowSeverity(workflow string) string {
	if severity, ok := workflowSeverityMap[workflow]; ok {
		return severity
	}
	return sev3
}

func (t *workflowStatusTask) Name() string {
	return "workflow_status"
}

func (t *workflowStatusTask) Spec() string {
	return "@every 30s"
}

func (t *workflowStatusTask) Parallelism() int64 {
	return 1
}

func (t *workflowStatusTask) Enabled() bool {
	return !t.config.Cron.DisableWorkflowStatus
}

func (t *workflowStatusTask) DelayStartDuration() time.Duration {
	return 0
}

func (t *workflowStatusTask) Run(ctx context.Context) error {
	// Get the expected workflows for the current config
	expectedWorkflowRunning := make(map[string]bool)
	for _, wf := range t.config.SLA.ExpectedWorkflows {
		expectedWorkflowRunning[workflowPrefix+wf] = false
	}

	t.logger.Info("running workflow status task", zap.Reflect("expected_workflows", expectedWorkflowRunning))

	// Mark open workflows twice because sometimes the result misses some workflows due to `ContinueAsNew`
	if err := t.markOpenWorkflows(ctx, expectedWorkflowRunning); err != nil {
		return xerrors.Errorf("failed to mark open workflows: %w", err)
	}
	time.Sleep(1 * time.Second)
	if err := t.markOpenWorkflows(ctx, expectedWorkflowRunning); err != nil {
		return xerrors.Errorf("failed to mark open workflows: %w", err)
	}

	for workflowName, isRunning := range expectedWorkflowRunning {
		if !isRunning {
			t.metrics.countersError[workflowName].Inc(1)
		} else {
			t.metrics.countersSuccess[workflowName].Inc(1)
		}
	}

	t.logger.Info("finished workflow status task", zap.Reflect("workflow_running", expectedWorkflowRunning))

	return nil
}

func (t *workflowStatusTask) markOpenWorkflows(ctx context.Context, expectedWorkflowMap map[string]bool) error {
	openWorkflows, err := t.runtime.ListOpenWorkflows(ctx, t.config.Cadence.Domain, maxPageSize)
	if err != nil {
		return xerrors.Errorf("failed to get open workflows: %w", err)
	}
	for _, wf := range openWorkflows.Executions {
		openWorkflow := wf.Execution.WorkflowId
		if _, ok := expectedWorkflowMap[openWorkflow]; ok {
			expectedWorkflowMap[openWorkflow] = true
		}
	}
	return nil
}
