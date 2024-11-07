package cadence

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/timesource"
)

type (
	testRuntime struct {
		env    *TestEnv
		logger *zap.Logger
	}

	testWorkflowRun struct{}
)

func newTestRuntime(env *TestEnv, logger *zap.Logger) (Runtime, error) {
	return &testRuntime{
		env:    env,
		logger: logger,
	}, nil
}

func (r *testRuntime) OnStart(ctx context.Context) error {
	return nil
}

func (r *testRuntime) OnStop(ctx context.Context) error {
	return nil
}

func (r *testRuntime) RegisterWorkflow(w any, options workflow.RegisterOptions) {
	r.env.RegisterWorkflowWithOptions(w, options)
}

func (r *testRuntime) RegisterActivity(a any, options activity.RegisterOptions) {
	if r.env.IsActivityEnv() {
		r.env.testActivityEnvironment.RegisterActivityWithOptions(a, options)
		return
	}

	r.env.RegisterActivityWithOptions(a, options)
}

func (r *testRuntime) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, request any) (client.WorkflowRun, error) {
	r.env.ExecuteWorkflow(workflow, request)
	if !r.env.IsWorkflowCompleted() {
		return nil, errors.New("workflow not completed")
	}

	if err := r.env.GetWorkflowError(); err != nil {
		return nil, fmt.Errorf("workflow failed: %w", err)
	}

	return testWorkflowRun{}, nil
}

func (r *testRuntime) ExecuteActivity(ctx workflow.Context, activity any, request any, response any) error {
	if r.env.IsActivityEnv() {
		val, err := r.env.testActivityEnvironment.ExecuteActivity(activity, request)
		if err != nil {
			return err
		}

		return val.Get(response)
	}

	future := workflow.ExecuteActivity(ctx, activity, request)
	return future.Get(ctx, response)
}

func (r *testRuntime) GetLogger(ctx workflow.Context) *zap.Logger {
	return r.logger
}

func (r *testRuntime) GetMetricsHandler(ctx workflow.Context) client.MetricsHandler {
	return client.MetricsNopHandler
}

func (r *testRuntime) GetActivityLogger(ctx context.Context) *zap.Logger {
	return r.logger
}

func (r *testRuntime) GetTimeSource(ctx workflow.Context) timesource.TimeSource {
	if r.env.IsActivityEnv() {
		return timesource.NewRealTimeSource()
	}

	return timesource.NewWorkflowTimeSource(ctx)
}

func (r *testRuntime) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error {
	r.env.CancelWorkflow()
	return nil
}

func (t *testRuntime) ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	return nil, nil
}

func (t testWorkflowRun) GetID() string {
	return ""
}

func (t testWorkflowRun) GetRunID() string {
	return ""
}

func (t testWorkflowRun) Get(ctx context.Context, valuePtr any) error {
	return nil
}

func (t testWorkflowRun) GetWithOptions(ctx context.Context, valuePtr any, options client.WorkflowRunGetOptions) error {
	return nil
}
