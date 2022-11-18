package cadence

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
)

type (
	TestEnv struct {
		*testsuite.TestWorkflowEnvironment
		testActivityEnvironment *testsuite.TestActivityEnvironment
		isActivityEnv           bool
	}

	WorkflowTestSuite interface {
		T() *testing.T
		NewTestWorkflowEnvironment() *testsuite.TestWorkflowEnvironment
		NewTestActivityEnvironment() *testsuite.TestActivityEnvironment
	}

	emptyCtx struct{}
)

func NewTestEnv(ts WorkflowTestSuite) *TestEnv {
	return &TestEnv{
		TestWorkflowEnvironment: ts.NewTestWorkflowEnvironment(),
		testActivityEnvironment: ts.NewTestActivityEnvironment(),
	}
}

func NewTestActivityEnv(ts WorkflowTestSuite) *TestEnv {
	return &TestEnv{
		TestWorkflowEnvironment: ts.NewTestWorkflowEnvironment(),
		testActivityEnvironment: ts.NewTestActivityEnvironment(),
		isActivityEnv:           true,
	}
}

func WithTestEnv(testEnv *TestEnv) fx.Option {
	return fx.Provide(func() *TestEnv {
		return testEnv
	})
}

func (e *TestEnv) BackgroundContext() workflow.Context {
	return new(emptyCtx)
}

func (e *TestEnv) IsActivityEnv() bool {
	return e.isActivityEnv
}

func (*emptyCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*emptyCtx) Done() workflow.Channel {
	return nil
}

func (*emptyCtx) Err() error {
	return nil
}

func (*emptyCtx) Value(key interface{}) interface{} {
	return nil
}

func (e *emptyCtx) String() string {
	return "context.Background"
}
