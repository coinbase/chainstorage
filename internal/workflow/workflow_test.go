package workflow

import (
	"fmt"
	"testing"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity/errors"
)

type (
	messenger interface {
		message() string
	}

	temporalError struct {
		messenger
		originalFailure *failure.Failure
	}

	// activityError is a fake error for temporal ActivityError
	// https://github.com/temporalio/sdk-go/blob/059ec4f6af6a0361493bac5db2ea1ea78598ca9c/internal/error.go#L192
	activityError struct {
		temporalError
		scheduledEventID int64
		startedEventID   int64
		identity         string
		activityType     *common.ActivityType
		activityID       string
		retryState       enums.RetryState
		cause            error
	}
)

func (e *activityError) Error() string {
	var msg string
	if e.cause != nil {
		msg = fmt.Sprintf("%s: %v", e.message(), e.cause)
	}
	return msg
}

func (e *activityError) message() string {
	return "activity error"
}

func (e *activityError) Unwrap() error {
	return e.cause
}

func TestIsErrSessionFailed(t *testing.T) {
	require := testutil.Require(t)

	activityErr := &activityError{
		cause: temporal.NewApplicationError(workflow.ErrSessionFailed.Error(), ""),
	}
	err := xerrors.Errorf("fake activity error: %w", activityErr)
	require.False(xerrors.Is(err, workflow.ErrSessionFailed))
	require.True(IsErrSessionFailed(err))
}

func TestIsScheduleToStartTimeout(t *testing.T) {
	require := testutil.Require(t)

	err := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_START, nil)
	require.True(IsScheduleToStartTimeout(err))

	err = temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, nil)
	require.False(IsScheduleToStartTimeout(err))
}

func TestIsNodeProviderFailed(t *testing.T) {
	require := testutil.Require(t)

	activityErr := &activityError{
		cause: temporal.NewApplicationError(xerrors.New("Error Test").Error(), errors.ErrTypeNodeProvider),
	}
	err := xerrors.Errorf("fake activity error: %w", activityErr)
	require.True(IsNodeProviderFailed(err))

	activityErr = &activityError{
		cause: temporal.NewApplicationError(xerrors.New("Error Test").Error(), xerrors.New("Random Error").Error()),
	}
	err = xerrors.Errorf("fake activity error: %w", activityErr)
	require.False(IsNodeProviderFailed(err))
}
