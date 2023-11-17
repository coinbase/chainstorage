package instrument

import (
	"context"
	"testing"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
)

func TestInstrument(t *testing.T) {
	tests := []struct {
		name                   string
		hasError               bool
		successValue           int64
		successWithFilterValue int64
		errorValue             int64
		fn                     OperationFn
		opts                   []Option
		retry                  retry.Retry
	}{
		{
			name:         "success",
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) error { return nil },
		},
		{
			name:         "error",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) error { return xerrors.New("mock") },
		},
		{
			name:         "withRetry",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) error { return retry.Retryable(xerrors.New("mock")) },
			retry:        retry.New(retry.WithMaxAttempts(2)),
		},
		{
			name:                   "withFilter",
			hasError:               true,
			successWithFilterValue: 1,
			errorValue:             0,
			fn:                     func(ctx context.Context) error { return &MockError{} },
			opts: []Option{
				WithFilter(func(err error) bool {
					var target *MockError
					return xerrors.As(err, &target)
				}),
			},
		},
		{
			name:         "withIrrelevantFilter",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) error { return &MockError{} },
			opts: []Option{
				WithFilter(func(err error) bool {
					return false
				}),
			},
		},
		{
			name:         "withLogger",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) error { return xerrors.New("mock") },
			opts: []Option{
				WithLogger(zaptest.NewLogger(t), "instrument.test"),
			},
		},
		{
			name:         "withTimeSource",
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) error { return nil },
			opts: []Option{
				WithTimeSource(timesource.NewTickingTimeSource()),
			},
		},
		{
			name:         "withTracer",
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) error { return nil },
			opts: []Option{
				WithTracer("some.request", map[string]string{"method": "getBlock"}),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			metrics := tally.NewTestScope("foo", nil)
			i := New(metrics, "bar", test.opts...)
			if test.retry != nil {
				i = i.WithRetry(test.retry)
			}

			err := i.Instrument(context.Background(), test.fn)
			if test.hasError {
				require.Error(err)
			} else {
				require.NoError(err)
			}

			snapshot := metrics.Snapshot()

			successCounter := snapshot.Counters()["foo.bar+result_type=success"]
			require.NotNil(successCounter)
			require.Equal(test.successValue, successCounter.Value())
			require.Equal(successTags, successCounter.Tags())

			successWithFilterCounter := snapshot.Counters()["foo.bar+filtered=true,result_type=success"]
			require.NotNil(successWithFilterCounter)
			require.Equal(test.successWithFilterValue, successWithFilterCounter.Value())
			require.Equal(successWithFilterTags, successWithFilterCounter.Tags())

			errorCounter := snapshot.Counters()["foo.bar+result_type=error"]
			require.NotNil(errorCounter)
			require.Equal(test.errorValue, errorCounter.Value())
			require.Equal(errTags, errorCounter.Tags())

			latency := snapshot.Timers()["foo.bar.latency+"]
			require.NotNil(latency)
			require.Equal(1, len(latency.Values()))
			require.True(latency.Values()[0] > 0)
		})
	}
}

func TestInstrumentWithRetry(t *testing.T) {
	const maxAttempts = 3
	require := testutil.Require(t)

	metrics := tally.NewTestScope("foo", nil)
	i := New(metrics, "bar").WithRetry(retry.New(retry.WithMaxAttempts(maxAttempts)))

	numCalls := 0
	operation := func(ctx context.Context) error {
		numCalls += 1
		if numCalls < maxAttempts {
			return retry.Retryable(xerrors.New("mock"))
		}
		return nil
	}
	err := i.Instrument(context.Background(), operation)
	require.NoError(err)
	require.Equal(maxAttempts, numCalls)

	snapshot := metrics.Snapshot()

	successCounter := snapshot.Counters()["foo.bar+result_type=success"]
	require.NotNil(successCounter)
	require.Equal(int64(1), successCounter.Value())
	require.Equal(successTags, successCounter.Tags())

	errorCounter := snapshot.Counters()["foo.bar+result_type=error"]
	require.NotNil(errorCounter)
	require.Equal(int64(0), errorCounter.Value())
	require.Equal(errTags, errorCounter.Tags())

	latency := snapshot.Timers()["foo.bar.latency+"]
	require.NotNil(latency)
	require.Equal(1, len(latency.Values()))
	require.True(latency.Values()[0] > 0)
}

func TestInstrumentWithResult(t *testing.T) {
	const (
		statusSuccess = "success"
		statusError   = "error"
	)
	tests := []struct {
		name                   string
		hasError               bool
		successValue           int64
		successWithFilterValue int64
		errorValue             int64
		fn                     OperationWithResultFn[string]
		opts                   []Option
		retry                  retry.RetryWithResult[string]
	}{
		{
			name:         statusSuccess,
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) (string, error) { return statusSuccess, nil },
		},
		{
			name:         statusError,
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) (string, error) { return statusError, xerrors.New("mock") },
		},
		{
			name:         "withRetry",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) (string, error) { return statusError, retry.Retryable(xerrors.New("mock")) },
			retry:        retry.NewWithResult[string](retry.WithMaxAttempts(2)),
		},
		{
			name:                   "withFilter",
			hasError:               true,
			successWithFilterValue: 1,
			errorValue:             0,
			fn:                     func(ctx context.Context) (string, error) { return statusError, &MockError{} },
			opts: []Option{
				WithFilter(func(err error) bool {
					var target *MockError
					return xerrors.As(err, &target)
				}),
			},
		},
		{
			name:         "withIrrelevantFilter",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) (string, error) { return statusError, &MockError{} },
			opts: []Option{
				WithFilter(func(err error) bool {
					return false
				}),
			},
		},
		{
			name:         "withLogger",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) (string, error) { return statusError, xerrors.New("mock") },
			opts: []Option{
				WithLogger(zaptest.NewLogger(t), "instrument.test"),
			},
		},
		{
			name:         "withTimeSource",
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) (string, error) { return statusSuccess, nil },
			opts: []Option{
				WithTimeSource(timesource.NewTickingTimeSource()),
			},
		},
		{
			name:         "withTracer",
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) (string, error) { return statusSuccess, nil },
			opts: []Option{
				WithTracer("some.request", map[string]string{"method": "getBlock"}),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			metrics := tally.NewTestScope("foo", nil)
			i := NewWithResult[string](metrics, "bar", test.opts...)
			if test.retry != nil {
				i = i.WithRetry(test.retry)
			}

			res, err := i.Instrument(context.Background(), test.fn)
			if test.hasError {
				require.Error(err)
				require.Equal(statusError, res)
			} else {
				require.NoError(err)
				require.Equal(statusSuccess, res)
			}

			snapshot := metrics.Snapshot()

			successCounter := snapshot.Counters()["foo.bar+result_type=success"]
			require.NotNil(successCounter)
			require.Equal(test.successValue, successCounter.Value())
			require.Equal(successTags, successCounter.Tags())

			successWithFilterCounter := snapshot.Counters()["foo.bar+filtered=true,result_type=success"]
			require.NotNil(successWithFilterCounter)
			require.Equal(test.successWithFilterValue, successWithFilterCounter.Value())
			require.Equal(successWithFilterTags, successWithFilterCounter.Tags())

			errorCounter := snapshot.Counters()["foo.bar+result_type=error"]
			require.NotNil(errorCounter)
			require.Equal(test.errorValue, errorCounter.Value())
			require.Equal(errTags, errorCounter.Tags())

			latency := snapshot.Timers()["foo.bar.latency+"]
			require.NotNil(latency)
			require.Equal(1, len(latency.Values()))
			require.True(latency.Values()[0] > 0)
		})
	}
}

type MockError struct{}

func (e *MockError) Error() string {
	return "mock"
}
