package instrument

import (
	"context"
	"testing"

	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
)

func TestInstrument(t *testing.T) {
	tests := []struct {
		name         string
		hasError     bool
		successValue int64
		errorValue   int64
		fn           OperationFn
		opts         []CallOption
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
			fn:           func(ctx context.Context) error { return xerrors.New("mock") },
			opts: []CallOption{
				WithRetry(retry.New(retry.WithMaxAttempts(2))),
			},
		},
		{
			name:         "withFilter",
			hasError:     true,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) error { return &MockError{} },
			opts: []CallOption{
				WithFilter(func(err error) bool {
					var target *MockError
					return xerrors.As(err, &target)
				}),
			},
		},
		{
			name:         "withLogger",
			hasError:     true,
			successValue: 0,
			errorValue:   1,
			fn:           func(ctx context.Context) error { return xerrors.New("mock") },
			opts: []CallOption{
				WithLogger(zaptest.NewLogger(t), "instrument.test"),
			},
		},
		{
			name:         "withTimeSource",
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) error { return nil },
			opts: []CallOption{
				WithTimeSource(timesource.NewTickingTimeSource()),
			},
		},
		{
			name:         "withTracer",
			hasError:     false,
			successValue: 1,
			errorValue:   0,
			fn:           func(ctx context.Context) error { return nil },
			opts: []CallOption{
				WithTracer("some.request", map[string]string{"method": "getBlock"}),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			metrics := tally.NewTestScope("foo", nil)
			call := NewCall(metrics, "bar", test.opts...)
			err := call.Instrument(context.Background(), test.fn)
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
