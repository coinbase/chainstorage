package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

var errMock = errors.New("mock error")

func TestRetry_Success(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		return nil
	})
	require.NoError(err)
	require.Equal(1, numCalls)
}

func TestWrap_Success(t *testing.T) {
	require := testutil.Require(t)

	numCalls := 0
	err := Wrap(
		context.Background(),
		func(ctx context.Context) error {
			numCalls += 1
			return nil
		},
		WithBackoffFactory(backoffFactory),
	)
	require.NoError(err)
	require.Equal(1, numCalls)
}

func TestRetry_NonRetryable(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < 4 {
			return errors.New("mock")
		}

		return nil
	})
	require.Error(err)
	require.Equal(1, numCalls)
}

func TestRetry_Retryable(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < DefaultMaxAttempts {
			return Retryable(errMock)
		}

		return nil
	})
	require.NoError(err)
	require.Equal(DefaultMaxAttempts, numCalls)
}

func TestRetry_RateLimit(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < DefaultMaxAttempts {
			return RateLimit(errMock)
		}

		return nil
	})
	require.NoError(err)
	require.Equal(DefaultMaxAttempts, numCalls)
}

func TestRetry_RetryableWrapped(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < DefaultMaxAttempts {
			return fmt.Errorf("wrapped error: %w", Retryable(errMock))
		}

		return nil
	})
	require.NoError(err)
	require.Equal(DefaultMaxAttempts, numCalls)
}

func TestRetry_Permanent(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < DefaultMaxAttempts+1 {
			return Retryable(errMock)
		}

		return errMock
	})
	require.Error(err)
	require.Equal(DefaultMaxAttempts, numCalls)
	require.True(errors.Is(err, errMock))
}

func TestRetry_MaxAttemptsExceeded(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < 20 {
			return Retryable(errMock)
		}

		return nil
	})
	require.Error(err)
	require.Equal(DefaultMaxAttempts, numCalls)
	require.True(errors.Is(err, errMock))
}

func TestRetry_WithOptions(t *testing.T) {
	require := testutil.Require(t)

	r := New(
		WithBackoffFactory(backoffFactory),
		WithMaxAttempts(5),
		WithLogger(zap.L()),
	)
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < 20 {
			return Retryable(errMock)
		}

		return nil
	})
	require.Error(err)
	require.Equal(5, numCalls)
	require.True(errors.Is(err, errMock))
}

func TestRetryWithResult(t *testing.T) {
	require := testutil.Require(t)

	r := NewWithResult[string](WithBackoffFactory(backoffFactory))
	numCalls := 0
	res, err := r.Retry(context.Background(), func(ctx context.Context) (string, error) {
		numCalls += 1
		return "success", nil
	})
	require.NoError(err)
	require.Equal("success", res)
	require.Equal(1, numCalls)
}

func TestWrapWithResult(t *testing.T) {
	require := testutil.Require(t)

	numCalls := 0
	res, err := WrapWithResult(
		context.Background(),
		func(ctx context.Context) (string, error) {
			numCalls += 1
			return "success", nil
		},
		WithBackoffFactory(backoffFactory),
	)
	require.NoError(err)
	require.Equal("success", res)
	require.Equal(1, numCalls)
}

func TestRetryWithResult_NonRetryable(t *testing.T) {
	require := testutil.Require(t)

	r := NewWithResult[string](WithBackoffFactory(backoffFactory))
	numCalls := 0
	res, err := r.Retry(context.Background(), func(ctx context.Context) (string, error) {
		numCalls += 1
		if numCalls < 4 {
			return "failure", errors.New("mock")
		}

		return "success", nil
	})
	require.Error(err)
	require.Equal("failure", res)
	require.Equal(1, numCalls)
}

func TestRetryWithResult_Retryable(t *testing.T) {
	require := testutil.Require(t)

	r := NewWithResult[string](WithBackoffFactory(backoffFactory))
	numCalls := 0
	res, err := r.Retry(context.Background(), func(ctx context.Context) (string, error) {
		numCalls += 1
		if numCalls < DefaultMaxAttempts {
			return "failure", Retryable(errMock)
		}

		return "success", nil
	})
	require.NoError(err)
	require.Equal("success", res)
	require.Equal(DefaultMaxAttempts, numCalls)
}

func backoffFactory() Backoff {
	return &backoff.ZeroBackOff{}
}
