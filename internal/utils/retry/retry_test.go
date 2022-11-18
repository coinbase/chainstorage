package retry

import (
	"context"
	"testing"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

var errMock = xerrors.New("mock error")

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

func TestRetry_NonRetryable(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < 4 {
			return xerrors.New("mock")
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

func TestRetry_RetryableWrapped(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory))
	numCalls := 0
	err := r.Retry(context.Background(), func(ctx context.Context) error {
		numCalls += 1
		if numCalls < DefaultMaxAttempts {
			return xerrors.Errorf("wrapped error: %w", Retryable(errMock))
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
	require.True(xerrors.Is(err, errMock))
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
	require.True(xerrors.Is(err, errMock))
}

func TestRetry_WithOptions(t *testing.T) {
	require := testutil.Require(t)

	r := New(WithBackoffFactory(backoffFactory), WithMaxAttempts(5), WithLogger(zap.L()))
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
	require.True(xerrors.Is(err, errMock))
}

func backoffFactory() Backoff {
	return &backoff.ZeroBackOff{}
}
