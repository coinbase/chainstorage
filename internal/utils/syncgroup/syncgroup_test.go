package syncgroup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestSyncGroup_Success(t *testing.T) {
	const (
		iterations = 20
	)

	require := testutil.Require(t)

	group, _ := New(context.Background())
	var calls int32
	for i := 0; i < iterations; i++ {
		group.Go(func() error {
			atomic.AddInt32(&calls, 1)
			return nil
		})
	}

	err := group.Wait()
	require.NoError(err)
	require.Equal(int32(iterations), calls)
}

func TestSyncGroup_Cancelled(t *testing.T) {
	const (
		iterations = 20
	)

	require := testutil.Require(t)

	group, ctx := New(context.Background())
	var calls int32
	for i := 0; i < iterations; i++ {
		i := i
		group.Go(func() error {
			if i == iterations-1 {
				// Simulate a worker that fails right away.
				return errors.New("bad worker")
			}

			// Simulate a slow worker.
			delay := time.After(time.Second * 10)
			select {
			case <-ctx.Done():
				return nil
			case <-delay:
			}

			atomic.AddInt32(&calls, 1)
			return nil
		})
	}

	err := group.Wait()
	require.Error(err)
	require.Equal("bad worker", err.Error())

	// None of the workers should finish.
	require.Equal(int32(0), calls)
}

func TestSyncGroup_WithThrottling(t *testing.T) {
	const (
		iterations = 20
		limit      = 4
	)

	require := testutil.Require(t)

	group, _ := New(context.Background(), WithThrottling(limit))
	sem := semaphore.NewWeighted(limit)
	var calls int32
	for i := 0; i < iterations; i++ {
		group.Go(func() error {
			defer sem.Release(1)

			if !sem.TryAcquire(1) {
				return errors.New("too many workers running in parallel")
			}

			time.Sleep(time.Millisecond * 20)
			atomic.AddInt32(&calls, 1)
			return nil
		})
	}

	err := group.Wait()
	require.NoError(err)
	require.Equal(int32(iterations), calls)
}

func TestSyncGroup_CancelledWithThrottling(t *testing.T) {
	const (
		iterations = 20
		limit      = 4
	)

	require := testutil.Require(t)

	group, _ := New(context.Background(), WithThrottling(limit))
	for i := 0; i < iterations; i++ {
		i := i
		group.Go(func() error {
			if i == limit-1 {
				// Simulate a worker that fails right away.
				return errors.New("bad worker")
			}

			time.Sleep(time.Millisecond * 20)
			return nil
		})
	}

	err := group.Wait()
	require.Error(err)
	require.Equal("bad worker", err.Error())
}

func TestSyncGroup_WithFilter(t *testing.T) {
	const (
		iterations = 20
	)

	require := testutil.Require(t)

	errIgnored := xerrors.New("ignore this error")
	group, _ := New(context.Background(), WithFilter(func(err error) error {
		if xerrors.Is(err, errIgnored) {
			return nil
		}

		return err
	}))
	var calls int32
	for i := 0; i < iterations; i++ {
		group.Go(func() error {
			atomic.AddInt32(&calls, 1)
			return errIgnored
		})
	}

	err := group.Wait()
	require.NoError(err)
	require.Equal(int32(iterations), calls)
}

func TestSyncGroup_WithThrottlingAndFilter(t *testing.T) {
	const (
		iterations = 20
		limit      = 4
	)

	require := testutil.Require(t)

	errIgnored := xerrors.New("ignore this error")
	group, _ := New(
		context.Background(),
		WithThrottling(limit),
		WithFilter(func(err error) error {
			if xerrors.Is(err, errIgnored) {
				return nil
			}

			return err
		}))
	var calls int32
	for i := 0; i < iterations; i++ {
		group.Go(func() error {
			atomic.AddInt32(&calls, 1)
			return errIgnored
		})
	}

	err := group.Wait()
	require.NoError(err)
	require.Equal(int32(iterations), calls)
}
