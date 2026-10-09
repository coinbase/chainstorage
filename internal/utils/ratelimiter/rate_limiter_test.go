package ratelimiter

import (
	"context"
	"testing"
	"time"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestNoLimit(t *testing.T) {
	require := testutil.Require(t)
	var limiter *RateLimiter
	require.Equal(0, limiter.Limit())
	require.True(tryLimiter(limiter))

	limiter = New(0)
	require.Equal(0, limiter.Limit())
	require.True(tryLimiter(limiter))

	limiter = New(-100)
	require.Equal(0, limiter.Limit())
	require.True(tryLimiter(limiter))

	limiter = New(120)
	require.Equal(120, limiter.Limit())
	require.True(tryLimiter(limiter))

	limiter = New(120)
	require.True(tryLimiterN(limiter))
}

func TestLimit(t *testing.T) {
	require := testutil.Require(t)

	limiter := New(80)
	require.Equal(80, limiter.Limit())
	require.False(tryLimiter(limiter))
}

func TestNoWait(t *testing.T) {
	require := testutil.Require(t)

	limiter := New(0)
	require.Equal(0, limiter.Limit())
	start := time.Now()
	for i := 0; i < 100; i++ {
		limiter.WaitN(context.TODO(), 1)
	}
	require.True(time.Now().Sub(start).Seconds() < 0.01)
}

func TestWait(t *testing.T) {
	/**
	first 10 requests, should take no time
	then receive 1 token per 1/10 second
	therefore requests 11 - 21 should take 0.1 second each
	**/
	require := testutil.Require(t)

	limiter := New(10)
	require.Equal(10, limiter.Limit())
	start := time.Now()
	for i := 0; i < 21; i++ {
		require.Nil(limiter.WaitN(context.TODO(), 1))
	}
	duration := time.Now().Sub(start).Seconds()
	require.True(duration >= 1.0)
	require.True(duration <= 1.11)
}

func TestWaitWeight(t *testing.T) {
	require := testutil.Require(t)

	// nil limiter is unlimited.
	var nilLimiter *RateLimiter
	require.NoError(nilLimiter.WaitWeight(context.Background(), 100))

	// WaitN fails when n exceeds the burst, while WaitWeight waits in chunks.
	limiter := New(20)
	require.Error(limiter.WaitN(context.Background(), 25))

	start := time.Now()
	require.NoError(limiter.WaitWeight(context.Background(), 25))
	// 25 tokens at 20 rps with burst 20: at least 5 tokens must be waited for.
	require.True(time.Since(start) >= 200*time.Millisecond, "expected chunked wait, elapsed=%v", time.Since(start))
}

func TestWaitWeightClampsNonPositive(t *testing.T) {
	require := testutil.Require(t)

	for _, n := range []int{0, -5} {
		limiter := New(1)
		start := time.Now()
		// A non-positive weight consumes a single token from the full bucket,
		// so it must return immediately without error.
		require.NoError(limiter.WaitWeight(context.Background(), n))
		require.True(time.Since(start) < 500*time.Millisecond)
		// The single token is spent: the next request is not allowed.
		require.False(limiter.Allow())
	}
}

func tryLimiter(limiter *RateLimiter) bool {
	for i := 0; i < 100; i++ {
		if !limiter.Allow() {
			return false
		}
	}

	return true
}

func tryLimiterN(limiter *RateLimiter) bool {
	for i := 0; i < 5; i++ {
		if !limiter.AllowN(20) {
			return false
		}
	}

	return true
}
