package ratelimiter

import (
	"testing"

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
