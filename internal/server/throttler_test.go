package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
)

func TestThrottler(t *testing.T) {
	require := require.New(t)
	cfg := &config.ApiConfig{
		Auth: config.AuthConfig{
			Clients: []config.AuthClient{
				{
					ClientID: "foo",
					Token:    "foo_token",
					RPS:      50,
				},
				{
					ClientID: "bar",
					Token:    "bar_token",
				},
			},
			DefaultRPS: 100,
		},
		RateLimit: config.RateLimitConfig{
			GlobalRPS:    500,
			PerClientRPS: 300,
		},
	}

	throttler := NewThrottler(cfg)
	require.Equal(50, throttler.getPerClientRateLimiter("foo").Limit())
	require.Equal(100, throttler.getPerClientRateLimiter("bar").Limit())
	require.Equal(300, throttler.getPerClientRateLimiter("baz").Limit())
	require.Equal(500, throttler.globalRateLimiter.Limit())
	for i := 0; i < 10; i++ {
		require.True(throttler.Allow("foo"))
		require.True(throttler.Allow("bar"))
		require.True(throttler.Allow("baz"))
	}
}

func TestThrottler_NoPerClientLimit(t *testing.T) {
	require := require.New(t)
	cfg := &config.ApiConfig{
		Auth: config.AuthConfig{
			Clients: []config.AuthClient{
				{
					ClientID: "foo",
					Token:    "foo_token",
					RPS:      50,
				},
				{
					ClientID: "bar",
					Token:    "bar_token",
				},
			},
			DefaultRPS: 100,
		},
		RateLimit: config.RateLimitConfig{
			GlobalRPS: 500,
		},
	}

	throttler := NewThrottler(cfg)
	require.Equal(50, throttler.getPerClientRateLimiter("foo").Limit())
	require.Equal(100, throttler.getPerClientRateLimiter("bar").Limit())
	require.Equal(0, throttler.getPerClientRateLimiter("baz").Limit())
	require.Equal(500, throttler.globalRateLimiter.Limit())
	for i := 0; i < 10; i++ {
		require.True(throttler.Allow("foo"))
		require.True(throttler.Allow("bar"))
		require.True(throttler.Allow("baz"))
	}
}

func TestThrottler_NoGlobalLimit(t *testing.T) {
	require := require.New(t)
	cfg := &config.ApiConfig{
		Auth: config.AuthConfig{
			Clients: []config.AuthClient{
				{
					ClientID: "foo",
					Token:    "foo_token",
					RPS:      50,
				},
				{
					ClientID: "bar",
					Token:    "bar_token",
				},
			},
			DefaultRPS: 100,
		},
		RateLimit: config.RateLimitConfig{
			PerClientRPS: 300,
		},
	}

	throttler := NewThrottler(cfg)
	require.Equal(50, throttler.getPerClientRateLimiter("foo").Limit())
	require.Equal(100, throttler.getPerClientRateLimiter("bar").Limit())
	require.Equal(300, throttler.getPerClientRateLimiter("baz").Limit())
	require.Equal(0, throttler.globalRateLimiter.Limit())
	for i := 0; i < 10; i++ {
		require.True(throttler.Allow("foo"))
		require.True(throttler.Allow("bar"))
		require.True(throttler.Allow("baz"))
	}
}
