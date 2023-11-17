package server

import (
	"sync"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/ratelimiter"
)

type (
	// Throttler allows a request only if all the following conditions are met:
	// 1. Global rate limit, shared by all client IDs, is not reached. This is configured via "api.rate_limit.global_rps".
	// 2. Per-client rate limit is not reached. This is configured via either "api.auth.clients" or "api.rate_limit.per_client_rps".
	//    The former supports client-specific rate limits, while the latter uses the same rate limit for all client IDs.
	//    See throttler_test.go for sample configs.
	Throttler struct {
		globalRateLimiter     *ratelimiter.RateLimiter
		perClientRateLimiters *sync.Map
		rateLimiterPool       *sync.Pool
	}
)

func NewThrottler(cfg *config.ApiConfig) *Throttler {
	global := ratelimiter.New(cfg.RateLimit.GlobalRPS)

	perClient := new(sync.Map)
	for _, client := range cfg.Auth.Clients {
		rps := client.RPS
		if rps == 0 {
			rps = cfg.Auth.DefaultRPS
		}
		perClient.Store(client.ClientID, ratelimiter.New(rps))
	}

	pool := &sync.Pool{
		New: func() any {
			return ratelimiter.New(cfg.RateLimit.PerClientRPS)
		},
	}

	return &Throttler{
		globalRateLimiter:     global,
		perClientRateLimiters: perClient,
		rateLimiterPool:       pool,
	}
}

func (t *Throttler) Allow(clientID string) bool {
	return t.AllowN(clientID, 1)
}

func (t *Throttler) AllowN(clientID string, units int) bool {
	perClientRateLimiter := t.getPerClientRateLimiter(clientID)

	// Evaluate both rate limiters without short circuit evaluation.
	perClientAllowed := perClientRateLimiter.AllowN(units)
	globalAllowed := t.globalRateLimiter.AllowN(units)
	return perClientAllowed && globalAllowed
}

func (t *Throttler) getPerClientRateLimiter(clientID string) *ratelimiter.RateLimiter {
	// Cache new limiter in a pool to avoid creating a new object for every request.
	newRateLimiter := t.rateLimiterPool.Get()
	perClientRateLimiter, loaded := t.perClientRateLimiters.LoadOrStore(clientID, newRateLimiter)
	if loaded {
		// After the service is warmed up, loaded should hold true for most requests.
		t.rateLimiterPool.Put(newRateLimiter)
	}
	return perClientRateLimiter.(*ratelimiter.RateLimiter)
}
