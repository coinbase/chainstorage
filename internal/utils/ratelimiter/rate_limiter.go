package ratelimiter

import (
	"time"

	"golang.org/x/time/rate"
)

type (
	RateLimiter struct {
		limiter *rate.Limiter
	}
)

// New returns a new Limiter that allows events up to r requests per second.
func New(rps int) *RateLimiter {
	if rps == 0 {
		// Unlimited
		return nil
	}

	limiter := rate.NewLimiter(rate.Limit(rps), rps)
	return &RateLimiter{limiter: limiter}
}

// Allow returns true if the request should be allowed.
func (l *RateLimiter) Allow() bool {
	if l == nil {
		return true
	}

	return l.limiter.Allow()
}

// AllowN returns true if N requests should be allowed.
func (l *RateLimiter) AllowN(n int) bool {
	if l == nil {
		return true
	}

	if n == 0 {
		return true
	}

	return l.limiter.AllowN(time.Now(), n)
}

// Limit returns the maximum overall event rate.
// Zero is returned when there is no limit specified.
func (l *RateLimiter) Limit() int {
	if l == nil {
		return 0
	}

	return int(l.limiter.Limit())
}
