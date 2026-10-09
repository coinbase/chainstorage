package ratelimiter

import (
	"context"
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
	if rps <= 0 {
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

// WaitN blocks until it permits n events to happen
func (l *RateLimiter) WaitN(ctx context.Context, n int) (err error) {
	if l == nil {
		return nil
	}

	return l.limiter.WaitN(ctx, n)
}

// WaitWeight blocks until it permits n events to happen. Unlike WaitN, n may
// exceed the limiter's burst size: the wait is split into burst-sized chunks
// so that the total number of tokens consumed is still n. A non-positive n is
// treated as 1.
func (l *RateLimiter) WaitWeight(ctx context.Context, n int) error {
	if l == nil {
		return nil
	}

	if n <= 0 {
		n = 1
	}

	burst := l.limiter.Burst()
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}

		if err := l.limiter.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}

	return nil
}

// Limit returns the maximum overall event rate.
// Zero is returned when there is no limit specified.
func (l *RateLimiter) Limit() int {
	if l == nil {
		return 0
	}

	return int(l.limiter.Limit())
}
