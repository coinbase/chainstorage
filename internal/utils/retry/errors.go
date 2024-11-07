package retry

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

type (
	RetryableError struct {
		Err error
	}

	RateLimitError struct {
		Err error
	}
)

var (
	_ errors.Wrapper = (*RetryableError)(nil)
	_ errors.Wrapper = (*RateLimitError)(nil)
)

// Retryable returns an error that indicates that the operation should be retried.
func Retryable(err error) error {
	return &RetryableError{
		Err: err,
	}
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("RetryableError: %v", e.Err.Error())
}

func (e *RetryableError) Unwrap() error {
	// Implement `errors.Wrapper` so that the original error can be unwrapped.
	return e.Err
}

// RateLimit returns an error that indicates that the operation should be retried after a delay.
func RateLimit(err error) error {
	return &RateLimitError{
		Err: err,
	}
}
func (e *RateLimitError) Error() string {
	return fmt.Sprintf("RateLimitError: %v", e.Err.Error())
}

func (e *RateLimitError) Unwrap() error {
	// Implement `errors.Wrapper` so that the original error can be unwrapped.
	return e.Err
}
