package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

type (
	// Retry is a simple wrapper on top of "cenkalti/backoff" to provide retry functionalities.
	// Main differences with "cenkalti/backoff":
	// * By default, only RetryableError is retried. In "cenkalti/backoff", all errors except for PermanentError are retried.
	// * It is compatible with xerrors, i.e. you may wrap a RetryableError and the default Filter uses xerrors.As to determine if the error is a RetryableError.
	// * Retry is aborted if either MaxElapsedTime or MaxAttempts is exceeded.
	Retry interface {
		Retry(ctx context.Context, operation OperationFn) error
	}

	RetryableError struct {
		Err error
	}

	OperationFn func(ctx context.Context) error
	Backoff     backoff.BackOff

	// Filter should return true if the error is retryable.
	Filter func(err error) bool

	// BackoffFactory returns a new instance of backoff policy.
	BackoffFactory func() Backoff

	Option func(r *retryImpl)

	retryImpl struct {
		maxAttempts    int
		filter         Filter
		backoffFactory BackoffFactory
		logger         *zap.Logger
	}
)

const (
	DefaultMaxAttempts         = 3
	defaultInitialInterval     = time.Second
	defaultRandomizationFactor = 0.5
	defaultMultiplier          = 2
	defaultMaxInterval         = 15 * time.Second
	defaultMaxElapsedTime      = 10 * time.Minute
)

var _ xerrors.Wrapper = (*RetryableError)(nil)

func New(opts ...Option) Retry {
	r := &retryImpl{
		maxAttempts:    DefaultMaxAttempts,
		filter:         defaultFilter,
		backoffFactory: defaultBackoffFactory,
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

func WithMaxAttempts(maxAttempts int) Option {
	return func(r *retryImpl) {
		r.maxAttempts = maxAttempts
	}
}

func WithFilter(filter Filter) Option {
	return func(r *retryImpl) {
		r.filter = filter
	}
}

func WithBackoffFactory(backoffFactory BackoffFactory) Option {
	return func(r *retryImpl) {
		r.backoffFactory = backoffFactory
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(r *retryImpl) {
		r.logger = logger
	}
}

func Retryable(err error) error {
	return &RetryableError{
		Err: err,
	}
}

func (r *retryImpl) Retry(ctx context.Context, operation OperationFn) error {
	b := backoff.WithContext(
		r.backoffFactory(),
		ctx,
	)

	attempts := 0
	decoratedOperation := func() error {
		err := operation(ctx)
		attempts += 1
		if err != nil {
			if retryable := r.filter(err); !retryable {
				if r.logger != nil {
					r.logger.Warn(
						"encountered a permanent error",
						zap.Int("attempts", attempts),
						zap.Error(err),
					)
				}
				return backoff.Permanent(err)
			}

			if attempts >= r.maxAttempts {
				if r.logger != nil {
					r.logger.Warn(
						"max attempts exceeded",
						zap.Int("attempts", attempts),
						zap.Error(err),
					)
				}
				return backoff.Permanent(err)
			}

			if r.logger != nil {
				r.logger.Warn(
					"encountered a retryable error",
					zap.Int("attempts", attempts),
					zap.Error(err),
				)
			}

			return err
		}

		return nil
	}

	return backoff.Retry(decoratedOperation, b)
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("RetryableError: %v", e.Err.Error())
}

func (e *RetryableError) Unwrap() error {
	// Implement `xerrors.Wrapper` so that the original error can be unwrapped.
	return e.Err
}

// defaultFilter retries only the retryable error.
func defaultFilter(err error) bool {
	var retryable *RetryableError
	return xerrors.As(err, &retryable)
}

// defaultBackoffFactory creates an exponential backoff policy.
func defaultBackoffFactory() Backoff {
	return &backoff.ExponentialBackOff{
		InitialInterval:     defaultInitialInterval,
		RandomizationFactor: defaultRandomizationFactor,
		Multiplier:          defaultMultiplier,
		MaxInterval:         defaultMaxInterval,
		MaxElapsedTime:      defaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
}
