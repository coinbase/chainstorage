package retry

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

type (
	// Retry is a simple wrapper on top of "cenkalti/backoff" to provide retry functionalities.
	// Main differences with "cenkalti/backoff":
	// * By default, only RetryableError and RateLimitError are retried. In "cenkalti/backoff", all errors except for PermanentError are retried.
	// * It is compatible with xerrors, i.e. you may wrap a RetryableError and the default Filter uses xerrors.As to determine if the error is a RetryableError.
	// * Retry is aborted if either MaxElapsedTime or MaxAttempts is exceeded.
	Retry interface {
		Retry(ctx context.Context, operation OperationFn) error
	}

	// RetryWithResult is similar to Retry except that the operation returns a generic result.
	RetryWithResult[T any] interface {
		Retry(ctx context.Context, operation OperationWithResultFn[T]) (T, error)
	}

	OperationFn                  func(ctx context.Context) error
	OperationWithResultFn[T any] func(ctx context.Context) (T, error)
	Backoff                      backoff.BackOff

	// BackoffFactory returns a new instance of backoff policy.
	BackoffFactory func() Backoff

	Option func(r *retryOptions)

	retry struct {
		impl RetryWithResult[struct{}]
	}

	retryWithResult[T any] struct {
		maxAttempts    int
		backoffFactory BackoffFactory
		logger         *zap.Logger
	}

	retryOptions struct {
		maxAttempts    int
		backoffFactory BackoffFactory
		logger         *zap.Logger
	}
)

const (
	DefaultMaxAttempts         = 4
	defaultInitialInterval     = 200 * time.Millisecond
	defaultRandomizationFactor = 0.5
	defaultMultiplier          = 2
	defaultMaxInterval         = 15 * time.Second
	defaultMaxElapsedTime      = 10 * time.Minute
)

// New creates a new instance of Retry that works with `func(ctx context.Context) error`.
func New(opts ...Option) Retry {
	return &retry{
		impl: NewWithResult[struct{}](opts...),
	}
}

// Wrap wraps the given operation with retry functionalities.
func Wrap(ctx context.Context, operation OperationFn, opts ...Option) error {
	return New(opts...).Retry(ctx, operation)
}

// ToRetryWithResult converts Retry to RetryWithResult[struct{}].
func ToRetryWithResult(r Retry) RetryWithResult[struct{}] {
	return r.(*retry).impl
}

// NewWithResult creates a new instance of RetryWithResult that works with `func(ctx context.Context) (T, error)`.
func NewWithResult[T any](opts ...Option) RetryWithResult[T] {
	options := &retryOptions{
		maxAttempts:    DefaultMaxAttempts,
		backoffFactory: defaultBackoffFactory,
		logger:         zap.NewNop(),
	}
	for _, opt := range opts {
		opt(options)
	}

	r := &retryWithResult[T]{
		maxAttempts:    options.maxAttempts,
		backoffFactory: options.backoffFactory,
		logger:         options.logger,
	}

	return r
}

// WrapWithResult wraps the given operation with retry functionalities.
func WrapWithResult[T any](ctx context.Context, operation OperationWithResultFn[T], opts ...Option) (T, error) {
	return NewWithResult[T](opts...).Retry(ctx, operation)
}

// WithMaxAttempts sets the maximum number of attempts.
// The default value is 3.
// Note that when maxAttempts is 1, the operation is executed only once without any retry.
func WithMaxAttempts(maxAttempts int) Option {
	if maxAttempts < 1 {
		maxAttempts = DefaultMaxAttempts
	}

	return func(o *retryOptions) {
		o.maxAttempts = maxAttempts
	}
}

// WithBackoffFactory sets the backoff factory.
// The default backoff factory creates an exponential backoff policy.
func WithBackoffFactory(backoffFactory BackoffFactory) Option {
	return func(o *retryOptions) {
		o.backoffFactory = backoffFactory
	}
}

// WithLogger sets the logger.
// The default logger is zap.NewNop().
func WithLogger(logger *zap.Logger) Option {
	return func(o *retryOptions) {
		o.logger = logger
	}
}

func (r *retry) Retry(ctx context.Context, operation OperationFn) error {
	_, err := r.impl.Retry(ctx, func(ctx context.Context) (struct{}, error) {
		err := operation(ctx)
		return struct{}{}, err
	})
	return err
}

func (r *retryWithResult[T]) Retry(ctx context.Context, operation OperationWithResultFn[T]) (T, error) {
	backoffContext := backoff.WithContext(
		r.backoffFactory(),
		ctx,
	)

	attempts := 0
	decoratedOperation := func() (T, error) {
		res, err := operation(ctx)
		attempts += 1
		if err != nil {
			if retryable := filter(err); !retryable {
				r.logger.Warn(
					"encountered a permanent error",
					zap.Int("attempts", attempts),
					zap.Error(err),
				)
				return res, backoff.Permanent(err)
			}

			if attempts >= r.maxAttempts {
				r.logger.Warn(
					"max attempts exceeded",
					zap.Int("attempts", attempts),
					zap.Error(err),
				)
				return res, backoff.Permanent(err)
			}

			r.logger.Warn(
				"encountered a retryable error",
				zap.Int("attempts", attempts),
				zap.Error(err),
			)

			return res, err
		}

		return res, nil
	}

	onError := func(err error, duration time.Duration) {
		var rateLimitErr *RateLimitError
		if xerrors.As(err, &rateLimitErr) {
			// In case of a RateLimitError, introduce an extra backoff.
			select {
			case <-backoffContext.Context().Done():
			case <-time.After(duration + time.Second):
			}
		}
	}

	return backoff.RetryNotifyWithData[T](decoratedOperation, backoffContext, onError)
}

// filter returns if the error is retryable.
func filter(err error) bool {
	var retryableErr *RetryableError
	var rateLimitErr *RateLimitError
	return xerrors.As(err, &retryableErr) || xerrors.As(err, &rateLimitErr)
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
