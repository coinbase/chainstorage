package instrument

import (
	"context"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
)

type (
	Instrument interface {
		Instrument(ctx context.Context, operation OperationFn, opts ...InstrumentOption) error
		WithRetry(retry retry.Retry) Instrument
	}

	InstrumentWithResult[T any] interface {
		Instrument(ctx context.Context, operation OperationWithResultFn[T], opts ...InstrumentOption) (T, error)
		WithRetry(retry retry.RetryWithResult[T]) InstrumentWithResult[T]
	}

	OperationFn                  func(ctx context.Context) error
	OperationWithResultFn[T any] func(ctx context.Context) (T, error)
	FilterFn                     func(err error) bool

	Option           func(c *options)
	InstrumentOption func(options *instrumentOptions)

	instrument struct {
		impl InstrumentWithResult[struct{}]
	}

	instrumentWithResult[T any] struct {
		name              string
		err               tally.Counter
		success           tally.Counter
		successWithFilter tally.Counter
		latency           tally.Timer
		retry             retry.RetryWithResult[T]
		*options
	}

	options struct {
		filter     FilterFn
		timeSource timesource.TimeSource
		logger     *zap.Logger
		loggerMsg  string
		tracerMsg  string
		tracerTags map[string]string
	}

	instrumentOptions struct {
		loggerFields []zap.Field
	}
)

const (
	resultTypeTag     = "result_type"
	resultTypeError   = "error"
	resultTypeSuccess = "success"
	latencySuffix     = "latency"
	durationTag       = "duration"
	filteredTag       = "filtered"
)

var (
	errTags = map[string]string{
		resultTypeTag: resultTypeError,
	}

	successTags = map[string]string{
		resultTypeTag: resultTypeSuccess,
	}

	successWithFilterTags = map[string]string{
		resultTypeTag: resultTypeSuccess,
		filteredTag:   "true",
	}
)

func New(scope tally.Scope, name string, opts ...Option) Instrument {
	return &instrument{
		impl: NewWithResult[struct{}](scope, name, opts...),
	}
}

func Wrap(
	scope tally.Scope,
	name string,
	ctx context.Context,
	operation OperationFn,
	opts ...Option,
) error {
	return New(scope, name, opts...).Instrument(ctx, operation)
}

func NewWithResult[T any](scope tally.Scope, name string, opts ...Option) InstrumentWithResult[T] {
	options := &options{
		filter:     nil,
		timeSource: timesource.NewRealTimeSource(),
		logger:     zap.NewNop(),
		loggerMsg:  name,
		tracerMsg:  name,
		tracerTags: make(map[string]string),
	}
	for _, opt := range opts {
		opt(options)
	}

	c := &instrumentWithResult[T]{
		name:              name,
		err:               scope.Tagged(errTags).Counter(name),
		success:           scope.Tagged(successTags).Counter(name),
		successWithFilter: scope.Tagged(successWithFilterTags).Counter(name),
		latency:           scope.SubScope(name).Timer(latencySuffix),
		options:           options,
	}

	return c
}

func WrapWithResult[T any](
	scope tally.Scope,
	name string,
	ctx context.Context,
	operation OperationWithResultFn[T],
	opts ...Option,
) (T, error) {
	return NewWithResult[T](scope, name, opts...).Instrument(ctx, operation)
}

func WithFilter(filter FilterFn) Option {
	return func(o *options) {
		o.filter = filter
	}
}

func WithLogger(logger *zap.Logger, msg string) Option {
	return func(o *options) {
		o.logger = logger
		o.loggerMsg = msg
	}
}

func WithTracer(msg string, tags map[string]string) Option {
	return func(o *options) {
		o.tracerMsg = msg
		for k, v := range tags {
			o.tracerTags[k] = v
		}
	}
}

func WithTimeSource(timeSource timesource.TimeSource) Option {
	return func(o *options) {
		o.timeSource = timeSource
	}
}

func WithLoggerFields(fields ...zap.Field) InstrumentOption {
	return func(options *instrumentOptions) {
		options.loggerFields = append(options.loggerFields, fields...)
	}
}

func (i *instrument) Instrument(ctx context.Context, operation OperationFn, opts ...InstrumentOption) error {
	_, err := i.impl.Instrument(ctx, func(ctx context.Context) (struct{}, error) {
		err := operation(ctx)
		return struct{}{}, err
	}, opts...)
	return err
}

func (i *instrument) WithRetry(r retry.Retry) Instrument {
	return &instrument{
		impl: i.impl.WithRetry(retry.ToRetryWithResult(r)),
	}
}

func (i *instrumentWithResult[T]) Instrument(ctx context.Context, operation OperationWithResultFn[T], opts ...InstrumentOption) (T, error) {
	options := new(instrumentOptions)
	for _, opt := range opts {
		opt(options)
	}

	startTime := i.timeSource.Now()
	span, ctx := i.startSpan(ctx, startTime)
	var res T
	var err error
	if i.retry == nil {
		res, err = operation(ctx)
	} else {
		res, err = i.retry.Retry(ctx, retry.OperationWithResultFn[T](operation))
	}

	finishTime := i.timeSource.Now()
	duration := finishTime.Sub(startTime)
	i.latency.Record(duration)

	logger := i.logger.With(zap.String(durationTag, duration.String()))
	if len(options.loggerFields) > 0 {
		logger = logger.With(options.loggerFields...)
	}

	if err != nil {
		if i.filter != nil && i.filter(err) {
			i.onSuccessWithFilter(logger, span, finishTime, err)
		} else {
			i.onError(logger, span, finishTime, err)
		}
		return res, err
	}

	i.onSuccess(logger, span, finishTime)
	return res, nil
}

func (i *instrumentWithResult[T]) WithRetry(retry retry.RetryWithResult[T]) InstrumentWithResult[T] {
	clone := *i
	clone.retry = retry
	return &clone
}

func (i *instrumentWithResult[T]) startSpan(ctx context.Context, startTime time.Time) (tracer.Span, context.Context) {
	opts := []tracer.StartSpanOption{
		tracer.SpanType("custom"),
		tracer.StartTime(startTime),
	}
	for k, v := range i.tracerTags {
		opts = append(opts, tracer.Tag(k, v))
	}
	return tracer.StartSpanFromContext(ctx, i.tracerMsg, opts...)
}

func (i *instrumentWithResult[T]) onSuccess(logger *zap.Logger, span tracer.Span, finishTime time.Time) {
	i.success.Inc(1)
	logger.Debug(i.loggerMsg)
	span.Finish(tracer.FinishTime(finishTime))
}

func (i *instrumentWithResult[T]) onSuccessWithFilter(logger *zap.Logger, span tracer.Span, finishTime time.Time, err error) {
	i.successWithFilter.Inc(1)
	logger.Debug(i.loggerMsg, zap.Error(err))
	span.Finish(tracer.FinishTime(finishTime), tracer.WithError(err))
}

func (i *instrumentWithResult[T]) onError(logger *zap.Logger, span tracer.Span, finishTime time.Time, err error) {
	i.err.Inc(1)
	logger.Warn(i.loggerMsg, zap.Error(err))
	span.Finish(tracer.FinishTime(finishTime), tracer.WithError(err))
}
