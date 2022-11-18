package instrument

import (
	"context"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
)

type (
	Call interface {
		Instrument(ctx context.Context, operation OperationFn, opts ...InstrumentOption) error
	}

	OperationFn func(ctx context.Context) error
	FilterFn    func(err error) bool

	CallOption       func(c *callImpl)
	InstrumentOption func(options *instrumentOptions)

	callImpl struct {
		name       string
		err        tally.Counter
		success    tally.Counter
		latency    tally.Timer
		retry      retry.Retry
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
	resultType        = "result_type"
	resultTypeError   = "error"
	resultTypeSuccess = "success"
	latencySuffix     = "latency"
	durationTag       = "duration"
)

var (
	errTags = map[string]string{
		resultType: resultTypeError,
	}

	successTags = map[string]string{
		resultType: resultTypeSuccess,
	}
)

func NewCall(scope tally.Scope, name string, opts ...CallOption) Call {
	c := &callImpl{
		name:       name,
		err:        scope.Tagged(errTags).Counter(name),
		success:    scope.Tagged(successTags).Counter(name),
		latency:    scope.SubScope(name).Timer(latencySuffix),
		timeSource: timesource.NewRealTimeSource(),
		logger:     zap.NewNop(),
		loggerMsg:  name,
		tracerMsg:  name,
		tracerTags: make(map[string]string),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func WithRetry(retry retry.Retry) CallOption {
	return func(c *callImpl) {
		c.retry = retry
	}
}

func WithFilter(filter FilterFn) CallOption {
	return func(c *callImpl) {
		c.filter = filter
	}
}

func WithLogger(logger *zap.Logger, msg string) CallOption {
	return func(c *callImpl) {
		c.logger = logger
		c.loggerMsg = msg
	}
}

func WithTracer(msg string, tags map[string]string) CallOption {
	return func(c *callImpl) {
		c.tracerMsg = msg
		for k, v := range tags {
			c.tracerTags[k] = v
		}
	}
}

func WithTimeSource(timeSource timesource.TimeSource) CallOption {
	return func(c *callImpl) {
		c.timeSource = timeSource
	}
}

func WithLoggerFields(fields ...zap.Field) InstrumentOption {
	return func(options *instrumentOptions) {
		options.loggerFields = append(options.loggerFields, fields...)
	}
}

func (c *callImpl) Instrument(ctx context.Context, operation OperationFn, opts ...InstrumentOption) error {
	options := new(instrumentOptions)
	for _, opt := range opts {
		opt(options)
	}

	startTime := c.timeSource.Now()
	span, ctx := c.startSpan(ctx, startTime)
	var err error
	if c.retry == nil {
		err = operation(ctx)
	} else {
		err = c.retry.Retry(ctx, retry.OperationFn(operation))
	}

	finishTime := c.timeSource.Now()
	duration := finishTime.Sub(startTime)
	c.latency.Record(duration)

	logger := c.logger.With(zap.String(durationTag, duration.String()))
	if len(options.loggerFields) > 0 {
		logger = logger.With(options.loggerFields...)
	}

	if err != nil {
		if c.filter != nil && c.filter(err) {
			c.onSuccess(logger, span, finishTime)
		} else {
			c.onError(logger, span, finishTime, err)
		}

		return err
	}

	c.onSuccess(logger, span, finishTime)
	return nil
}

func (c *callImpl) startSpan(ctx context.Context, startTime time.Time) (tracer.Span, context.Context) {
	opts := []tracer.StartSpanOption{
		tracer.SpanType("custom"),
		tracer.StartTime(startTime),
	}
	for k, v := range c.tracerTags {
		opts = append(opts, tracer.Tag(k, v))
	}
	return tracer.StartSpanFromContext(ctx, c.tracerMsg, opts...)
}

func (c *callImpl) onSuccess(logger *zap.Logger, span tracer.Span, finishTime time.Time) {
	c.success.Inc(1)
	logger.Debug(c.loggerMsg)
	span.Finish(tracer.FinishTime(finishTime))
}

func (c *callImpl) onError(logger *zap.Logger, span tracer.Span, finishTime time.Time, err error) {
	c.err.Inc(1)
	logger.Warn(c.loggerMsg, zap.Error(err))
	span.Finish(tracer.FinishTime(finishTime), tracer.WithError(err))
}
