package instrument

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/cadence"
)

type (
	Instrument struct {
		runtime cadence.Runtime
		name    string
		msg     string
	}

	Fn       func() error
	FilterFn func(err error) bool

	Option func(options *instrumentOptions)

	instrumentOptions struct {
		scopeTags    map[string]string
		loggerFields []zap.Field
		filter       func(err error) bool
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

func New(runtime cadence.Runtime, name string, msg string) *Instrument {
	return &Instrument{
		runtime: runtime,
		name:    name,
		msg:     msg,
	}
}

func WithScopeTag(name string, value string) Option {
	return func(options *instrumentOptions) {
		options.scopeTags[name] = value
	}
}

func WithLoggerField(field zap.Field) Option {
	return func(options *instrumentOptions) {
		options.loggerFields = append(options.loggerFields, field)
	}
}

func WithFilter(filter func(err error) bool) Option {
	return func(options *instrumentOptions) {
		options.filter = filter
	}
}

func (i *Instrument) Instrument(ctx workflow.Context, operation Fn, opts ...Option) error {
	options := newOptions(opts...)

	metricsHandler := i.runtime.GetMetricsHandler(ctx)
	if len(options.scopeTags) > 0 {
		metricsHandler = metricsHandler.WithTags(options.scopeTags)
	}
	errCounter := metricsHandler.WithTags(errTags).Counter(i.name)
	successCounter := metricsHandler.WithTags(successTags).Counter(i.name)
	latencyTimer := metricsHandler.Timer(i.name + "." + latencySuffix)

	timeSource := i.runtime.GetTimeSource(ctx)
	startTime := timeSource.Now()

	err := operation()

	finishTime := timeSource.Now()
	duration := finishTime.Sub(startTime)
	latencyTimer.Record(duration)

	logger := i.runtime.GetLogger(ctx).With(zap.String(durationTag, duration.String()))
	if len(options.loggerFields) > 0 {
		logger = logger.With(options.loggerFields...)
	}
	if err != nil {
		if options.filter != nil && options.filter(err) {
			i.onSuccess(successCounter, logger, err)
		} else {
			i.onError(errCounter, logger, err)
		}
		return err
	}

	i.onSuccess(successCounter, logger, nil)
	return nil
}

func newOptions(opts ...Option) *instrumentOptions {
	options := &instrumentOptions{
		scopeTags:    make(map[string]string),
		loggerFields: []zap.Field{zap.String("package", "workflow")}, // Pretend this is the workflow package.
	}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func (i *Instrument) onSuccess(successCounter client.MetricsCounter, logger *zap.Logger, err error) {
	successCounter.Inc(1)
	logger.Debug(i.msg, zap.Error(err))
}

func (i *Instrument) onError(errCounter client.MetricsCounter, logger *zap.Logger, err error) {
	errCounter.Inc(1)
	logger.Error(i.msg, zap.Error(err))
}
