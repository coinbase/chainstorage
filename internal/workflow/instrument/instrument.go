package instrument

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
)

type (
	Instrument struct {
		runtime cadence.Runtime
		name    string
		msg     string
	}

	Option func(options *instrumentOptions)

	Fn = func() error

	instrumentOptions struct {
		scopeTags    map[string]string
		loggerFields []zap.Field
		filter       func(err error) bool
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

func (i *Instrument) Instrument(ctx workflow.Context, fn Fn, opts ...Option) error {
	options := newOptions(opts...)

	scope := i.runtime.GetScope(ctx)
	if len(options.scopeTags) > 0 {
		scope = scope.Tagged(options.scopeTags)
	}

	logger := i.runtime.GetLogger(ctx)
	if len(options.loggerFields) > 0 {
		logger = logger.With(options.loggerFields...)
	}

	call := instrument.NewCall(
		scope,
		i.name,
		instrument.WithLogger(logger, i.msg),
		instrument.WithFilter(options.filter),
		instrument.WithTimeSource(i.runtime.GetTimeSource(ctx)),
	)
	// workflow.Context is incompatible with context.Context;
	// therefore, an empty context.Context is used here.
	return call.Instrument(context.Background(), func(ctx context.Context) error {
		return fn()
	})
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
