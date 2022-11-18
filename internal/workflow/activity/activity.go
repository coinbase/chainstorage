package activity

import (
	"context"

	"github.com/go-playground/validator/v10"
	"github.com/uber-go/tally"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/workflow/instrument"
)

const (
	ActivityExtractor      = "activity.extractor"
	ActivityLoader         = "activity.loader"
	ActivitySyncer         = "activity.syncer"
	ActivityReader         = "activity.reader"
	ActivityValidator      = "activity.validator"
	ActivityStreamer       = "activity.streamer"
	ActivityCrossValidator = "activity.cross_validator"

	loggerMsg = "activity.request"
)

type baseActivity struct {
	name       string
	runtime    cadence.Runtime
	validate   *validator.Validate
	instrument *instrument.Instrument
}

func newBaseActivity(name string, runtime cadence.Runtime) baseActivity {
	return baseActivity{
		name:       name,
		runtime:    runtime,
		validate:   validator.New(),
		instrument: instrument.New(runtime, name, loggerMsg),
	}
}

func (a *baseActivity) register(activityFn interface{}) {
	a.runtime.RegisterActivity(activityFn, activity.RegisterOptions{
		Name: a.name,
	})
}

func (a *baseActivity) executeActivity(ctx workflow.Context, request interface{}, response interface{}, opts ...instrument.Option) error {
	opts = append(
		opts,
		instrument.WithLoggerField(zap.String("activity", a.name)),
		instrument.WithLoggerField(zap.Reflect("request", request)),
		instrument.WithFilter(IsCanceledError),
	)
	return a.instrument.Instrument(ctx, func() error {
		if err := a.validateRequest(request); err != nil {
			return err
		}

		if err := a.runtime.ExecuteActivity(ctx, a.name, request, response); err != nil {
			return xerrors.Errorf("failed to execute activity (name=%v): %w", a.name, err)
		}

		return nil
	}, opts...)
}

func (a *baseActivity) validateRequest(request interface{}) error {
	if err := a.validate.Struct(request); err != nil {
		return xerrors.Errorf("invalid activity request (name=%v, request=%+v): %w", a.name, request, err)
	}

	return nil
}

func (a *baseActivity) getScope(ctx context.Context) tally.Scope {
	return a.runtime.GetActivityScope(ctx).SubScope(a.name)
}

func (a *baseActivity) getLogger(ctx context.Context) *zap.Logger {
	info := activity.GetInfo(ctx)
	logger := a.runtime.GetActivityLogger(ctx)
	logger = log.WithPackage(logger)
	logger = log.WithSpan(ctx, logger)
	logger = logger.With(
		zap.Int32("Attempt", info.Attempt),
		zap.Time("StartedTime", info.StartedTime),
		zap.Time("Deadline", info.Deadline),
	)
	return logger
}

func IsCanceledError(err error) bool {
	return xerrors.Is(err, workflow.ErrCanceled)
}
