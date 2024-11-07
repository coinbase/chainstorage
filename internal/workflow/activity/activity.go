package activity

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/workflow/instrument"
)

const (
	ActivityExtractor       = "activity.extractor"
	ActivityLoader          = "activity.loader"
	ActivitySyncer          = "activity.syncer"
	ActivityLivenessCheck   = "activity.liveness_check"
	ActivityReader          = "activity.reader"
	ActivityValidator       = "activity.validator"
	ActivityStreamer        = "activity.streamer"
	ActivityCrossValidator  = "activity.cross_validator"
	ActivityEventReader     = "activity.event_reader"
	ActivityEventReconciler = "activity.event_reconciler"
	ActivityEventLoader     = "activity.event_loader"
	ActivityReplicator      = "activity.replicator"
	ActivityUpdateWatermark = "activity.update_watermark"

	loggerMsg = "activity.request"

	resultTypeTag     = "result_type"
	resultTypeSuccess = "success"
	resultTypeError   = "error"

	reorgDistanceCheckMetric = "reorg_distance_check"
)

type baseActivity struct {
	name       string
	runtime    cadence.Runtime
	validate   *validator.Validate
	instrument *instrument.Instrument
}

var (
	errTags = map[string]string{
		resultTypeTag: resultTypeError,
	}
	successTags = map[string]string{
		resultTypeTag: resultTypeSuccess,
	}
)

func newBaseActivity(name string, runtime cadence.Runtime) baseActivity {
	return baseActivity{
		name:       name,
		runtime:    runtime,
		validate:   validator.New(),
		instrument: instrument.New(runtime, name, loggerMsg),
	}
}

func (a *baseActivity) register(activityFn any) {
	a.runtime.RegisterActivity(activityFn, activity.RegisterOptions{
		Name: a.name,
	})
}

func (a *baseActivity) executeActivity(ctx workflow.Context, request any, response any, opts ...instrument.Option) error {
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
			return fmt.Errorf("failed to execute activity (name=%v): %w", a.name, err)
		}

		return nil
	}, opts...)
}

func (a *baseActivity) validateRequest(request any) error {
	if err := a.validate.Struct(request); err != nil {
		return fmt.Errorf("invalid activity request (name=%v, request=%+v): %w", a.name, request, err)
	}

	return nil
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
	return errors.Is(err, workflow.ErrCanceled)
}
