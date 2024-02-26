package workflow

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

var Module = fx.Options(
	activity.Module,
	fx.Provide(NewManager),
	fx.Provide(NewBackfiller),
	fx.Provide(NewPoller),
	fx.Provide(NewBenchmarker),
	fx.Provide(NewMonitor),
	fx.Provide(NewStreamer),
	fx.Provide(NewCrossValidator),
	fx.Provide(NewEventBackfiller),
	fx.Provide(NewReplicator),
)

const (
	resultTypeTag     = "result_type"
	resultTypeSuccess = "success"
	resultTypeError   = "error"
)
