package activity

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewHeartbeater),
	fx.Provide(NewExtractor),
	fx.Provide(NewLoader),
	fx.Provide(NewSyncer),
	fx.Provide(NewLivenessCheck),
	fx.Provide(NewReader),
	fx.Provide(NewValidator),
	fx.Provide(NewStreamer),
	fx.Provide(NewCrossValidator),
	fx.Provide(NewEventReader),
	fx.Provide(NewEventReconciler),
	fx.Provide(NewEventLoader),
	fx.Provide(NewReplicator),
)
