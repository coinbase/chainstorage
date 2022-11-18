package tracer

import "go.uber.org/fx"

// Module is only need for Cadence right now which uses opentracing interface
var Module = fx.Options(
	fx.Provide(NewTracer),
)
