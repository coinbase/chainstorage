package rosetta

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "rosetta",
		Target: NewRosettaClientFactory,
	}),
)
