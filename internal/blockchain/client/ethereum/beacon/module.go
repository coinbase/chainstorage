package beacon

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "ethereum/beacon",
		Target: NewClientFactory,
	}),
)
