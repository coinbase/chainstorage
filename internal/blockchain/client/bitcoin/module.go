package bitcoin

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "bitcoin",
		Target: NewBitcoinClientFactory,
	}),
)
