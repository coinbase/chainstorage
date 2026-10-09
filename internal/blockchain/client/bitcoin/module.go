package bitcoin

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "bitcoin",
		Target: NewBitcoinClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "dash",
		Target: NewDashClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "zcash",
		Target: NewZcashClientFactory,
	}),
)
