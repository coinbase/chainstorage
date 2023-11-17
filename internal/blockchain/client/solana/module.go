package solana

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "solana",
		Target: NewSolanaClientFactory,
	}),
)
