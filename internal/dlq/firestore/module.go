package firestore

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "dlq/firestore",
		Target: NewFactory,
	}),
)
