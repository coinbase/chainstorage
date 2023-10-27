package sqs

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "dlq/sqs",
		Target: NewFactory,
	}),
)
