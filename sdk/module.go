package sdk

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(newClient),
	fx.Provide(newSession),
)
