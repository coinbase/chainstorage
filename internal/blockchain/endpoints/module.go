package endpoints

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewEndpointProvider),
	fx.Provide(NewRosettaEndpointProvider),
	fx.Provide(NewFailoverManager),
)
