package ethereum

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/client/ethereum/beacon"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "arbitrum",
		Target: NewArbitrumClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "avacchain",
		Target: NewAvacchainClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "base",
		Target: NewBaseClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "bsc",
		Target: NewBscClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum",
		Target: NewEthereumClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "fantom",
		Target: NewFantomClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "optimism",
		Target: NewOptimismClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "polygon",
		Target: NewPolygonClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "tron",
		Target: NewTronClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "story",
		Target: NewStoryClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereumclassic",
		Target: NewEthereumClassicClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "plasma",
		Target: NewPlasmaClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "monad",
		Target: NewMonadClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "abstract",
		Target: NewAbstractClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "megaeth",
		Target: NewMegaethClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "seismic",
		Target: NewSeismicClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "tempo",
		Target: NewTempoClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "robinhood",
		Target: NewRobinhoodClientFactory,
	}),
	fx.Provide(fx.Annotated{
		Name:   "arc",
		Target: NewArcClientFactory,
	}),
	beacon.Module,
)
