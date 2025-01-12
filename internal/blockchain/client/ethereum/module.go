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
	beacon.Module,
)
