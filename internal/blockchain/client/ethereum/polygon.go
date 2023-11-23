package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
)

func NewPolygonClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Polygon shares the same data schema as Ethereum.
	return NewEthereumClientFactory(params)
}
