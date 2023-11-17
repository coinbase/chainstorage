package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
)

func NewOptimismClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Optimism shares the same data schema as Ethereum since it is an EVM chain.
	return NewEthereumClientFactory(params)
}
