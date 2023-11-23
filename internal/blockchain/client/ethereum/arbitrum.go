package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
)

func NewArbitrumClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Arbitrum shares the same data schema as Ethereum since it is an EVM chain.
	return NewEthereumClientFactory(params)
}
