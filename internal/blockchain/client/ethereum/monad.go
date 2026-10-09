package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
)

func NewMonadClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Reuse the Ethereum client factory since it is an EVM chain.
	return NewEthereumClientFactory(params)
}
