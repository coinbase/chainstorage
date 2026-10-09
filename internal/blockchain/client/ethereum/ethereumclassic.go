package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
)

func NewEthereumClassicClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Ethereum Classic shares the same data schema as Ethereum since it is an EVM chain.
	return NewEthereumClientFactory(params)
}
