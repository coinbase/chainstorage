package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
)

func NewRobinhoodClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Robinhood Chain is an Arbitrum Nitro based EVM L2 and shares the same data schema as Ethereum.
	return NewEthereumClientFactory(params)
}
