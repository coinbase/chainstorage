package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
)

func NewSeismicClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Shares the same data schema as Ethereum since it is an EVM chain.
	// Seismic timestamps are in milliseconds, so we convert them to seconds.
	return NewEthereumClientFactory(params, WithEthereumTimestampInMs())
}
