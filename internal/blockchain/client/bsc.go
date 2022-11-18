package client

import "github.com/coinbase/chainstorage/internal/blockchain/types"

func NewBscClientFactory(params EthereumClientParams) ClientFactory {
	// BSC shares the same data schema as Ethereum, except QuickNode uses the Erigon client for block trace generation.
	return NewEthereumClientFactory(params, WithEthereumTraceType(types.TraceType_ERIGON))
}
