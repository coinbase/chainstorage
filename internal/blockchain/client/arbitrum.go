package client

import "github.com/coinbase/chainstorage/internal/blockchain/types"

func NewArbitrumClientFactory(params EthereumClientParams) ClientFactory {
	// Arbitrum shares the same data schema as Ethereum since it is an EVM chain.
	// The difference is quick node Arbitrum API provides parity traces that in flattened style which is different from geth
	return NewEthereumClientFactory(params, WithEthereumTraceType(types.TraceType_PARITY))
}
