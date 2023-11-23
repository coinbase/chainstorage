package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
)

func NewFantomClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Fantom shares the same data schema as Ethereum since it is an EVM chain.
	// The difference is quick node Fantom API provides parity traces that in flattened style which is different from geth
	return NewEthereumClientFactory(params, WithEthereumTraceType(types.TraceType_PARITY))
}
