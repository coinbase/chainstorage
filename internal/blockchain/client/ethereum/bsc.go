package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
)

func NewBscClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// BSC shares the same data schema as Ethereum, except QuickNode uses the Erigon client for block trace generation.
	return NewEthereumClientFactory(params, WithEthereumTraceType(types.TraceType_ERIGON))
}
