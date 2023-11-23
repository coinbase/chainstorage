package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewFantomNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Fantom shares the same data schema as Ethereum since its an EVM chain except use parity for its trace type
	opts = append(opts, WithTraceType(types.TraceType_PARITY))
	return NewEthereumNativeParser(params, opts...)
}
