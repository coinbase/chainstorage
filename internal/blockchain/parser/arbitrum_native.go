package parser

import "github.com/coinbase/chainstorage/internal/blockchain/types"

func NewArbitrumNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	// Arbitrum shares the same data schema as Ethereum since its an EVM chain except use parity for its trace type
	opts = append(opts, WithTraceType(types.TraceType_PARITY))
	return NewEthereumNativeParser(params, opts...)
}
