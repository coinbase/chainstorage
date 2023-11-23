package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewBaseNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Base shares the same data schema as Ethereum since it is an internal EVM chain.
	return NewEthereumNativeParser(params, opts...)
}
