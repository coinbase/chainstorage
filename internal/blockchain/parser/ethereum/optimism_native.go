package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewOptimismNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Optimism shares the same data schema as Ethereum since its an EVM chain.
	return NewEthereumNativeParser(params, opts...)
}
