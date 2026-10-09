package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewMonadNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Reuse the Ethereum native parser since its an EVM chain.
	return NewEthereumNativeParser(params, opts...)
}
