package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewAvacchainNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Avalanche shares the same data schema as Ethereum since its an EVM chain.
	return NewEthereumNativeParser(params, opts...)
}
