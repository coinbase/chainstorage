package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewPolygonNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Polygon shares the same data schema as Ethereum
	return NewEthereumNativeParser(params, opts...)
}
