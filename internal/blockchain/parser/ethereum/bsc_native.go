package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewBscNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// BSC shares the same data schema as Ethereums.
	return NewEthereumNativeParser(params, opts...)
}
