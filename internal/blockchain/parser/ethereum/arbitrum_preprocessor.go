package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewArbitrumChecker(params internal.ParserParams) (internal.Checker, error) {
	return NewEthereumChecker(params)
}
