package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewBaseChecker(params internal.ParserParams) (internal.Checker, error) {
	return NewEthereumChecker(params)
}
