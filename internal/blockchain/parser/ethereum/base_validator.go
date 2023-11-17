package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func NewBaseValidator(params internal.ParserParams) internal.TrustlessValidator {
	// Reuse the same implementation as Ethereum.
	return NewEthereumValidator(params)
}
