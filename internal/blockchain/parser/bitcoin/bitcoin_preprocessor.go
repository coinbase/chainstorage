package bitcoin

import (
	"context"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	bitcoinPreProcessor   struct{}
	bitcoinRosettaChecker struct{}
)

func NewBitcoinChecker(params internal.ParserParams) (internal.Checker, error) {
	return internal.NewChecker(params, &bitcoinPreProcessor{}, &bitcoinRosettaChecker{})
}

func (p *bitcoinPreProcessor) PreProcessNativeBlock(expected, actual *api.NativeBlock) error {
	return nil
}

func (c *bitcoinRosettaChecker) ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return internal.ErrNotImplemented
}
