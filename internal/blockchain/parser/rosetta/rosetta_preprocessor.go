package rosetta

import (
	"context"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	rosettaPreProcessor   struct{}
	rosettaRosettaChecker struct{}
)

func NewRosettaChecker(params internal.ParserParams) (internal.Checker, error) {
	return internal.NewChecker(params, &rosettaPreProcessor{}, &rosettaRosettaChecker{})
}

func (p *rosettaPreProcessor) PreProcessNativeBlock(expected, actual *api.NativeBlock) error {
	return nil
}

func (c *rosettaRosettaChecker) ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return internal.ErrNotImplemented
}
