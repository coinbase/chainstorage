package internal

import (
	"context"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type RosettaChecker interface {
	ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error
}

type defaultRosettaProcessor struct {
}

func (c *defaultRosettaProcessor) ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return nil
}
