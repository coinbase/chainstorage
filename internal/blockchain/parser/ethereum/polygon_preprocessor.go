package ethereum

import (
	"context"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	polygonPreProcessor struct {
		ethereumPreProcessor
	}
	polygonRosettaChecker struct{}
)

func NewPolygonChecker(params internal.ParserParams) (internal.Checker, error) {
	// Any method not defined below is inherited from ethereumPreProcessor.
	preprocessor := &polygonPreProcessor{
		ethereumPreProcessor{
			config: params.Config,
		},
	}

	return internal.NewChecker(params, preprocessor, &polygonRosettaChecker{})
}

func (c *polygonRosettaChecker) ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return internal.ErrNotImplemented
}
