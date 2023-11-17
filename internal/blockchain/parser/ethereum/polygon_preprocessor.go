package ethereum

import (
	"context"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	// https://github.cbhq.net/c3/chainstdio/blob/1f349f989a98bab3deb2c5a5481b44004973c26e/pkg/blockchain/polygon/polygon.go#L54
	polygonCurrencyMap = map[string]bool{
		"0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619": true,
		"0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174": true,
	}
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
