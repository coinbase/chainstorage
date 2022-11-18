package parser

import (
	"context"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	notImplementedRosettaParserImpl struct {
	}
)

func NewNotImplementedRosettaParser(params ParserParams, nativeParser NativeParser, opts ...ParserFactoryOption) (RosettaParser, error) {
	return &notImplementedRosettaParserImpl{}, nil
}

func (p notImplementedRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	return nil, ErrNotImplemented
}
