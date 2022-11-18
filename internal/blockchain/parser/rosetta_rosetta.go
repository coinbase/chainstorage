package parser

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	rosettaRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser NativeParser
	}
)

// NewRosettaRosettaParser returns a parser that parses rosetta blockchains into RosettaBlock objects.
func NewRosettaRosettaParser(params ParserParams, nativeParser NativeParser, opts ...ParserFactoryOption) (RosettaParser, error) {
	return &rosettaRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

func (p *rosettaRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	nativeBlock, err := p.nativeParser.ParseBlock(ctx, rawBlock)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block into native format: %w", err)
	}

	rosettaBlock := nativeBlock.GetRosetta()

	if rosettaBlock == nil {
		return nil, xerrors.Errorf("the rosetta block is not found")
	}

	return &api.RosettaBlock{
		Block: nativeBlock.GetRosetta(),
	}, nil
}
