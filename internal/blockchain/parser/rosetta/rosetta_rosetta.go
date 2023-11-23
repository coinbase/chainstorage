package rosetta

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	rosettaRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser internal.NativeParser
	}
)

// NewRosettaRosettaParser returns a parser that parses rosetta blockchains into RosettaBlock objects.
func NewRosettaRosettaParser(params internal.ParserParams, nativeParser internal.NativeParser, opts ...internal.ParserFactoryOption) (internal.RosettaParser, error) {
	return &rosettaRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

func (p *rosettaRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	if p.config.Chain.Rosetta.FromRosetta {
		return p.parseBlockFromRosetta(ctx, rawBlock)
	}

	return p.parseBlockFromNative(ctx, rawBlock)
}

func (p *rosettaRosettaParserImpl) parseBlockFromNative(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
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

func (p *rosettaRosettaParserImpl) parseBlockFromRosetta(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, xerrors.New("metadata not found")
	}

	blobdata := rawBlock.GetRosetta()
	if blobdata == nil {
		return nil, xerrors.New("blobData not found for rosetta")
	}

	rosettaBlock, err := ParseRosettaBlock(blobdata.Header)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rosetta block: %w", err)
	}

	otherTransactions, err := ParseOtherTransactions(blobdata.OtherTransactions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rosetta other transactions: %w", err)
	}

	if len(otherTransactions) > 0 {
		if rosettaBlock == nil {
			return nil, xerrors.Errorf("rosetta block is nil while having other transactions")
		}
		rosettaBlock.Transactions = append(rosettaBlock.Transactions, otherTransactions...)
	}

	return &api.RosettaBlock{
		Block: rosettaBlock,
	}, nil
}
