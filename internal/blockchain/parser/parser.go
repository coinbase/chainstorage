package parser

import (
	"go.uber.org/fx"
	"golang.org/x/net/context"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	ParserFactory interface {
		NewParser() (Parser, error)
	}

	Parser interface {
		ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error)
		ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
	}

	NativeParser interface {
		ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error)
	}

	RosettaParser interface {
		ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
	}

	Params struct {
		fx.In
		fxparams.Params
		Bitcoin   ParserFactory `name:"bitcoin"`
		Bsc       ParserFactory `name:"bsc"`
		Ethereum  ParserFactory `name:"ethereum"`
		Rosetta   ParserFactory `name:"rosetta"`
		Solana    ParserFactory `name:"solana"`
		Polygon   ParserFactory `name:"polygon"`
		Avacchain ParserFactory `name:"avacchain"`
		Arbitrum  ParserFactory `name:"arbitrum"`
		Optimism  ParserFactory `name:"optimism"`
	}

	ParserParams struct {
		fx.In
		fxparams.Params
	}

	ParserFactoryOption  func(options interface{})
	ParserFactoryFactory func(params ParserParams) ParserFactory
	NativeParserFactory  func(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error)
	RosettaParserFactory func(params ParserParams, nativeParser NativeParser, opts ...ParserFactoryOption) (RosettaParser, error)

	parserFactoryImpl struct {
		params               ParserParams
		nativeParserFactory  NativeParserFactory
		rosettaParserFactory RosettaParserFactory
	}

	parserImpl struct {
		nativeParser  NativeParser
		rosettaParser RosettaParser
	}
)

func WithParserFactory() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotated{
			Name:   "bitcoin",
			Target: NewParserFactory(NewBitcoinNativeParser, NewNotImplementedRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "bsc",
			Target: NewParserFactory(NewBscNativeParser, NewNotImplementedRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "ethereum",
			Target: NewParserFactory(NewEthereumNativeParser, NewEthereumRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "rosetta",
			Target: NewParserFactory(NewRosettaNativeParser, NewRosettaRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "solana",
			Target: NewParserFactory(NewSolanaNativeParser, NewNotImplementedRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "polygon",
			Target: NewParserFactory(NewPolygonNativeParser, NewPolygonRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "avacchain",
			Target: NewParserFactory(NewAvacchainNativeParser, NewNotImplementedRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "arbitrum",
			Target: NewParserFactory(NewArbitrumNativeParser, NewNotImplementedRosettaParser),
		}),
		fx.Provide(fx.Annotated{
			Name:   "optimism",
			Target: NewParserFactory(NewOptimismNativeParser, NewNotImplementedRosettaParser),
		}),
	)
}

func NewParserFactory(
	nativeParserFactory NativeParserFactory,
	rosettaParserFactory RosettaParserFactory,
) ParserFactoryFactory {
	return func(params ParserParams) ParserFactory {
		return &parserFactoryImpl{
			params:               params,
			nativeParserFactory:  nativeParserFactory,
			rosettaParserFactory: rosettaParserFactory,
		}
	}
}

func (f *parserFactoryImpl) NewParser() (Parser, error) {
	nativeParser, err := f.nativeParserFactory(f.params)
	if err != nil {
		return nil, xerrors.Errorf("failed to create native parser: %w", err)
	}

	rosettaParser, err := f.rosettaParserFactory(f.params, nativeParser)
	if err != nil {
		return nil, xerrors.Errorf("failed to create rosetta parser: %w", err)
	}

	return &parserImpl{
		nativeParser:  nativeParser,
		rosettaParser: rosettaParser,
	}, nil
}

func NewParser(params Params) (Parser, error) {
	var factory ParserFactory
	blockchain := params.Config.Chain.Blockchain
	switch blockchain {
	case common.Blockchain_BLOCKCHAIN_BITCOIN:
		factory = params.Bitcoin
	case common.Blockchain_BLOCKCHAIN_BSC:
		factory = params.Bsc
	case common.Blockchain_BLOCKCHAIN_ETHEREUM:
		factory = params.Ethereum
	case common.Blockchain_BLOCKCHAIN_SOLANA:
		factory = params.Solana
	case common.Blockchain_BLOCKCHAIN_POLYGON:
		factory = params.Polygon
	case common.Blockchain_BLOCKCHAIN_AVACCHAIN:
		factory = params.Avacchain
	case common.Blockchain_BLOCKCHAIN_ARBITRUM:
		factory = params.Arbitrum
	case common.Blockchain_BLOCKCHAIN_OPTIMISM:
		factory = params.Optimism
	default:
		if params.Config.IsRosetta() {
			factory = params.Rosetta
		}
	}

	if factory == nil {
		return nil, xerrors.Errorf("parser is not implemented: %v", blockchain)
	}

	parser, err := factory.NewParser()
	if err != nil {
		return nil, xerrors.Errorf("failed to create parser: %w", err)
	}

	scope := params.Metrics
	logger := log.WithPackage(params.Logger)
	parser = WithInstrumentInterceptor(parser, scope, logger)
	return parser, nil
}

func (p *parserImpl) ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	return p.nativeParser.ParseBlock(ctx, rawBlock)
}

func (p *parserImpl) ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	return p.rosettaParser.ParseBlock(ctx, rawBlock)
}
