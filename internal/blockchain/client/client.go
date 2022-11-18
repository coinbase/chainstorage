package client

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Client interface {
		// BatchGetBlockMetadata fetches the BlockMetadata given a block range [from, to).
		// The implementation should consider the following optimizations whenever possible:
		// - Query the block header only.
		// - Use the batch API to reduce the number of network round-trips.
		// - If batch API is unavailable, it may use goroutines to parallelize the query.
		BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error)

		// GetBlockByHeight fetches the raw block given a block height.
		// Optionally it may implement blockchain-specific handling using opts.
		// For example, in ethereum transaction trace may be skipped if WithBestEffort is enabled.
		GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...ClientOption) (*api.Block, error)

		// GetBlockByHash fetches the raw block given a block hash and/or height.
		// For most blockchains, the unique identifier of a block is the hash, and the hash should be used in the query.
		// If the blockchain uses the height as the unique identifier, or orphaned blocks are skipped, the height may be used in the query.
		// Optionally it may implement blockchain-specific handling using opts.
		GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...ClientOption) (*api.Block, error)

		// GetLatestHeight returns the height of the latest block.
		GetLatestHeight(ctx context.Context) (uint64, error)

		// UpgradeBlock upgrades a block to a new tag.
		// It is used in backfiller when UpgradeFromTag is enabled.
		// It should return ErrNotImplemented until there is a need for a new tag.
		UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error)

		// CanReprocess returns true if the block is retryable during dlq processing.
		// It should return false unless WithBestEffort is applicable to the blockchain.
		CanReprocess(tag uint32, height uint64) bool
	}

	ClientFactory interface {
		Master() Client
		Slave() Client
		Validator() Client
	}

	Params struct {
		fx.In
		fxparams.Params
		Parser    parser.Parser
		Bitcoin   ClientFactory `name:"bitcoin"`
		Bsc       ClientFactory `name:"bsc"`
		Ethereum  ClientFactory `name:"ethereum"`
		Rosetta   ClientFactory `name:"rosetta"`
		Solana    ClientFactory `name:"solana"`
		Polygon   ClientFactory `name:"polygon"`
		Avacchain ClientFactory `name:"avacchain"`
		Arbitrum  ClientFactory `name:"arbitrum"`
		Optimism  ClientFactory `name:"optimism"`
	}

	ClientParams struct {
		fx.In
		Master    Client `name:"master"`
		Slave     Client `name:"slave"`
		Validator Client `name:"validator"`
	}

	Result struct {
		fx.Out
		Master    Client `name:"master"`
		Slave     Client `name:"slave"`
		Validator Client `name:"validator"`
	}

	ClientOption func(options *clientOptions)

	clientOptions struct {
		bestEffort bool
	}
)

const (
	contextKeyOptions = "options"
	tagEndpointGroup  = "endpoint_group"
)

var (
	ErrBlockNotFound  = xerrors.New("block not found")
	ErrNotImplemented = xerrors.New("not implemented")
)

func WithClientFactory() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotated{
			Name:   "bitcoin",
			Target: NewBitcoinClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "bsc",
			Target: NewBscClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "ethereum",
			Target: NewEthereumClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "rosetta",
			Target: NewRosettaClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "solana",
			Target: NewSolanaClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "polygon",
			Target: NewPolygonClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "avacchain",
			Target: NewAvacchainClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "arbitrum",
			Target: NewArbitrumClientFactory,
		}),
		fx.Provide(fx.Annotated{
			Name:   "optimism",
			Target: NewOptimismClientFactory,
		}),
	)
}

func NewClient(params Params) (Result, error) {
	var factory ClientFactory
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
		return Result{}, xerrors.Errorf("client is not implemented: %v ", blockchain)
	}

	scope := params.Metrics
	logger := log.WithPackage(params.Logger)

	master := factory.Master()
	master = WithInstrumentInterceptor(
		master,
		scope.Tagged(map[string]string{tagEndpointGroup: "master"}),
		logger.With(zap.String(tagEndpointGroup, "master")),
	)
	master = WithParserInterceptor(master, params.Parser)

	slave := factory.Slave()
	slave = WithInstrumentInterceptor(
		slave,
		scope.Tagged(map[string]string{tagEndpointGroup: "slave"}),
		logger.With(zap.String(tagEndpointGroup, "slave")),
	)
	slave = WithParserInterceptor(slave, params.Parser)

	validator := factory.Validator()
	validator = WithInstrumentInterceptor(
		validator,
		scope.Tagged(map[string]string{tagEndpointGroup: "validator"}),
		logger.With(zap.String(tagEndpointGroup, "validator")),
	)
	validator = WithParserInterceptor(validator, params.Parser)

	return Result{
		Master:    master,
		Slave:     slave,
		Validator: validator,
	}, nil
}

// WithBestEffort enables the client to query data in a best-effort manner.
// It may return partial results when this option is enabled.
func WithBestEffort() ClientOption {
	return func(options *clientOptions) {
		options.bestEffort = true
	}
}

func defaultClientOptions() *clientOptions {
	return &clientOptions{
		bestEffort: false,
	}
}

func contextWithOptions(ctx context.Context, opts ...ClientOption) context.Context {
	options := defaultClientOptions()
	for _, opt := range opts {
		opt(options)
	}

	return context.WithValue(ctx, contextKeyOptions, options)
}

func optionsFromContext(ctx context.Context) *clientOptions {
	options, _ := ctx.Value(contextKeyOptions).(*clientOptions)
	return options
}

func getParentHeight(height uint64) uint64 {
	if height == 0 {
		return 0
	}

	return height - 1
}
