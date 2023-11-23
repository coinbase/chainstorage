package internal

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

		// GetAccountProof returns the account proof for an given account at a target block.
		// Note that, in req, height is required while blockHash is optional.
		GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error)
	}

	Params struct {
		fx.In
		fxparams.Params
		Parser         parser.Parser
		Bitcoin        ClientFactory `name:"bitcoin" optional:"true"`
		Bsc            ClientFactory `name:"bsc" optional:"true"`
		Ethereum       ClientFactory `name:"ethereum" optional:"true"`
		Rosetta        ClientFactory `name:"rosetta" optional:"true"`
		Solana         ClientFactory `name:"solana" optional:"true"`
		Polygon        ClientFactory `name:"polygon" optional:"true"`
		Avacchain      ClientFactory `name:"avacchain" optional:"true"`
		Arbitrum       ClientFactory `name:"arbitrum" optional:"true"`
		Optimism       ClientFactory `name:"optimism" optional:"true"`
		Base           ClientFactory `name:"base" optional:"true"`
		Fantom         ClientFactory `name:"fantom" optional:"true"`
		Aptos          ClientFactory `name:"aptos" optional:"true"`
		EthereumBeacon ClientFactory `name:"ethereum/beacon" optional:"true"`
		CosmosStaking  ClientFactory `name:"cosmos/staking" optional:"true"`
		CardanoStaking ClientFactory `name:"cardano/staking" optional:"true"`
	}

	ClientParams struct {
		fx.In
		Master    Client `name:"master"`
		Slave     Client `name:"slave"`
		Validator Client `name:"validator"`
		Consensus Client `name:"consensus"`
	}

	Result struct {
		fx.Out
		Master    Client `name:"master"`
		Slave     Client `name:"slave"`
		Validator Client `name:"validator"`
		Consensus Client `name:"consensus"`
	}

	ClientOption func(options *ClientOptions)

	ClientOptions struct {
		BestEffort bool
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

func NewClient(params Params) (Result, error) {
	var factory ClientFactory
	blockchain := params.Config.Chain.Blockchain
	sidechain := params.Config.Chain.Sidechain
	if sidechain == api.SideChain_SIDECHAIN_NONE {
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
		case common.Blockchain_BLOCKCHAIN_FANTOM:
			factory = params.Fantom
		case common.Blockchain_BLOCKCHAIN_OPTIMISM:
			factory = params.Optimism
		case common.Blockchain_BLOCKCHAIN_BASE:
			factory = params.Base
		case common.Blockchain_BLOCKCHAIN_APTOS:
			factory = params.Aptos
		default:
			if params.Config.IsRosetta() {
				factory = params.Rosetta
			}
		}
	}
	if factory == nil {
		return Result{}, xerrors.Errorf("client is not implemented: blockchain(%v)-sidechain(%v)", blockchain, sidechain)
	}

	scope := params.Metrics
	logger := log.WithPackage(params.Logger)

	master := factory.Master()
	master = WithInstrumentInterceptor(
		master,
		scope.Tagged(map[string]string{tagEndpointGroup: "master"}),
		logger.With(zap.String(tagEndpointGroup, "master")),
	)
	master = WithParserInterceptor(master, params.Parser, params.Config, logger.With(zap.String(tagEndpointGroup, "master")))

	slave := factory.Slave()
	slave = WithInstrumentInterceptor(
		slave,
		scope.Tagged(map[string]string{tagEndpointGroup: "slave"}),
		logger.With(zap.String(tagEndpointGroup, "slave")),
	)
	slave = WithParserInterceptor(slave, params.Parser, params.Config, logger.With(zap.String(tagEndpointGroup, "slave")))

	validator := factory.Validator()
	validator = WithInstrumentInterceptor(
		validator,
		scope.Tagged(map[string]string{tagEndpointGroup: "validator"}),
		logger.With(zap.String(tagEndpointGroup, "validator")),
	)
	validator = WithParserInterceptor(validator, params.Parser, params.Config, logger.With(zap.String(tagEndpointGroup, "validator")))

	// Skip WithParserInterceptor for consensus intentionally to omit unnecessary steps, e.g. validateBlock.
	consensus := factory.Consensus()
	consensus = WithInstrumentInterceptor(
		consensus,
		scope.Tagged(map[string]string{tagEndpointGroup: "consensus"}),
		logger.With(zap.String(tagEndpointGroup, "consensus")),
	)

	return Result{
		Master:    master,
		Slave:     slave,
		Validator: validator,
		Consensus: consensus,
	}, nil
}

// WithBestEffort enables the client to query data in a best-effort manner.
// It may return partial results when this option is enabled.
func WithBestEffort() ClientOption {
	return func(options *ClientOptions) {
		options.BestEffort = true
	}
}

func defaultClientOptions() *ClientOptions {
	return &ClientOptions{
		BestEffort: false,
	}
}

func ContextWithOptions(ctx context.Context, opts ...ClientOption) context.Context {
	options := defaultClientOptions()
	for _, opt := range opts {
		opt(options)
	}

	return context.WithValue(ctx, contextKeyOptions, options)
}

func OptionsFromContext(ctx context.Context) *ClientOptions {
	options, _ := ctx.Value(contextKeyOptions).(*ClientOptions)
	return options
}

func GetParentHeight(height uint64) uint64 {
	if height == 0 {
		return 0
	}

	return height - 1
}
