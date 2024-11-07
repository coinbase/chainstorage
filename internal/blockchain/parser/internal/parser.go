package internal

import (
	"context"
	"fmt"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Parser interface {
		ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error)
		GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error)
		ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
		CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock, actualBlock *api.NativeBlock) error
		// ValidateBlock Given a native block, validates whether the block data is cryptographically correct.
		ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error
		// ValidateAccountState Given an account's state verification request and the target block, verifies that the account state is valid. If successful, return the stored account state. Otherwise, return error.
		ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error)
		// ValidateRosettaBlock Given other block source (native, etc), validates whether transaction operations show the correct balance transfer.
		ValidateRosettaBlock(ctx context.Context, req *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error
	}

	NativeParser interface {
		ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error)
		GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error)
	}

	RosettaParser interface {
		ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
	}

	// TrustlessValidator validates the block payload and account state using cryptographic algorithms.
	// The goal is to have the level of cryptographic security that is comparable to the native blockchain.
	TrustlessValidator interface {
		ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error
		ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error)
	}

	Params struct {
		fx.In
		fxparams.Params
		Aleo           ParserFactory `name:"aleo" optional:"true"`
		Bitcoin        ParserFactory `name:"bitcoin" optional:"true"`
		Bsc            ParserFactory `name:"bsc" optional:"true"`
		Ethereum       ParserFactory `name:"ethereum" optional:"true"`
		Rosetta        ParserFactory `name:"rosetta" optional:"true"`
		Solana         ParserFactory `name:"solana" optional:"true"`
		Polygon        ParserFactory `name:"polygon" optional:"true"`
		Avacchain      ParserFactory `name:"avacchain" optional:"true"`
		Arbitrum       ParserFactory `name:"arbitrum" optional:"true"`
		Optimism       ParserFactory `name:"optimism" optional:"true"`
		Fantom         ParserFactory `name:"fantom" optional:"true"`
		Base           ParserFactory `name:"base" optional:"true"`
		Aptos          ParserFactory `name:"aptos" optional:"true"`
		EthereumBeacon ParserFactory `name:"ethereum/beacon" optional:"true"`
		CosmosStaking  ParserFactory `name:"cosmos/staking" optional:"true"`
		CardanoStaking ParserFactory `name:"cardano/staking" optional:"true"`
	}

	ParserParams struct {
		fx.In
		fxparams.Params
	}

	parserImpl struct {
		nativeParser  NativeParser
		rosettaParser RosettaParser
		checker       Checker
		validator     TrustlessValidator
	}
)

func NewParser(params Params) (Parser, error) {
	var factory ParserFactory
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
		case common.Blockchain_BLOCKCHAIN_OPTIMISM:
			factory = params.Optimism
		case common.Blockchain_BLOCKCHAIN_BASE:
			factory = params.Base
		case common.Blockchain_BLOCKCHAIN_FANTOM:
			factory = params.Fantom
		case common.Blockchain_BLOCKCHAIN_APTOS:
			factory = params.Aptos
		default:
			if params.Config.IsRosetta() {
				factory = params.Rosetta
			}
		}
	} else {
		switch sidechain {
		case api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON, api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON:
			factory = params.EthereumBeacon
		}
	}

	if factory == nil {
		return nil, fmt.Errorf("parser is not implemented: blockchain(%v)-sidechain(%v)", blockchain, sidechain)
	}

	parser, err := factory.NewParser()
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}

	scope := params.Metrics
	logger := log.WithPackage(params.Logger)
	parser = WithInstrumentInterceptor(parser, scope, logger)
	return parser, nil
}

func (p *parserImpl) ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	return p.nativeParser.ParseBlock(ctx, rawBlock)
}

func (p *parserImpl) GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return p.nativeParser.GetTransaction(ctx, nativeBlock, transactionHash)
}

func (p *parserImpl) ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	return p.rosettaParser.ParseBlock(ctx, rawBlock)
}

func (p *parserImpl) CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock, actualBlock *api.NativeBlock) error {
	return p.checker.CompareNativeBlocks(ctx, height, expectedBlock, actualBlock)
}

func (p *parserImpl) ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error {
	return p.validator.ValidateBlock(ctx, nativeBlock)
}

func (p *parserImpl) ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error) {
	return p.validator.ValidateAccountState(ctx, req)
}

func (p *parserImpl) ValidateRosettaBlock(ctx context.Context, req *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return p.checker.ValidateRosettaBlock(ctx, req, actualRosettaBlock)
}
