package internal

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	parserInterceptor struct {
		client Client
		parser parser.Parser
		config *config.Config
		logger *zap.Logger
	}

	instrumentInterceptor struct {
		client                          Client
		instrumentBatchGetBlockMetadata instrument.InstrumentWithResult[[]*api.BlockMetadata]
		instrumentGetBlockByHeight      instrument.InstrumentWithResult[*api.Block]
		instrumentGetBlockByHash        instrument.InstrumentWithResult[*api.Block]
		instrumentGetLatestHeight       instrument.InstrumentWithResult[uint64]
		instrumentUpgradeBlock          instrument.InstrumentWithResult[*api.Block]
		instrumentGetAccountProof       instrument.InstrumentWithResult[*api.GetAccountProofResponse]
	}
)

var (
	_ Client = (*parserInterceptor)(nil)
	_ Client = (*instrumentInterceptor)(nil)
)

// WithParserInterceptor returns a new client which validates the data using the specified parser.
func WithParserInterceptor(client Client, parser parser.Parser, config *config.Config, logger *zap.Logger) Client {
	return &parserInterceptor{
		client: client,
		parser: parser,
		config: config,
		logger: logger,
	}
}

func (p *parserInterceptor) BatchGetBlockMetadata(ctx context.Context, tag uint32, from, to uint64) ([]*api.BlockMetadata, error) {
	return p.client.BatchGetBlockMetadata(ctx, tag, from, to)
}

func (p *parserInterceptor) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...ClientOption) (*api.Block, error) {
	block, err := p.client.GetBlockByHeight(ctx, tag, height, opts...)
	if err != nil {
		return nil, err
	}

	if err := p.validateBlock(ctx, block, tag, height, ""); err != nil {
		return nil, err
	}

	return block, nil
}

func (p *parserInterceptor) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...ClientOption) (*api.Block, error) {
	block, err := p.client.GetBlockByHash(ctx, tag, height, hash, opts...)
	if err != nil {
		return nil, err
	}

	if err := p.validateBlock(ctx, block, tag, height, hash); err != nil {
		return nil, err
	}

	return block, nil
}

func (p *parserInterceptor) GetLatestHeight(ctx context.Context) (uint64, error) {
	return p.client.GetLatestHeight(ctx)
}

func (p *parserInterceptor) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	newBlock, err := p.client.UpgradeBlock(ctx, block, newTag)
	if err != nil {
		return nil, err
	}

	if newTag != newBlock.Metadata.Tag {
		return nil, xerrors.Errorf("unexpected tag: expected=%v, actual=%v", newTag, block.Metadata.Tag)
	}

	return newBlock, nil
}

func (p *parserInterceptor) CanReprocess(tag uint32, height uint64) bool {
	return p.client.CanReprocess(tag, height)
}

func (p *parserInterceptor) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return p.client.GetAccountProof(ctx, req)
}

func (p *parserInterceptor) validateBlock(ctx context.Context, block *api.Block, tag uint32, height uint64, hash string) error {
	metadata := block.Metadata
	if metadata == nil {
		return xerrors.New("block metadata is null")
	}

	if metadata.Tag != tag {
		return xerrors.Errorf("expected tag %v in metadata: {%+v}", tag, metadata)
	}

	if metadata.Height != height {
		return xerrors.Errorf("expected height %v in metadata: {%+v}", height, metadata)
	}

	if hash != "" && metadata.Hash != hash {
		return xerrors.Errorf("expected hash %v in metadata: {%+v}", hash, metadata)
	}

	nativeBlock, err := p.parser.ParseNativeBlock(ctx, block)
	if err != nil {
		return xerrors.Errorf("failed to parse block to native format {%+v}: %w", metadata, err)
	}

	if p.config.Chain.Feature.BlockValidationEnabled {
		if err := p.parser.ValidateBlock(ctx, nativeBlock); err != nil {
			p.logger.Warn("block validation failed",
				zap.Error(err),
				zap.Reflect("metadata", metadata),
			)

			if p.config.Chain.Feature.BlockValidationMuted {
				return nil
			}
			return xerrors.Errorf("failed to validate block {%+v}: %w", metadata, err)
		}
	}

	return nil
}

// WithInstrumentInterceptor returns an instrumented client.
func WithInstrumentInterceptor(client Client, scope tally.Scope, logger *zap.Logger) Client {
	scope = scope.SubScope("client")

	return &instrumentInterceptor{
		client:                          client,
		instrumentBatchGetBlockMetadata: newInstrument[[]*api.BlockMetadata]("batch_get_block_metadata", scope, logger),
		instrumentGetBlockByHeight:      newInstrument[*api.Block]("get_block_by_height", scope, logger),
		instrumentGetBlockByHash:        newInstrument[*api.Block]("get_block_by_hash", scope, logger),
		instrumentGetLatestHeight:       newInstrument[uint64]("get_latest_height", scope, logger),
		instrumentUpgradeBlock:          newInstrument[*api.Block]("upgrade_block", scope, logger),
		instrumentGetAccountProof:       newInstrument[*api.GetAccountProofResponse]("get_account_proof", scope, logger),
	}
}

func newInstrument[T any](method string, scope tally.Scope, logger *zap.Logger) instrument.InstrumentWithResult[T] {
	return instrument.NewWithResult[T](scope, method, instrument.WithLogger(logger, fmt.Sprintf("client.%v", method)))
}

func (i *instrumentInterceptor) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	return i.instrumentBatchGetBlockMetadata.Instrument(
		ctx,
		func(ctx context.Context) ([]*api.BlockMetadata, error) {
			return i.client.BatchGetBlockMetadata(ctx, tag, from, to)
		},
		instrument.WithLoggerFields(
			zap.Uint32("tag", tag),
			zap.Uint64("from", from),
			zap.Uint64("to", to),
		),
	)
}

func (i *instrumentInterceptor) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...ClientOption) (*api.Block, error) {
	return i.instrumentGetBlockByHeight.Instrument(
		ctx,
		func(ctx context.Context) (*api.Block, error) {
			return i.client.GetBlockByHeight(ctx, tag, height, opts...)
		},
		instrument.WithLoggerFields(
			zap.Uint32("tag", tag),
			zap.Uint64("height", height),
		),
	)
}

func (i *instrumentInterceptor) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...ClientOption) (*api.Block, error) {
	return i.instrumentGetBlockByHash.Instrument(
		ctx,
		func(ctx context.Context) (*api.Block, error) {
			return i.client.GetBlockByHash(ctx, tag, height, hash, opts...)
		},
		instrument.WithLoggerFields(
			zap.Uint32("tag", tag),
			zap.Uint64("height", height),
			zap.String("hash", hash),
		),
	)
}

func (i *instrumentInterceptor) GetLatestHeight(ctx context.Context) (uint64, error) {
	return i.instrumentGetLatestHeight.Instrument(
		ctx,
		func(ctx context.Context) (uint64, error) {
			return i.client.GetLatestHeight(ctx)
		},
	)
}

func (i *instrumentInterceptor) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	return i.instrumentUpgradeBlock.Instrument(
		ctx,
		func(ctx context.Context) (*api.Block, error) {
			return i.client.UpgradeBlock(ctx, block, newTag)
		},
		instrument.WithLoggerFields(
			zap.Reflect("block", block.Metadata),
			zap.Uint32("newTag", newTag),
		),
	)
}

func (i *instrumentInterceptor) CanReprocess(tag uint32, height uint64) bool {
	return i.client.CanReprocess(tag, height)
}

func (i *instrumentInterceptor) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return i.instrumentGetAccountProof.Instrument(
		ctx,
		func(ctx context.Context) (*api.GetAccountProofResponse, error) {
			return i.client.GetAccountProof(ctx, req)
		},
		instrument.WithLoggerFields(
			zap.String("account", req.Req.Account),
			zap.Uint64("height", req.Req.Height),
			zap.String("blockHash", req.Req.Account),
		),
	)
}
