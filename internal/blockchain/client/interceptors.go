package client

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	parserInterceptor struct {
		client Client
		parser parser.Parser
	}

	instrumentInterceptor struct {
		client                          Client
		instrumentBatchGetBlockMetadata instrument.Call
		instrumentGetBlockByHeight      instrument.Call
		instrumentGetBlockByHash        instrument.Call
		instrumentGetLatestHeight       instrument.Call
		instrumentUpgradeBlock          instrument.Call
	}
)

var (
	_ Client = (*parserInterceptor)(nil)
	_ Client = (*instrumentInterceptor)(nil)
)

// WithParserInterceptor returns a new client which validates the data using the specified parser.
func WithParserInterceptor(client Client, parser parser.Parser) Client {
	return &parserInterceptor{
		client: client,
		parser: parser,
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

	if _, err := p.parser.ParseNativeBlock(ctx, block); err != nil {
		return xerrors.Errorf("failed to validate block {%+v}: %w", metadata, err)
	}

	return nil
}

// WithInstrumentInterceptor returns an instrumented client.
func WithInstrumentInterceptor(client Client, scope tally.Scope, logger *zap.Logger) Client {
	scope = scope.SubScope("client")

	newCall := func(method string) instrument.Call {
		return instrument.NewCall(scope, method, instrument.WithLogger(logger, fmt.Sprintf("client.%v", method)))
	}

	return &instrumentInterceptor{
		client:                          client,
		instrumentBatchGetBlockMetadata: newCall("batch_get_block_metadata"),
		instrumentGetBlockByHeight:      newCall("get_block_by_height"),
		instrumentGetBlockByHash:        newCall("get_block_by_hash"),
		instrumentGetLatestHeight:       newCall("get_latest_height"),
		instrumentUpgradeBlock:          newCall("upgrade_block"),
	}
}

func (i *instrumentInterceptor) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	var result []*api.BlockMetadata
	err := i.instrumentBatchGetBlockMetadata.Instrument(
		ctx,
		func(ctx context.Context) error {
			blockMetadata, err := i.client.BatchGetBlockMetadata(ctx, tag, from, to)
			if err != nil {
				return err
			}

			result = blockMetadata
			return nil
		},
		instrument.WithLoggerFields(
			zap.Uint32("tag", tag),
			zap.Uint64("from", from),
			zap.Uint64("to", to),
		),
	)

	return result, err
}

func (i *instrumentInterceptor) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...ClientOption) (*api.Block, error) {
	var result *api.Block
	err := i.instrumentGetBlockByHeight.Instrument(
		ctx,
		func(ctx context.Context) error {
			block, err := i.client.GetBlockByHeight(ctx, tag, height, opts...)
			if err != nil {
				return err
			}

			result = block
			return nil
		},
		instrument.WithLoggerFields(
			zap.Uint32("tag", tag),
			zap.Uint64("height", height),
		),
	)

	return result, err
}

func (i *instrumentInterceptor) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...ClientOption) (*api.Block, error) {
	var result *api.Block
	err := i.instrumentGetBlockByHash.Instrument(
		ctx,
		func(ctx context.Context) error {
			block, err := i.client.GetBlockByHash(ctx, tag, height, hash, opts...)
			if err != nil {
				return err
			}

			result = block
			return nil
		},
		instrument.WithLoggerFields(
			zap.Uint32("tag", tag),
			zap.Uint64("height", height),
			zap.String("hash", hash),
		),
	)

	return result, err
}

func (i *instrumentInterceptor) GetLatestHeight(ctx context.Context) (uint64, error) {
	var result uint64
	err := i.instrumentGetLatestHeight.Instrument(
		ctx,
		func(ctx context.Context) error {
			height, err := i.client.GetLatestHeight(ctx)
			if err != nil {
				return err
			}

			result = height
			return nil
		},
	)

	return result, err
}

func (i *instrumentInterceptor) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	var result *api.Block
	err := i.instrumentUpgradeBlock.Instrument(
		ctx,
		func(ctx context.Context) error {
			newBlock, err := i.client.UpgradeBlock(ctx, block, newTag)
			if err != nil {
				return err
			}

			result = newBlock
			return nil
		},
		instrument.WithLoggerFields(
			zap.Reflect("block", block.Metadata),
			zap.Uint32("newTag", newTag),
		),
	)

	return result, err
}

func (i *instrumentInterceptor) CanReprocess(tag uint32, height uint64) bool {
	return i.client.CanReprocess(tag, height)
}
