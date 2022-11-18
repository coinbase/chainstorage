package parser

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	instrumentInterceptor struct {
		parser                      Parser
		instrumentParseNativeBlock  instrument.Call
		instrumentParseRosettaBlock instrument.Call
	}
)

func WithInstrumentInterceptor(parser Parser, scope tally.Scope, logger *zap.Logger) Parser {
	scope = scope.SubScope("parser")

	newCall := func(method string) instrument.Call {
		return instrument.NewCall(
			scope,
			method,
			instrument.WithLogger(logger, fmt.Sprintf("parser.%v", method)),
			instrument.WithFilter(func(err error) bool {
				return xerrors.Is(err, ErrNotImplemented)
			}),
		)
	}

	return &instrumentInterceptor{
		parser:                      parser,
		instrumentParseNativeBlock:  newCall("parse_native_block"),
		instrumentParseRosettaBlock: newCall("parse_rosetta_block"),
	}
}

func (i *instrumentInterceptor) ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	var result *api.NativeBlock
	err := i.instrumentParseNativeBlock.Instrument(
		ctx,
		func(ctx context.Context) error {
			block, err := i.parser.ParseNativeBlock(ctx, rawBlock)
			if err != nil {
				return err
			}

			result = block
			return nil
		},
		instrument.WithLoggerFields(
			zap.Reflect("block", rawBlock.Metadata),
		),
	)

	return result, err
}

func (i *instrumentInterceptor) ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	var result *api.RosettaBlock
	err := i.instrumentParseRosettaBlock.Instrument(
		ctx,
		func(ctx context.Context) error {
			block, err := i.parser.ParseRosettaBlock(ctx, rawBlock)
			if err != nil {
				return err
			}

			result = block
			return nil
		},
		instrument.WithLoggerFields(
			zap.Reflect("block", rawBlock.Metadata),
		),
	)

	return result, err
}
