package internal

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	instrumentInterceptor struct {
		parser                         Parser
		instrumentParseNativeBlock     instrument.InstrumentWithResult[*api.NativeBlock]
		instrumentGetNativeTransaction instrument.InstrumentWithResult[*api.NativeTransaction]
		instrumentParseRosettaBlock    instrument.InstrumentWithResult[*api.RosettaBlock]
		instrumentCompareNativeBlocks  instrument.Instrument
		instrumentValidateBlock        instrument.Instrument
		instrumentValidateAccountState instrument.InstrumentWithResult[*api.ValidateAccountStateResponse]
		instrumentValidateRosettaBlock instrument.Instrument
	}
)

func WithInstrumentInterceptor(parser Parser, scope tally.Scope, logger *zap.Logger) Parser {
	scope = scope.SubScope("parser")

	return &instrumentInterceptor{
		parser:                         parser,
		instrumentParseNativeBlock:     newInstrumentWithResult[*api.NativeBlock]("parse_native_block", scope, logger),
		instrumentGetNativeTransaction: newInstrumentWithResult[*api.NativeTransaction]("get_native_transaction", scope, logger),
		instrumentParseRosettaBlock:    newInstrumentWithResult[*api.RosettaBlock]("parse_rosetta_block", scope, logger),
		instrumentCompareNativeBlocks:  newInstrument("compare_native_blocks", scope, logger),
		instrumentValidateBlock:        newInstrument("validate_block", scope, logger),
		instrumentValidateAccountState: newInstrumentWithResult[*api.ValidateAccountStateResponse]("validate_account_state", scope, logger),
		instrumentValidateRosettaBlock: newInstrument("validate_rosetta_block", scope, logger),
	}
}

func newInstrument(method string, scope tally.Scope, logger *zap.Logger) instrument.Instrument {
	return instrument.New(
		scope,
		method,
		instrument.WithLogger(logger, fmt.Sprintf("parser.%v", method)),
		instrument.WithFilter(func(err error) bool {
			return xerrors.Is(err, ErrNotImplemented)
		}),
	)
}

func newInstrumentWithResult[T any](method string, scope tally.Scope, logger *zap.Logger) instrument.InstrumentWithResult[T] {
	return instrument.NewWithResult[T](
		scope,
		method,
		instrument.WithLogger(logger, fmt.Sprintf("parser.%v", method)),
		instrument.WithFilter(func(err error) bool {
			return xerrors.Is(err, ErrNotImplemented)
		}),
	)
}

func (i *instrumentInterceptor) ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	return i.instrumentParseNativeBlock.Instrument(
		ctx,
		func(ctx context.Context) (*api.NativeBlock, error) {
			return i.parser.ParseNativeBlock(ctx, rawBlock)
		},
		instrument.WithLoggerFields(
			zap.Reflect("block", rawBlock.Metadata),
		),
	)
}

func (i *instrumentInterceptor) GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return i.instrumentGetNativeTransaction.Instrument(
		ctx,
		func(ctx context.Context) (*api.NativeTransaction, error) {
			return i.parser.GetNativeTransaction(ctx, nativeBlock, transactionHash)
		},
		instrument.WithLoggerFields(
			zap.Uint64("blockHeight", nativeBlock.Height),
			zap.String("blockHash", nativeBlock.Hash),
			zap.String("transactionHash", transactionHash),
		),
	)
}

func (i *instrumentInterceptor) ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	return i.instrumentParseRosettaBlock.Instrument(
		ctx,
		func(ctx context.Context) (*api.RosettaBlock, error) {
			return i.parser.ParseRosettaBlock(ctx, rawBlock)
		},
		instrument.WithLoggerFields(
			zap.Reflect("block", rawBlock.Metadata),
		),
	)
}

func (i *instrumentInterceptor) CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock, actualBlock *api.NativeBlock) error {
	return i.instrumentCompareNativeBlocks.Instrument(
		ctx,
		func(ctx context.Context) error {
			return i.parser.CompareNativeBlocks(ctx, height, expectedBlock, actualBlock)
		},
		instrument.WithLoggerFields(
			zap.Uint64("height", height),
		),
	)
}

func (i *instrumentInterceptor) ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error {
	return i.instrumentValidateBlock.Instrument(
		ctx,
		func(ctx context.Context) error {
			return i.parser.ValidateBlock(ctx, nativeBlock)
		},
		instrument.WithLoggerFields(
			zap.Uint64("blockHeight", nativeBlock.Height),
			zap.String("blockHash", nativeBlock.Hash),
		),
	)
}

func (i *instrumentInterceptor) ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error) {
	return i.instrumentValidateAccountState.Instrument(
		ctx,
		func(ctx context.Context) (*api.ValidateAccountStateResponse, error) {
			return i.parser.ValidateAccountState(ctx, req)
		},
		instrument.WithLoggerFields(
			zap.String("account", req.AccountReq.Account),
			zap.String("blockHash", req.GetBlock().GetHash()),
		),
	)
}

func (i *instrumentInterceptor) ValidateRosettaBlock(ctx context.Context, req *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return i.instrumentValidateRosettaBlock.Instrument(
		ctx,
		func(ctx context.Context) error {
			return i.parser.ValidateRosettaBlock(ctx, req, actualRosettaBlock)
		},
		instrument.WithLoggerFields(
			zap.Int64("blockHeight", actualRosettaBlock.Block.BlockIdentifier.Index),
			zap.String("blockHash", actualRosettaBlock.Block.BlockIdentifier.Hash),
		),
	)
}
