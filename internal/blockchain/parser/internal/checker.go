package internal

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// Checker defines the interface to compare blocks (in different format) for parity check purposes.
	// For example, CompareRosettaBlocks can be added to the interface when we support Rosetta format.
	Checker interface {
		GetPreProcessor() PreProcessor
		CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock *api.NativeBlock, actualBlock *api.NativeBlock) error
		ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error
	}
)

type (
	checkerImpl struct {
		Logger         *zap.Logger
		Metrics        tally.Scope
		PreProcessor   PreProcessor
		RosettaChecker RosettaChecker
	}
)

type (
	ParityCheckFailedError struct {
		Err  error
		Diff string
	}
)

const (
	parityCheck                     = "parity_check"
	parityCheckFailure              = "parity_check_failure"
	parityCheckOnTransaction        = "transaction_check"
	parityCheckOnTransactionFailure = "transaction_check_failure"
	parityCheckFailureReasonKey     = "reason"

	notSupportedBlockchainFailure        = "not_supported_blockchain"
	blockLedgerMismatchFailure           = "block_ledger_mismatch"
	blockTransactionsMismatchFailure     = "block_transactions_mismatch"
	blockTransactionsSizeMismatchFailure = "block_transactions_size_mismatch"
	transactionMismatchFailure           = "transaction_mismatch"
	transactionOperationsMismatchFailure = "transaction_operations_mismatch"
	preProcessingFailure                 = "preprocessing_failure"

	resultType        = "result_type"
	resultTypeError   = "error"
	resultTypeSuccess = "success"
	resultTypeSkip    = "skip"
)

var (
	errTags = map[string]string{
		resultType: resultTypeError,
	}

	successTags = map[string]string{
		resultType: resultTypeSuccess,
	}

	skipTags = map[string]string{
		resultType: resultTypeSkip,
	}
)

func NewChecker(params ParserParams, preProcessor PreProcessor, rosettaChecker RosettaChecker) (Checker, error) {
	return &checkerImpl{
		Logger:         log.WithPackage(params.Logger),
		Metrics:        params.Metrics.SubScope("parity_checker"),
		PreProcessor:   preProcessor,
		RosettaChecker: rosettaChecker,
	}, nil
}

func (p *checkerImpl) GetPreProcessor() PreProcessor {
	return p.PreProcessor
}

func (p *checkerImpl) CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock *api.NativeBlock, actualBlock *api.NativeBlock) error {
	expectedBlock = proto.Clone(expectedBlock).(*api.NativeBlock)
	actualBlock = proto.Clone(actualBlock).(*api.NativeBlock)

	// Ignore differences in "Timestamp"
	actualBlock.Timestamp = expectedBlock.Timestamp

	if err := p.PreProcessor.PreProcessNativeBlock(expectedBlock, actualBlock); err != nil {
		return xerrors.Errorf("failed to preprocess blocks: %w", err)
	}

	if !proto.Equal(expectedBlock, actualBlock) {
		diff := cmp.Diff(expectedBlock, actualBlock, protocmp.Transform())
		return &ParityCheckFailedError{
			Err:  xerrors.Errorf("parity issue detected with native blocks: height=%v", height),
			Diff: diff,
		}
	}

	return nil
}

func (p *checkerImpl) ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return p.RosettaChecker.ValidateRosettaBlock(ctx, request, actualRosettaBlock)
}

func (e *ParityCheckFailedError) Error() string {
	return e.Err.Error()
}
