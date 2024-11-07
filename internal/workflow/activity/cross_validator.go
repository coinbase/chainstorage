package activity

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/picker"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
)

type (
	CrossValidator struct {
		baseActivity
		blobStorage          blobstorage.BlobStorage
		metaStorage          metastorage.MetaStorage
		validatorClient      client.Client
		parser               parser.Parser
		picker               picker.Picker
		validationPercentage int
		metrics              *crossValidatorMetrics
	}

	CrossValidatorParams struct {
		fx.In
		fxparams.Params
		Runtime         cadence.Runtime
		ValidatorClient client.Client `name:"validator"`
		MetaStorage     metastorage.MetaStorage
		StorageClient   blobstorage.BlobStorage
		Parser          parser.Parser
	}

	CrossValidatorRequest struct {
		Tag                     uint32 `validate:"required"`
		StartHeight             uint64
		ValidationHeightPadding uint64 `validate:"required"`
		MaxHeightsToValidate    uint64 `validate:"required"`
		Parallelism             int    `validate:"required,gt=0"`
	}

	CrossValidatorResponse struct {
		EndHeight uint64
		BlockGap  uint64
	}

	crossValidatorMetrics struct {
		instrumentValidateHeight             instrument.Instrument
		nativeFormatValidationFailureCounter tally.Counter
		skipValidationCounter                tally.Counter
	}

	validatorMode int
)

const (
	crossValidationFailure = "cross_validation_failure"
	reasonKey              = "reason"

	diffMaxLength = 3000
)

const (
	validatorModeUnknown validatorMode = iota
	validationMode
	validationModePassThrough
)

func NewCrossValidator(params CrossValidatorParams) *CrossValidator {
	validationPercentage := params.Config.Workflows.CrossValidator.ValidationPercentage

	var choices []*picker.Choice
	if validationPercentage > 0 {
		choices = append(choices, &picker.Choice{
			Item:   validationMode,
			Weight: validationPercentage,
		})
	}
	if validationPercentage < 100 {
		choices = append(choices, &picker.Choice{
			Item:   validationModePassThrough,
			Weight: 100 - validationPercentage,
		})
	}
	picker := picker.New(choices)

	v := &CrossValidator{
		baseActivity:         newBaseActivity(ActivityCrossValidator, params.Runtime),
		blobStorage:          params.StorageClient,
		metaStorage:          params.MetaStorage,
		validatorClient:      params.ValidatorClient,
		parser:               params.Parser,
		picker:               picker,
		validationPercentage: validationPercentage,
		metrics:              newCrossValidatorMetrics(params.Metrics),
	}
	v.register(v.execute)
	return v
}

func newCrossValidatorMetrics(scope tally.Scope) *crossValidatorMetrics {
	scope = scope.SubScope(ActivityCrossValidator)
	return &crossValidatorMetrics{
		instrumentValidateHeight:             instrument.New(scope, "cross_validate_height"),
		nativeFormatValidationFailureCounter: newCrossValidationFailureCounter(scope, "native_format_validation_failure"),
		skipValidationCounter:                newCrossValidationFailureCounter(scope, "skip"),
	}
}

func (v *CrossValidator) Execute(ctx workflow.Context, request *CrossValidatorRequest) (*CrossValidatorResponse, error) {
	var response CrossValidatorResponse
	err := v.executeActivity(ctx, request, &response)
	return &response, err
}

func (v *CrossValidator) execute(ctx context.Context, request *CrossValidatorRequest) (*CrossValidatorResponse, error) {
	if err := v.validateRequest(request); err != nil {
		return nil, err
	}
	logger := v.getLogger(ctx).With(zap.Reflect("request", request))

	tag := request.Tag
	startHeight := request.StartHeight

	latestBlock, err := v.metaStorage.GetLatestBlock(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}
	latestBlockHeight := latestBlock.Height

	tipOfChain, err := v.validatorClient.GetLatestHeight(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tipOfChain from validator client: %w", err)
	}

	endHeight, err := v.getValidationEndHeight(startHeight, latestBlockHeight, tipOfChain, request, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate validation end height: %w", err)
	}

	logger.Info(
		"validating data range",
		zap.Uint32("tag", tag),
		zap.Uint64("tip_of_chain", tipOfChain),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
	)

	err = v.validateRange(ctx, logger, tag, startHeight, endHeight, request.Parallelism)
	if err != nil {
		return nil, fmt.Errorf("failed to validate range tag=%d, range=[%d, %d): %w", tag, startHeight, endHeight, err)
	}

	blockGap := tipOfChain - endHeight
	if tipOfChain < endHeight {
		blockGap = 0
	}

	return &CrossValidatorResponse{
		EndHeight: endHeight,
		BlockGap:  blockGap,
	}, nil
}

func (v *CrossValidator) getValidationEndHeight(startHeight uint64, latestPersistedHeight uint64, tipOfChain uint64, request *CrossValidatorRequest, logger *zap.Logger) (uint64, error) {
	endHeight := tipOfChain - request.ValidationHeightPadding
	if latestPersistedHeight < endHeight {
		endHeight = latestPersistedHeight
	}

	if endHeight > startHeight+request.MaxHeightsToValidate {
		endHeight = startHeight + request.MaxHeightsToValidate
	}

	// Since no sticky session applies to validator cluster, cross validation workflow may experience false alarm of reorg
	// when node of validator rewinds to an older block height.
	// For such case, cross validation will stay at startHeight and wait till nodes catch up.
	if endHeight < startHeight {
		logger.Info("Invalid startHeight",
			zap.Uint64("start_height", startHeight),
			zap.Uint64("end_height", endHeight),
			zap.Uint64("tip", tipOfChain),
			zap.Uint64("latest_persisted", latestPersistedHeight),
		)
		endHeight = startHeight
	}
	return endHeight, nil
}

func (v *CrossValidator) validateRange(ctx context.Context, logger *zap.Logger, tag uint32, startHeight uint64, endHeight uint64, parallelism int) error {
	if startHeight == endHeight {
		return nil
	}

	validationSize := int(endHeight - startHeight)
	if parallelism > validationSize {
		parallelism = validationSize
	}

	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(parallelism))
	for h := startHeight; h < endHeight; h++ {
		height := h
		if v.getMode() == validationModePassThrough {
			logger.Debug("skip block validation", zap.Uint64("height", height))
			v.metrics.skipValidationCounter.Inc(1)
			continue
		}

		group.Go(func() error {
			err := v.validateHeight(ctx, logger, height, tag)
			if err != nil {
				return fmt.Errorf("failed to validate block=%d: %w", height, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("failed to validate blocks: %w", err)
	}
	return nil
}

func (v *CrossValidator) validateHeight(ctx context.Context, logger *zap.Logger, height uint64, tag uint32) error {
	logger = logger.With(zap.Uint64("height", height), zap.Uint32("tag", tag))
	return v.metrics.instrumentValidateHeight.Instrument(ctx, func(ctx context.Context) error {
		persistedBlockMetaData, err := v.metaStorage.GetBlockByHeight(ctx, tag, height)
		if err != nil {
			return fmt.Errorf("failed to get block meta data from meta storage (height=%d): %w", height, err)
		}
		persistedRawBlock, err := v.blobStorage.Download(ctx, persistedBlockMetaData)
		if err != nil {
			return fmt.Errorf("failed to get block from blobStorage (key=%s): %w", persistedBlockMetaData.ObjectKeyMain, err)
		}

		hash := persistedBlockMetaData.Hash
		expectedRawBlock, err := v.validatorClient.GetBlockByHash(ctx, tag, height, hash)
		if err != nil {
			logger.Warn("fetch block failed",
				zap.String("hash", hash),
				zap.Error(err),
			)
			return fmt.Errorf("failed to fetch block: %w", err)
		}

		persistedNativeBlock, err := v.parser.ParseNativeBlock(ctx, persistedRawBlock)
		if err != nil {
			return fmt.Errorf("failed to parse actual raw block using native parser for block {%+v}: %w", persistedRawBlock.Metadata, err)
		}

		expectedNativeBlock, err := v.parser.ParseNativeBlock(ctx, expectedRawBlock)
		if err != nil {
			return fmt.Errorf("failed to parse expected raw block using native parser for block {%+v}: %w", expectedRawBlock.Metadata, err)
		}

		if err := v.parser.CompareNativeBlocks(ctx, height, expectedNativeBlock, persistedNativeBlock); err != nil {
			var diff string
			var checkerErr *parser.ParityCheckFailedError
			if errors.As(err, &checkerErr) {
				diff = checkerErr.Diff
				if len(diff) > diffMaxLength {
					diff = diff[0:diffMaxLength]
				}
			}

			logger.Error("cross validation failed",
				zap.Error(err),
				zap.String("hash", hash),
				zap.Reflect("expected_metadata", expectedRawBlock.Metadata),
				zap.Reflect("actual_metadata", persistedRawBlock.Metadata),
				zap.String("diff", diff),
			)
			v.metrics.nativeFormatValidationFailureCounter.Inc(1)
			return nil
		}
		logger.Info("validated block successfully")
		return nil
	})
}

func (v *CrossValidator) getMode() validatorMode {
	mode := v.picker.Next().(validatorMode)
	return mode
}

func newCrossValidationFailureCounter(scope tally.Scope, reason string) tally.Counter {
	return scope.Tagged(map[string]string{reasonKey: reason}).Counter(crossValidationFailure)
}
