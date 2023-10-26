package activity

import (
	"context"

	"github.com/uber-go/tally"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/internal/workflow/activity/errors"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Validator struct {
		baseActivity
		metaStorage     metastorage.MetaStorage
		masterClient    client.Client
		slaveClient     client.Client
		parser          parser.Parser
		blobStorage     blobstorage.BlobStorage
		metrics         *validatorMetrics
		failoverManager endpoints.FailoverManager
	}

	ValidatorParams struct {
		fx.In
		fxparams.Params
		Runtime         cadence.Runtime
		MetaStorage     metastorage.MetaStorage
		Client          client.ClientParams
		Parser          parser.Parser
		StorageClient   blobstorage.BlobStorage
		FailoverManager endpoints.FailoverManager
	}

	ValidatorRequest struct {
		Tag                       uint32 `validate:"required"`
		StartHeight               uint64
		ValidationHeightPadding   uint64 `validate:"required"`
		MaxHeightsToValidate      uint64 `validate:"required"`
		StartEventId              int64
		MaxEventsToValidate       uint64 `validate:"required"`
		Parallelism               int    `validate:"required,gt=0"`
		ValidatorMaxReorgDistance uint64
		EventTag                  uint32
		Failover                  bool
	}

	ValidatorResponse struct {
		LastValidatedHeight      uint64
		BlockGap                 uint64
		LastValidatedEventId     int64
		LastValidatedEventHeight uint64
		EventGap                 int64
		EventTag                 uint32
	}

	validatorMetrics struct {
		instrumentValidateHeight    instrument.Call
		instrumentValidateEvents    instrument.Call
		instrumentBatchGetBlockMeta instrument.Call
		s3MetadataMismatchCounter   tally.Counter
		ddbMetadataMismatchCounter  tally.Counter
		nativeParsingErrCounter     tally.Counter
		rosettaParsingErrCounter    tally.Counter
		dataParityErrCounter        tally.Counter
		timeSinceLastBlock          tally.Gauge
		numTransactionsGauge        tally.Gauge
		rawBlockSizeGauge           tally.Gauge
		nativeBlockSizeGauge        tally.Gauge
		rosettaBlockSizeGauge       tally.Gauge
	}

	BlockValidation struct {
		Height   uint64
		Metadata *api.BlockMetadata
	}
)

const (
	blockValidationFailure         = "block_validation_failure"
	failureReasonKey               = "reason"
	numOfExtraPrevEventsToValidate = int64(20)
)

func NewValidator(params ValidatorParams) *Validator {
	a := &Validator{
		baseActivity:    newBaseActivity(ActivityValidator, params.Runtime),
		metaStorage:     params.MetaStorage,
		masterClient:    params.Client.Master,
		slaveClient:     params.Client.Slave,
		parser:          params.Parser,
		blobStorage:     params.StorageClient,
		metrics:         newValidatorMetrics(params.Metrics),
		failoverManager: params.FailoverManager,
	}
	a.register(a.execute)
	return a
}

func newValidatorMetrics(scope tally.Scope) *validatorMetrics {
	scope = scope.SubScope(ActivityValidator)
	return &validatorMetrics{
		instrumentValidateHeight:    instrument.NewCall(scope, "validate_height"),
		instrumentValidateEvents:    instrument.NewCall(scope, "validate_events"),
		instrumentBatchGetBlockMeta: instrument.NewCall(scope, "batch_get_block_metadata"),
		s3MetadataMismatchCounter:   newValidationFailureCounter(scope, "s3_metadata_mismatch"),
		ddbMetadataMismatchCounter:  newValidationFailureCounter(scope, "ddb_metadata_mismatch"),
		nativeParsingErrCounter:     newValidationFailureCounter(scope, "native_parsing_error"),
		rosettaParsingErrCounter:    newValidationFailureCounter(scope, "rosetta_parsing_error"),
		dataParityErrCounter:        newValidationFailureCounter(scope, "data_parity_error"),
		timeSinceLastBlock:          scope.Gauge("time_since_last_block"),
		numTransactionsGauge:        scope.Gauge("num_transactions"),
		rawBlockSizeGauge:           scope.Gauge("raw_block_size"),
		nativeBlockSizeGauge:        scope.Gauge("native_block_size"),
		rosettaBlockSizeGauge:       scope.Gauge("rosetta_block_size"),
	}
}

func newValidationFailureCounter(scope tally.Scope, reason string) tally.Counter {
	return scope.Tagged(map[string]string{failureReasonKey: reason}).Counter(blockValidationFailure)
}

func (v *Validator) Execute(ctx workflow.Context, request *ValidatorRequest) (*ValidatorResponse, error) {
	var response ValidatorResponse
	err := v.executeActivity(ctx, request, &response)
	return &response, err
}

func (v *Validator) execute(ctx context.Context, request *ValidatorRequest) (*ValidatorResponse, error) {
	if err := v.validateRequest(request); err != nil {
		return nil, err
	}

	if request.StartEventId < metastorage.EventIdStartValue {
		request.StartEventId = metastorage.EventIdStartValue
	}

	logger := v.getLogger(ctx).With(zap.Reflect("request", request))

	if request.Failover {
		failoverCtx, err := v.failoverManager.WithFailoverContext(ctx)
		if err != nil {
			return nil, xerrors.Errorf("failed to create failover context: %w", err)
		}
		ctx = failoverCtx
	}

	eventTag := request.EventTag
	ingestedLatestBlock, err := v.metaStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest ingested block: %w", err)
	}

	tipOfChain, err := v.masterClient.GetLatestHeight(ctx)
	if err != nil {
		return nil, temporal.NewApplicationError(
			xerrors.Errorf("failed to get tipOfChain from blockChainClient: %w", err).Error(), errors.ErrTypeNodeProvider)
	}

	startHeight := request.StartHeight
	endHeight, err := getValidationEndHeight(
		startHeight,
		ingestedLatestBlock,
		tipOfChain,
		request,
		logger,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate endHeight: %w", err)
	}

	logger.Info(
		"validating data",
		zap.Reflect("ingestedLatestBlock", ingestedLatestBlock),
		zap.Uint64("tipOfChain", tipOfChain),
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
		zap.Int64("startEventId", request.StartEventId),
		zap.Uint32("eventTag", eventTag),
	)

	if err = v.validateLatestBlock(ctx, logger, ingestedLatestBlock); err != nil {
		return nil, xerrors.Errorf("failed to validate latest block: %+v: %w", ingestedLatestBlock, err)
	}

	lastValidatedHeight, err := v.validateRange(ctx, logger, request.Tag, startHeight, endHeight, request.Parallelism)
	if err != nil {
		return nil, xerrors.Errorf("failed to validate range tag=%d, range=[%d, %d): %w", request.Tag, startHeight, endHeight, err)
	}

	blockGap := tipOfChain - lastValidatedHeight

	maxEventId, err := v.metaStorage.GetMaxEventId(ctx, eventTag)
	if err != nil {
		if xerrors.Is(err, storage.ErrNoEventHistory) {
			return &ValidatorResponse{
				LastValidatedHeight: lastValidatedHeight,
				BlockGap:            blockGap,
				EventTag:            eventTag,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get max event id for eventTag=%v: %w", eventTag, err)
	}

	lastValidatedEventId, lastValidatedEventHeight, err := v.validateEvents(ctx, eventTag, request.StartEventId, request.MaxEventsToValidate)
	if err != nil {
		return nil, xerrors.Errorf("failed to validate events (StartEventId:%d, MaxEventsToValidate:%d, EventTag:%d): %w", request.StartEventId, request.MaxEventsToValidate, eventTag, err)
	}

	eventGap := maxEventId - lastValidatedEventId

	return &ValidatorResponse{
		LastValidatedHeight:      lastValidatedHeight,
		BlockGap:                 blockGap,
		LastValidatedEventId:     lastValidatedEventId,
		LastValidatedEventHeight: lastValidatedEventHeight,
		EventGap:                 eventGap,
		EventTag:                 eventTag,
	}, nil
}

func (v *Validator) validateLatestBlock(ctx context.Context, logger *zap.Logger, metadata *api.BlockMetadata) error {
	rawBlock, err := v.blobStorage.Download(ctx, metadata)
	if err != nil {
		return xerrors.Errorf("failed to download latest block: %w", err)
	}

	nativeBlock, err := v.parser.ParseNativeBlock(ctx, rawBlock)
	if err != nil {
		return xerrors.Errorf("failed to parse native block: %w", err)
	}

	rosettaBlock, err := v.parser.ParseRosettaBlock(ctx, rawBlock)
	if err != nil && !xerrors.Is(err, parser.ErrNotImplemented) {
		return xerrors.Errorf("failed to parse rosetta block: %w", err)
	}

	timeSinceLastBlock := utils.SinceTimestamp(nativeBlock.GetTimestamp())
	if timeSinceLastBlock != 0 {
		v.metrics.timeSinceLastBlock.Update(timeSinceLastBlock.Seconds())
	}

	numTransactions := nativeBlock.NumTransactions
	v.metrics.numTransactionsGauge.Update(float64(numTransactions))

	rawBlockSize := proto.Size(rawBlock)
	v.metrics.rawBlockSizeGauge.Update(float64(rawBlockSize))
	nativeBlockSize := proto.Size(nativeBlock)
	v.metrics.nativeBlockSizeGauge.Update(float64(nativeBlockSize))

	var rosettaBlockSize int
	if rosettaBlock == nil {
		rosettaBlockSize = -1 // not parsed successfully due to parser.ErrNotImplemented, use -1 as block-size to log
	} else {
		rosettaBlockSize = proto.Size(rosettaBlock)
		v.metrics.rosettaBlockSizeGauge.Update(float64(rosettaBlockSize))
	}

	logger.Info("latest block",
		zap.Uint64("height", metadata.Height),
		zap.Reflect("metadata", metadata),
		zap.String("time_since_last_block", timeSinceLastBlock.String()),
		zap.Uint64("num_transactions", numTransactions),
		zap.Int("raw_block_size", rawBlockSize),
		zap.Int("native_block_size", nativeBlockSize),
		zap.Int("rosetta_block_size", rosettaBlockSize),
	)

	return nil
}

func (v *Validator) validateEvents(ctx context.Context, eventTag uint32, startEventId int64, maxEvents uint64) (int64, uint64, error) {
	var events []*model.EventEntry
	if err := v.metrics.instrumentValidateEvents.Instrument(ctx, func(ctx context.Context) error {
		// fetch some extra events from earlier ids so we can better test continuity
		startEventId = startEventId - numOfExtraPrevEventsToValidate
		if startEventId < metastorage.EventIdStartValue {
			startEventId = metastorage.EventIdStartValue
		}
		maxEvents += uint64(numOfExtraPrevEventsToValidate)
		fetchedEvents, err := v.metaStorage.GetEventsAfterEventId(ctx, eventTag, startEventId, maxEvents)
		events = fetchedEvents
		return err
	}); err != nil {
		return 0, 0, xerrors.Errorf("failed to call GetEventsAfterEventId (startEventId=%v, maxEvents=%v, eventTag=%v): %w", startEventId, maxEvents, eventTag, err)
	}
	if len(events) == 0 {
		return 0, 0, xerrors.Errorf("received empty event list (startEventId=%v, maxEvents=%v, eventTag=%v)", startEventId, maxEvents, eventTag)
	}
	lastEvent := events[len(events)-1]
	return lastEvent.EventId, lastEvent.BlockHeight, nil
}

func getValidationEndHeight(startHeight uint64, ingestedLatestBlock *api.BlockMetadata, tipOfChain uint64, request *ValidatorRequest, logger *zap.Logger) (uint64, error) {
	endHeight := tipOfChain - request.ValidationHeightPadding
	if ingestedLatestBlock.Height < endHeight {
		endHeight = ingestedLatestBlock.Height
	}
	if endHeight > startHeight+request.MaxHeightsToValidate-1 {
		endHeight = startHeight + request.MaxHeightsToValidate - 1
	}
	// TODO: Set default ValidatorMaxReorgDistance for running workflows, safe to delete late
	validatorMaxReorgDistance := request.ValidatorMaxReorgDistance
	if validatorMaxReorgDistance == 0 {
		validatorMaxReorgDistance = 100
	}
	if endHeight < startHeight {
		logger.Info("Invalid startHeight",
			zap.Uint64("startHeight", startHeight),
			zap.Uint64("endHeight", endHeight),
		)

		if startHeight-endHeight < validatorMaxReorgDistance {
			endHeight = startHeight
		} else {
			return 0, xerrors.Errorf("InvalidStartHeight: the gap of calculated endHeight %d and startHeight %d is greater than %d", endHeight, startHeight, validatorMaxReorgDistance)
		}
	}
	return endHeight, nil
}

func (v *Validator) validateRange(ctx context.Context, logger *zap.Logger, tag uint32,
	startHeight uint64, endHeight uint64, parallelism int) (uint64, error) {
	if startHeight == endHeight {
		return startHeight, nil
	}

	var firstNonSkipBlock *api.BlockMetadata
	var lastBlock *api.BlockMetadata
	blockMetadataList, err := v.metaStorage.GetBlocksByHeightRange(ctx, tag, startHeight, endHeight+1)
	if err != nil {
		return 0, xerrors.Errorf("failed to get blocks [%v, %v]: %w", startHeight, endHeight, err)
	}

	for _, block := range blockMetadataList {
		if !block.Skipped {
			firstNonSkipBlock = block
			break
		}
	}

	if firstNonSkipBlock != nil && firstNonSkipBlock.Height > 0 {
		lastBlock, err = v.metaStorage.GetBlockByHeight(ctx, tag, firstNonSkipBlock.ParentHeight)
		if err != nil {
			return 0, xerrors.Errorf(
				"failed to get parentHeight %v of the firstNonSkipBlock %v in [%v, %v]: %w",
				firstNonSkipBlock.ParentHeight, firstNonSkipBlock.Height, startHeight, endHeight, err)
		}
	}

	// Check if chain is continuous
	err = parser.ValidateChain(blockMetadataList, lastBlock)
	if err != nil {
		return 0, xerrors.Errorf("block chain is not continuous: %w", err)
	}

	var expectedMetadataList []*api.BlockMetadata
	if err := v.metrics.instrumentBatchGetBlockMeta.Instrument(ctx, func(ctx context.Context) error {
		metadataList, err := v.slaveClient.BatchGetBlockMetadata(ctx, tag, startHeight, endHeight+1)
		if err != nil {
			return temporal.NewApplicationError(
				xerrors.Errorf("failed to get block metadata from %v to %v: %w", startHeight, endHeight+1, err).Error(), errors.ErrTypeNodeProvider)
		}
		expectedMetadataList = metadataList
		return nil
	}); err != nil {
		return 0, err
	}

	validationSize := int(endHeight - startHeight + 1)
	if parallelism > validationSize {
		parallelism = validationSize
	}

	g, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(parallelism))
	index := 0
	for height := startHeight; height <= endHeight; height++ {
		validateBlock := &BlockValidation{
			Height:   height,
			Metadata: expectedMetadataList[index],
		}
		index++
		g.Go(func() error {
			_, err := v.validateHeight(ctx, logger, validateBlock.Height, tag, validateBlock.Metadata)
			if err != nil {
				return xerrors.Errorf("failed to get validate height %d: %w", validateBlock.Height, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, xerrors.Errorf("failed to validate block: %w", err)
	}
	return endHeight, nil
}

func (v *Validator) validateHeight(ctx context.Context,
	logger *zap.Logger, height uint64, tag uint32,
	expectedMetadata *api.BlockMetadata) (string, error) {

	logger = logger.With(zap.Uint64("height", height))
	var validatedBlockHash string
	err := v.metrics.instrumentValidateHeight.Instrument(ctx, func(ctx context.Context) error {
		validatedBlockHash = expectedMetadata.Hash
		actualBlockMetaData, err := v.metaStorage.GetBlockByHeight(ctx, tag, height)
		if err != nil {
			return xerrors.Errorf("failed to get block meta data from meta storage (height=%d): %w", height, err)
		}
		if expectedMetadata.Tag != actualBlockMetaData.Tag ||
			expectedMetadata.Hash != actualBlockMetaData.Hash ||
			expectedMetadata.ParentHash != actualBlockMetaData.ParentHash ||
			expectedMetadata.Height != actualBlockMetaData.Height ||
			expectedMetadata.ParentHeight != actualBlockMetaData.ParentHeight ||
			expectedMetadata.Skipped != actualBlockMetaData.Skipped {
			logger.Error("detected mismatch in block metadata stored in ddb",
				zap.Reflect("expected", expectedMetadata),
				zap.Reflect("actual", actualBlockMetaData),
			)
			v.metrics.ddbMetadataMismatchCounter.Inc(1)
			return nil
		}
		actualRawBlock, err := v.blobStorage.Download(ctx, actualBlockMetaData)
		if err != nil {
			return xerrors.Errorf("failed to get block using storage client (key=%s): %w", actualBlockMetaData.ObjectKeyMain, err)
		}
		if expectedMetadata.Hash != actualRawBlock.Metadata.Hash {
			logger.Error("detected mismatch in block hash stored in s3",
				zap.Reflect("expected", expectedMetadata),
				zap.Reflect("actual", actualRawBlock.Metadata.Hash),
			)
			v.metrics.s3MetadataMismatchCounter.Inc(1)
			return nil
		}
		_, err = v.parser.ParseNativeBlock(ctx, actualRawBlock)
		if err != nil {
			logger.Error("failed to parse raw block using native parser",
				zap.Error(err),
				zap.Reflect("actualRawBlock", actualRawBlock.Metadata),
			)
			v.metrics.nativeParsingErrCounter.Inc(1)
			return nil
		}

		_, err = v.parser.ParseRosettaBlock(ctx, actualRawBlock)
		// ParseRosettaBlock is not necessarily implemented for all chains, skip if so
		if err != nil && !xerrors.Is(err, parser.ErrNotImplemented) {
			logger.Error("failed to parse raw block using rosetta parser",
				zap.Error(err),
				zap.Reflect("actualRawBlock", actualRawBlock.Metadata),
			)
			v.metrics.rosettaParsingErrCounter.Inc(1)
			return nil
		}

		return nil
	})
	return validatedBlockHash, err
}
