package workflow

import (
	"context"
	"sort"
	"strconv"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Backfiller struct {
		baseWorkflow
		reader           *activity.Reader
		extractor        *activity.Extractor
		loader           *activity.Loader
		blockStartHeight uint64
	}

	BackfillerParams struct {
		fx.In
		fxparams.Params
		Runtime   cadence.Runtime
		Reader    *activity.Reader
		Extractor *activity.Extractor
		Loader    *activity.Loader
	}

	BackfillerRequest struct {
		Tag                     uint32
		StartHeight             uint64
		EndHeight               uint64 `validate:"gt=0,gtfield=StartHeight"`
		UpdateWatermark         bool
		NumConcurrentExtractors int     // Optional. If not specified, it is read from the workflow config.
		BatchSize               uint64  // Optional. If not specified, it is read from the workflow config.
		MiniBatchSize           uint64  // Optional. If not specified, it is read from the workflow config.
		CheckpointSize          uint64  // Optional. If not specified, it is read from the workflow config.
		MaxReprocessedPerBatch  uint64  // Optional. If not specified, it is read from the workflow config.
		RehydrateFromTag        *uint32 // Optional. If not specified, rehydration is disabled.
		UpgradeFromTag          *uint32 // Optional. If not specified, upgrade is disabled.
		DataCompression         string  // Optional. If not specified, it is read from the workflow config.
		Failover                bool    // Optional. If not specified, it is set as false.
	}
)

var (
	_ InstrumentedRequest = (*BackfillerRequest)(nil)
)

const (
	// backfiller metrics. need to have `workflow.backfiller` as prefix
	backfillerHeightGauge        = "workflow.backfiller.height"
	backfillerReprocessedCounter = "workflow.backfiller.reprocessed"
)

func NewBackfiller(params BackfillerParams) *Backfiller {
	w := &Backfiller{
		baseWorkflow:     newBaseWorkflow(&params.Config.Workflows.Backfiller, params.Runtime),
		reader:           params.Reader,
		extractor:        params.Extractor,
		loader:           params.Loader,
		blockStartHeight: params.Config.Chain.BlockStartHeight,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Backfiller) Execute(ctx context.Context, request *BackfillerRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (w *Backfiller) execute(ctx workflow.Context, request *BackfillerRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}

		var cfg config.BackfillerWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		miniBatchSize := uint64(1)
		if cfg.MiniBatchSize > 0 {
			miniBatchSize = cfg.MiniBatchSize
		}
		if request.MiniBatchSize > 0 {
			miniBatchSize = request.MiniBatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		maxReprocessedPerBatch := cfg.MaxReprocessedPerBatch
		if request.MaxReprocessedPerBatch > 0 {
			maxReprocessedPerBatch = request.MaxReprocessedPerBatch
		}

		numConcurrentExtractors := cfg.NumConcurrentExtractors
		if request.NumConcurrentExtractors > 0 {
			numConcurrentExtractors = request.NumConcurrentExtractors
		}

		var dataCompression api.Compression
		var err error
		dataCompression = cfg.Storage.DataCompression
		if request.DataCompression != "" {
			dataCompression, err = utils.ParseCompression(request.DataCompression)
			if err != nil {
				return xerrors.Errorf("failed to parse data compression: %w", err)
			}
		}

		failover := false
		if request.Failover {
			failover = request.Failover
		}

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
		})
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		logger.Info("workflow started")
		ctx = w.withActivityOptions(ctx)

		lastBlock, err := w.readLastBlock(ctx, tag, request.StartHeight)
		if err != nil {
			logger.Error("failed to read last block", zap.Error(err))
			return xerrors.Errorf("failed to read last block: %w", err)
		}
		logger.Info("last block", zap.Reflect("metadata", lastBlock))

		for batchStart := request.StartHeight; batchStart < request.EndHeight; batchStart += batchSize {
			if batchStart-request.StartHeight >= checkpointSize {
				newRequest := *request
				newRequest.StartHeight = batchStart
				logger.Info(
					"checkpoint reached",
					zap.Reflect("newRequest", newRequest),
				)
				return w.continueAsNew(ctx, &newRequest)
			}

			batchEnd := batchStart + batchSize
			if batchEnd > request.EndHeight {
				batchEnd = request.EndHeight
			}

			batch, err := w.processBatch(
				ctx,
				logger,
				metrics,
				tag,
				batchStart,
				batchEnd,
				miniBatchSize,
				numConcurrentExtractors,
				maxReprocessedPerBatch,
				lastBlock,
				request.UpdateWatermark,
				request.RehydrateFromTag,
				request.UpgradeFromTag,
				dataCompression,
				failover,
			)
			if err != nil {
				logger.Error(
					"failed to process batch",
					zap.Uint64("batchStart", batchStart),
					zap.Uint64("batchEnd", batchEnd),
					zap.Error(err),
				)
				return xerrors.Errorf("failed to process batch [%v, %v): %w", batchStart, batchEnd, err)
			}
			lastBlock = batch[len(batch)-1]

			metrics.Gauge(backfillerHeightGauge).Update(float64(batchEnd - 1))
			logger.Info(
				"processed batch",
				zap.Uint64("batchStart", batchStart),
				zap.Uint64("batchEnd", batchEnd),
			)
		}

		logger.Info("workflow finished")

		return nil
	})
}

func (w *Backfiller) readLastBlock(ctx workflow.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
	if height == 0 || height == w.blockStartHeight {
		return nil, nil
	}

	request := &activity.ReaderRequest{
		Tag:    tag,
		Height: height - 1,
	}
	response, err := w.reader.Execute(ctx, request)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute reader (request=%v): %w", request, err)
	}

	return response.Metadata, nil
}

func (w *Backfiller) processBatch(
	ctx workflow.Context,
	logger *zap.Logger,
	metrics client.MetricsHandler,
	tag uint32,
	batchStart uint64,
	batchEnd uint64,
	miniBatchSize uint64,
	numConcurrentExtractors int,
	maxReprocessedPerBatch uint64,
	lastBlock *api.BlockMetadata,
	updateWatermark bool,
	rehydrateFromTag *uint32,
	upgradeFromTag *uint32,
	dataCompression api.Compression,
	failover bool,
) ([]*api.BlockMetadata, error) {
	batchSize := int(batchEnd - batchStart)
	batchMetadata := make([]*api.BlockMetadata, batchSize)

	wg := workflow.NewWaitGroup(ctx)
	wg.Add(numConcurrentExtractors)

	inputChannel := workflow.NewNamedBufferedChannel(ctx, "backfiller.input", batchSize)
	for i := batchStart; i < batchEnd; i++ {
		inputChannel.Send(ctx, i)
	}
	inputChannel.Close()

	outputChannel := workflow.NewNamedBufferedChannel(ctx, "backfiller.output", batchSize)
	defer outputChannel.Close()

	reprocessChannel := workflow.NewNamedBufferedChannel(ctx, "backfiller.reprocess", batchSize)
	defer reprocessChannel.Close()

	// Phase 1: running extractors in parallel.
	for i := 0; i < numConcurrentExtractors; i++ {
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer wg.Done()

			for {
				heights := make([]uint64, 0, miniBatchSize)
				for i := uint64(0); i < miniBatchSize; i++ {
					var height uint64
					if ok := inputChannel.Receive(ctx, &height); !ok {
						break
					}

					heights = append(heights, height)
				}

				if len(heights) == 0 {
					break
				}

				extractorRequest := &activity.ExtractorRequest{
					Tag:              tag,
					Heights:          heights,
					RehydrateFromTag: rehydrateFromTag,
					UpgradeFromTag:   upgradeFromTag,
					DataCompression:  dataCompression,
					Failover:         failover,
				}
				extractorResponse, err := w.extractor.Execute(ctx, extractorRequest)
				if err != nil {
					// Reprocess the request in phase 2.
					reprocessChannel.Send(ctx, extractorRequest)
					logger.Warn(
						"queued extractor for reprocessing",
						zap.Error(err),
						zap.Reflect("heights", heights),
					)
					continue
				}

				outputChannel.Send(ctx, extractorResponse)
			}
		})
	}
	wg.Wait(ctx)

	// The request will be reprocessed sequentially in a best-effort manner.
	var reprocessRequests []*activity.ExtractorRequest
	for {
		var reprocessRequest *activity.ExtractorRequest
		if ok := reprocessChannel.ReceiveAsync(&reprocessRequest); !ok {
			break
		}

		reprocessRequest.WithBestEffort = true
		reprocessRequests = append(reprocessRequests, reprocessRequest)
	}

	// Phase 2: reprocess any failed requests sequentially.
	// This should happen rarely (only if we over stress the cluster or the cluster itself was crashing).
	if numReprocessRequests := len(reprocessRequests); numReprocessRequests > 0 {
		metrics.Counter(backfillerReprocessedCounter).Inc(int64(numReprocessRequests))
		logger.Info("reprocessing extractors", zap.Int("count", numReprocessRequests))

		// If too many extractors have failed, there may be a bug on our end.
		// Before further increasing the threshold, please double check if the blockchain nodes are overloaded.
		if numReprocessRequests > int(maxReprocessedPerBatch) {
			return nil, xerrors.Errorf("too many extractors to reprocess: %v", numReprocessRequests)
		}

		for _, reprocessRequest := range reprocessRequests {
			reprocessResponse, err := w.extractor.Execute(ctx, reprocessRequest)
			if err != nil {
				return nil, xerrors.Errorf("failed to reprocess extractor (request=%v): %w", reprocessRequest, err)
			}

			outputChannel.Send(ctx, reprocessResponse)
			logger.Info(
				"reprocessed extractor",
				zap.Reflect("request", reprocessRequest),
				zap.Reflect("response", reprocessResponse),
			)
		}
	}

	for i := 0; i < batchSize; {
		var response *activity.ExtractorResponse
		if ok := outputChannel.Receive(ctx, &response); !ok {
			return nil, xerrors.Errorf("failed to receive from output channel: %v", i)
		}

		if len(response.Metadatas) == 0 {
			return nil, xerrors.Errorf("received invalid extractor response: %+v", response)
		}

		for _, metadata := range response.Metadatas {
			batchMetadata[i] = metadata
			i++
		}
	}

	// Phase 3: run loader to commit the results.
	// Sort the blocks and validate if the chain is continuous.
	// Note that lastBlock is the end of the previous batch or checkpoint.
	sort.Slice(batchMetadata, func(i, j int) bool {
		return batchMetadata[i].Height < batchMetadata[j].Height
	})

	if err := parser.ValidateChain(batchMetadata, lastBlock); err != nil {
		return nil, xerrors.Errorf("failed to validate chain: %w", err)
	}

	if _, err := w.loader.Execute(ctx, &activity.LoaderRequest{
		UpdateWatermark: updateWatermark,
		Metadata:        batchMetadata,
		LastBlock:       lastBlock,
	}); err != nil {
		return nil, xerrors.Errorf("failed to execute loader: %w", err)
	}

	return batchMetadata, nil
}

func (r *BackfillerRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}
