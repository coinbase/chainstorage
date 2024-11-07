package activity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	workflowerrors "github.com/coinbase/chainstorage/internal/workflow/activity/errors"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// Based on https://etherscan.io/uncles, reorg distance is usually less than 4.
// We should keep an eye on the "reorg_distance" gauge and adjust these magic numbers as needed.
const (
	parallelismOfBestEffort int = 2

	initialStep   uint64 = 3   // initial step size to search backwards for the fork block in the local copy of block chain
	maxSearchStep uint64 = 100 // maximum step size to search backwards for the fork block in the local copy of block chain
	factor               = 2.0 // exponential factor to increase the step size to search backwards for the fork block

	consensusValidationCheckMetric = "consensus_validation_check"

	consensusValidationBlockHashMismatch = "block_hash_mismatch"
	consensusValidationNodeError         = "consensus_node_error"
	consensusValidationDDBMetadataError  = "ddb_metadata_error"
	consensusValidationInvalidData       = "invalid_data"
)

type (
	Syncer struct {
		baseActivity
		heartbeater               Heartbeater
		metaStorage               metastorage.MetaStorage
		blobStorage               blobstorage.BlobStorage
		masterBlockchainClient    client.Client // Used as the source-of-truth for resolving the canonical chain. It should be connected to a single-node cluster.
		slaveBlockchainClient     client.Client // Used to ingest data concurrently. It may be connected to multiple clusters for load balancing purposes.
		consensusBlockchainClient client.Client // Used to run consensus validation. It may be connected to multiple clusters for load balancing purposes.
		failoverManager           endpoints.FailoverManager
		blockStartHeight          uint64
		metrics                   *syncerMetrics
	}

	SyncerParams struct {
		fx.In
		fxparams.Params
		Runtime          cadence.Runtime
		Heartbeater      Heartbeater
		MetaStorage      metastorage.MetaStorage
		BlobStorage      blobstorage.BlobStorage
		BlockchainClient client.ClientParams
		FailoverManager  endpoints.FailoverManager
	}

	SyncerRequest struct {
		Tag                          uint32 `validate:"required"`
		MinStartHeight               uint64
		MaxBlocksToSync              uint64 `validate:"required"`
		Parallelism                  int    `validate:"required"`
		DataCompression              api.Compression
		Failover                     bool   // Failover will switch master/slave to failover clusters.
		ConsensusFailover            bool   // ConsensusFailover will switch consensus client to failover clusters.
		FastSync                     bool   // FastSync should be enabled if the chain has no reorg.
		IrreversibleDistance         uint64 // A reorg will be rejected if it exceeds this distance.
		NumBlocksToSkip              uint64 // Skip the last N blocks to work around syncing delays in the nodes.
		TransactionsWriteParallelism int    // Parallelism set to the concurrency of adding/updating transactions table, it is also the feature flag to enable transaction processing if value > 0.
		ConsensusValidation          bool   // ConsensusValidation is a feature flag to enable consensus layer validation.
		ConsensusValidationMuted     bool   // ConsensusValidationMuted is a feature flag to mute consensus layer validation failures.
	}

	SyncerResponse struct {
		LatestSyncedHeight uint64
		SyncGap            uint64
		TimeSinceLastBlock time.Duration
	}

	reorgResult struct {
		forkBlock               *api.BlockMetadata
		forkHeight              uint64
		canonicalChainTipHeight uint64
	}

	syncerMetrics struct {
		scope                               tally.Scope
		instrumentHandleReorg               instrument.InstrumentWithResult[*reorgResult]
		instrumentSearchForkBlock           instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentSafeGetBlock              instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentAddTransactionsInParallel instrument.Instrument
		instrumentConsensusValidation       instrument.Instrument
		reorgCounter                        tally.Counter
		reorgDistanceGauge                  tally.Gauge
		reorgDistanceExceedThresholdCounter tally.Counter
		reorgDistanceWithinThresholdCounter tally.Counter
		safeGetBlockFallbackCounter         tally.Counter
		reprocessCounter                    tally.Counter
		rawBlockSizeGauge                   tally.Gauge
	}
)

func NewSyncer(params SyncerParams) *Syncer {
	a := &Syncer{
		baseActivity:              newBaseActivity(ActivitySyncer, params.Runtime),
		heartbeater:               params.Heartbeater,
		metaStorage:               params.MetaStorage,
		blobStorage:               params.BlobStorage,
		masterBlockchainClient:    params.BlockchainClient.Master,
		slaveBlockchainClient:     params.BlockchainClient.Slave,
		consensusBlockchainClient: params.BlockchainClient.Consensus,
		failoverManager:           params.FailoverManager,
		blockStartHeight:          params.Config.Chain.BlockStartHeight,
		metrics:                   newSyncerMetrics(params.Metrics),
	}
	a.register(a.execute)
	return a
}

func newSyncerMetrics(scope tally.Scope) *syncerMetrics {
	scope = scope.SubScope(ActivitySyncer)
	return &syncerMetrics{
		scope:                               scope,
		instrumentHandleReorg:               instrument.NewWithResult[*reorgResult](scope, "handle_reorg"),
		instrumentSearchForkBlock:           instrument.NewWithResult[*api.BlockMetadata](scope, "search_fork_block"),
		instrumentSafeGetBlock:              instrument.NewWithResult[*api.BlockMetadata](scope, "safe_get_block"),
		instrumentAddTransactionsInParallel: instrument.New(scope, "add_transactions_in_parallel"),
		instrumentConsensusValidation:       instrument.New(scope, "consensus_validation"),
		reorgCounter:                        scope.Counter("reorg"),
		reorgDistanceGauge:                  scope.Gauge("reorg_distance"),
		safeGetBlockFallbackCounter:         scope.Counter("safe_get_block_fallback"),
		reprocessCounter:                    scope.Counter("reprocess"),
		reorgDistanceWithinThresholdCounter: scope.Tagged(successTags).Counter(reorgDistanceCheckMetric),
		reorgDistanceExceedThresholdCounter: scope.Tagged(errTags).Counter(reorgDistanceCheckMetric),
		rawBlockSizeGauge:                   scope.Gauge("raw_block_size"),
	}
}

func (a *Syncer) Execute(ctx workflow.Context, request *SyncerRequest) (*SyncerResponse, error) {
	var response SyncerResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Syncer) execute(ctx context.Context, request *SyncerRequest) (*SyncerResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

	if request.Failover {
		failoverCtx, err := a.failoverManager.WithFailoverContext(ctx, endpoints.MasterSlaveClusters)
		if err != nil {
			return nil, fmt.Errorf("failed to create failover context for master and slave client: %w", err)
		}
		ctx = failoverCtx
	}

	if request.ConsensusFailover {
		failoverCtx, err := a.failoverManager.WithFailoverContext(ctx, endpoints.ConsensusCluster)
		if err != nil {
			return nil, fmt.Errorf("failed to create failover context for consensus client: %w", err)
		}
		ctx = failoverCtx
	}

	result, err := a.handleReorg(ctx, logger, request.Tag, request.FastSync, request.IrreversibleDistance, request.NumBlocksToSkip)
	if err != nil {
		return nil, fmt.Errorf("failed to handle reorg: %w", err)
	}
	if result.forkHeight >= result.canonicalChainTipHeight {
		// The master block node is behind the local block store.
		return &SyncerResponse{LatestSyncedHeight: result.forkHeight, SyncGap: 0}, nil
	}

	myLatest := result.forkHeight
	theirLatest := result.canonicalChainTipHeight
	forkBlock := result.forkBlock
	logger.Info(
		"syncing data",
		zap.Uint64("my_latest", myLatest),
		zap.Uint64("their_latest", theirLatest),
	)

	start := myLatest + 1
	if request.MinStartHeight > start {
		start = request.MinStartHeight
	}
	end := theirLatest + 1
	if end > start+request.MaxBlocksToSync {
		end = start + request.MaxBlocksToSync
	}

	var inMetadatas []*api.BlockMetadata
	if request.FastSync {
		// In fast-sync mode, we assume there is no reorg;
		// therefore, we can simply fetch the blocks by their heights.
		inMetadatas = make([]*api.BlockMetadata, 0, end-start)
		for height := start; height < end; height++ {
			inMetadatas = append(inMetadatas, &api.BlockMetadata{
				Tag:    request.Tag,
				Height: height,
			})
		}
	} else {
		inMetadatas, err = a.masterBlockchainClient.BatchGetBlockMetadata(ctx, request.Tag, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata for blocks from %d to %d: %w", start, end-1, err)
		}

		// Check if the first block to be synced is a valid descendant of the local fork block.
		// If the condition is not met, it is likely that master node experienced a block chain reorg right after
		// we find the fork block. In this case, we skip the current syncer activity and retry in subsequent iterations.
		if start == myLatest+1 {
			var nextMasterBlock *api.BlockMetadata
			for _, block := range inMetadatas {
				if !block.Skipped {
					nextMasterBlock = block
					break
				}
			}

			parentHash := nextMasterBlock.GetParentHash()
			if parentHash != "" && parentHash != forkBlock.GetHash() {
				logger.Warn(
					"skip syncing data in current activity to avoid race condition in master node",
					zap.Reflect("local_fork_block", forkBlock),
					zap.Reflect("next_master_block", nextMasterBlock))
				return &SyncerResponse{LatestSyncedHeight: result.forkHeight, SyncGap: theirLatest - start + 1}, nil
			}
		}
	}

	logger.Info(
		"syncing data in range of heights",
		zap.Uint64("start", start),
		zap.Uint64("end", end),
		zap.Bool("fast_sync", request.FastSync),
	)

	outMetadatas, err := a.getBlocks(ctx, logger, inMetadatas, request.Parallelism, request.DataCompression, request.FastSync, request.TransactionsWriteParallelism)
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks from %d to %d: %w", start, end, err)
	}

	var timeSinceLastBlock time.Duration
	latestSyncedHeight := myLatest
	outMetadatas = a.sanitizeBlocks(outMetadatas)
	if len(outMetadatas) > 0 {
		lastBlock := outMetadatas[len(outMetadatas)-1]
		latestSyncedHeight = lastBlock.Height

		if request.ConsensusValidation {
			if err := a.validateConsensus(ctx, logger, request.Tag, latestSyncedHeight, myLatest, request.IrreversibleDistance, outMetadatas); err != nil {
				logger.Warn("consensus validation failed",
					zap.Uint64("latest_synced_height", latestSyncedHeight),
					zap.Uint64("my_latest", myLatest),
					zap.Uint64("irreversible_distance", request.IrreversibleDistance),
					zap.Error(err),
				)
				if !request.ConsensusValidationMuted {
					return nil, fmt.Errorf("failed to validate consensus when latestSyncedHeight=%v, irreversibleDistance=%v: %w", latestSyncedHeight, request.IrreversibleDistance, err)
				}
			}
		}

		if err := a.metaStorage.PersistBlockMetas(ctx, true, outMetadatas, forkBlock); err != nil {
			return nil, fmt.Errorf("failed to upload metadata for blocks from %d to %d: %w", start, end, err)
		}

		// The node may produce blocks in small batches.
		// We should use the last block time as the checkpoint time.
		timeSinceLastBlock = utils.SinceTimestamp(lastBlock.GetTimestamp())
		logger.Info(
			"syncer finished",
			zap.Int("number_of_blocks", len(outMetadatas)),
			zap.Uint64("latest_synced_height", latestSyncedHeight),
			zap.Duration("time_since_last_block", timeSinceLastBlock),
		)
	}

	return &SyncerResponse{
		LatestSyncedHeight: latestSyncedHeight,
		SyncGap:            theirLatest - start + 1,
		TimeSinceLastBlock: timeSinceLastBlock,
	}, nil
}

func (a *Syncer) validateConsensus(ctx context.Context, logger *zap.Logger, tag uint32, latestSyncedHeight uint64, myLatest uint64, irreversibleDistance uint64, outMetadatas []*api.BlockMetadata) error {
	var failureReason string
	if err := a.metrics.instrumentConsensusValidation.Instrument(ctx, func(ctx context.Context) error {
		finalizedHeight := int64(latestSyncedHeight) - int64(irreversibleDistance) + 1
		blockHeight := finalizedHeight
		for blockHeight >= int64(a.blockStartHeight) {
			logger.Info("validating finalized block hash",
				zap.Int64("height", blockHeight),
				zap.Int64("original_height", finalizedHeight),
				zap.Uint64("latest_synced_height", latestSyncedHeight),
				zap.Uint64("my_latest", myLatest),
				zap.Uint64("irreversible_distance", irreversibleDistance),
			)

			var actualFinalizedBlock *api.BlockMetadata
			var err error
			if blockHeight > int64(myLatest) {
				// This is for the case where the latestFinalizedBlock has not been persisted to metaStorage
				// Local:  [10] <- [11]
				// Master: [10] <- [11'] <- [12'] <- [13'] <- [14']
				// For above example, myLatest = 10, outMetadatas = [11', 14']
				// when irreversibleDistance = 3, latestFinalizedBlock = 14 - 3 + 1 = 12, which has not been written to metaStorage
				metadataIndex := blockHeight - int64(myLatest) - 1
				if metadataIndex >= int64(len(outMetadatas)) {
					failureReason = consensusValidationInvalidData
					return fmt.Errorf("unexpected metadataIndex=%v, outMetadatasSize=%v", metadataIndex, len(outMetadatas))
				}
				actualFinalizedBlock = outMetadatas[metadataIndex]
			} else {
				actualFinalizedBlock, err = a.metaStorage.GetBlockByHeight(ctx, tag, uint64(blockHeight))
				if err != nil {
					failureReason = consensusValidationDDBMetadataError
					return fmt.Errorf("failed to fetch metadata for block %v: %w", blockHeight, err)
				}
			}

			expectedFinalizedBlocks, err := a.consensusBlockchainClient.BatchGetBlockMetadata(ctx, tag, uint64(blockHeight), uint64(blockHeight)+1)
			if err != nil {
				failureReason = consensusValidationNodeError
				return fmt.Errorf("failed to fetch metadata from consensus client for block %v: %w", blockHeight, err)
			}

			if len(expectedFinalizedBlocks) != 1 {
				failureReason = consensusValidationNodeError
				return fmt.Errorf("unexpected number of block metadata: %v", len(expectedFinalizedBlocks))
			}

			if actualFinalizedBlock.Skipped && expectedFinalizedBlocks[0].Skipped {
				blockHeight -= 1
			} else if actualFinalizedBlock.GetHash() != expectedFinalizedBlocks[0].GetHash() {
				failureReason = consensusValidationBlockHashMismatch
				return fmt.Errorf("detected mismatch block hash for block=%v, expectedBlock={%+v}, actualBlock={%+v}", blockHeight, expectedFinalizedBlocks[0], actualFinalizedBlock)
			} else {
				return nil
			}
		}

		logger.Info("no finalized block",
			zap.Int64("height", blockHeight),
			zap.Int64("original_height", finalizedHeight),
			zap.Uint64("latest_synced_height", latestSyncedHeight),
			zap.Uint64("my_latest", myLatest),
			zap.Uint64("irreversible_distance", irreversibleDistance),
		)
		return nil
	}); err != nil {
		incConsensusValidationCheckMetric(a.metrics.scope, failureReason)

		errType := parseConsensusErrorType(failureReason)
		return temporal.NewApplicationError(fmt.Errorf("consensus validation failed: %w", err).Error(), errType)
	}

	incConsensusValidationCheckMetric(a.metrics.scope, resultTypeSuccess)
	return nil
}

// handleReorg checks if there was a chain reorg and rewinds the watermark to the fork.
// This function is a no-op if there was no reorg.
func (a *Syncer) handleReorg(ctx context.Context, logger *zap.Logger, tag uint32, fastSync bool, irreversibleDistance uint64, numBlocksToSkip uint64) (*reorgResult, error) {
	return a.metrics.instrumentHandleReorg.Instrument(ctx, func(ctx context.Context) (*reorgResult, error) {
		theirLatest, err := a.masterBlockchainClient.GetLatestHeight(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get their latest block: %w", err)
		}
		if theirLatest >= numBlocksToSkip {
			theirLatest -= numBlocksToSkip
		}

		myLatestBlock, err := a.metaStorage.GetLatestBlock(ctx, tag)
		if err != nil {
			return nil, fmt.Errorf("failed to get my latest block: %w", err)
		}

		myLatest := myLatestBlock.Height

		logger.Info("handle chain reorg",
			zap.Uint64("block_node_tip_height", theirLatest),
			zap.Uint64("local_watermark_height", myLatest),
			zap.Uint64("num_blocks_to_skip", numBlocksToSkip),
		)

		if fastSync || myLatest >= theirLatest {
			a.metrics.reorgDistanceWithinThresholdCounter.Inc(1)

			result := &reorgResult{
				forkBlock:               myLatestBlock,
				forkHeight:              myLatest,
				canonicalChainTipHeight: theirLatest,
			}
			return result, nil
		}

		step := initialStep
		// search range is a half open interval [from, to)
		to := myLatest + 1
		from := to - step
		if from < 0 {
			from = 0
		}
		var forkBlock *api.BlockMetadata
		for from < to {
			logger.Info("searching fork block in range",
				zap.Uint64("from", from),
				zap.Uint64("to", to-1))
			forkBlock, err = a.searchForkBlock(ctx, tag, from, to)
			if err != nil {
				return nil, fmt.Errorf("failed to search for fork block from height %d to %d due to: %w", from, to-1, err)
			}
			if forkBlock != nil {
				reorgDistance := myLatest - forkBlock.GetHeight()
				a.metrics.reorgDistanceGauge.Update(float64(reorgDistance))
				logger.Info("found fork block",
					zap.Uint64("local_watermark_height", myLatest),
					zap.Uint64("fork_block_height", forkBlock.GetHeight()))

				// Check irreversible distance explanation in https://docs.google.com/document/d/18DhoFKh2lt7uJIg57XfiSHAFpnr-f0MVswT8wsZ6qgw/edit?usp=sharing
				// Note: when irreversibleDistance = 0, it means NO reorg validation in poller.
				if irreversibleDistance > 0 && reorgDistance >= irreversibleDistance {
					logger.Error("reorg distance should be less than irreversible distance threshold",
						zap.Uint64("reorg_distance", reorgDistance),
						zap.Uint64("irreversible_distance", irreversibleDistance),
						zap.Reflect("my_latest_block", myLatestBlock),
						zap.Reflect("fork_block", forkBlock),
					)
					a.metrics.reorgDistanceExceedThresholdCounter.Inc(1)
				} else {
					a.metrics.reorgDistanceWithinThresholdCounter.Inc(1)
				}

				if reorgDistance != 0 {
					a.metrics.reorgCounter.Inc(1)
					logger.Info("detected reorg",
						zap.Reflect("my_latest_block", myLatestBlock),
						zap.Reflect("fork_block", forkBlock),
						zap.Uint64("reorg_distance", reorgDistance))

					err = a.metaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{forkBlock}, nil)
					if err != nil {
						return nil, fmt.Errorf("failed to set watermark block for height %d due to: %w", forkBlock.GetHeight(), err)
					}
				}
				break
			}
			step = uint64(float64(step) * factor)
			if step > maxSearchStep {
				step = maxSearchStep
			}
			to = from
			from = to - step
			if from < 0 {
				from = 0
			}
		}

		result := &reorgResult{
			forkBlock:               forkBlock,
			forkHeight:              forkBlock.Height,
			canonicalChainTipHeight: theirLatest,
		}
		return result, nil
	})
}

func (a *Syncer) searchForkBlock(ctx context.Context, tag uint32, from, to uint64) (*api.BlockMetadata, error) {
	// Note that result is nil when the fork is below the search range,
	// i.e. hash of the "from" block doesn't match between "my state" and "their state".
	return a.metrics.instrumentSearchForkBlock.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		group, groupCtx := syncgroup.New(ctx)

		var theirBlockMetadatas []*api.BlockMetadata
		group.Go(func() error {
			out, err := a.masterBlockchainClient.BatchGetBlockMetadata(groupCtx, tag, from, to)
			if err != nil {
				return fmt.Errorf("failed to query block range from blockchain client (from=%v, to=%v): %w", from, to, err)
			}

			theirBlockMetadatas = out
			return nil
		})

		var myBlockMetadatas []*api.BlockMetadata
		group.Go(func() error {
			out, err := a.metaStorage.GetBlocksByHeightRange(groupCtx, tag, from, to)
			if err != nil {
				return fmt.Errorf("failed to query block range from meta storage (from=%v, to=%v): %w", from, to, err)
			}

			myBlockMetadatas = out
			return nil
		})

		if err := group.Wait(); err != nil {
			return nil, fmt.Errorf("failed to query block range (from=%v, to=%v): %w", from, to, err)
		}

		var result *api.BlockMetadata
		for i := 0; i < len(myBlockMetadatas); i++ {
			myBlock := myBlockMetadatas[i]
			theirBlock := theirBlockMetadatas[i]

			if myBlock.Skipped && theirBlock.Skipped {
				continue
			}

			if myBlock.GetHash() != theirBlock.GetHash() {
				break
			}

			result = myBlock
		}

		a.heartbeater.RecordHeartbeat(ctx)
		return result, nil
	})
}

func (a *Syncer) getBlocks(
	ctx context.Context,
	logger *zap.Logger,
	blockMetadatas []*api.BlockMetadata,
	parallelism int,
	dataCompression api.Compression,
	fastSync bool,
	transactionIndexingParallelism int,
) ([]*api.BlockMetadata, error) {
	batchSize := len(blockMetadatas)
	outMetadatas := make([]*api.BlockMetadata, 0, batchSize)
	processed, reprocess, err := a.getBlocksInParallel(ctx, logger, blockMetadatas, parallelism, false, dataCompression, fastSync, transactionIndexingParallelism)
	if err != nil {
		return nil, err
	}
	outMetadatas = append(outMetadatas, processed...)
	if len(reprocess) > 0 {
		// Enable best-effort tracing when reprocessing blocks that are failed in the first time.
		// This option is only applied to the reprocessing phase so that chances of getting partial
		// block data is minimized.
		a.metrics.reprocessCounter.Inc(int64(len(reprocess)))
		logger.Warn(
			"reprocessing blocks",
			zap.Int("numBlocks", len(reprocess)),
			zap.Reflect("blocks", a.shortenBlocks(reprocess)),
		)
		reprocessed, failed, err := a.getBlocksInParallel(ctx, logger, reprocess, parallelismOfBestEffort, true, dataCompression, fastSync, transactionIndexingParallelism)
		if err != nil {
			return nil, err
		}
		if len(failed) > 0 {
			return nil, fmt.Errorf("failed to get %d blocks after attempting twice: %+v", len(failed), failed)
		}
		outMetadatas = append(outMetadatas, reprocessed...)
	}

	// Sort by block height.
	sort.Slice(outMetadatas, func(i, j int) bool {
		return outMetadatas[i].GetHeight() < outMetadatas[j].GetHeight()
	})

	return outMetadatas, nil
}

func (a *Syncer) getBlocksInParallel(
	ctx context.Context,
	logger *zap.Logger,
	blockMetadatas []*api.BlockMetadata,
	parallelism int,
	withBestEffort bool,
	dataCompression api.Compression,
	fastSync bool,
	transactionIndexingParallelism int,
) ([]*api.BlockMetadata, []*api.BlockMetadata, error) {
	batchSize := len(blockMetadatas)
	if parallelism > batchSize {
		parallelism = batchSize
	}
	inputChannel := make(chan *api.BlockMetadata, batchSize)
	for _, metadata := range blockMetadatas {
		inputChannel <- metadata
	}
	close(inputChannel)

	outChannel := make(chan *api.BlockMetadata, batchSize)
	defer close(outChannel)
	reprocessChannel := make(chan *api.BlockMetadata, batchSize)
	defer close(reprocessChannel)
	g, ctx := syncgroup.New(ctx)

	for i := 0; i < parallelism; i++ {
		g.Go(func() error {
			for metadata := range inputChannel {
				block, err := a.safeGetBlock(ctx, logger, metadata, withBestEffort, dataCompression, fastSync, transactionIndexingParallelism)
				if err != nil {
					logger.Warn("failed to get block",
						zap.Error(err),
						zap.Bool("best_effort", withBestEffort),
						zap.Reflect("block", metadata))
					reprocessChannel <- metadata
					outChannel <- nil
				} else {
					reprocessChannel <- nil
					outChannel <- block
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("failed to get blocks: %w", err)
	}
	processed := make([]*api.BlockMetadata, 0, batchSize)
	reprocess := make([]*api.BlockMetadata, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		processedBlock := <-outChannel
		if processedBlock != nil {
			processed = append(processed, processedBlock)
		}
		failed := <-reprocessChannel
		if failed != nil {
			reprocess = append(reprocess, failed)
		}
	}
	return processed, reprocess, nil
}

func (a *Syncer) safeGetBlock(
	ctx context.Context,
	logger *zap.Logger,
	inBlock *api.BlockMetadata,
	withBestEffort bool,
	dataCompression api.Compression,
	fastSync bool,
	transactionIndexingParallelism int,
) (*api.BlockMetadata, error) {
	return a.metrics.instrumentSafeGetBlock.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		var outBlock *api.Block
		var err error
		if withBestEffort {
			// Use master cluster when enabling bestEffort and getting transaction trace
			outBlock, err = a.getBlockFromClient(ctx, inBlock, fastSync, a.masterBlockchainClient, client.WithBestEffort())
			if err != nil {
				return nil, fmt.Errorf("failed to get block from master (height=%v, hash=%v): %w", inBlock.Height, inBlock.Hash, err)
			}
		} else {
			outBlock, err = a.getBlockFromClient(ctx, inBlock, fastSync, a.slaveBlockchainClient)
			if err != nil {
				if !errors.Is(err, client.ErrBlockNotFound) {
					return nil, fmt.Errorf("failed to get block from slave (height=%v, hash=%v): %w", inBlock.Height, inBlock.Hash, err)
				}

				// Fall back to master if the block cannot be found from slave.
				a.heartbeater.RecordHeartbeat(ctx)
				a.metrics.safeGetBlockFallbackCounter.Inc(1)
				logger.Warn(
					"falling back to master",
					zap.Error(err),
					zap.Uint64("height", inBlock.Height),
					zap.String("hash", inBlock.Hash),
				)
				outBlock, err = a.getBlockFromClient(ctx, inBlock, fastSync, a.masterBlockchainClient)
				if err != nil {
					return nil, fmt.Errorf("failed to get block from master (height=%v, hash=%v): %w", inBlock.Height, inBlock.Hash, err)
				}
			}
		}

		rawBlockSize := proto.Size(outBlock)
		a.metrics.rawBlockSizeGauge.Update(float64(rawBlockSize))

		// TODO: transactionIndexingParallelism is used for testing, it enables when parallelism set a value > 0 in syncer.request
		//   after testing finishes, we need to:
		//   1. Hardcode the parallelism number as a fixed value
		//   2. Enable transaction indexing with a feature flag instead
		if transactionIndexingParallelism > 0 {
			group, ctx := syncgroup.New(ctx)
			group.Go(func() error {
				return a.uploadBlockToBlobStorage(ctx, outBlock, dataCompression)
			})
			group.Go(func() error {
				err := a.addTransactionsInParallel(ctx, logger, outBlock, transactionIndexingParallelism)
				if err != nil {
					return fmt.Errorf("failed to add or update transaction: %w", err)
				}

				return nil
			})

			if err := group.Wait(); err != nil {
				return nil, err
			}
		} else {
			err := a.uploadBlockToBlobStorage(ctx, outBlock, dataCompression)
			if err != nil {
				return nil, err
			}
		}

		a.heartbeater.RecordHeartbeat(ctx)
		result := outBlock.Metadata
		return result, nil
	})
}

func (a *Syncer) uploadBlockToBlobStorage(ctx context.Context, outBlock *api.Block, dataCompression api.Compression) error {
	objectKey, err := a.blobStorage.Upload(ctx, outBlock, dataCompression)
	if err != nil {
		return fmt.Errorf("failed to upload block hash %s to blob storage: %w", outBlock.Metadata.Hash, err)
	}
	outBlock.Metadata.ObjectKeyMain = objectKey

	return nil
}

func (a *Syncer) addTransactionsInParallel(
	ctx context.Context,
	logger *zap.Logger,
	block *api.Block,
	parallelism int) error {
	if block.Metadata.Skipped {
		return nil
	}
	transactionMetadata := block.GetTransactionMetadata()
	if transactionMetadata == nil {
		logger.Error("transaction metadata is nil")
		return errors.New("transaction metadata is nil")
	}
	transactionsHashList := transactionMetadata.Transactions
	if len(transactionsHashList) == 0 {
		return nil
	}

	if err := a.metrics.instrumentAddTransactionsInParallel.Instrument(ctx, func(ctx context.Context) error {
		transactions := make([]*model.Transaction, len(transactionsHashList))
		for i, txnHash := range transactionsHashList {
			transaction := &model.Transaction{
				Hash:        txnHash,
				BlockNumber: block.Metadata.Height,
				BlockHash:   block.Metadata.Hash,
				BlockTag:    block.Metadata.Tag,
			}
			transactions[i] = transaction
		}

		err := a.metaStorage.AddTransactions(ctx, transactions, parallelism)
		if err != nil {
			logger.Error("failed to add transactions", zap.Error(err))
			return fmt.Errorf("failed to add transactions: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (a *Syncer) getBlockFromClient(
	ctx context.Context,
	inBlock *api.BlockMetadata,
	fastSync bool,
	client client.Client,
	opts ...client.ClientOption,
) (*api.Block, error) {
	if fastSync {
		// In fast-sync mode, we assume there is no reorg and we can simply fetch the block by its block height.
		return client.GetBlockByHeight(ctx, inBlock.Tag, inBlock.Height, opts...)
	}

	return client.GetBlockByHash(ctx, inBlock.Tag, inBlock.Height, inBlock.Hash, opts...)
}

// sanitizeBlocks ensures the latest block is not skipped.
func (a *Syncer) sanitizeBlocks(blocks []*api.BlockMetadata) []*api.BlockMetadata {
	numTrailingSkippedBlocks := 0
	for i := len(blocks) - 1; i >= 0; i-- {
		if !blocks[i].Skipped {
			break
		}
		numTrailingSkippedBlocks += 1
	}

	if numTrailingSkippedBlocks > 0 {
		newSize := len(blocks) - numTrailingSkippedBlocks
		blocks = blocks[:newSize]
	}

	return blocks
}

func (a *Syncer) shortenBlocks(blocks []*api.BlockMetadata) []*api.BlockMetadata {
	const maxBlocks = 10

	if len(blocks) <= maxBlocks {
		return blocks
	}

	return blocks[:maxBlocks]
}

func incConsensusValidationCheckMetric(metrics tally.Scope, resultType string) {
	tag := map[string]string{resultTypeTag: resultType}
	metrics.Tagged(tag).Counter(consensusValidationCheckMetric).Inc(1)
}

func parseConsensusErrorType(failureReason string) string {
	if failureReason == consensusValidationNodeError {
		return workflowerrors.ErrTypeConsensusClusterFailure
	}
	return workflowerrors.ErrTypeConsensusValidationFailure
}
