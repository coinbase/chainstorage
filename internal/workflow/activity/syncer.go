package activity

import (
	"context"
	"sort"
	"time"

	"github.com/uber-go/tally"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// Based on https://etherscan.io/uncles, reorg distance is usually less than 4.
// We should keep an eye on the "reorg_distance" gauge and adjust these magic numbers as needed.
const (
	parallelismOfBestEffort int = 2

	initialStep   uint64 = 3   // initial step size to search backwards for the fork block in the local copy of block chain
	maxSearchStep uint64 = 100 // maximum step size to search backwards for the fork block in the local copy of block chain
	factor               = 2.0 // exponential factor to increase the step size to search backwards for the fork block
)

type (
	Syncer struct {
		baseActivity
		heartbeater            Heartbeater
		metaStorage            metastorage.MetaStorage
		blobStorage            blobstorage.BlobStorage
		masterBlockchainClient client.Client // Used as the source-of-truth for resolving the canonical chain. It should be connected to a single-node cluster.
		slaveBlockchainClient  client.Client // Used to ingest data concurrently. It may be connected to multiple clusters for load balancing purposes.
		failoverManager        endpoints.FailoverManager
		metrics                *syncerMetrics
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
		Tag             uint32 `validate:"required"`
		MinStartHeight  uint64
		MaxBlocksToSync uint64 `validate:"required"`
		Parallelism     int    `validate:"required"`
		DataCompression api.Compression
		Failover        bool
		FastSync        bool // FastSync should be enabled if the chain has no reorg.
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
		instrumentHandleReorg       instrument.Call
		instrumentSearchForkBlock   instrument.Call
		instrumentSafeGetBlock      instrument.Call
		reorgCounter                tally.Counter
		reorgDistanceGauge          tally.Gauge
		safeGetBlockFallbackCounter tally.Counter
		reprocessCounter            tally.Counter
	}
)

func NewSyncer(params SyncerParams) *Syncer {
	a := &Syncer{
		baseActivity:           newBaseActivity(ActivitySyncer, params.Runtime),
		heartbeater:            params.Heartbeater,
		metaStorage:            params.MetaStorage,
		blobStorage:            params.BlobStorage,
		masterBlockchainClient: params.BlockchainClient.Master,
		slaveBlockchainClient:  params.BlockchainClient.Slave,
		failoverManager:        params.FailoverManager,
		metrics:                newSyncerMetrics(params.Metrics),
	}
	a.register(a.execute)
	return a
}

func newSyncerMetrics(scope tally.Scope) *syncerMetrics {
	scope = scope.SubScope(ActivitySyncer)
	return &syncerMetrics{
		instrumentHandleReorg:       instrument.NewCall(scope, "handle_reorg"),
		instrumentSearchForkBlock:   instrument.NewCall(scope, "search_fork_block"),
		instrumentSafeGetBlock:      instrument.NewCall(scope, "safe_get_block"),
		reorgCounter:                scope.Counter("reorg"),
		reorgDistanceGauge:          scope.Gauge("reorg_distance"),
		safeGetBlockFallbackCounter: scope.Counter("safe_get_block_fallback"),
		reprocessCounter:            scope.Counter("reprocess"),
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
		failoverCtx, err := a.failoverManager.WithFailoverContext(ctx)
		if err != nil {
			return nil, xerrors.Errorf("failed to create failover context: %w", err)
		}
		ctx = failoverCtx
	}

	result, err := a.handleReorg(ctx, request.Tag, request.FastSync, logger)
	if err != nil {
		return nil, xerrors.Errorf("failed to handle reorg: %w", err)
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
			return nil, xerrors.Errorf("failed to get metadata for blocks from %d to %d: %w", start, end-1, err)
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

	outMetadatas, err := a.getBlocks(ctx, logger, inMetadatas, request.Parallelism, request.DataCompression, request.FastSync)
	if err != nil {
		return nil, xerrors.Errorf("failed to get blocks from %d to %d: %w", start, end, err)
	}

	var timeSinceLastBlock time.Duration
	latestSyncedHeight := myLatest
	outMetadatas = a.sanitizeBlocks(outMetadatas)
	if len(outMetadatas) > 0 {
		if err := a.metaStorage.PersistBlockMetas(ctx, true, outMetadatas, forkBlock); err != nil {
			return nil, xerrors.Errorf("failed to upload metadata for blocks from %d to %d: %w", start, end, err)
		}

		timeSinceLastBlock = utils.SinceTimestamp(outMetadatas[0].GetTimestamp())
		latestSyncedHeight = outMetadatas[len(outMetadatas)-1].Height
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

// handleReorg checks if there was a chain reorg and rewinds the watermark to the fork.
// This function is a no-op if there was no reorg.
func (a *Syncer) handleReorg(ctx context.Context, tag uint32, fastSync bool, logger *zap.Logger) (*reorgResult, error) {
	var result *reorgResult
	if err := a.metrics.instrumentHandleReorg.Instrument(ctx, func(ctx context.Context) error {
		theirLatest, err := a.masterBlockchainClient.GetLatestHeight(ctx)
		if err != nil {
			return xerrors.Errorf("failed to get their latest block: %w", err)
		}
		myLatestBlock, err := a.metaStorage.GetLatestBlock(ctx, tag)
		if err != nil {
			return xerrors.Errorf("failed to get my latest block: %w", err)
		}

		myLatest := myLatestBlock.Height

		logger.Info("handle chain reorg",
			zap.Uint64("block_node_tip_height", theirLatest),
			zap.Uint64("local_watermark_height", myLatest))

		if fastSync || myLatest >= theirLatest {
			result = &reorgResult{
				forkBlock:               myLatestBlock,
				forkHeight:              myLatest,
				canonicalChainTipHeight: theirLatest,
			}
			return nil
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
				return xerrors.Errorf("failed to search for fork block from height %d to %d due to: %w", from, to-1, err)
			}
			if forkBlock != nil {
				reorgDistance := myLatest - forkBlock.GetHeight()
				a.metrics.reorgDistanceGauge.Update(float64(reorgDistance))
				logger.Info("found fork block",
					zap.Uint64("local_watermark_height", myLatest),
					zap.Uint64("fork_block_height", forkBlock.GetHeight()))
				if reorgDistance != 0 {
					a.metrics.reorgCounter.Inc(1)
					logger.Info("detected reorg",
						zap.Reflect("my_latest_block", myLatestBlock),
						zap.Reflect("fork_block", forkBlock),
						zap.Uint64("reorg_distance", reorgDistance))
					err = a.metaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{forkBlock}, nil)
					if err != nil {
						return xerrors.Errorf("failed to set watermark block for height %d due to: %w", forkBlock.GetHeight(), err)
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

		result = &reorgResult{
			forkBlock:               forkBlock,
			forkHeight:              forkBlock.Height,
			canonicalChainTipHeight: theirLatest,
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (a *Syncer) searchForkBlock(ctx context.Context, tag uint32, from, to uint64) (*api.BlockMetadata, error) {
	// Note that result is nil when the fork is below the search range,
	// i.e. hash of the "from" block doesn't match between "my state" and "their state".
	var result *api.BlockMetadata
	if err := a.metrics.instrumentSearchForkBlock.Instrument(ctx, func(ctx context.Context) error {
		group, groupCtx := syncgroup.New(ctx)

		var theirBlockMetadatas []*api.BlockMetadata
		group.Go(func() error {
			out, err := a.masterBlockchainClient.BatchGetBlockMetadata(groupCtx, tag, from, to)
			if err != nil {
				return xerrors.Errorf("failed to query block range from blockchain client (from=%v, to=%v): %w", from, to, err)
			}

			theirBlockMetadatas = out
			return nil
		})

		var myBlockMetadatas []*api.BlockMetadata
		group.Go(func() error {
			out, err := a.metaStorage.GetBlocksByHeightRange(groupCtx, tag, from, to)
			if err != nil {
				return xerrors.Errorf("failed to query block range from meta storage (from=%v, to=%v): %w", from, to, err)
			}

			myBlockMetadatas = out
			return nil
		})

		if err := group.Wait(); err != nil {
			return xerrors.Errorf("failed to query block range (from=%v, to=%v): %w", from, to, err)
		}

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
		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (a *Syncer) getBlocks(
	ctx context.Context,
	logger *zap.Logger,
	blockMetadatas []*api.BlockMetadata,
	parallelism int,
	dataCompression api.Compression,
	fastSync bool,
) ([]*api.BlockMetadata, error) {
	batchSize := len(blockMetadatas)
	outMetadatas := make([]*api.BlockMetadata, 0, batchSize)
	processed, reprocess, err := a.getBlocksInParallel(ctx, logger, blockMetadatas, parallelism, false, dataCompression, fastSync)
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
		reprocessed, failed, err := a.getBlocksInParallel(ctx, logger, reprocess, parallelismOfBestEffort, true, dataCompression, fastSync)
		if err != nil {
			return nil, err
		}
		if len(failed) > 0 {
			return nil, xerrors.Errorf("failed to get %d blocks after attempting twice: %+v", len(failed), failed)
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
				block, err := a.safeGetBlock(ctx, logger, metadata, withBestEffort, dataCompression, fastSync)
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
		return nil, nil, xerrors.Errorf("failed to get blocks: %w", err)
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
) (*api.BlockMetadata, error) {
	var result *api.BlockMetadata
	if err := a.metrics.instrumentSafeGetBlock.Instrument(ctx, func(ctx context.Context) error {
		var outBlock *api.Block
		var err error
		if withBestEffort {
			// Use master cluster when enabling bestEffort and getting transaction trace
			outBlock, err = a.getBlockFromClient(ctx, inBlock, fastSync, a.masterBlockchainClient, client.WithBestEffort())
			if err != nil {
				return xerrors.Errorf("failed to get block from master (height=%v, hash=%v): %w", inBlock.Height, inBlock.Hash, err)
			}
		} else {
			outBlock, err = a.getBlockFromClient(ctx, inBlock, fastSync, a.slaveBlockchainClient)
			if err != nil {
				if !xerrors.Is(err, client.ErrBlockNotFound) {
					return xerrors.Errorf("failed to get block from slave (height=%v, hash=%v): %w", inBlock.Height, inBlock.Hash, err)
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
					return xerrors.Errorf("failed to get block from master (height=%v, hash=%v): %w", inBlock.Height, inBlock.Hash, err)
				}
			}
		}
		objectKey, err := a.blobStorage.Upload(ctx, outBlock, dataCompression)
		if err != nil {
			return xerrors.Errorf("failed to upload block hash %s to blob storage: %w", inBlock.Hash, err)
		}
		outBlock.Metadata.ObjectKeyMain = objectKey

		a.heartbeater.RecordHeartbeat(ctx)
		result = outBlock.Metadata
		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
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
