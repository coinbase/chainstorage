package activity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	dynamodb_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	postgres_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Migrator struct {
		baseActivity
		config       *config.Config
		awsConfig    aws.Config
		dynamoClient *dynamodb.Client
		blockTable   string
		metrics      tally.Scope
	}

	BlockToMigrate struct {
		Height     uint64
		Hash       string
		ParentHash string
		EventSeq   int64 // Event sequence for ordering
		Skipped    bool  // Whether this is a skipped block
	}

	GetLatestBlockHeightActivity struct {
		baseActivity
		config       *config.Config
		awsConfig    aws.Config
		dynamoClient *dynamodb.Client
		blockTable   string
		metrics      tally.Scope
	}

	GetLatestBlockFromPostgresActivity struct {
		baseActivity
		config  *config.Config
		metrics tally.Scope
	}

	GetLatestEventFromPostgresActivity struct {
		baseActivity
		config  *config.Config
		metrics tally.Scope
	}

	GetMaxEventIdActivity struct {
		baseActivity
		config    *config.Config
		awsConfig aws.Config
		metrics   tally.Scope
	}

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime   cadence.Runtime
		AWSConfig aws.Config
	}

	MigratorRequest struct {
		StartEventSequence int64 // Start event sequence (no more StartHeight!)
		EndEventSequence   int64 // End event sequence (exclusive)
		EventTag           uint32
		Tag                uint32
		Parallelism        int // Number of concurrent workers
	}

	MigratorResponse struct {
		BlocksMigrated int
		EventsMigrated int
		Success        bool
		Message        string
	}

	MigrationData struct {
		SourceStorage metastorage.MetaStorage
		DestStorage   metastorage.MetaStorage
		Config        *config.Config
		DynamoClient  *dynamodb.Client
		BlockTable    string
	}
)

func NewMigrator(params MigratorParams) *Migrator {
	a := &Migrator{
		baseActivity: newBaseActivity(ActivityMigrator, params.Runtime),
		config:       params.Config,
		awsConfig:    params.AWSConfig,
		dynamoClient: dynamodb.NewFromConfig(params.AWSConfig),
		blockTable:   params.Config.AWS.DynamoDB.BlockTable,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetLatestBlockHeightActivity(params MigratorParams) *GetLatestBlockHeightActivity {
	a := &GetLatestBlockHeightActivity{
		baseActivity: newBaseActivity(ActivityGetLatestBlockHeight, params.Runtime),
		config:       params.Config,
		awsConfig:    params.AWSConfig,
		dynamoClient: dynamodb.NewFromConfig(params.AWSConfig),
		blockTable:   params.Config.AWS.DynamoDB.BlockTable,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetLatestBlockFromPostgresActivity(params MigratorParams) *GetLatestBlockFromPostgresActivity {
	a := &GetLatestBlockFromPostgresActivity{
		baseActivity: newBaseActivity(ActivityGetLatestBlockFromPostgres, params.Runtime),
		config:       params.Config,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetLatestEventFromPostgresActivity(params MigratorParams) *GetLatestEventFromPostgresActivity {
	a := &GetLatestEventFromPostgresActivity{
		baseActivity: newBaseActivity(ActivityGetLatestEventFromPostgres, params.Runtime),
		config:       params.Config,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func NewGetMaxEventIdActivity(params MigratorParams) *GetMaxEventIdActivity {
	a := &GetMaxEventIdActivity{
		baseActivity: newBaseActivity(ActivityGetMaxEventId, params.Runtime),
		config:       params.Config,
		awsConfig:    params.AWSConfig,
		metrics:      params.Metrics,
	}
	a.register(a.execute)
	return a
}

func (a *Migrator) Execute(ctx workflow.Context, request *MigratorRequest) (*MigratorResponse, error) {
	var response MigratorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Migrator) execute(ctx context.Context, request *MigratorRequest) (*MigratorResponse, error) {
	startTime := time.Now()
	logger := a.getLogger(ctx).With(
		zap.Int64("startEventSequence", request.StartEventSequence),
		zap.Int64("endEventSequence", request.EndEventSequence),
		zap.Uint32("eventTag", request.EventTag),
		zap.Int("parallelism", request.Parallelism))

	logger.Info("Starting event-driven migration")

	// Validate event sequence range
	if request.EndEventSequence < request.StartEventSequence {
		return nil, xerrors.Errorf("invalid request: EndEventSequence (%d) cannot be less than StartEventSequence (%d)",
			request.EndEventSequence, request.StartEventSequence)
	}

	// Add heartbeat mechanism - send heartbeat every 10 seconds to avoid timeout
	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	go func() {
		for range heartbeatTicker.C {
			select {
			case <-ctx.Done():
				return
			default:
				activity.RecordHeartbeat(ctx, fmt.Sprintf("Processing events [%d, %d), elapsed: %v",
					request.StartEventSequence, request.EndEventSequence, time.Since(startTime)))
			}
		}
	}()

	// Create storage instances
	migrationData, err := a.createStorageInstances(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to create storage instances: %w", err)
	}

	// Step 1: Fetch events by sequence (with parallelism)
	events, err := a.fetchEventsBySequence(ctx, logger, migrationData, request)
	if err != nil {
		return nil, xerrors.Errorf("failed to fetch events: %w", err)
	}

	if len(events) == 0 {
		logger.Info("No events found in sequence range")
		return &MigratorResponse{
			Success: true,
			Message: "No events to migrate",
		}, nil
	}

	// Step 2: Split events into segments and migrate blocks
	blocksMigrated, err := a.migrateExtractedBlocks(ctx, logger, migrationData, request, events)
	if err != nil {
		return nil, xerrors.Errorf("failed to migrate blocks: %w", err)
	}

	// Step 3: Migrate events
	eventsMigrated := 0
	if len(events) > 0 {
		eventsMigrated, err = a.persistEvents(ctx, logger, migrationData, request, events)
		if err != nil {
			return nil, xerrors.Errorf("failed to migrate events: %w", err)
		}
	}

	duration := time.Since(startTime)
	logger.Info("Event-driven migration completed",
		zap.Int("blocksMigrated", blocksMigrated),
		zap.Int("eventsMigrated", eventsMigrated),
		zap.Duration("duration", duration))

	return &MigratorResponse{
		BlocksMigrated: blocksMigrated,
		EventsMigrated: eventsMigrated,
		Success:        true,
		Message: fmt.Sprintf("Migrated %d blocks and %d events in %v",
			blocksMigrated, eventsMigrated, duration),
	}, nil
}

func (a *Migrator) createStorageInstances(ctx context.Context) (*MigrationData, error) {
	logger := a.getLogger(ctx)

	// Create DynamoDB storage directly
	dynamoDBParams := dynamodb_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
		AWSConfig: a.awsConfig,
	}
	sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create DynamoDB storage: %w", err)
	}

	// Create PostgreSQL storage using shared connection pool
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}
	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	return &MigrationData{
		SourceStorage: sourceResult.MetaStorage,
		DestStorage:   destResult.MetaStorage,
		Config:        a.config,
		DynamoClient:  a.dynamoClient,
		BlockTable:    a.blockTable,
	}, nil
}

func (a *Migrator) fetchEventsBySequence(ctx context.Context, logger *zap.Logger,
	data *MigrationData, request *MigratorRequest) ([]*model.EventEntry, error) {

	startSeq := request.StartEventSequence
	endSeq := request.EndEventSequence
	totalSequences := endSeq - startSeq

	logger.Info("Fetching events by sequence range",
		zap.Int64("startSeq", startSeq),
		zap.Int64("endSeq", endSeq),
		zap.Int64("totalSequences", totalSequences))

	// Record heartbeat before starting the fetch
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Starting to fetch events [%d, %d)", startSeq, endSeq))

	// GetEventsByEventIdRange already handles the fetching efficiently internally
	// No need for additional batching/parallelism as DynamoDB Query operation
	// is already optimized for sequential range scans
	events, err := data.SourceStorage.GetEventsByEventIdRange(
		ctx, request.EventTag, startSeq, endSeq)

	if err != nil {
		// Events must be continuous - any missing events indicate data integrity issues
		return nil, xerrors.Errorf("failed to fetch events [%d,%d): %w",
			startSeq, endSeq, err)
	}

	// Record heartbeat after fetch completes
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetched %d events from range [%d, %d)",
		len(events), startSeq, endSeq))

	// Validate we got the expected number of events
	expectedCount := int(totalSequences)
	if len(events) != expectedCount {
		return nil, xerrors.Errorf("missing events: expected %d events but got %d for range [%d,%d)",
			expectedCount, len(events), startSeq, endSeq)
	}

	// Sort events by EventId to ensure proper ordering for gap validation
	// DynamoDB Query should return sorted results, but we sort to be defensive
	sort.Slice(events, func(i, j int) bool {
		return events[i].EventId < events[j].EventId
	})

	// Validate event IDs are continuous (no gaps)
	for i, event := range events {
		expectedEventId := startSeq + int64(i)
		if event.EventId != expectedEventId {
			return nil, xerrors.Errorf("gap in event sequence: expected event ID %d but got %d at index %d",
				expectedEventId, event.EventId, i)
		}
	}

	logger.Info("Fetched events successfully",
		zap.Int("totalEvents", len(events)))

	return events, nil
}

func (a *Migrator) migrateExtractedBlocks(ctx context.Context, logger *zap.Logger,
	data *MigrationData, request *MigratorRequest,
	events []*model.EventEntry) (int, error) {

	// Build segments directly from events
	segments := a.buildSegmentsFromEvents(logger, events)

	// If no segments were created (edge case), return
	if len(segments) == 0 {
		logger.Warn("No segments created from blocks")
		return 0, nil
	}

	totalBlocksPersistedCount := 0

	// Process each segment
	for segmentIdx, segment := range segments {
		if len(segment.Blocks) == 0 {
			continue
		}

		logger.Info("Processing block segment",
			zap.Int("segmentIndex", segmentIdx),
			zap.Int("blockCount", len(segment.Blocks)))

		// Fetch actual block data from DynamoDB for this segment (with order preservation)
		segmentBlocksWithInfo, err := a.fetchBlockData(ctx, logger, data, request, segment.Blocks)
		if err != nil {
			return totalBlocksPersistedCount, xerrors.Errorf("failed to fetch block data for segment %d: %w", segmentIdx, err)
		}

		// Convert to BlockMetadata slice
		segmentBlocks := make([]*api.BlockMetadata, len(segmentBlocksWithInfo))
		for i, blockWithInfo := range segmentBlocksWithInfo {
			segmentBlocks[i] = blockWithInfo.BlockMetadata
		}

		// Get parent block for validation if not genesis
		var lastBlock *api.BlockMetadata
		if len(segmentBlocks) > 0 && segmentBlocks[0].Height > 0 {
			parentHeight := segmentBlocks[0].Height - 1
			parentBlock, err := data.DestStorage.GetBlockByHeight(ctx, request.Tag, parentHeight)
			if err == nil {
				lastBlock = parentBlock
			} else if !errors.Is(err, storage.ErrItemNotFound) {
				logger.Warn("Could not fetch parent block",
					zap.Uint64("parentHeight", parentHeight),
					zap.Error(err))
			}
		}

		// Bulk persist the segment with validation
		err = data.DestStorage.PersistBlockMetas(ctx, false, segmentBlocks, lastBlock)
		if err != nil {
			logger.Error("Failed to persist segment",
				zap.Int("segmentIndex", segmentIdx),
				zap.Int("blockCount", len(segmentBlocks)),
				zap.Error(err))
			return totalBlocksPersistedCount, xerrors.Errorf("failed to persist segment %d: %w", segmentIdx, err)
		}

		totalBlocksPersistedCount += len(segmentBlocks)

		// Record heartbeat if in activity context
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("Migrated segment %d/%d: %d blocks (total: %d)",
				segmentIdx+1, len(segments), len(segmentBlocks), totalBlocksPersistedCount))
		}
	}

	logger.Info("All segments migrated successfully",
		zap.Int("totalSegments", len(segments)),
		zap.Int("totalBlocksPersisted", totalBlocksPersistedCount))

	return totalBlocksPersistedCount, nil
}

// BlockSegment represents a continuous segment of blocks to persist together
type BlockSegment struct {
	Blocks []BlockToMigrate
}

// buildSegmentsFromEvents creates block segments from events, starting new segments after BLOCK_REMOVED events
func (a *Migrator) buildSegmentsFromEvents(logger *zap.Logger,
	events []*model.EventEntry) []*BlockSegment {

	var segments []*BlockSegment
	var currentSegment *BlockSegment = nil

	// Process events in order (already sorted by EventSeq)
	for _, event := range events {
		switch event.EventType {
		case api.BlockchainEvent_BLOCK_REMOVED:
			// End current segment
			currentSegment = nil

		case api.BlockchainEvent_BLOCK_ADDED:
			if currentSegment == nil {
				// Create new segment
				currentSegment = &BlockSegment{
					Blocks: []BlockToMigrate{},
				}
				segments = append(segments, currentSegment)
			}
			// Create block directly from event and append to current segment
			currentSegment.Blocks = append(currentSegment.Blocks, BlockToMigrate{
				Height:     event.BlockHeight,
				Hash:       event.BlockHash,
				ParentHash: event.ParentHash,
				EventSeq:   event.EventId,
				Skipped:    event.BlockSkipped,
			})
		}
	}

	// Log segments for debugging
	for i, segment := range segments {
		if len(segment.Blocks) > 0 {
			logger.Debug("Block segment",
				zap.Int("index", i),
				zap.Int("blocks", len(segment.Blocks)),
				zap.Uint64("firstHeight", segment.Blocks[0].Height),
				zap.Uint64("lastHeight", segment.Blocks[len(segment.Blocks)-1].Height))
		}
	}

	return segments
}

type BlockWithInfo struct {
	*api.BlockMetadata
	Height   uint64
	Hash     string
	EventSeq int64
}

// WorkItem for parallel fetching with index preservation
type WorkItem struct {
	Block BlockToMigrate
	Index int
}

func (a *Migrator) fetchBlockData(ctx context.Context, logger *zap.Logger,
	data *MigrationData, request *MigratorRequest,
	blocksToMigrate []BlockToMigrate) ([]*BlockWithInfo, error) {

	if len(blocksToMigrate) == 0 {
		return nil, nil
	}

	// Determine parallelism
	parallelism := request.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > len(blocksToMigrate) {
		parallelism = 1
	}

	logger.Debug("Fetching block data in parallel",
		zap.Int("totalBlocks", len(blocksToMigrate)),
		zap.Int("parallelism", parallelism))

	// Pre-allocate result array
	results := make([]*BlockWithInfo, len(blocksToMigrate))

	// Channel for work items (index, block)
	workChan := make(chan WorkItem, len(blocksToMigrate))

	// Send work items with their index
	for i, block := range blocksToMigrate {
		workChan <- WorkItem{Block: block, Index: i}
	}
	close(workChan)

	// Worker function - fetches block and writes directly to results[index]
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workChan {
				var blockMeta *api.BlockMetadata

				// For skipped blocks, create metadata directly
				if item.Block.Skipped {
					blockMeta = &api.BlockMetadata{
						Tag:          request.Tag,
						Height:       item.Block.Height,
						Skipped:      true,
						Hash:         "",
						ParentHash:   "",
						ParentHeight: 0,
						Timestamp:    nil,
					}
				} else {
					// For regular blocks, fetch from source storage
					var err error
					blockMeta, err = data.SourceStorage.GetBlockByHash(ctx, request.Tag, item.Block.Height, item.Block.Hash)
					if err != nil {
						logger.Warn("Failed to fetch block by hash",
							zap.Uint64("height", item.Block.Height),
							zap.String("hash", item.Block.Hash),
							zap.Error(err))
						continue // Skip this block
					}
				}

				// Write directly to the correct index
				results[item.Index] = &BlockWithInfo{
					BlockMetadata: blockMeta,
					Height:        item.Block.Height,
					Hash:          item.Block.Hash,
					EventSeq:      item.Block.EventSeq,
				}
			}
		}()
	}

	// Wait for all workers to complete
	wg.Wait()

	// Filter out nils (from failed fetches) and count skipped
	var validResults []*BlockWithInfo
	skippedCount := 0
	for _, blockInfo := range results {
		if blockInfo != nil {
			validResults = append(validResults, blockInfo)
			if blockInfo.BlockMetadata != nil && blockInfo.BlockMetadata.Skipped {
				skippedCount++
			}
		}
	}

	logger.Debug("Completed parallel block data fetch",
		zap.Int("requested", len(blocksToMigrate)),
		zap.Int("fetched", len(validResults)),
		zap.Int("skippedBlocks", skippedCount))

	return validResults, nil
}

func (a *Migrator) persistEvents(ctx context.Context, logger *zap.Logger,
	data *MigrationData, request *MigratorRequest,
	events []*model.EventEntry) (int, error) {

	if len(events) == 0 {
		return 0, nil
	}

	// Events are already sorted by EventId from fetchEventsBySequence
	firstId := events[0].EventId
	lastId := events[len(events)-1].EventId

	logger.Info("Persisting events batch",
		zap.Int("totalEvents", len(events)),
		zap.Int64("firstEventId", firstId),
		zap.Int64("lastEventId", lastId))

	// Check for gaps in event sequences (for debugging)
	var gaps []string
	for i := 1; i < len(events); i++ {
		if events[i].EventId != events[i-1].EventId+1 {
			gap := fmt.Sprintf("position=%d: %d->%d (gap=%d)",
				i, events[i-1].EventId, events[i].EventId,
				events[i].EventId-events[i-1].EventId-1)
			gaps = append(gaps, gap)
		}
	}
	if len(gaps) > 0 {
		logger.Warn("Found gaps in event sequences",
			zap.Strings("gaps", gaps))
	}

	// Persist all events in a single call
	persistStart := time.Now()
	if err := data.DestStorage.AddEventEntries(ctx, request.EventTag, events); err != nil {
		return 0, xerrors.Errorf("failed to persist events: %w", err)
	}

	persistDuration := time.Since(persistStart)
	logger.Info("Events persisted successfully",
		zap.Int("eventCount", len(events)),
		zap.Duration("persistDuration", persistDuration),
		zap.Float64("eventsPerSecond", float64(len(events))/persistDuration.Seconds()))

	return len(events), nil
}

type GetLatestBlockHeightRequest struct {
	Tag uint32
}

type GetLatestBlockHeightResponse struct {
	Height uint64
}

type GetLatestBlockFromPostgresRequest struct {
	Tag uint32
}

type GetLatestBlockFromPostgresResponse struct {
	Height uint64
	Found  bool // true if a block was found, false if no blocks exist yet
}

type GetLatestEventFromPostgresRequest struct {
	EventTag uint32
}

type GetLatestEventFromPostgresResponse struct {
	Sequence int64  // Event sequence number
	Height   uint64 // Block height (for backward compatibility)
	Found    bool   // true if events were found, false if no events exist yet
}

type GetMaxEventIdRequest struct {
	EventTag uint32
}

type GetMaxEventIdResponse struct {
	MaxEventId int64 // Maximum event ID in DynamoDB
	Found      bool  // true if events were found
}

func (a *Migrator) GetLatestBlockHeight(ctx context.Context, req *GetLatestBlockHeightRequest) (*GetLatestBlockHeightResponse, error) {
	migrationData, err := a.createStorageInstances(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to create storage instances: %w", err)
	}
	latestBlock, err := migrationData.SourceStorage.GetLatestBlock(ctx, req.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block from DynamoDB: %w", err)
	}
	return &GetLatestBlockHeightResponse{Height: latestBlock.Height}, nil
}

func (a *GetLatestBlockHeightActivity) Execute(ctx workflow.Context, request *GetLatestBlockHeightRequest) (*GetLatestBlockHeightResponse, error) {
	var response GetLatestBlockHeightResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetLatestBlockHeightActivity) execute(ctx context.Context, request *GetLatestBlockHeightRequest) (*GetLatestBlockHeightResponse, error) {
	migrationData, err := a.createStorageInstances(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to create storage instances: %w", err)
	}
	latestBlock, err := migrationData.SourceStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block from DynamoDB: %w", err)
	}
	return &GetLatestBlockHeightResponse{Height: latestBlock.Height}, nil
}

func (a *GetLatestBlockFromPostgresActivity) Execute(ctx workflow.Context, request *GetLatestBlockFromPostgresRequest) (*GetLatestBlockFromPostgresResponse, error) {
	var response GetLatestBlockFromPostgresResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetLatestBlockFromPostgresActivity) execute(ctx context.Context, request *GetLatestBlockFromPostgresRequest) (*GetLatestBlockFromPostgresResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

	// Create PostgreSQL storage using shared connection pool to query destination
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}
	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	latestBlock, err := destResult.MetaStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		// Check if it's a "not found" error, which means no blocks migrated yet
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "not found") || strings.Contains(errStr, "no rows") {
			logger.Info("No blocks found in PostgreSQL destination - starting from beginning")
			return &GetLatestBlockFromPostgresResponse{
				Height: 0,
				Found:  false,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get latest block from PostgreSQL: %w", err)
	}

	logger.Info("Found latest block in PostgreSQL destination", zap.Uint64("height", latestBlock.Height))
	return &GetLatestBlockFromPostgresResponse{
		Height: latestBlock.Height,
		Found:  true,
	}, nil
}

func (a *GetLatestEventFromPostgresActivity) Execute(ctx workflow.Context, request *GetLatestEventFromPostgresRequest) (*GetLatestEventFromPostgresResponse, error) {
	var response GetLatestEventFromPostgresResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetLatestEventFromPostgresActivity) execute(ctx context.Context, request *GetLatestEventFromPostgresRequest) (*GetLatestEventFromPostgresResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

	// Create PostgreSQL storage using shared connection pool to query destination
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}

	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	// Get the latest event sequence from PostgreSQL
	maxEventId, err := destResult.MetaStorage.GetMaxEventId(ctx, request.EventTag)
	if err != nil {
		// Check if it's a "no event history" error, which means no events migrated yet
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "no event history") || strings.Contains(errStr, "not found") || strings.Contains(errStr, "no rows") {
			logger.Info("No events found in PostgreSQL destination - starting from beginning")
			return &GetLatestEventFromPostgresResponse{
				Sequence: 0,
				Height:   0,
				Found:    false,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get latest event sequence from PostgreSQL: %w", err)
	}

	// Get the event entry to also return the height for backward compatibility
	eventEntry, err := destResult.MetaStorage.GetEventByEventId(ctx, request.EventTag, maxEventId)
	if err != nil {
		// If we can't get the event entry, just return the sequence
		logger.Warn("Failed to get event entry for max sequence, returning sequence only",
			zap.Int64("maxEventId", maxEventId), zap.Error(err))
		return &GetLatestEventFromPostgresResponse{
			Sequence: maxEventId,
			Height:   0,
			Found:    true,
		}, nil
	}

	logger.Info("Found latest event in PostgreSQL destination",
		zap.Int64("sequence", maxEventId),
		zap.Uint64("height", eventEntry.BlockHeight))
	return &GetLatestEventFromPostgresResponse{
		Sequence: maxEventId,
		Height:   eventEntry.BlockHeight,
		Found:    true,
	}, nil
}

func (a *GetMaxEventIdActivity) Execute(ctx workflow.Context, request *GetMaxEventIdRequest) (*GetMaxEventIdResponse, error) {
	var response GetMaxEventIdResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *GetMaxEventIdActivity) execute(ctx context.Context, request *GetMaxEventIdRequest) (*GetMaxEventIdResponse, error) {
	logger := a.getLogger(ctx).With(
		zap.Uint32("eventTag", request.EventTag),
	)
	logger.Info("getting max event ID from DynamoDB")

	// Create DynamoDB storage directly
	dynamoDBParams := dynamodb_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
		AWSConfig: a.awsConfig,
	}
	sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create DynamoDB storage: %w", err)
	}

	// Get max event ID from DynamoDB
	maxEventId, err := sourceResult.MetaStorage.GetMaxEventId(ctx, request.EventTag)
	if err != nil {
		if errors.Is(err, storage.ErrItemNotFound) {
			logger.Warn("no events found in DynamoDB")
			return &GetMaxEventIdResponse{
				MaxEventId: 0,
				Found:      false,
			}, nil
		}
		return nil, xerrors.Errorf("failed to get max event ID: %w", err)
	}

	logger.Info("found max event ID in DynamoDB",
		zap.Int64("maxEventId", maxEventId))

	return &GetMaxEventIdResponse{
		MaxEventId: maxEventId,
		Found:      true,
	}, nil
}

func (a *GetLatestBlockHeightActivity) createStorageInstances(ctx context.Context) (*MigrationData, error) {
	logger := a.getLogger(ctx)

	// Create DynamoDB storage directly
	dynamoDBParams := dynamodb_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
		AWSConfig: a.awsConfig,
	}
	sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create DynamoDB storage: %w", err)
	}

	// Create PostgreSQL storage using shared connection pool
	postgresParams := postgres_storage.Params{
		Params: fxparams.Params{
			Config:  a.config,
			Logger:  logger,
			Metrics: a.metrics,
		},
	}
	destResult, err := postgres_storage.NewMetaStorage(postgresParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	return &MigrationData{
		SourceStorage: sourceResult.MetaStorage,
		DestStorage:   destResult.MetaStorage,
		Config:        a.config,
		DynamoClient:  a.dynamoClient,
		BlockTable:    a.blockTable,
	}, nil
}

const (
	ActivityMigrator                   = "activity.migrator"
	ActivityGetLatestBlockHeight       = "activity.migrator.GetLatestBlockHeight"
	ActivityGetLatestBlockFromPostgres = "activity.migrator.GetLatestBlockFromPostgres"
	ActivityGetLatestEventFromPostgres = "activity.migrator.GetLatestEventFromPostgres"
	ActivityGetMaxEventId              = "activity.migrator.GetMaxEventId"
)
