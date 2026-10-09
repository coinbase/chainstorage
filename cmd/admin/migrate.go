package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	dynamodb_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	postgres_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	migrateFlags struct {
		startHeight     uint64
		endHeight       uint64
		eventTag        uint32
		tag             uint32
		batchSize       int
		miniBatchSize   int
		checkpointSize  int
		parallelism     int
		skipEvents      bool
		skipBlocks      bool
		continuousSync  bool
		syncInterval    string
		backoffInterval string
		autoResume      bool
	}
)

var (
	migrateCmd = &cobra.Command{
		Use:   "migrate",
		Short: "Migrate data from DynamoDB to PostgreSQL with optional continuous sync",
		Long: `Migrate block metadata and events from DynamoDB to PostgreSQL.

Block Migration:
- Handles reorgs by migrating non-canonical blocks first, then canonical blocks last
- Captures complete reorg data by querying DynamoDB directly for all blocks at each height
- Maintains canonical block identification in PostgreSQL through migration order

Event Migration:
- Uses event ID-based iteration for efficient migration
- Gets first event ID from start height, last event ID from end height
- Migrates events sequentially by event ID range in batches
- Event IDs in DynamoDB correspond directly to event sequences in PostgreSQL

Continuous Sync Mode:
- Enables infinite loop mode for real-time data synchronization
- When enabled and current batch completes:
  - Sets new StartHeight to current EndHeight
  - Resets EndHeight to 0 (meaning "sync to latest")
  - Waits for SyncInterval duration
  - Restarts migration with new parameters
- Validation: EndHeight must be 0 OR greater than StartHeight when ContinuousSync is enabled

Performance Parameters:
- BatchSize: Number of blocks to process in each workflow batch
- MiniBatchSize: Number of blocks to process in each activity mini-batch (for parallelism)
- CheckpointSize: Number of blocks to process before creating a workflow checkpoint
- Parallelism: Number of parallel workers for processing mini-batches

End Height:
- If --end-height is not provided, the tool will automatically query the latest block
  from DynamoDB and use that as the end height (exclusive)
- Note: Auto-detection is only available in the migrate command, not in workflow mode

Note: Block metadata must be migrated before events since events reference blocks via foreign keys.

Examples:
  # Migrate blocks and events from height 1000000 to latest block (auto-detected)
  go run cmd/admin/*.go migrate \
    --env=local \
    --blockchain=ethereum \
    --network=mainnet \
    --start-height=1000000 \
    --tag=2 \
    --event-tag=3

  # Migrate specific height range with custom batch sizes
  go run cmd/admin/*.go migrate \
    --env=local \
    --blockchain=ethereum \
    --network=mainnet \
    --start-height=100 \
    --end-height=152 \
    --tag=2 \
    --event-tag=3 \
    --batch-size=50 \
    --mini-batch-size=10 \
    --parallelism=4

  # Continuous sync mode - syncs continuously with 30 second intervals
  go run cmd/admin/*.go migrate \
    --env=local \
    --blockchain=ethereum \
    --network=mainnet \
    --start-height=1000000 \
    --tag=2 \
    --event-tag=3 \
    --continuous-sync \
    --sync-interval=30s \
    --batch-size=100 \
    --mini-batch-size=20 \
    --parallelism=2

  # Migrate blocks only (skip events)
  go run cmd/admin/*.go migrate \
    --env=local \
    --blockchain=ethereum \
    --network=mainnet \
    --start-height=1000000 \
    --end-height=1001000 \
    --tag=2 \
    --event-tag=3 \
    --skip-events

  # Migrate events only (requires blocks to exist first)
  go run cmd/admin/*.go migrate \
    --env=local \
    --blockchain=ethereum \
    --network=mainnet \
    --start-height=1000000 \
    --end-height=1001000 \
    --tag=2 \
    --event-tag=3 \
    --skip-blocks \
    --backoff-interval=1s

  # High throughput migration with checkpoints
  go run cmd/admin/*.go migrate \
    --env=local \
    --blockchain=ethereum \
    --network=mainnet \
    --start-height=1000000 \
    --end-height=2000000 \
    --tag=2 \
    --event-tag=3 \
    --batch-size=1000 \
    --mini-batch-size=100 \
    --checkpoint-size=10000 \
    --parallelism=8

  # Auto-resume from where previous migration left off
  go run cmd/admin/*.go migrate \
    --env=local \
    --blockchain=ethereum \
    --network=mainnet \
    --auto-resume \
    --tag=2 \
    --event-tag=3`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate flag combinations
			if !migrateFlags.autoResume && migrateFlags.startHeight == 0 {
				return xerrors.New("start-height is required unless --auto-resume is enabled")
			}
			if migrateFlags.autoResume && migrateFlags.startHeight != 0 {
				return xerrors.New("cannot specify both --auto-resume and --start-height (auto-resume will determine start height)")
			}

			var deps struct {
				fx.In
				Config    *config.Config
				AWSConfig awssdk.Config
				Params    fxparams.Params
			}

			app := startApp(
				aws.Module,
				storage.Module,
				fx.Populate(&deps),
			)
			defer func() {
				app.Close()
			}()

			// Create DynamoDB storage directly
			dynamoDBParams := dynamodb_storage.Params{
				Params:    deps.Params,
				AWSConfig: deps.AWSConfig,
			}
			sourceResult, err := dynamodb_storage.NewMetaStorage(dynamoDBParams)
			if err != nil {
				return xerrors.Errorf("failed to create DynamoDB storage: %w", err)
			}

			// Create PostgreSQL storage directly
			postgresParams := postgres_storage.Params{
				Params: deps.Params,
			}
			destResult, err := postgres_storage.NewMetaStorage(postgresParams)
			if err != nil {
				return xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
			}

			// Note: Validation will happen after end height auto-detection

			if migrateFlags.batchSize <= 0 {
				migrateFlags.batchSize = 100
			}

			if migrateFlags.miniBatchSize <= 0 {
				migrateFlags.miniBatchSize = migrateFlags.batchSize / 10
				if migrateFlags.miniBatchSize <= 0 {
					migrateFlags.miniBatchSize = 10
				}
			}

			if migrateFlags.checkpointSize <= 0 {
				migrateFlags.checkpointSize = 10000
			}

			if migrateFlags.parallelism <= 0 {
				migrateFlags.parallelism = 1
			}

			// Both skip flags cannot be true
			if migrateFlags.skipEvents && migrateFlags.skipBlocks {
				return xerrors.New("cannot skip both events and blocks - nothing to migrate")
			}

			// Validate continuous sync parameters
			if migrateFlags.continuousSync {
				logger.Warn("WARNING: Continuous sync is not supported in direct migration mode")
				logger.Warn("Continuous sync is only available when using the migrator workflow")
				logger.Warn("This tool will perform a one-time migration and exit")

				if migrateFlags.endHeight != 0 && migrateFlags.endHeight <= migrateFlags.startHeight {
					return xerrors.Errorf("with continuous sync enabled, end height (%d) must be 0 OR greater than start height (%d)",
						migrateFlags.endHeight, migrateFlags.startHeight)
				}
			}

			// Warn about skip-blocks requirements
			if migrateFlags.skipBlocks && !migrateFlags.skipEvents {
				logger.Warn("IMPORTANT: Using --skip-blocks (events-only migration)")
				logger.Warn("Block metadata MUST already exist in PostgreSQL for the specified height range")
				logger.Warn("If block metadata is missing, the migration will fail with foreign key errors")
				logger.Warn("To fix: First migrate blocks with --skip-events, then migrate events with --skip-blocks")

				prompt := "Are you sure block metadata already exists in PostgreSQL for this range? (y/N): "
				if !confirm(prompt) {
					logger.Info("Migration cancelled - migrate blocks first with --skip-events")
					return nil
				}
			}

			ctx := context.Background()

			// Handle auto-resume functionality
			if migrateFlags.autoResume && migrateFlags.startHeight == 0 {
				logger.Info("AutoResume enabled, querying PostgreSQL destination for latest migrated block")
				latestBlock, err := destResult.MetaStorage.GetLatestBlock(ctx, migrateFlags.tag)
				if err != nil {
					// Check if it's a "not found" error, which means no blocks migrated yet
					errStr := strings.ToLower(err.Error())
					if strings.Contains(errStr, "not found") || strings.Contains(errStr, "no rows") {
						logger.Info("Auto-resume: no blocks found in PostgreSQL destination, starting from beginning")
						migrateFlags.startHeight = 0
					} else {
						return xerrors.Errorf("failed to get latest block height from PostgreSQL: %w", err)
					}
				} else {
					// Resume from the next block after the latest migrated block
					migrateFlags.startHeight = latestBlock.Height + 1
					logger.Info("Auto-resume: found latest block in PostgreSQL destination",
						zap.Uint64("latestHeight", latestBlock.Height),
						zap.Uint64("resumeFromHeight", migrateFlags.startHeight))
				}
			}

			// Handle end height - if not provided, query latest block from DynamoDB
			if migrateFlags.endHeight == 0 {
				logger.Info("No end height provided, querying latest block from DynamoDB...")

				// Query latest block from DynamoDB
				latestBlock, err := sourceResult.MetaStorage.GetLatestBlock(ctx, migrateFlags.tag)
				if err != nil {
					return xerrors.Errorf("failed to get latest block from DynamoDB: %w", err)
				}

				migrateFlags.endHeight = latestBlock.Height + 1 // Make it exclusive
				logger.Info("Found latest block in DynamoDB",
					zap.Uint64("latestHeight", latestBlock.Height),
					zap.Uint64("endHeight", migrateFlags.endHeight),
					zap.String("latestHash", latestBlock.Hash))
			}

			// Validate flags after end height auto-detection and auto-resume
			if !migrateFlags.continuousSync && migrateFlags.startHeight >= migrateFlags.endHeight {
				// Special case: if auto-resume found we're already caught up
				if migrateFlags.autoResume {
					logger.Info("Auto-resume detected: already caught up, no migration needed",
						zap.Uint64("startHeight", migrateFlags.startHeight),
						zap.Uint64("endHeight", migrateFlags.endHeight))
					return nil // Successfully completed with no work to do
				}
				return xerrors.Errorf("startHeight (%d) must be less than endHeight (%d)",
					migrateFlags.startHeight, migrateFlags.endHeight)
			}
			// Create DynamoDB client for direct queries
			dynamoClient := dynamodb.NewFromConfig(deps.AWSConfig)
			blockTable := deps.Config.AWS.DynamoDB.BlockTable

			migrator := &DataMigrator{
				sourceStorage: sourceResult.MetaStorage,
				destStorage:   destResult.MetaStorage,
				config:        deps.Config,
				logger:        logger,
				dynamoClient:  dynamoClient,
				blockTable:    blockTable,
			}

			// Confirmation prompt
			prompt := fmt.Sprintf("This will migrate data from height %d to %d. Continue? (y/N): ",
				migrateFlags.startHeight, migrateFlags.endHeight)
			if !confirm(prompt) {
				logger.Info("Migration cancelled")
				return nil
			}

			migrateParams := MigrationParams{
				StartHeight:     migrateFlags.startHeight,
				EndHeight:       migrateFlags.endHeight,
				EventTag:        migrateFlags.eventTag,
				Tag:             migrateFlags.tag,
				BatchSize:       migrateFlags.batchSize,
				MiniBatchSize:   migrateFlags.miniBatchSize,
				CheckpointSize:  migrateFlags.checkpointSize,
				Parallelism:     migrateFlags.parallelism,
				SkipEvents:      migrateFlags.skipEvents,
				SkipBlocks:      migrateFlags.skipBlocks,
				ContinuousSync:  migrateFlags.continuousSync,
				SyncInterval:    migrateFlags.syncInterval,
				BackoffInterval: migrateFlags.backoffInterval,
				AutoResume:      migrateFlags.autoResume,
			}

			return migrator.Migrate(ctx, migrateParams)
		},
	}
)

type MigrationParams struct {
	StartHeight     uint64
	EndHeight       uint64
	EventTag        uint32
	Tag             uint32
	BatchSize       int
	MiniBatchSize   int
	CheckpointSize  int
	Parallelism     int
	SkipEvents      bool
	SkipBlocks      bool
	ContinuousSync  bool
	SyncInterval    string
	BackoffInterval string
	AutoResume      bool
}

type DataMigrator struct {
	sourceStorage metastorage.MetaStorage
	destStorage   metastorage.MetaStorage
	config        *config.Config
	logger        *zap.Logger
	// Direct DynamoDB access for querying all blocks
	dynamoClient *dynamodb.Client
	blockTable   string
}

func (m *DataMigrator) Migrate(ctx context.Context, params MigrationParams) error {
	m.logger.Info("Starting migration",
		zap.Uint64("startHeight", params.StartHeight),
		zap.Uint64("endHeight", params.EndHeight),
		zap.Bool("skipBlocks", params.SkipBlocks),
		zap.Bool("skipEvents", params.SkipEvents))

	startTime := time.Now()

	// Phase 1: Migrate block metadata FIRST (required for foreign key references)
	if !params.SkipBlocks {
		if err := m.migrateBlocksPerHeight(ctx, params); err != nil {
			return xerrors.Errorf("failed to migrate blocks: %w", err)
		}
	}

	// Phase 2: Migrate events AFTER blocks (depends on block metadata foreign keys)
	if !params.SkipEvents {
		if err := m.migrateEvents(ctx, params); err != nil {
			return xerrors.Errorf("failed to migrate events: %w", err)
		}
	}

	duration := time.Since(startTime)
	m.logger.Info("Migration completed successfully",
		zap.Duration("duration", duration),
		zap.Uint64("heightRange", params.EndHeight-params.StartHeight))

	return nil
}

func (m *DataMigrator) migrateBlocksPerHeight(ctx context.Context, params MigrationParams) error {
	m.logger.Info("Starting height-by-height block metadata migration with complete reorg support")

	totalHeights := params.EndHeight - params.StartHeight
	processedHeights := uint64(0)
	totalNonCanonicalBlocks := 0

	for height := params.StartHeight; height < params.EndHeight; height++ {
		nonCanonicalCount, err := m.migrateBlocksAtHeight(ctx, params, height)
		if err != nil {
			return xerrors.Errorf("failed to migrate blocks at height %d: %w", height, err)
		}

		totalNonCanonicalBlocks += nonCanonicalCount
		processedHeights++

		// Progress logging every 100 heights
		if processedHeights%100 == 0 {
			percentage := float64(processedHeights) / float64(totalHeights) * 100
			m.logger.Info("Block migration progress",
				zap.Uint64("processed", processedHeights),
				zap.Uint64("total", totalHeights),
				zap.Float64("percentage", percentage),
				zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))
		}
	}

	m.logger.Info("Height-by-height block metadata migration completed",
		zap.Int("totalNonCanonicalBlocks", totalNonCanonicalBlocks))
	return nil
}

func (m *DataMigrator) migrateBlocksAtHeight(ctx context.Context, params MigrationParams, height uint64) (int, error) {
	blockPid := fmt.Sprintf("%d-%d", params.Tag, height)

	// Phase 1: Get and persist non-canonical blocks first
	// Query: BlockPid = "{tag}-{height}" AND BlockRid != "canonical"
	nonCanonicalBlocks, err := m.getNonCanonicalBlocksAtHeight(ctx, blockPid)
	if err != nil && !xerrors.Is(err, storage.ErrItemNotFound) {
		return 0, xerrors.Errorf("failed to get non-canonical blocks at height %d: %w", height, err)
	}

	nonCanonicalCount := len(nonCanonicalBlocks)
	if nonCanonicalCount > 0 {
		m.logger.Debug("Found non-canonical (reorg) blocks at height",
			zap.Uint64("height", height),
			zap.Int("count", nonCanonicalCount))

		// Persist non-canonical blocks FIRST
		err = m.destStorage.PersistBlockMetas(ctx, false, nonCanonicalBlocks, nil)
		if err != nil {
			return 0, xerrors.Errorf("failed to persist non-canonical blocks at height %d: %w", height, err)
		}
	}

	// Phase 2: Get and persist canonical block LAST
	// Query: BlockPid = "{tag}-{height}" AND BlockRid = "canonical"
	canonicalBlock, err := m.getCanonicalBlockAtHeight(ctx, blockPid)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			m.logger.Debug("No canonical block found at height", zap.Uint64("height", height))
			return nonCanonicalCount, nil
		}
		return 0, xerrors.Errorf("failed to get canonical block at height %d: %w", height, err)
	}

	m.logger.Debug("Found canonical block at height",
		zap.Uint64("height", height),
		zap.String("hash", canonicalBlock.Hash),
		zap.Int("reorgBlockCount", nonCanonicalCount))

	// Persist canonical block LAST - this ensures it becomes canonical in PostgreSQL
	err = m.destStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{canonicalBlock}, nil)
	if err != nil {
		return 0, xerrors.Errorf("failed to persist canonical block at height %d: %w", height, err)
	}

	return nonCanonicalCount, nil
}

func (m *DataMigrator) getNonCanonicalBlocksAtHeight(ctx context.Context, blockPid string) ([]*api.BlockMetadata, error) {
	// Query DynamoDB for ALL blocks at this height: BlockPid = blockPid
	// Then filter out the canonical one client-side
	input := &dynamodb.QueryInput{
		TableName:              awssdk.String(m.blockTable),
		KeyConditionExpression: awssdk.String("block_pid = :blockPid"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":blockPid": &dynamodbtypes.AttributeValueMemberS{Value: blockPid},
		},
		ConsistentRead: awssdk.Bool(true),
	}

	result, err := m.dynamoClient.Query(ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("failed to query blocks at height: %w", err)
	}

	// Filter out canonical blocks client-side
	var nonCanonicalBlocks []*api.BlockMetadata
	for _, item := range result.Items {
		var blockEntry model.BlockMetaDataDDBEntry
		err := attributevalue.UnmarshalMap(item, &blockEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal DynamoDB item: %w", err)
		}

		// Skip canonical blocks (BlockRid = "canonical")
		if blockEntry.BlockRid == "canonical" {
			continue
		}

		nonCanonicalBlocks = append(nonCanonicalBlocks, model.BlockMetadataToProto(&blockEntry))
	}

	return nonCanonicalBlocks, nil
}

func (m *DataMigrator) getCanonicalBlockAtHeight(ctx context.Context, blockPid string) (*api.BlockMetadata, error) {
	// Query DynamoDB directly: BlockPid = blockPid AND BlockRid = "canonical"
	input := &dynamodb.QueryInput{
		TableName:              awssdk.String(m.blockTable),
		KeyConditionExpression: awssdk.String("block_pid = :blockPid AND block_rid = :canonical"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":blockPid":  &dynamodbtypes.AttributeValueMemberS{Value: blockPid},
			":canonical": &dynamodbtypes.AttributeValueMemberS{Value: "canonical"},
		},
		ConsistentRead: awssdk.Bool(true),
	}

	result, err := m.dynamoClient.Query(ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("failed to query canonical block: %w", err)
	}

	if len(result.Items) == 0 {
		return nil, storage.ErrItemNotFound
	}

	if len(result.Items) > 1 {
		return nil, xerrors.Errorf("multiple canonical blocks found for %s", blockPid)
	}

	var blockEntry model.BlockMetaDataDDBEntry
	err = attributevalue.UnmarshalMap(result.Items[0], &blockEntry)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal canonical block: %w", err)
	}

	return model.BlockMetadataToProto(&blockEntry), nil
}

func (m *DataMigrator) migrateEvents(ctx context.Context, params MigrationParams) error {
	m.logger.Info("Starting event ID-based migration")

	// Step 1: Get the first event ID at start height
	startEventId, err := m.sourceStorage.GetFirstEventIdByBlockHeight(ctx, params.EventTag, params.StartHeight)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			m.logger.Info("No events found at start height", zap.Uint64("startHeight", params.StartHeight))
			return nil
		}
		return xerrors.Errorf("failed to get first event ID at start height %d: %w", params.StartHeight, err)
	}

	// Step 2: Find the last event ID within the height range [startHeight, endHeight)
	var endEventId int64
	if params.EndHeight > params.StartHeight {
		endEventId, err = m.findLastEventIdInRange(ctx, params.EventTag, params.StartHeight, params.EndHeight)
		if err != nil {
			return xerrors.Errorf("failed to find last event ID in range [%d, %d): %w", params.StartHeight, params.EndHeight, err)
		}

		// If no events found in the range beyond startEventId, just process starting event
		if endEventId < startEventId {
			endEventId = startEventId
		}
	} else {
		endEventId = startEventId
	}

	m.logger.Info("Event ID range determined",
		zap.Int64("startEventId", startEventId),
		zap.Int64("endEventId", endEventId),
		zap.Int64("totalEvents", endEventId-startEventId+1))

	if endEventId < startEventId {
		m.logger.Info("No events to migrate (end event ID < start event ID)")
		return nil
	}

	// Step 3: Migrate events by event ID range in batches
	totalEvents := endEventId - startEventId + 1
	processedEvents := int64(0)
	batchSize := int64(params.BatchSize)

	for currentEventId := startEventId; currentEventId <= endEventId; currentEventId += batchSize {
		// Calculate the end of this batch
		batchEndEventId := currentEventId + batchSize - 1
		if batchEndEventId > endEventId {
			batchEndEventId = endEventId
		}

		// Get events in this range from DynamoDB
		sourceEvents, err := m.sourceStorage.GetEventsByEventIdRange(ctx, params.EventTag, currentEventId, batchEndEventId+1)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				m.logger.Debug("No events found in event ID range",
					zap.Int64("startEventId", currentEventId),
					zap.Int64("endEventId", batchEndEventId))
				processedEvents += batchEndEventId - currentEventId + 1
				continue
			}
			return xerrors.Errorf("failed to get events in range [%d, %d]: %w", currentEventId, batchEndEventId, err)
		}

		if len(sourceEvents) == 0 {
			processedEvents += batchEndEventId - currentEventId + 1
			continue
		}

		m.logger.Debug("Migrating event batch",
			zap.Int("count", len(sourceEvents)),
			zap.Int64("startEventId", currentEventId),
			zap.Int64("endEventId", batchEndEventId))

		// Migrate this batch to PostgreSQL
		err = m.destStorage.AddEventEntries(ctx, params.EventTag, sourceEvents)
		if err != nil {
			return xerrors.Errorf("failed to add events batch [%d, %d] to PostgreSQL: %w", currentEventId, batchEndEventId, err)
		}

		processedEvents += int64(len(sourceEvents))

		// Progress logging every 1000 events
		if processedEvents%1000 == 0 || processedEvents == totalEvents {
			percentage := float64(processedEvents) / float64(totalEvents) * 100
			m.logger.Info("Event migration progress",
				zap.Int64("processed", processedEvents),
				zap.Int64("total", totalEvents),
				zap.Float64("percentage", percentage))
		}
	}

	m.logger.Info("Event ID-based migration completed",
		zap.Int64("totalEventsMigrated", processedEvents))
	return nil
}

// findLastEventIdInRange finds the maximum event ID within the specified height range
// by searching backwards from endHeight-1 until an event is found or reaching startHeight
func (m *DataMigrator) findLastEventIdInRange(ctx context.Context, eventTag uint32, startHeight, endHeight uint64) (int64, error) {
	m.logger.Debug("Finding last event ID in height range",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight))

	// Search backwards from endHeight-1 to startHeight to find the last event
	for height := endHeight - 1; height >= startHeight; height-- {
		events, err := m.sourceStorage.GetEventsByBlockHeight(ctx, eventTag, height)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				// No events at this height, continue searching backwards
				m.logger.Debug("No events found at height", zap.Uint64("height", height))
				continue
			}
			return 0, xerrors.Errorf("failed to get events at height %d: %w", height, err)
		}

		// Find the maximum event ID at this height
		var maxEventId int64 = -1
		for _, event := range events {
			if event.EventId > maxEventId {
				maxEventId = event.EventId
			}
		}

		if maxEventId >= 0 {
			m.logger.Debug("Found last event in range",
				zap.Uint64("height", height),
				zap.Int64("eventId", maxEventId))
			return maxEventId, nil
		}
	}

	// No events found in the entire range
	m.logger.Debug("No events found in the specified height range")
	return -1, storage.ErrItemNotFound
}

func init() {
	migrateCmd.Flags().Uint64Var(&migrateFlags.startHeight, "start-height", 0, "start block height (inclusive)")
	migrateCmd.Flags().Uint64Var(&migrateFlags.endHeight, "end-height", 0, "end block height (exclusive, optional - if not provided, will query latest block from DynamoDB)")
	migrateCmd.Flags().Uint32Var(&migrateFlags.eventTag, "event-tag", 0, "event tag for migration")
	migrateCmd.Flags().Uint32Var(&migrateFlags.tag, "tag", 1, "block tag for migration")
	migrateCmd.Flags().IntVar(&migrateFlags.batchSize, "batch-size", 100, "number of blocks to process in each workflow batch")
	migrateCmd.Flags().IntVar(&migrateFlags.miniBatchSize, "mini-batch-size", 0, "number of blocks to process in each activity mini-batch (default: batch-size/10)")
	migrateCmd.Flags().IntVar(&migrateFlags.checkpointSize, "checkpoint-size", 10000, "number of blocks to process before creating a workflow checkpoint")
	migrateCmd.Flags().IntVar(&migrateFlags.parallelism, "parallelism", 1, "number of parallel workers for processing mini-batches")
	migrateCmd.Flags().BoolVar(&migrateFlags.skipEvents, "skip-events", false, "skip event migration (blocks only)")
	migrateCmd.Flags().BoolVar(&migrateFlags.skipBlocks, "skip-blocks", false, "skip block migration (events only)")
	migrateCmd.Flags().BoolVar(&migrateFlags.continuousSync, "continuous-sync", false, "enable continuous sync mode (infinite loop, workflow only)")
	migrateCmd.Flags().StringVar(&migrateFlags.syncInterval, "sync-interval", "1m", "time duration to wait between continuous sync cycles (e.g., '1m', '30s')")
	migrateCmd.Flags().StringVar(&migrateFlags.backoffInterval, "backoff-interval", "", "time duration to wait between batches (e.g., '1s', '500ms')")
	migrateCmd.Flags().BoolVar(&migrateFlags.autoResume, "auto-resume", false, "automatically determine start height from latest block in PostgreSQL destination")

	// start-height is required unless auto-resume is enabled
	// end-height is optional - if not provided, will query latest block from DynamoDB

	rootCmd.AddCommand(migrateCmd)
}
