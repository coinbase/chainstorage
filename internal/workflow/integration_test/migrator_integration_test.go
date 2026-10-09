package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type MigratorIntegrationTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

type testDependencies struct {
	fx.In
	Migrator            *workflow.Migrator
	SingleBlockUploader blobstorage.SingleBlockUploader
	MetaStorage         metastorage.MetaStorage // DynamoDB
	MetaStoragePG       metastorage.MetaStorage `name:"pg"` // PostgreSQL
	Parser              parser.Parser
	Client              client.Client `name:"slave"`
}

func TestIntegrationMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(MigratorIntegrationTestSuite))
}

// Helper function to create test app with common dependencies
func (s *MigratorIntegrationTestSuite) createTestApp(env *cadence.TestEnv, timeout time.Duration) (testapp.TestApp, *testDependencies) {
	var deps testDependencies
	env.SetTestTimeout(timeout)

	app := testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET),
		cadence.WithTestEnv(env),
		workflow.Module,
		client.Module,
		jsonrpc.Module,
		restapi.Module,
		s3.Module,
		storage.Module,
		parser.Module,
		dlq.Module,
		// Provide PostgreSQL MetaStorage with name "pg" for migration tests
		fx.Provide(fx.Annotated{
			Name: "pg",
			Target: func(params postgres.Params) (metastorage.MetaStorage, error) {
				result, err := postgres.NewMetaStorage(params)
				if err != nil {
					return nil, err
				}
				return result.MetaStorage, nil
			},
		}),
		fx.Populate(&deps),
	)

	return app, &deps
}

// Helper function to create test events with blocks
func (s *MigratorIntegrationTestSuite) createTestEvents(
	ctx context.Context,
	deps *testDependencies,
	tag uint32,
	eventTag uint32,
	startSequence, endSequence int64,
	includeReorg bool,
) error {
	require := testutil.Require(s.T())

	events := make([]*model.EventEntry, 0, endSequence-startSequence)

	// Create events with some BLOCK_ADDED and BLOCK_REMOVED events
	for seq := startSequence; seq < endSequence; seq++ {
		var eventType api.BlockchainEvent_Type
		var blockHeight uint64
		var blockHash string
		var parentHash string

		// Every 10th event is a BLOCK_ADDED
		if seq%10 == 0 {
			eventType = api.BlockchainEvent_BLOCK_ADDED
			blockHeight = uint64(17035140 + seq/10)
			blockHash = generateHash(blockHeight, 0)
			if blockHeight > 17035140 {
				parentHash = generateHash(blockHeight-1, 0)
			}
		} else if includeReorg && seq%100 == 5 {
			// Add some BLOCK_REMOVED events for reorg simulation
			eventType = api.BlockchainEvent_BLOCK_REMOVED
			blockHeight = uint64(17035140 + seq/10)
			blockHash = generateHash(blockHeight, 1) // Different hash for removed block
			parentHash = generateHash(blockHeight-1, 0)
		} else {
			// Most events are other types (not block-related)
			eventType = api.BlockchainEvent_UNKNOWN
			blockHeight = uint64(17035140 + seq/10)
			blockHash = ""
			parentHash = ""
		}

		event := &model.EventEntry{
			EventId:     seq,
			EventType:   eventType,
			BlockHeight: blockHeight,
			BlockHash:   blockHash,
			ParentHash:  parentHash,
			Tag:         tag,
			EventTag:    eventTag,
		}
		events = append(events, event)

		// Add reorg at specific height
		if includeReorg && seq == startSequence+50 {
			// Add another BLOCK_ADDED at same height (reorg)
			reorgEvent := &model.EventEntry{
				EventId:     seq + 1,
				EventType:   api.BlockchainEvent_BLOCK_ADDED,
				BlockHeight: blockHeight,
				BlockHash:   generateHash(blockHeight, 2), // Different hash for reorg
				ParentHash:  parentHash,
				Tag:         tag,
				EventTag:    eventTag,
			}
			events = append(events, reorgEvent)
			seq++ // Skip next sequence since we used it
		}
	}

	// Store events in DynamoDB
	err := deps.MetaStorage.AddEventEntries(ctx, eventTag, events)
	require.NoError(err, "Failed to store events in DynamoDB")

	// For each BLOCK_ADDED event, fetch and store the actual block
	for _, event := range events {
		if event.EventType == api.BlockchainEvent_BLOCK_ADDED {
			// Fetch block from blockchain
			block, err := deps.Client.GetBlockByHeight(ctx, tag, event.BlockHeight)
			if err != nil {
				// If can't fetch from chain, create a mock block
				block = &api.Block{
					Metadata: &api.BlockMetadata{
						Tag:          tag,
						Height:       event.BlockHeight,
						Hash:         event.BlockHash,
						ParentHash:   event.ParentHash,
						ParentHeight: event.BlockHeight - 1,
					},
				}
			}

			// Upload to blob storage
			objectKey, err := deps.SingleBlockUploader.Upload(ctx, block, api.Compression_GZIP)
			require.NoError(err, "Failed to upload block at height %d", event.BlockHeight)

			// Update metadata with object key
			block.Metadata.ObjectKeyMain = objectKey

			// Store in DynamoDB metadata storage
			err = deps.MetaStorage.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil)
			require.NoError(err, "Failed to store block in DynamoDB at height %d", event.BlockHeight)
		}
	}

	return nil
}

// Helper to generate deterministic hash for testing
func generateHash(height uint64, variant int) string {
	return fmt.Sprintf("0x%x", height*1000+uint64(variant))
}

// Test event-driven migration
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_EventDriven() {
	const (
		tag           = uint32(1)
		eventTag      = uint32(3)
		startSequence = int64(1000)
		endSequence   = int64(1500)
		batchSize     = 100
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 30*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create test events with blocks
	err := s.createTestEvents(ctx, deps, tag, eventTag, startSequence, endSequence, false)
	require.NoError(err)

	// Execute event-driven migrator workflow
	migratorRequest := &workflow.MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          uint64(batchSize),
		Parallelism:        4,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	// Verify migration
	// Check that events were migrated to PostgreSQL
	maxEventId, err := deps.MetaStoragePG.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.GreaterOrEqual(maxEventId, endSequence-1)

	app.Logger().Info("Event-driven migration test passed",
		zap.Int64("startSequence", startSequence),
		zap.Int64("endSequence", endSequence),
		zap.Int64("maxEventId", maxEventId))
}

// Test event-driven migration with reorgs
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_WithReorgs() {
	const (
		tag           = uint32(1)
		eventTag      = uint32(3)
		startSequence = int64(2000)
		endSequence   = int64(2200)
		batchSize     = 50
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 20*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create test events with reorgs
	err := s.createTestEvents(ctx, deps, tag, eventTag, startSequence, endSequence, true)
	require.NoError(err)

	// Execute event-driven migrator workflow
	migratorRequest := &workflow.MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          uint64(batchSize),
		Parallelism:        2, // Lower parallelism for reorg handling
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	app.Logger().Info("Migration with reorgs test passed",
		zap.Int64("startSequence", startSequence),
		zap.Int64("endSequence", endSequence))
}

// Test auto-resume functionality
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_AutoResume() {
	const (
		tag          = uint32(1)
		eventTag     = uint32(3)
		initialStart = int64(3000)
		midPoint     = int64(3100)
		endSequence  = int64(3200)
		batchSize    = 50
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 20*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create all test events
	err := s.createTestEvents(ctx, deps, tag, eventTag, initialStart, endSequence, false)
	require.NoError(err)

	// First migration - partial
	firstRequest := &workflow.MigratorRequest{
		StartEventSequence: initialStart,
		EndEventSequence:   midPoint,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          uint64(batchSize),
		Parallelism:        4,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, firstRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	// Second migration - auto-resume
	resumeRequest := &workflow.MigratorRequest{
		StartEventSequence: 0, // Will be auto-detected
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          uint64(batchSize),
		Parallelism:        4,
		AutoResume:         true,
	}

	migratorRun, err = deps.Migrator.Execute(ctx, resumeRequest)
	require.NoError(err)

	err = migratorRun.Get(ctx, nil)
	require.NoError(err)

	// Verify all events migrated
	maxEventId, err := deps.MetaStoragePG.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.GreaterOrEqual(maxEventId, endSequence-1)

	app.Logger().Info("Auto-resume migration test passed",
		zap.Int64("finalMaxEventId", maxEventId))
}

// Test large batch migration with checkpointing
func (s *MigratorIntegrationTestSuite) TestMigratorIntegration_LargeBatch() {
	const (
		tag            = uint32(1)
		eventTag       = uint32(3)
		startSequence  = int64(10000)
		endSequence    = int64(15000) // 5000 events
		batchSize      = 500
		checkpointSize = 2000
	)

	require := testutil.Require(s.T())

	// Setup
	env := cadence.NewTestEnv(s)
	app, deps := s.createTestApp(env, 30*time.Minute)
	defer app.Close()

	ctx := context.Background()

	// Create test events
	err := s.createTestEvents(ctx, deps, tag, eventTag, startSequence, endSequence, false)
	require.NoError(err)

	// Execute with checkpoint configuration
	migratorRequest := &workflow.MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          uint64(batchSize),
		CheckpointSize:     uint64(checkpointSize),
		Parallelism:        8,
	}

	migratorRun, err := deps.Migrator.Execute(ctx, migratorRequest)
	require.NoError(err)

	// This should trigger checkpoints during execution
	err = migratorRun.Get(ctx, nil)
	// May get continue-as-new error due to checkpointing
	if err != nil {
		require.Contains(err.Error(), "continue as new")
	}

	app.Logger().Info("Large batch migration test completed",
		zap.Int64("startSequence", startSequence),
		zap.Int64("endSequence", endSequence))
}
