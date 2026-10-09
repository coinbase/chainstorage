package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

const (
	migratorCheckpointSize = 50000
	migratorBatchSize      = 5000
)

type migratorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	migrator *Migrator
	app      testapp.TestApp
	cfg      *config.Config
}

func TestMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(migratorTestSuite))
}

func (s *migratorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	// Override config to speed up the test
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Migrator.BatchSize = migratorBatchSize
	cfg.Workflows.Migrator.CheckpointSize = migratorCheckpointSize
	cfg.Workflows.Migrator.BackoffInterval = time.Second
	cfg.Workflows.Migrator.Parallelism = 8
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(s.cfg),
		cadence.WithTestEnv(s.env),
		fx.Populate(&s.migrator),
	)
}

func (s *migratorTestSuite) TearDownTest() {
	s.app.Close()
	s.env.AssertExpectations(s.T())
}

func (s *migratorTestSuite) TestMigrator_EventDriven_Success() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := int64(6000) // 5000 events, fits in one batch
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			require.Equal(tag, request.Tag)
			expectedEventTag := s.cfg.Workflows.Migrator.GetEffectiveEventTag(eventTag)
			require.Equal(expectedEventTag, request.EventTag)

			// Event-driven migration processes both blocks and events
			eventCount := request.EndEventSequence - request.StartEventSequence
			blockCount := eventCount / 10 // Assume roughly 10% are BLOCK_ADDED events

			return &activity.MigratorResponse{
				BlocksMigrated: int(blockCount),
				EventsMigrated: int(eventCount),
				Success:        true,
				Message:        "Event-driven migration completed successfully",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_WithCheckpoint() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := startSequence + int64(migratorCheckpointSize) + 10000 // Exceed checkpoint
	tag := uint32(1)
	eventTag := uint32(0)

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(&activity.MigratorResponse{
			BlocksMigrated: 500,
			EventsMigrated: 5000,
			Success:        true,
			Message:        "Migration batch completed",
		}, nil)

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *migratorTestSuite) TestMigrator_CustomBatchSize() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := int64(3000)
	tag := uint32(1)
	eventTag := uint32(0)
	customBatchSize := uint64(500)

	callCount := 0
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			callCount++
			// Each batch should be customBatchSize
			batchSize := request.EndEventSequence - request.StartEventSequence
			require.LessOrEqual(batchSize, int64(customBatchSize))

			return &activity.MigratorResponse{
				BlocksMigrated: 50,
				EventsMigrated: int(batchSize),
				Success:        true,
				Message:        "Batch completed",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          customBatchSize,
	})
	require.NoError(err)
	// Should have been called 4 times (2000 events / 500 per batch)
	require.Equal(4, callCount)
}

func (s *migratorTestSuite) TestMigrator_AutoResume() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	eventTag := uint32(3)

	// Mock GetLatestEventFromPostgres to return a sequence
	s.env.OnActivity(activity.ActivityGetLatestEventFromPostgres, mock.Anything, mock.Anything).
		Return(&activity.GetLatestEventFromPostgresResponse{
			Sequence: int64(5000),
			Height:   uint64(1000),
			Found:    true,
		}, nil).Once()

	// The workflow should resume from sequence 5001
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			// Should start from next sequence after latest
			require.Equal(int64(5001), request.StartEventSequence)

			return &activity.MigratorResponse{
				BlocksMigrated: 100,
				EventsMigrated: 1000,
				Success:        true,
				Message:        "Resumed migration",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: 0, // Will be auto-detected
		EndEventSequence:   10000,
		Tag:                tag,
		EventTag:           eventTag,
		AutoResume:         true,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_ContinuousSync() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	tag := uint32(1)
	eventTag := uint32(0)

	// Mock GetMaxEventId activity since EndEventSequence is 0
	s.env.OnActivity(activity.ActivityGetMaxEventId, mock.Anything, mock.Anything).
		Return(&activity.GetMaxEventIdResponse{
			MaxEventId: int64(6000),
			Found:      true,
		}, nil).Once()

	callCount := 0
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			callCount++
			if callCount > 2 {
				// Stop after 2 iterations to prevent infinite loop in test
				s.env.CancelWorkflow()
			}

			return &activity.MigratorResponse{
				BlocksMigrated: 100,
				EventsMigrated: 1000,
				Success:        true,
				Message:        "Batch migrated",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   0, // Will be auto-detected for continuous sync
		Tag:                tag,
		EventTag:           eventTag,
		ContinuousSync:     true,
		SyncInterval:       "1s",
	})

	// Should get a continue-as-new error for continuous sync
	if err != nil {
		require.True(IsContinueAsNewError(err) || s.env.IsWorkflowCompleted())
	}
}

func (s *migratorTestSuite) TestMigrator_Parallelism() {
	require := testutil.Require(s.T())

	startSequence := int64(1000)
	endSequence := int64(6000)
	tag := uint32(1)
	eventTag := uint32(0)
	parallelism := 4

	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			// Verify parallelism is passed through
			require.Equal(parallelism, request.Parallelism)

			eventCount := request.EndEventSequence - request.StartEventSequence
			return &activity.MigratorResponse{
				BlocksMigrated: int(eventCount / 10),
				EventsMigrated: int(eventCount),
				Success:        true,
				Message:        "Parallel migration completed",
			}, nil
		})

	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		Parallelism:        parallelism,
	})
	require.NoError(err)
}

func (s *migratorTestSuite) TestMigrator_LargeMigration() {
	require := testutil.Require(s.T())

	startSequence := int64(1000000)
	endSequence := int64(2000000) // 1 million events
	tag := uint32(1)
	eventTag := uint32(0)

	batchCount := 0
	s.env.OnActivity(activity.ActivityMigrator, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.MigratorRequest) (*activity.MigratorResponse, error) {
			batchCount++

			// Each batch should be the configured batch size
			batchSize := request.EndEventSequence - request.StartEventSequence
			require.LessOrEqual(batchSize, int64(migratorBatchSize))

			// Simulate processing
			blockCount := batchSize / 10

			return &activity.MigratorResponse{
				BlocksMigrated: int(blockCount),
				EventsMigrated: int(batchSize),
				Success:        true,
				Message:        "Large batch processed",
			}, nil
		})

	// This should trigger checkpoints
	_, err := s.migrator.Execute(context.Background(), &MigratorRequest{
		StartEventSequence: startSequence,
		EndEventSequence:   endSequence,
		Tag:                tag,
		EventTag:           eventTag,
		BatchSize:          migratorBatchSize,
		CheckpointSize:     migratorCheckpointSize,
	})

	// Should hit checkpoint and continue-as-new
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	// Should have processed checkpoint size worth of events
	expectedBatches := int(migratorCheckpointSize / migratorBatchSize)
	require.Equal(expectedBatches, batchCount)
}
