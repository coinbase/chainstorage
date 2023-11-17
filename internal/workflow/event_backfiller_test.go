package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	eventBatchSize      = 25
	eventCheckpointSize = 100
)

type eventBackfillerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env              *cadence.TestEnv
	ctrl             *gomock.Controller
	metadataAccessor *metastoragemocks.MockMetaStorage
	eventBackfiller  *EventBackfiller
	app              testapp.TestApp
	cfg              *config.Config
}

func TestEventBackfillerTestSuite(t *testing.T) {
	suite.Run(t, new(eventBackfillerTestSuite))
}

func (s *eventBackfillerTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	// Override config to speed up the test.
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.EventBackfiller.BatchSize = eventBatchSize
	cfg.Workflows.EventBackfiller.CheckpointSize = eventCheckpointSize
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.metadataAccessor = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metadataAccessor
		}),
		fx.Provide(dlq.NewNop),
		fx.Populate(&s.eventBackfiller),
	)
}

func (s *eventBackfillerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_Success() {
	require := testutil.Require(s.T())

	const (
		startSequence = 16000000
		endSequence   = 16000003
	)
	lastEvent := &model.EventEntry{
		EventId:     15999999,
		BlockHash:   "a",
		ParentHash:  "p",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	eventData := make([]*model.EventEntry, 3)
	s.env.OnActivity(activity.ActivityEventReader, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, request *activity.EventReaderRequest) (*activity.EventReaderResponse, error) {
			if request.StartSequence == startSequence-1 {
				return &activity.EventReaderResponse{
					Eventdata: []*model.EventEntry{lastEvent},
				}, nil
			} else {
				return &activity.EventReaderResponse{
					Eventdata: eventData,
				}, nil
			}
		}).Times(2)
	reconciledEvents := make([]*model.EventEntry, 3)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     16000000,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     16000001,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 3,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:     16000002,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 4,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}

	s.env.OnActivity(activity.ActivityEventReconciler, mock.Anything, mock.Anything).Once().
		Return(&activity.EventReconcilerResponse{
			Eventdata: reconciledEvents,
		}, nil)
	s.env.OnActivity(activity.ActivityEventLoader, mock.Anything, mock.Anything).Once().
		Return(&activity.EventLoaderResponse{}, nil)

	_, err := s.eventBackfiller.Execute(context.Background(), &EventBackfillerRequest{
		Tag:                 1,
		EventTag:            1,
		UpgradeFromEventTag: 0,
		StartSequence:       startSequence,
		EndSequence:         endSequence,
		BatchSize:           eventBatchSize,
		CheckpointSize:      eventCheckpointSize,
	})

	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_SkippedEvents_Success() {
	require := testutil.Require(s.T())

	const (
		startSequence = 16000000
		endSequence   = 16000005
	)
	lastEvent := &model.EventEntry{
		EventId:     15999999,
		BlockHash:   "a",
		ParentHash:  "p",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	eventData := make([]*model.EventEntry, 5)
	s.env.OnActivity(activity.ActivityEventReader, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, request *activity.EventReaderRequest) (*activity.EventReaderResponse, error) {
			if request.StartSequence == startSequence-1 {
				return &activity.EventReaderResponse{
					Eventdata: []*model.EventEntry{lastEvent},
				}, nil
			} else {
				return &activity.EventReaderResponse{
					Eventdata: eventData,
				}, nil
			}
		}).Times(2)
	reconciledEvents := make([]*model.EventEntry, 5)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     16000000,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:      16000001,
		BlockHash:    "reorg",
		ParentHash:   "b",
		BlockHeight:  3,
		BlockSkipped: true,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		Tag:          1,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:      16000002,
		BlockHash:    "reorg",
		ParentHash:   "b",
		BlockHeight:  3,
		BlockSkipped: true,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		Tag:          1,
	}
	reconciledEvents[3] = &model.EventEntry{
		EventId:     16000003,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 3,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[4] = &model.EventEntry{
		EventId:     16000004,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 4,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}

	s.env.OnActivity(activity.ActivityEventReconciler, mock.Anything, mock.Anything).Once().
		Return(&activity.EventReconcilerResponse{
			Eventdata: reconciledEvents,
		}, nil)
	s.env.OnActivity(activity.ActivityEventLoader, mock.Anything, mock.Anything).Once().
		Return(&activity.EventLoaderResponse{}, nil)

	_, err := s.eventBackfiller.Execute(context.Background(), &EventBackfillerRequest{
		Tag:                 2,
		EventTag:            1,
		UpgradeFromEventTag: 0,
		StartSequence:       startSequence,
		EndSequence:         endSequence,
		BatchSize:           eventBatchSize,
		CheckpointSize:      eventCheckpointSize,
	})

	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_CheckpointReached_Err() {
	require := testutil.Require(s.T())

	const (
		startSequence = 16000000
		endSequence   = startSequence + checkpointSize + 1
	)
	lastEvent := &model.EventEntry{
		EventId:      15999999,
		BlockHash:    "a",
		ParentHash:   "p",
		BlockHeight:  1,
		BlockSkipped: true,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
	}
	eventData := make([]*model.EventEntry, 25)
	s.env.OnActivity(activity.ActivityEventReader, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, request *activity.EventReaderRequest) (*activity.EventReaderResponse, error) {
			if request.StartSequence == startSequence-1 {
				return &activity.EventReaderResponse{
					Eventdata: []*model.EventEntry{lastEvent},
				}, nil
			} else {
				return &activity.EventReaderResponse{
					Eventdata: eventData,
				}, nil
			}
		}).Times(5)
	var reconciledEvents []*model.EventEntry
	for i := 0; i < 25; i++ {
		reconciledEvents = append(reconciledEvents, &model.EventEntry{
			EventId:      16000001,
			BlockSkipped: true,
			EventType:    api.BlockchainEvent_BLOCK_ADDED,
		})
	}
	s.env.OnActivity(activity.ActivityEventReconciler, mock.Anything, mock.Anything).Times(4).
		Return(&activity.EventReconcilerResponse{
			Eventdata: reconciledEvents,
		}, nil)
	s.env.OnActivity(activity.ActivityEventLoader, mock.Anything, mock.Anything).Times(4).
		Return(&activity.EventLoaderResponse{}, nil)

	_, err := s.eventBackfiller.Execute(context.Background(), &EventBackfillerRequest{
		Tag:                 1,
		EventTag:            1,
		UpgradeFromEventTag: 0,
		StartSequence:       startSequence,
		EndSequence:         endSequence,
		BatchSize:           eventBatchSize,
		CheckpointSize:      eventCheckpointSize,
	})

	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_NoEndSequence_Err() {
	require := testutil.Require(s.T())

	const (
		startSequence = 16000000
	)
	_, err := s.eventBackfiller.Execute(context.Background(), &EventBackfillerRequest{
		Tag:                 1,
		EventTag:            1,
		UpgradeFromEventTag: 0,
		StartSequence:       startSequence,
		BatchSize:           eventBatchSize,
		CheckpointSize:      eventCheckpointSize,
	})

	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_StartEndSequenceEqual_Err() {
	require := testutil.Require(s.T())

	const (
		startSequence = 16000000
	)
	_, err := s.eventBackfiller.Execute(context.Background(), &EventBackfillerRequest{
		Tag:                 1,
		EventTag:            1,
		UpgradeFromEventTag: 0,
		StartSequence:       startSequence,
		EndSequence:         startSequence,
		BatchSize:           eventBatchSize,
		CheckpointSize:      eventCheckpointSize,
	})

	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_NilLastEvent() {
	require := testutil.Require(s.T())

	reconciledEvents := make([]*model.EventEntry, 3)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     16000000,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     16000001,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 3,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:     16000002,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 4,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}

	err := validateEventBlocks(reconciledEvents, nil)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_NilEvents() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:     15999999,
		BlockHash:   "a",
		ParentHash:  "p",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}

	err := validateEventBlocks(nil, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_WithoutRemovedEvents() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:     15999999,
		BlockHash:   "a",
		ParentHash:  "p",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}

	reconciledEvents := make([]*model.EventEntry, 3)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     16000000,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     16000001,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 3,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:     16000002,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 4,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_LastEventRemoved() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:     15999999,
		BlockHash:   "a",
		ParentHash:  "p",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_REMOVED,
	}

	reconciledEvents := make([]*model.EventEntry, 3)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     16000000,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     16000001,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:     16000002,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 3,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_FirstEventRemoved() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:      15999999,
		BlockHash:    "a",
		ParentHash:   "p",
		BlockHeight:  1,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		Tag:          1,
		BlockSkipped: true,
	}

	reconciledEvents := make([]*model.EventEntry, 3)
	reconciledEvents[0] = &model.EventEntry{
		EventId:      16000000,
		BlockHash:    "a",
		ParentHash:   "p",
		BlockHeight:  1,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		Tag:          1,
		BlockSkipped: true,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     16000001,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:     16000002,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_LastEventInListRemoved() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:     15999999,
		BlockHash:   "a",
		ParentHash:  "p",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}

	reconciledEvents := make([]*model.EventEntry, 3)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     16000000,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:      16000001,
		BlockHash:    "c",
		ParentHash:   "b",
		BlockHeight:  3,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		Tag:          1,
		BlockSkipped: true,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:      16000002,
		BlockHash:    "c",
		ParentHash:   "b",
		BlockHeight:  3,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		Tag:          1,
		BlockSkipped: true,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_AllEventsRemoved() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:     15999999,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 4,
		EventType:   api.BlockchainEvent_BLOCK_REMOVED,
	}

	reconciledEvents := make([]*model.EventEntry, 3)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     16000000,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 3,
		EventType:   api.BlockchainEvent_BLOCK_REMOVED,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     16000001,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 2,
		EventType:   api.BlockchainEvent_BLOCK_REMOVED,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:     16000002,
		BlockHash:   "a",
		ParentHash:  "p",
		BlockHeight: 1,
		EventType:   api.BlockchainEvent_BLOCK_REMOVED,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_PolygonBackfillReplicate() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:      26083913,
		BlockHash:    "0x58d22fc5c658a003564469499dc531baf75cab0b761fb989b97636122b423791",
		ParentHash:   "c",
		BlockHeight:  26081932,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: false,
		Tag:          2,
	}

	reconciledEvents := make([]*model.EventEntry, 6)
	reconciledEvents[0] = &model.EventEntry{
		EventId:      26083914,
		BlockHash:    "0x91544c19fd075f20eb449a8629ef9b1f0141a95fde491a613e7269d3a5eab8aa",
		ParentHash:   "0x58d22fc5c658a003564469499dc531baf75cab0b761fb989b97636122b423791",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: true,
		Tag:          1,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:      26083915,
		BlockHash:    "0x1d3d7961ba080264324ad6f54718b3fb1fedc51121d893bb4a72ba3e351623ec",
		ParentHash:   "0x91544c19fd075f20eb449a8629ef9b1f0141a95fde491a613e7269d3a5eab8aa",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: true,
		Tag:          1,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:      26083916,
		BlockHash:    "0x1d3d7961ba080264324ad6f54718b3fb1fedc51121d893bb4a72ba3e351623ec",
		ParentHash:   "0x91544c19fd075f20eb449a8629ef9b1f0141a95fde491a613e7269d3a5eab8aa",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          1,
	}
	reconciledEvents[3] = &model.EventEntry{
		EventId:      26083917,
		BlockHash:    "0x91544c19fd075f20eb449a8629ef9b1f0141a95fde491a613e7269d3a5eab8aa",
		ParentHash:   "0x58d22fc5c658a003564469499dc531baf75cab0b761fb989b97636122b423791",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          1,
	}
	reconciledEvents[4] = &model.EventEntry{
		EventId:      26083918,
		BlockHash:    "0x91544c19fd075f20eb449a8629ef9b1f0141a95fde491a613e7269d3a5eab8aa",
		ParentHash:   "0x58d22fc5c658a003564469499dc531baf75cab0b761fb989b97636122b423791",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: false,
		Tag:          2,
	}
	reconciledEvents[5] = &model.EventEntry{
		EventId:      26083919,
		BlockHash:    "0x1d3d7961ba080264324ad6f54718b3fb1fedc51121d893bb4a72ba3e351623ec",
		ParentHash:   "0x91544c19fd075f20eb449a8629ef9b1f0141a95fde491a613e7269d3a5eab8aa",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: false,
		Tag:          2,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_FalsePositveReorgs_Success() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:      26083913,
		BlockHash:    "b",
		ParentHash:   "a",
		BlockHeight:  26081932,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: false,
		Tag:          2,
	}

	reconciledEvents := make([]*model.EventEntry, 6)
	reconciledEvents[0] = &model.EventEntry{
		EventId:      26083914,
		BlockHash:    "c",
		ParentHash:   "b",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: true,
		Tag:          2,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:      26083915,
		BlockHash:    "d",
		ParentHash:   "c",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: true,
		Tag:          2,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:      26083916,
		BlockHash:    "d",
		ParentHash:   "c",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          2,
	}
	reconciledEvents[3] = &model.EventEntry{
		EventId:      26083917,
		BlockHash:    "c",
		ParentHash:   "b",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          2,
	}
	reconciledEvents[4] = &model.EventEntry{
		EventId:      26083918,
		BlockHash:    "c",
		ParentHash:   "b",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: false,
		Tag:          2,
	}
	reconciledEvents[5] = &model.EventEntry{
		EventId:      26083919,
		BlockHash:    "d",
		ParentHash:   "c",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockSkipped: false,
		Tag:          2,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.NoError(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_FalsePositveReorgs_TagError() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:     26083913,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 26081932,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}

	reconciledEvents := make([]*model.EventEntry, 6)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     26083914,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 26081933,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     26083915,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 26081934,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:      26083916,
		BlockHash:    "d",
		ParentHash:   "c",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          1,
	}
	reconciledEvents[3] = &model.EventEntry{
		EventId:      26083917,
		BlockHash:    "c",
		ParentHash:   "b",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          1,
	}
	reconciledEvents[4] = &model.EventEntry{
		EventId:     26083918,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 26081933,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[5] = &model.EventEntry{
		EventId:     26083919,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 26081934,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.Error(err)
}

func (s *eventBackfillerTestSuite) TestEventBackfiller_ValidateEventBlocks_FalsePositveReorgs_SkippedError() {
	require := testutil.Require(s.T())

	lastEvent := &model.EventEntry{
		EventId:     26083913,
		BlockHash:   "b",
		ParentHash:  "a",
		BlockHeight: 26081932,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}

	reconciledEvents := make([]*model.EventEntry, 6)
	reconciledEvents[0] = &model.EventEntry{
		EventId:     26083914,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 26081933,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[1] = &model.EventEntry{
		EventId:     26083915,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 26081934,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[2] = &model.EventEntry{
		EventId:      26083916,
		BlockHash:    "d",
		ParentHash:   "c",
		BlockHeight:  26081934,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          2,
	}
	reconciledEvents[3] = &model.EventEntry{
		EventId:      26083917,
		BlockHash:    "c",
		ParentHash:   "b",
		BlockHeight:  26081933,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockSkipped: true,
		Tag:          2,
	}
	reconciledEvents[4] = &model.EventEntry{
		EventId:     26083918,
		BlockHash:   "c",
		ParentHash:  "b",
		BlockHeight: 26081933,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}
	reconciledEvents[5] = &model.EventEntry{
		EventId:     26083919,
		BlockHash:   "d",
		ParentHash:  "c",
		BlockHeight: 26081934,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		Tag:         2,
	}

	err := validateEventBlocks(reconciledEvents, lastEvent)
	require.Error(err)
}
