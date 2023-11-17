package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type streamerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env         *cadence.TestEnv
	metaStorage *metastoragemocks.MockMetaStorage
	streamer    *Streamer
	config      *config.Config
	tag         uint32
	eventTag    uint32
	app         testapp.TestApp
	ctrl        *gomock.Controller
}

func TestStreamerTestSuite(t *testing.T) {
	suite.Run(t, new(streamerTestSuite))
}

func (s *streamerTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)

	s.env = cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewStreamer),
		fx.Provide(activity.NewStreamer),
		fx.Provide(activity.NewNopHeartbeater),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Populate(&s.config),
		fx.Populate(&s.streamer))
	s.tag = s.config.GetStableBlockTag()
	s.eventTag = s.config.Chain.EventTag.Stable
}

func (s *streamerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *streamerTestSuite) TestStreamer_HandleReorg_NoSkip() {
	// events: height 0-80 add, 81-90 has different hashes
	// blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	forkHeight := uint64(80)
	hashMismatchBlocks := uint64(10)
	maxEventId := int64(forkHeight + hashMismatchBlocks)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, int64(forkHeight), 0, forkHeight+1, s.tag,
	)
	hashMismatchEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, forkHeight+1, forkHeight+hashMismatchBlocks+1, s.tag,
		testutil.WithBlockHashFormat("streamer_TestHashMismatch0x%s"),
	)
	hashMismatchEvents[0].ParentHash = eventDDBEntries[len(eventDDBEntries)-1].BlockHash
	eventDDBEntries = append(eventDDBEntries, hashMismatchEvents...)
	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), gomock.Any()).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	seenEvents := make([]*model.BlockEvent, 0, maxHeight)
	s.metaStorage.EXPECT().AddEvents(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
			for _, event := range events {
				seenEvents = append(seenEvents, event)
			}
			return nil
		})
	streamerBatchSize := uint64(500)
	s.env.ExecuteWorkflow(s.streamer.execute, &StreamerRequest{
		BatchSize:             streamerBatchSize,
		CheckpointSize:        1,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
	require.Equal(int(streamerBatchSize+hashMismatchBlocks), len(seenEvents))
	for i, seenEvent := range seenEvents {
		if i < int(hashMismatchBlocks) {
			expectedEvent := model.NewBlockEventFromEventEntry(
				api.BlockchainEvent_BLOCK_REMOVED,
				hashMismatchEvents[len(hashMismatchEvents)-1-i],
			)
			require.Equal(expectedEvent, seenEvent)
		} else {
			expectedHeight := forkHeight + uint64(i) - hashMismatchBlocks + 1
			block := testutil.MakeBlockMetadata(expectedHeight, s.tag)
			expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
			require.Equal(expectedEvent, seenEvent)
		}
	}
}

func (s *streamerTestSuite) TestStreamer_HandleReorg_ForkBlockSkipped() {
	// TODO: re-enable once streamer can handle reorg correctly when the forkBlock is skipped
	s.T().Skip()

	//events: height 0-80 add, forkblock 80 is skipped, 81-90 has different hashes
	//blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	forkHeight := uint64(80)
	hashMismatchBlocks := uint64(10)
	maxEventId := int64(forkHeight + hashMismatchBlocks)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, int64(forkHeight), 0, forkHeight+1, s.tag,
	)
	hashMismatchEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, forkHeight+1, forkHeight+hashMismatchBlocks+1, s.tag,
		testutil.WithBlockHashFormat("streamer_TestHashMismatch0x%s"),
	)
	hashMismatchEvents[0].ParentHash = eventDDBEntries[len(eventDDBEntries)-1].BlockHash
	eventDDBEntries = append(eventDDBEntries, hashMismatchEvents...)
	// skipped blocks
	skippedHeights := []uint64{0, 3, 5, 6, 79, 80, 83, 87}
	for _, v := range skippedHeights {
		eventDDBEntries[v].ParentHash = ""
		eventDDBEntries[v].BlockHash = ""
		eventDDBEntries[v].BlockSkipped = true
	}

	// some blocks missing parent hash
	missingParentHashBlockIndexes := []int{7, 15, 81}
	for _, v := range missingParentHashBlockIndexes {
		eventDDBEntries[v].ParentHash = ""
	}

	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), gomock.Any()).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	seenEvents := make([]*model.BlockEvent, 0, maxHeight)
	s.metaStorage.EXPECT().AddEvents(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
			for _, event := range events {
				seenEvents = append(seenEvents, event)
			}
			return nil
		})
	streamerBatchSize := uint64(500)
	s.env.ExecuteWorkflow(s.streamer.execute, &StreamerRequest{
		BatchSize:             streamerBatchSize,
		CheckpointSize:        1,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
	require.Equal(int(streamerBatchSize+hashMismatchBlocks), len(seenEvents))
	for i, seenEvent := range seenEvents {
		if i < int(hashMismatchBlocks) {
			expectedEvent := model.NewBlockEventFromEventEntry(
				api.BlockchainEvent_BLOCK_REMOVED,
				hashMismatchEvents[len(hashMismatchEvents)-1-i],
			)
			require.Equal(expectedEvent, seenEvent)
		} else {
			expectedHeight := forkHeight + uint64(i) - hashMismatchBlocks + 1
			block := testutil.MakeBlockMetadata(expectedHeight, s.tag)
			expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
			require.Equal(expectedEvent, seenEvent)
		}
	}

}

func (s *streamerTestSuite) TestStreamer_NoReorg_WithSkippedBlocks() {
	//events: height 0-80 exists
	//blocks: new blocks 81 - 100
	require := testutil.Require(s.T())
	latestEventHeight := uint64(80)
	maxHeight := uint64(100)
	skippedHeights := []uint64{20, 21, 80, 81, 85, 86, 90, 100}
	skipped := make(map[uint64]bool, len(skippedHeights))
	for _, v := range skippedHeights {
		skipped[v] = true
	}

	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, int64(latestEventHeight), 0, latestEventHeight+1, s.tag,
	)
	for _, entry := range eventDDBEntries {
		if skipped[entry.BlockHeight] {
			entry.BlockSkipped = true
			entry.ParentHash = ""
			entry.BlockHash = ""
		}
	}

	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, s.tag)
	for _, block := range blocks {
		if skipped[block.Height] {
			block.Skipped = true
			block.ParentHash = ""
			block.Hash = ""
		}
	}
	populateParentHash(blocks)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(int64(latestEventHeight), nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	seenEvents := make([]*model.BlockEvent, 0, maxHeight)
	s.metaStorage.EXPECT().AddEvents(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
			for _, event := range events {
				require.Equal(skipped[event.GetBlockHeight()], event.GetBlockSkipped())
				seenEvents = append(seenEvents, event)
			}
			return nil
		})
	streamerBatchSize := uint64(500)
	s.env.ExecuteWorkflow(s.streamer.execute, &StreamerRequest{
		BatchSize:             streamerBatchSize,
		CheckpointSize:        1,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
	require.Equal(int(maxHeight-latestEventHeight), len(seenEvents))
}

func (s *streamerTestSuite) TestStreamer_NonDefaultEventTag() {
	// events: height 0-80 add, 81-90 has different hashes
	// blocks: height 1000
	require := testutil.Require(s.T())
	eventTag := uint32(1)
	maxHeight := uint64(1000)
	forkHeight := uint64(80)
	hashMismatchBlocks := uint64(10)
	maxEventId := int64(forkHeight + hashMismatchBlocks)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		eventTag, int64(forkHeight), 0, forkHeight+1, s.tag,
	)
	hashMismatchEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		eventTag, maxEventId, forkHeight+1, forkHeight+hashMismatchBlocks+1, s.tag,
		testutil.WithBlockHashFormat("streamer_TestHashMismatch0x%s"),
	)
	hashMismatchEvents[0].ParentHash = eventDDBEntries[len(eventDDBEntries)-1].BlockHash
	eventDDBEntries = append(eventDDBEntries, hashMismatchEvents...)
	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), eventTag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	seenEvents := make([]*model.BlockEvent, 0, maxHeight)
	s.metaStorage.EXPECT().AddEvents(gomock.Any(), eventTag, gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
			for _, event := range events {
				seenEvents = append(seenEvents, event)
			}
			return nil
		})
	streamerBatchSize := uint64(500)
	s.env.ExecuteWorkflow(s.streamer.execute, &StreamerRequest{
		BatchSize:             streamerBatchSize,
		CheckpointSize:        1,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              eventTag,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
	require.Equal(int(streamerBatchSize+hashMismatchBlocks), len(seenEvents))
	for i, seenEvent := range seenEvents {
		if i < int(hashMismatchBlocks) {
			expectedEvent := model.NewBlockEventFromEventEntry(
				api.BlockchainEvent_BLOCK_REMOVED,
				hashMismatchEvents[len(hashMismatchEvents)-1-i],
			)
			require.Equal(expectedEvent, seenEvent)
		} else {
			expectedHeight := forkHeight + uint64(i) - hashMismatchBlocks + 1
			block := testutil.MakeBlockMetadata(expectedHeight, s.tag)
			expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
			require.Equal(expectedEvent, seenEvent)
		}
	}
}

func (s *streamerTestSuite) TestStreamer_NonDefaultTag() {
	// events: height 0-80 add, 81-90 has different hashes
	// blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	forkHeight := uint64(80)
	hashMismatchBlocks := uint64(10)
	maxEventId := int64(forkHeight + hashMismatchBlocks)
	tag := s.tag + 10
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, int64(forkHeight), 0, forkHeight+1, tag,
	)
	hashMismatchEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, forkHeight+1, forkHeight+hashMismatchBlocks+1, tag,
		testutil.WithBlockHashFormat("streamer_TestHashMismatch0x%s"),
	)
	hashMismatchEvents[0].ParentHash = eventDDBEntries[len(eventDDBEntries)-1].BlockHash
	eventDDBEntries = append(eventDDBEntries, hashMismatchEvents...)
	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), gomock.Any()).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	seenEvents := make([]*model.BlockEvent, 0, maxHeight)
	s.metaStorage.EXPECT().AddEvents(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
			for _, event := range events {
				seenEvents = append(seenEvents, event)
			}
			return nil
		})
	streamerBatchSize := uint64(500)
	s.env.ExecuteWorkflow(s.streamer.execute, &StreamerRequest{
		BatchSize:             streamerBatchSize,
		CheckpointSize:        1,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		Tag:                   tag,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
	require.Equal(int(streamerBatchSize+hashMismatchBlocks), len(seenEvents))
	for i, seenEvent := range seenEvents {
		if i < int(hashMismatchBlocks) {
			expectedEvent := model.NewBlockEventFromEventEntry(
				api.BlockchainEvent_BLOCK_REMOVED,
				hashMismatchEvents[len(hashMismatchEvents)-1-i],
			)
			require.Equal(expectedEvent, seenEvent)
		} else {
			expectedHeight := forkHeight + uint64(i) - hashMismatchBlocks + 1
			block := testutil.MakeBlockMetadata(expectedHeight, tag)
			expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
			require.Equal(expectedEvent, seenEvent)
		}
	}
}

// TODO: refactor this into testutils
func populateParentHash(blocks []*api.BlockMetadata) {
	lastParentHash := ""
	lastParentHeight := uint64(0)
	for _, block := range blocks {
		block.ParentHash = lastParentHash
		block.ParentHeight = lastParentHeight

		if block.Hash != "" {
			lastParentHash = block.Hash
		}
		if !block.Skipped {
			lastParentHeight = block.Height
		}

	}
}

func (s *streamerTestSuite) TestStreamer_NoEventTagInRequest() {
	// events: height 0-80 add, 81-90 has different hashes
	// blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	forkHeight := uint64(80)
	hashMismatchBlocks := uint64(10)
	maxEventId := int64(forkHeight + hashMismatchBlocks)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, int64(forkHeight), 0, forkHeight+1, s.tag,
	)
	hashMismatchEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, forkHeight+1, forkHeight+hashMismatchBlocks+1, s.tag,
		testutil.WithBlockHashFormat("streamer_TestHashMismatch0x%s"),
	)
	hashMismatchEvents[0].ParentHash = eventDDBEntries[len(eventDDBEntries)-1].BlockHash
	eventDDBEntries = append(eventDDBEntries, hashMismatchEvents...)
	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), s.eventTag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	seenEvents := make([]*model.BlockEvent, 0, maxHeight)
	s.metaStorage.EXPECT().AddEvents(gomock.Any(), s.eventTag, gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
			for _, event := range events {
				seenEvents = append(seenEvents, event)
			}
			return nil
		})
	streamerBatchSize := uint64(500)
	s.env.ExecuteWorkflow(s.streamer.execute, &StreamerRequest{
		BatchSize:             streamerBatchSize,
		CheckpointSize:        1,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
	require.Equal(int(streamerBatchSize+hashMismatchBlocks), len(seenEvents))
	for i, seenEvent := range seenEvents {
		if i < int(hashMismatchBlocks) {
			expectedEvent := model.NewBlockEventFromEventEntry(
				api.BlockchainEvent_BLOCK_REMOVED,
				hashMismatchEvents[len(hashMismatchEvents)-1-i],
			)
			require.Equal(expectedEvent, seenEvent)
		} else {
			expectedHeight := forkHeight + uint64(i) - hashMismatchBlocks + 1
			block := testutil.MakeBlockMetadata(expectedHeight, s.tag)
			expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
			require.Equal(expectedEvent, seenEvent)
		}
	}
}
