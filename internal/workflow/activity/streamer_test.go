package activity

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type StreamerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	ctrl        *gomock.Controller
	metaStorage *metastoragemocks.MockMetaStorage
	app         testapp.TestApp
	config      *config.Config
	tag         uint32
	eventTag    uint32
	streamer    *Streamer
	logger      *zap.Logger
	env         *cadence.TestEnv
}

func TestStreamerTestSuite(t *testing.T) {
	suite.Run(t, new(StreamerTestSuite))
}

func (s *StreamerTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.env = cadence.NewTestActivityEnv(s)
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewStreamer),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Populate(&s.config),
		fx.Populate(&s.streamer),
		fx.Populate(&s.logger))
	s.tag = s.config.GetStableBlockTag()
	s.eventTag = uint32(0)
}

func (s *StreamerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *StreamerTestSuite) TestHandleReorgNoOp() {
	require := testutil.Require(s.T())
	maxHeight := uint64(100)
	maxEventId := int64(120)
	// only make one event to make sure we do not check event beyond it
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, maxHeight, maxHeight+1, s.tag,
	)
	// only make one block to make sure we do not check block beyond it
	blocks := testutil.MakeBlockMetadatasFromStartHeight(maxHeight, 1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(),
		s.eventTag,
		maxEventId+1-streamerBatchGetSize,
		maxEventId+1,
	).Times(1).Return(eventDDBEntries, nil)

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag,
		maxHeight+1-streamerBatchGetSize, maxHeight+1,
	).Times(1).Return(blocks, nil)

	result, err := s.streamer.handleReorg(context.TODO(), s.logger, &StreamerRequest{
		Tag:                   s.config.GetStableBlockTag(),
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(maxHeight, result.forkBlock.Height)
	require.Equal(maxHeight, result.canonicalChainTipHeight)
	require.Equal(0, len(result.updateEvents))
	require.Equal(s.eventTag, result.eventTag)
}

func (s *StreamerTestSuite) TestHandleReorgLowerMetaTip() {
	require := testutil.Require(s.T())
	maxHeight := uint64(100)
	maxEventId := int64(120)
	expectedReorg := uint64(10)
	// only make one block to make sure we do not check block beyond it
	blocks := testutil.MakeBlockMetadatasFromStartHeight(maxHeight, 1, s.tag)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag,
		maxEventId-int64(expectedReorg),
		maxHeight,
		maxHeight+expectedReorg+1,
		s.tag,
	)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(),
		s.eventTag,
		maxEventId+1-streamerBatchGetSize,
		maxEventId+1,
	).Times(1).Return(eventDDBEntries, nil)

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(),
		s.tag, maxHeight+1-streamerBatchGetSize, maxHeight+1,
	).Times(1).Return(blocks, nil)

	result, err := s.streamer.handleReorg(context.TODO(), s.logger, &StreamerRequest{
		Tag:                   s.config.GetStableBlockTag(),
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(maxHeight, result.forkBlock.Height)
	require.Equal(maxHeight, result.canonicalChainTipHeight)
	require.Equal(int(expectedReorg), len(result.updateEvents))
	require.Equal(s.eventTag, result.eventTag)
	for i, event := range result.updateEvents {
		expectedEvent := model.NewBlockEventFromEventEntry(
			api.BlockchainEvent_BLOCK_REMOVED,
			eventDDBEntries[len(eventDDBEntries)-1-i],
		)
		require.Equal(expectedEvent, event)
	}
}

func (s *StreamerTestSuite) TestHandleReorgHigherMetaTip() {
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	maxEventHeight := uint64(60)
	maxEventId := int64(100)
	// only make one event to make sure we do not check event beyond it
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, maxEventHeight, maxEventHeight+1, s.tag,
	)
	// only make one block to make sure we do not check block beyond it
	blocks := testutil.MakeBlockMetadatasFromStartHeight(maxEventHeight, 1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(),
		s.eventTag, maxEventId+1-streamerBatchGetSize, maxEventId+1,
	).Times(1).Return(eventDDBEntries, nil)

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(),
		s.tag, maxEventHeight+1-streamerBatchGetSize, maxEventHeight+1,
	).Times(1).Return(blocks, nil)

	result, err := s.streamer.handleReorg(context.TODO(), s.logger, &StreamerRequest{
		Tag:                   s.config.GetStableBlockTag(),
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(maxEventHeight, result.forkBlock.Height)
	require.Equal(maxHeight, result.canonicalChainTipHeight)
	require.Equal(0, len(result.updateEvents))
}

func (s *StreamerTestSuite) TestHandleReorgMixEventsNoOps() {
	// events: height 0-90 add, then remove from 81-90
	// blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	forkHeight := uint64(80)
	removedBlocks := uint64(10)
	maxEventId := int64(110)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, 0, forkHeight+removedBlocks+1, s.tag,
	)
	removeEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_REMOVED,
		s.eventTag, maxEventId, forkHeight+1, forkHeight+removedBlocks+1, s.tag,
	)
	// append remove events in the reverse order
	for i := len(removeEvents) - 1; i >= 0; i-- {
		removeEvents[i].EventId = eventDDBEntries[len(eventDDBEntries)-1].EventId + 1
		eventDDBEntries = append(eventDDBEntries, removeEvents[i])
	}

	// only make one block to make sure we do not check block beyond it
	blocks := testutil.MakeBlockMetadatasFromStartHeight(forkHeight, 1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(),
		s.eventTag, maxEventId+1-streamerBatchGetSize, maxEventId+1,
	).Times(1).Return(eventDDBEntries, nil)

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(),
		s.tag, forkHeight+1-streamerBatchGetSize, forkHeight+1,
	).Times(1).Return(blocks, nil)

	result, err := s.streamer.handleReorg(context.TODO(), s.logger, &StreamerRequest{
		Tag:                   s.config.GetStableBlockTag(),
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(forkHeight, result.forkBlock.Height)
	require.Equal(maxHeight, result.canonicalChainTipHeight)
	require.Equal(0, len(result.updateEvents))
}

func (s *StreamerTestSuite) TestHandleReorgHashMismatchHashes() {
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
		testutil.WithBlockHashFormat("streamer_TestHandleReorgHashMismatch0x%s"),
	)
	hashMismatchEvents[0].ParentHash = eventDDBEntries[len(eventDDBEntries)-1].BlockHash
	eventDDBEntries = append(eventDDBEntries, hashMismatchEvents...)

	blocks := testutil.MakeBlockMetadatasFromStartHeight(forkHeight, int(hashMismatchBlocks)+1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(),
		s.eventTag, maxEventId+1-streamerBatchGetSize, maxEventId+1,
	).Times(1).Return(eventDDBEntries, nil)

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(),
		s.tag, forkHeight+hashMismatchBlocks+1-streamerBatchGetSize, forkHeight+hashMismatchBlocks+1,
	).Times(1).Return(blocks, nil)

	result, err := s.streamer.handleReorg(context.TODO(), s.logger, &StreamerRequest{
		Tag:                   s.config.GetStableBlockTag(),
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(forkHeight, result.forkBlock.Height)
	require.Equal(maxHeight, result.canonicalChainTipHeight)
	require.Equal(int(hashMismatchBlocks), len(result.updateEvents))
	for i, event := range result.updateEvents {
		expectedEvent := model.NewBlockEventFromEventEntry(
			api.BlockchainEvent_BLOCK_REMOVED,
			hashMismatchEvents[len(hashMismatchEvents)-1-i],
		)
		require.Equal(expectedEvent, event)
	}
}

func (s *StreamerTestSuite) TestGetHeadEvent() {
	// events: height 0-1000 add, then remove from 991-1000, re-add 991-995
	require := testutil.Require(s.T())
	forkHeight := uint64(1000)
	removedBlocks := uint64(10)
	reAddedBlocks := uint64(5)
	maxEventId := int64(forkHeight + removedBlocks*2 + reAddedBlocks)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, int64(forkHeight+removedBlocks), 0, forkHeight+removedBlocks+1, s.tag,
	)
	removeEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_REMOVED,
		s.eventTag, int64(forkHeight+removedBlocks*2), forkHeight+1, forkHeight+removedBlocks+1, s.tag,
	)
	// append remove events in the reverse order
	for i := len(removeEvents) - 1; i >= 0; i-- {
		eventDDBEntries = append(eventDDBEntries, removeEvents[i])
	}
	reAddedEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, forkHeight+1, forkHeight+reAddedBlocks+1, s.tag,
	)
	eventDDBEntries = append(eventDDBEntries, reAddedEvents...)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), s.eventTag, gomock.Any(), gomock.Any(),
	).Times(int(math.Ceil(float64(len(eventDDBEntries)) / streamerBatchGetSize))).DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})
	minEventIdFetched := maxEventId + 1
	eventsToChainAdaptor := metastorage.NewEventsToChainAdaptor()
	for i := int64(forkHeight + reAddedBlocks); i >= metastorage.EventIdStartValue; i-- {
		event, err := s.streamer.getEventForTailBlock(context.TODO(), s.eventTag, &minEventIdFetched, eventsToChainAdaptor)
		require.NoError(err)
		require.Equal(uint64(i), event.BlockHeight)
	}
	require.Equal(metastorage.EventIdStartValue, minEventIdFetched)
	// should not go below event id 0
	_, err := s.streamer.getEventForTailBlock(context.TODO(), s.eventTag, &minEventIdFetched, eventsToChainAdaptor)
	require.Error(err)
}

func (s *StreamerTestSuite) TestGetHeadEvent_NonDefaultEventTag() {
	// events: height 0-1000 add, then remove from 991-1000, re-add 991-995
	require := testutil.Require(s.T())
	eventTag := uint32(1)
	forkHeight := uint64(1000)
	removedBlocks := uint64(10)
	reAddedBlocks := uint64(5)
	maxEventId := int64(forkHeight + removedBlocks*2 + reAddedBlocks)
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		eventTag, int64(forkHeight+removedBlocks), 0, forkHeight+removedBlocks+1, s.tag,
	)
	removeEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_REMOVED,
		eventTag, int64(forkHeight+removedBlocks*2), forkHeight+1, forkHeight+removedBlocks+1, s.tag,
	)
	// append remove events in the reverse order
	for i := len(removeEvents) - 1; i >= 0; i-- {
		eventDDBEntries = append(eventDDBEntries, removeEvents[i])
	}
	reAddedEvents := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		eventTag, maxEventId, forkHeight+1, forkHeight+reAddedBlocks+1, s.tag,
	)
	eventDDBEntries = append(eventDDBEntries, reAddedEvents...)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), eventTag, gomock.Any(), gomock.Any(),
	).Times(int(math.Ceil(float64(len(eventDDBEntries)) / streamerBatchGetSize))).DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return eventDDBEntries[minEventId:maxEventId], nil
		})
	minEventIdFetched := maxEventId + 1
	eventsToChainAdaptor := metastorage.NewEventsToChainAdaptor()
	for i := int64(forkHeight + reAddedBlocks); i >= metastorage.EventIdStartValue; i-- {
		event, err := s.streamer.getEventForTailBlock(context.TODO(), eventTag, &minEventIdFetched, eventsToChainAdaptor)
		require.NoError(err)
		require.Equal(uint64(i), event.BlockHeight)
	}
	require.Equal(metastorage.EventIdStartValue, minEventIdFetched)
	// should not go below event id 0
	_, err := s.streamer.getEventForTailBlock(context.TODO(), eventTag, &minEventIdFetched, eventsToChainAdaptor)
	require.Error(err)
}

func (s *StreamerTestSuite) TestGetHeadBlock() {
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	// blocks from height 0 - maxHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight+1), s.tag)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag,
		gomock.Any(), gomock.Any(),
	).Times(int(math.Ceil(float64(len(blocks)) / streamerBatchGetSize))).DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	minBlockHeightFetched := maxHeight + 1
	var minBlockFetchedParentHash string
	blockList := list.New()
	for i := int(maxHeight); i >= 0; i-- {
		block, err := s.streamer.getTailBlock(context.TODO(), s.tag, &minBlockHeightFetched, &minBlockFetchedParentHash, blockList)
		require.NoError(err)
		require.Equal(uint64(i), block.Height)
	}
	require.Equal(uint64(0), minBlockHeightFetched)
	// should not go below block height 0
	_, err := s.streamer.getTailBlock(context.TODO(), s.tag, &minBlockHeightFetched, &minBlockFetchedParentHash, blockList)
	require.Error(err)
}

func (s *StreamerTestSuite) TestGetHeadBlockRaceCondition() {
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	// blocks from height 0 - maxHeight
	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight+1), s.tag)
	// modify the hash at height maxHeight+1-streamerBatchGetSize to create race condition
	badHashHeight := int(maxHeight - streamerBatchGetSize)
	blocks[badHashHeight].Hash = "bad_hash"
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag,
		gomock.Any(), gomock.Any(),
	).Times(2).DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocks[startHeight:endHeight], nil
		})

	minBlockHeightFetched := maxHeight + 1
	var minBlockFetchedParentHash string
	blockList := list.New()
	for i := int(maxHeight); i >= badHashHeight; i-- {
		block, err := s.streamer.getTailBlock(context.TODO(), s.tag, &minBlockHeightFetched, &minBlockFetchedParentHash, blockList)
		if i > badHashHeight {
			require.NoError(err)
			require.Equal(uint64(i), block.Height)
		} else {
			require.Error(err)
		}
	}
}

func (s *StreamerTestSuite) TestExecuteReorg() {
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
		testutil.WithBlockHashFormat("streamer_TestHandleReorgHashMismatch0x%s"),
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
	response, err := s.streamer.Execute(s.env.BackgroundContext(), &StreamerRequest{
		Tag:                   s.tag,
		BatchSize:             streamerBatchSize,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(int(streamerBatchSize+hashMismatchBlocks), len(seenEvents))
	require.NotEmpty(response.TimeSinceLastBlock)
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
	require.Equal(streamerBatchSize+forkHeight, response.LatestStreamedHeight)
	require.Equal(maxHeight-forkHeight, response.Gap)
}

func (s *StreamerTestSuite) TestExecuteReorg_NonDefaultEventTag() {
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
		testutil.WithBlockHashFormat("streamer_TestHandleReorgHashMismatch0x%s"),
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
	response, err := s.streamer.Execute(s.env.BackgroundContext(), &StreamerRequest{
		Tag:                   s.tag,
		BatchSize:             streamerBatchSize,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              eventTag,
	})
	require.NoError(err)
	require.Equal(int(streamerBatchSize+hashMismatchBlocks), len(seenEvents))
	require.NotEmpty(response.TimeSinceLastBlock)
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
	require.Equal(streamerBatchSize+forkHeight, response.LatestStreamedHeight)
	require.Equal(maxHeight-forkHeight, response.Gap)
}

func (s *StreamerTestSuite) TestExecuteNoEventHistory() {
	// events: no event history
	// blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)

	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(int64(0), storage.ErrNoEventHistory)
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
	response, err := s.streamer.Execute(s.env.BackgroundContext(), &StreamerRequest{
		Tag:                   s.tag,
		BatchSize:             streamerBatchSize,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(int(streamerBatchSize), len(seenEvents))
	require.NotEmpty(response.TimeSinceLastBlock)
	for i, seenEvent := range seenEvents {
		expectedHeight := s.config.Chain.BlockStartHeight + uint64(i)
		block := testutil.MakeBlockMetadata(expectedHeight, s.tag)
		expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
		require.Equal(expectedEvent, seenEvent)
	}
	require.Equal(s.config.Chain.BlockStartHeight+streamerBatchSize-1, response.LatestStreamedHeight)
}

func (s *StreamerTestSuite) TestExecuteNoEventHistory_NonDefaultEventTag() {
	// events: no event history
	// blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)
	eventTag := uint32(1)

	blocks := testutil.MakeBlockMetadatasFromStartHeight(0, int(maxHeight)+1, s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), eventTag).Times(1).Return(int64(0), storage.ErrNoEventHistory)
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
	response, err := s.streamer.Execute(s.env.BackgroundContext(), &StreamerRequest{
		Tag:                   s.tag,
		BatchSize:             streamerBatchSize,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              eventTag,
	})
	require.NoError(err)
	require.Equal(int(streamerBatchSize), len(seenEvents))
	require.NotEmpty(response.TimeSinceLastBlock)
	require.Equal(response.EventTag, eventTag)
	for i, seenEvent := range seenEvents {
		expectedHeight := s.config.Chain.BlockStartHeight + uint64(i)
		block := testutil.MakeBlockMetadata(expectedHeight, s.tag)
		expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
		require.Equal(expectedEvent, seenEvent)
	}
	require.Equal(s.config.Chain.BlockStartHeight+streamerBatchSize-1, response.LatestStreamedHeight)
}

func (s *StreamerTestSuite) TestExecuteRaceCondition() {
	// events: no event history
	// blocks: height 1000
	require := testutil.Require(s.T())
	maxHeight := uint64(1000)

	blocks := testutil.MakeBlockMetadatasFromStartHeight(s.config.Chain.BlockStartHeight, int(maxHeight)+1, s.tag)
	// modify the hash at height maxHeight+1-streamerBatchGetSize to create race condition
	badHashHeight := s.config.Chain.BlockStartHeight + 20
	blocks[badHashHeight].Hash = "bad_hash"

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(int64(0), storage.ErrNoEventHistory)
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
	_, err := s.streamer.Execute(s.env.BackgroundContext(), &StreamerRequest{
		Tag:                   s.tag,
		BatchSize:             streamerBatchSize,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.Error(err)
	require.Equal(0, len(seenEvents))
	for i, seenEvent := range seenEvents {
		expectedHeight := s.config.Chain.BlockStartHeight + uint64(i)
		block := testutil.MakeBlockMetadata(expectedHeight, s.tag)
		expectedEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
		require.Equal(expectedEvent, seenEvent)
	}
}

func (s *StreamerTestSuite) TestExecuteFullyCaughtUp() {
	require := testutil.Require(s.T())
	maxHeight := uint64(100)
	maxEventId := int64(120)
	// only make one event to make sure we do not check event beyond it
	eventDDBEntries := testutil.MakeBlockEventEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		s.eventTag, maxEventId, maxHeight, maxHeight+1, s.tag,
	)
	blocks := testutil.MakeBlockMetadatasFromStartHeight(s.config.Chain.BlockStartHeight, int(maxHeight+1-s.config.Chain.BlockStartHeight), s.tag)

	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).Times(1).Return(maxEventId, nil)
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(),
		s.eventTag,
		maxEventId+1-streamerBatchGetSize,
		maxEventId+1,
	).Times(1).Return(eventDDBEntries, nil)

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).Times(1).
		Return(testutil.MakeBlockMetadata(maxHeight, s.tag), nil)
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			if startHeight >= endHeight {
				return nil, storage.ErrOutOfRange
			}
			return blocks[startHeight-s.config.Chain.BlockStartHeight : endHeight-s.config.Chain.BlockStartHeight], nil
		})

	streamerBatchSize := uint64(500)
	response, err := s.streamer.Execute(s.env.BackgroundContext(), &StreamerRequest{
		Tag:                   s.tag,
		BatchSize:             streamerBatchSize,
		MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
		EventTag:              s.eventTag,
	})
	require.NoError(err)
	require.Equal(maxHeight, response.LatestStreamedHeight)
	require.Equal(uint64(0), response.Gap)
	require.Empty(response.TimeSinceLastBlock)
}

func (s *StreamerTestSuite) TestExecuteReorgMultipleRounds() {
	// make random reorg on the existing chain at each trail,
	// then at the very end, when we reconstruct the chain using events
	// should match the chain from last trail
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	s.logger.Info("starting test with seed", zap.Int64("seed", seed))
	require := testutil.Require(s.T())
	events := make([]*model.EventEntry, 0)
	totalNumOfTrails := 1000
	blocksToMakePerTrail := 10
	blocksByTrail := make([][]*api.BlockMetadata, totalNumOfTrails)
	for i := 0; i < totalNumOfTrails; i++ {
		var blocks []*api.BlockMetadata
		if i == 0 {
			blocks = testutil.MakeBlockMetadatasFromStartHeight(s.config.Chain.BlockStartHeight, blocksToMakePerTrail, s.tag)
		} else {
			// randomly create some forks
			forkHeight := len(blocksByTrail[i-1]) - 1 - rand.Intn(blocksToMakePerTrail-1)
			blocks = make([]*api.BlockMetadata, len(blocksByTrail[i-1][:forkHeight]))
			copy(blocks, blocksByTrail[i-1][:forkHeight])
			parentHash := blocksByTrail[i-1][forkHeight].ParentHash
			newBlocks := testutil.MakeBlockMetadatasFromStartHeight(blocksByTrail[i-1][forkHeight].Height, blocksToMakePerTrail, s.tag)
			for _, block := range newBlocks {
				block.Hash = fmt.Sprintf("hash_trail_%d_%d", i, block.Height)
				block.ParentHash = parentHash
				parentHash = block.Hash
				blocks = append(blocks, block)
			}
		}
		blocksByTrail[i] = blocks
	}

	curTrail := 0
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTag).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32) (int64, error) {
			if len(events) == 0 {
				return 0, storage.ErrNoEventHistory
			} else {
				return int64(len(events)-1) + metastorage.EventIdStartValue, nil
			}
		})
	s.metaStorage.EXPECT().GetEventsByEventIdRange(
		gomock.Any(), s.eventTag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
			return events[minEventId-metastorage.EventIdStartValue : maxEventId-metastorage.EventIdStartValue], nil
		})

	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), s.tag).AnyTimes().
		DoAndReturn(func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			return blocksByTrail[curTrail][len(blocksByTrail[curTrail])-1], nil
		})
	s.metaStorage.EXPECT().GetBlocksByHeightRange(
		gomock.Any(), s.tag, gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return blocksByTrail[curTrail][startHeight:endHeight], nil
		})

	s.metaStorage.EXPECT().AddEvents(gomock.Any(), s.eventTag, gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, inputEvents []*model.BlockEvent) error {
			for _, ie := range inputEvents {
				event := model.NewEventEntry(s.eventTag, metastorage.EventIdStartValue+int64(len(events)), ie)
				events = append(events, event)
			}
			return nil
		})
	streamerBatchSize := uint64(500)
	for ; curTrail < totalNumOfTrails; curTrail++ {
		_, err := s.streamer.Execute(s.env.BackgroundContext(), &StreamerRequest{
			Tag:                   s.tag,
			BatchSize:             streamerBatchSize,
			MaxAllowedReorgHeight: s.config.Workflows.Streamer.IrreversibleDistance,
			EventTag:              s.eventTag,
		})
		require.NoError(err)
	}

	minEventIdFetched := metastorage.EventIdStartValue + int64(len(events))
	eventsToChainAdaptor := metastorage.NewEventsToChainAdaptor()
	// now if we call next event function to iterate though events backwards,
	// it should spill the blocks we expect to return in last trail, in reverse order
	for i := len(blocksByTrail[totalNumOfTrails-1]) - 1; i >= 0; i-- {
		block := blocksByTrail[totalNumOfTrails-1][i]
		event, err := s.streamer.getEventForTailBlock(context.TODO(), s.eventTag, &minEventIdFetched, eventsToChainAdaptor)
		require.NoError(err)
		require.Equal(block.Hash, event.BlockHash)
		require.Equal(block.Height, event.BlockHeight)
	}
}
