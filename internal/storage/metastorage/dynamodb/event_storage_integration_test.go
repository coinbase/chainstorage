package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type eventStorageTestSuite struct {
	suite.Suite
	storage  internal.MetaStorage
	config   *config.Config
	tag      uint32
	eventTag uint32
}

func (s *eventStorageTestSuite) SetupTest() {
	var storage internal.MetaStorage
	app := testapp.New(
		s.T(),
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&storage),
	)
	defer app.Close()
	s.storage = storage
	s.tag = 1
	s.eventTag = 0
}

func (s *eventStorageTestSuite) addEvents(eventTag uint32, startHeight uint64, numEvents uint64, tag uint32) {
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, startHeight, startHeight+numEvents, tag)
	ctx := context.TODO()
	err := s.storage.AddEvents(ctx, eventTag, blockEvents)
	if err != nil {
		panic(err)
	}
}

func (s *eventStorageTestSuite) verifyEvents(eventTag uint32, numEvents uint64, tag uint32) {
	require := testutil.Require(s.T())
	ctx := context.TODO()

	watermark, err := s.storage.GetMaxEventId(ctx, eventTag)
	if err != nil {
		panic(err)
	}
	require.Equal(watermark-internal.EventIdStartValue, int64(numEvents-1))

	// fetch range with missing item
	_, err = s.storage.GetEventsByEventIdRange(ctx, eventTag, internal.EventIdStartValue, internal.EventIdStartValue+int64(numEvents+100))
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	// fetch valid range
	fetchedEvents, err := s.storage.GetEventsByEventIdRange(ctx, eventTag, internal.EventIdStartValue, internal.EventIdStartValue+int64(numEvents))
	if err != nil {
		panic(err)
	}
	require.NotNil(fetchedEvents)
	require.Equal(uint64(len(fetchedEvents)), numEvents)

	numFollowingEventsToFetch := uint64(10)
	for i, event := range fetchedEvents {
		require.Equal(int64(i)+internal.EventIdStartValue, event.EventId)
		require.Equal(uint64(i), event.BlockHeight)
		require.Equal(api.BlockchainEvent_BLOCK_ADDED, event.EventType)
		require.Equal(tag, event.Tag)
		require.Equal(eventTag, event.EventTag)

		expectedNumEvents := numFollowingEventsToFetch
		if uint64(event.EventId)+numFollowingEventsToFetch >= numEvents {
			expectedNumEvents = numEvents - 1 - uint64(event.EventId-internal.EventIdStartValue)
		}
		followingEvents, err := s.storage.GetEventsAfterEventId(ctx, eventTag, event.EventId, numFollowingEventsToFetch)
		if err != nil {
			panic(err)
		}
		require.Equal(uint64(len(followingEvents)), expectedNumEvents)
		for j, followingEvent := range followingEvents {
			require.Equal(int64(i+j+1)+internal.EventIdStartValue, followingEvent.EventId)
			require.Equal(uint64(i+j+1), followingEvent.BlockHeight)
			require.Equal(api.BlockchainEvent_BLOCK_ADDED, followingEvent.EventType)
			require.Equal(eventTag, followingEvent.EventTag)
		}
	}
}

func (s *eventStorageTestSuite) TestSetMaxEventId() {
	require := testutil.Require(s.T())
	ctx := context.TODO()
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, s.tag)
	watermark, err := s.storage.GetMaxEventId(ctx, s.eventTag)
	require.NoError(err)
	require.Equal(internal.EventIdStartValue+int64(numEvents-1), watermark)

	// reset it to a new value
	newEventId := int64(5)
	err = s.storage.SetMaxEventId(ctx, s.eventTag, newEventId)
	require.NoError(err)
	watermark, err = s.storage.GetMaxEventId(ctx, s.eventTag)
	require.NoError(err)
	require.Equal(watermark, newEventId)

	// reset it to invalid value
	invalidEventId := int64(-1)
	err = s.storage.SetMaxEventId(ctx, s.eventTag, invalidEventId)
	require.Error(err)

	// reset it to value bigger than current max
	invalidEventId = newEventId + 10
	err = s.storage.SetMaxEventId(ctx, s.eventTag, invalidEventId)
	require.Error(err)

	// reset it to EventIdDeleted
	err = s.storage.SetMaxEventId(ctx, s.eventTag, internal.EventIdDeleted)
	require.NoError(err)
	_, err = s.storage.GetMaxEventId(ctx, s.eventTag)
	require.Error(err)
	require.Equal(errors.ErrNoEventHistory, err)
}

func (s *eventStorageTestSuite) TestSetMaxEventIdNonDefaultEventTag() {
	require := testutil.Require(s.T())
	ctx := context.TODO()
	numEvents := uint64(100)
	eventTag := uint32(1)
	s.addEvents(eventTag, 0, numEvents, s.tag)
	watermark, err := s.storage.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.Equal(internal.EventIdStartValue+int64(numEvents-1), watermark)

	// reset it to a new value
	newEventId := int64(5)
	err = s.storage.SetMaxEventId(ctx, eventTag, newEventId)
	require.NoError(err)
	watermark, err = s.storage.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.Equal(watermark, newEventId)

	// reset it to invalid value
	invalidEventId := int64(-1)
	err = s.storage.SetMaxEventId(ctx, eventTag, invalidEventId)
	require.Error(err)

	// reset it to value bigger than current max
	invalidEventId = newEventId + 10
	err = s.storage.SetMaxEventId(ctx, eventTag, invalidEventId)
	require.Error(err)

	// reset it to EventIdDeleted
	err = s.storage.SetMaxEventId(ctx, eventTag, internal.EventIdDeleted)
	require.NoError(err)
	_, err = s.storage.GetMaxEventId(ctx, eventTag)
	require.Error(err)
	require.Equal(errors.ErrNoEventHistory, err)
}

func (s *eventStorageTestSuite) TestAddEvents() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, s.tag)
	s.verifyEvents(s.eventTag, numEvents, s.tag)
}

func (s *eventStorageTestSuite) TestAddEventsNonDefaultEventTag() {
	numEvents := uint64(100)
	s.addEvents(uint32(1), 0, numEvents, s.tag)
	s.verifyEvents(uint32(1), numEvents, s.tag)
}

func (s *eventStorageTestSuite) TestAddEventsDefaultTag() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, 0)
	s.verifyEvents(s.eventTag, numEvents, internal.DefaultBlockTag)
}

func (s *eventStorageTestSuite) TestAddEventsNonDefaultTag() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, 2)
	s.verifyEvents(s.eventTag, numEvents, 2)
}

func (s *eventStorageTestSuite) TestAddEventsMultipleTimes() {
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, s.tag)
	s.addEvents(s.eventTag, numEvents, numEvents, s.tag)
	numEvents = numEvents * 2
	s.verifyEvents(s.eventTag, numEvents, s.tag)
}

func (s *eventStorageTestSuite) TestAddEventsMultipleTimesNonDefaultEventTag() {
	numEvents := uint64(100)
	eventTag := uint32(1)
	s.addEvents(eventTag, 0, numEvents, s.tag)
	s.addEvents(eventTag, numEvents, numEvents, s.tag)
	numEvents = numEvents * 2
	s.verifyEvents(eventTag, numEvents, s.tag)
}

func (s *eventStorageTestSuite) TestAddEventsDiscontinuousChain_NotSkipped() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	ctx := context.TODO()
	err := s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	if err != nil {
		panic(err)
	}
	// have add event for height numEvents-1 again, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents-1, numEvents+4, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// missing event for height numEvents, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+7, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// hash mismatch, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+7, s.tag, testutil.WithBlockHashFormat("HashMismatch0x%s"))
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// continuous, should be able to add them
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents, numEvents+7, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
}

func (s *eventStorageTestSuite) TestAddEventsDiscontinuousChain_Skipped() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	ctx := context.TODO()
	err := s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	if err != nil {
		panic(err)
	}

	// chain normal growing case, [+0(skipped), +1]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents, numEvents+1, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+1, numEvents+2, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// chain normal growing case, +0(skipped), +1, [+2, +3(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+3, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+3, numEvents+4, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// chain normal growing case, +0(skipped), +1, +2, +3(skipped), [+4(skipped), +5(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+4, numEvents+5, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+5, numEvents+6, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// rollback case, +6, +7, +8(skipped), [-8(skipped), -7]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+6, numEvents+8, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+7, numEvents+8, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// rollback case, +7(skipped), +8, [-8, -7(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+8, numEvents+9, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+8, numEvents+9, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// rollback case, +7(skipped), +8(skipped), [-8(skipped), -7(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
}

func (s *eventStorageTestSuite) TestGetFirstEventIdByBlockHeight() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	ctx := context.TODO()
	err := s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	if err != nil {
		panic(err)
	}
	// add the remove events again so for each height, there should be two events
	for i := int64(numEvents - 1); i >= 0; i-- {
		removeEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, uint64(i), uint64(i+1), s.tag)
		err := s.storage.AddEvents(ctx, s.eventTag, removeEvents)
		if err != nil {
			panic(err)
		}
		eventId, err := s.storage.GetFirstEventIdByBlockHeight(ctx, s.eventTag, uint64(i))
		if err != nil {
			panic(err)
		}
		require.Equal(i+internal.EventIdStartValue, eventId)
	}
}

func (s *eventStorageTestSuite) TestGetFirstEventIdByBlockHeightNonDefaultEventTag() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	eventTag := uint32(1)
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	ctx := context.TODO()
	err := s.storage.AddEvents(ctx, eventTag, blockEvents)
	require.NoError(err)

	// fetch event for blockHeight=0
	eventId, err := s.storage.GetFirstEventIdByBlockHeight(ctx, eventTag, uint64(0))
	require.NoError(err)
	require.Equal(eventId, internal.EventIdStartValue)

	// add the remove events again so for each height, there should be two events
	for i := int64(numEvents - 1); i >= 0; i-- {
		removeEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, uint64(i), uint64(i+1), s.tag)
		err := s.storage.AddEvents(ctx, eventTag, removeEvents)
		require.NoError(err)
		eventId, err := s.storage.GetFirstEventIdByBlockHeight(ctx, eventTag, uint64(i))
		require.NoError(err)
		require.Equal(i+internal.EventIdStartValue, eventId)
	}
}

func (s *eventStorageTestSuite) TestGetEventByEventId() {
	const (
		eventId   = int64(10)
		numEvents = uint64(20)
	)

	require := testutil.Require(s.T())
	ctx := context.TODO()

	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	err := s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	event, err := s.storage.GetEventByEventId(ctx, s.eventTag, eventId)
	require.NoError(err)
	require.Equal(event.EventId, eventId)
	require.Equal(event.BlockHeight, uint64(eventId-1))
}

func (s *eventStorageTestSuite) TestGetEventByEventId_InvalidEventId() {
	const (
		eventId   = int64(30)
		numEvents = uint64(20)
	)

	require := testutil.Require(s.T())
	ctx := context.TODO()

	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	err := s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	_, err = s.storage.GetEventByEventId(ctx, s.eventTag, eventId)
	require.Error(err)
}

func (s *eventStorageTestSuite) TestGetEventsByBlockHeight() {
	const (
		blockHeight = uint64(19)
		numEvents   = uint64(20)
	)

	require := testutil.Require(s.T())
	ctx := context.TODO()

	// +0, +1, ..., +19, -19,
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	err := s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents-1, numEvents, s.tag)
	err = s.storage.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	events, err := s.storage.GetEventsByBlockHeight(ctx, s.eventTag, blockHeight)
	require.NoError(err)
	require.Equal(2, len(events))
	for _, event := range events {
		require.Equal(blockHeight, event.BlockHeight)
	}
}

func TestIntegrationEventStorageTestSuite(t *testing.T) {
	require := testutil.Require(t)
	// Test with eth-mainnet for stream version
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &eventStorageTestSuite{config: cfg})
}
