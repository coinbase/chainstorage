package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type eventStorageTestSuite struct {
	suite.Suite
	accessor internal.MetaStorage
	config   *config.Config
	tag      uint32
	eventTag uint32
	db       *sql.DB
}

func (s *eventStorageTestSuite) SetupSuite() {
	require := testutil.Require(s.T())
	cfg, err := config.New()
	require.NoError(err)
	if !cfg.IsIntegrationTest() || cfg.AWS.Postgres == nil {
		return
	}

	db, err := newDBConnection(context.Background(), cfg.AWS.Postgres)
	require.NoError(err)
	defer db.Close()
	require.NoError(runMigrations(context.Background(), db))
}

func (s *eventStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())
	var accessor internal.MetaStorage
	cfg, err := config.New()
	require.NoError(err)

	// Skip tests if Postgres is not configured
	if cfg.AWS.Postgres == nil {
		s.T().Skip("Postgres not configured, skipping test suite")
		return
	}

	app := testapp.New(
		s.T(),
		fx.Provide(NewMetaStorage),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&accessor),
	)
	defer app.Close()
	s.accessor = accessor
	s.tag = 1
	s.eventTag = 0

	// Get database connection for cleanup
	db, err := newDBConnection(context.Background(), cfg.AWS.Postgres)
	require.NoError(err)
	s.db = db
}

func (s *eventStorageTestSuite) TearDownTest() {
	if s.db != nil {
		ctx := context.Background()
		s.T().Log("Clearing database tables after test")
		// Clear all tables in reverse order due to foreign key constraints
		tables := []string{"block_events", "block_consolidation_shadow", "canonical_blocks", "block_metadata"}
		for _, table := range tables {
			_, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", table))
			if err != nil {
				s.T().Logf("Failed to clear table %s: %v", table, err)
			}
		}
	}
}

func (s *eventStorageTestSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
}
func (s *eventStorageTestSuite) addEvents(eventTag uint32, startHeight uint64, numEvents uint64, tag uint32) {
	// First, add block metadata for the events
	blockMetas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(numEvents), tag)
	ctx := context.TODO()
	err := s.accessor.PersistBlockMetas(ctx, true, blockMetas, nil)
	if err != nil {
		panic(err)
	}

	// Then add events
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, startHeight, startHeight+numEvents, tag)
	err = s.accessor.AddEvents(ctx, eventTag, blockEvents)
	if err != nil {
		panic(err)
	}
}
func (s *eventStorageTestSuite) verifyEvents(eventTag uint32, numEvents uint64, tag uint32) {
	require := testutil.Require(s.T())
	ctx := context.TODO()

	watermark, err := s.accessor.GetMaxEventId(ctx, eventTag)
	if err != nil {
		panic(err)
	}
	require.Equal(watermark-model.EventIdStartValue, int64(numEvents-1))

	// fetch range with missing item
	_, err = s.accessor.GetEventsByEventIdRange(ctx, eventTag, model.EventIdStartValue, model.EventIdStartValue+int64(numEvents+100))
	require.Error(err)
	require.True(xerrors.Is(err, errors.ErrItemNotFound))

	// fetch valid range
	fetchedEvents, err := s.accessor.GetEventsByEventIdRange(ctx, eventTag, model.EventIdStartValue, model.EventIdStartValue+int64(numEvents))
	if err != nil {
		panic(err)
	}
	require.NotNil(fetchedEvents)
	require.Equal(uint64(len(fetchedEvents)), numEvents)

	numFollowingEventsToFetch := uint64(10)
	for i, event := range fetchedEvents {
		require.Equal(int64(i)+model.EventIdStartValue, event.EventId)
		require.Equal(uint64(i), event.BlockHeight)
		require.Equal(api.BlockchainEvent_BLOCK_ADDED, event.EventType)
		require.Equal(tag, event.Tag)
		require.Equal(eventTag, event.EventTag)

		expectedNumEvents := numFollowingEventsToFetch
		if uint64(event.EventId)+numFollowingEventsToFetch >= numEvents {
			expectedNumEvents = numEvents - 1 - uint64(event.EventId-model.EventIdStartValue)
		}
		followingEvents, err := s.accessor.GetEventsAfterEventId(ctx, eventTag, event.EventId, numFollowingEventsToFetch)
		if err != nil {
			panic(err)
		}
		require.Equal(uint64(len(followingEvents)), expectedNumEvents)
		for j, followingEvent := range followingEvents {
			require.Equal(int64(i+j+1)+model.EventIdStartValue, followingEvent.EventId)
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
	watermark, err := s.accessor.GetMaxEventId(ctx, s.eventTag)
	require.NoError(err)
	require.Equal(model.EventIdStartValue+int64(numEvents-1), watermark)

	// reset it to a new value
	newEventId := int64(5)
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, newEventId)
	require.NoError(err)
	watermark, err = s.accessor.GetMaxEventId(ctx, s.eventTag)
	require.NoError(err)
	require.Equal(watermark, newEventId)

	// reset it to invalid value
	invalidEventId := int64(-1)
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, invalidEventId)
	require.Error(err)

	// reset it to value bigger than current max
	invalidEventId = newEventId + 10
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, invalidEventId)
	require.Error(err)

	// reset it to EventIdDeleted
	err = s.accessor.SetMaxEventId(ctx, s.eventTag, model.EventIdDeleted)
	require.NoError(err)
	_, err = s.accessor.GetMaxEventId(ctx, s.eventTag)
	require.Error(err)
	require.Equal(errors.ErrNoEventHistory, err)
}

func (s *eventStorageTestSuite) TestSetMaxEventIdNonDefaultEventTag() {
	require := testutil.Require(s.T())
	ctx := context.TODO()
	numEvents := uint64(100)
	eventTag := uint32(1)
	s.addEvents(eventTag, 0, numEvents, s.tag)
	watermark, err := s.accessor.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.Equal(model.EventIdStartValue+int64(numEvents-1), watermark)

	// reset it to a new value
	newEventId := int64(5)
	err = s.accessor.SetMaxEventId(ctx, eventTag, newEventId)
	require.NoError(err)
	watermark, err = s.accessor.GetMaxEventId(ctx, eventTag)
	require.NoError(err)
	require.Equal(watermark, newEventId)

	// reset it to invalid value
	invalidEventId := int64(-1)
	err = s.accessor.SetMaxEventId(ctx, eventTag, invalidEventId)
	require.Error(err)

	// reset it to value bigger than current max
	invalidEventId = newEventId + 10
	err = s.accessor.SetMaxEventId(ctx, eventTag, invalidEventId)
	require.Error(err)

	// reset it to EventIdDeleted
	err = s.accessor.SetMaxEventId(ctx, eventTag, model.EventIdDeleted)
	require.NoError(err)
	_, err = s.accessor.GetMaxEventId(ctx, eventTag)
	require.Error(err)
	require.Equal(errors.ErrNoEventHistory, err)
}

////////////////////////////////////////////////////////////

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
	s.verifyEvents(s.eventTag, numEvents, model.DefaultBlockTag)
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

////////////////////////////////////////////////////////////

func (s *eventStorageTestSuite) TestAddEventsDiscontinuousChain_NotSkipped() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)

	// First, add block metadata for the initial events
	blockMetas := testutil.MakeBlockMetadatasFromStartHeight(0, int(numEvents), s.tag)
	ctx := context.TODO()
	err := s.accessor.PersistBlockMetas(ctx, true, blockMetas, nil)
	require.NoError(err)

	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, 0, numEvents, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	if err != nil {
		panic(err)
	}

	// Add block metadata for the additional events that will be tested
	additionalBlockMetas := testutil.MakeBlockMetadatasFromStartHeight(numEvents, 10, s.tag)
	err = s.accessor.PersistBlockMetas(ctx, true, additionalBlockMetas, nil)
	require.NoError(err)

	// have add event for height numEvents-1 again, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents-1, numEvents+4, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// missing event for height numEvents, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+7, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// hash mismatch, invalid
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+7, s.tag, testutil.WithBlockHashFormat("HashMismatch0x%s"))
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.Error(err)

	// continuous, should be able to add them
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents, numEvents+7, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)
}

func (s *eventStorageTestSuite) TestAddEventsDiscontinuousChain_Skipped() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	ctx := context.TODO()

	// Use the helper method to create initial events
	s.addEvents(s.eventTag, 0, numEvents, s.tag)

	// Create block metadata ONLY for non-skipped heights
	// Heights 101, 102, 106, 107, 108, 109 will have block metadata (non-skipped)
	// Heights 100, 103, 104, 105 will be skipped (automatically created as skipped)
	nonSkippedBlockMetas := []*api.BlockMetadata{
		testutil.MakeBlockMetadata(numEvents+1, s.tag), // height 101
		testutil.MakeBlockMetadata(numEvents+2, s.tag), // height 102
		testutil.MakeBlockMetadata(numEvents+6, s.tag), // height 106
		testutil.MakeBlockMetadata(numEvents+7, s.tag), // height 107
		testutil.MakeBlockMetadata(numEvents+8, s.tag), // height 108
		testutil.MakeBlockMetadata(numEvents+9, s.tag), // height 109
	}

	// Set proper parent relationships for blocks that come after gaps
	// Block 106 should point to block 102 (since 103, 104, 105 are skipped/missing)
	nonSkippedBlockMetas[2].ParentHeight = nonSkippedBlockMetas[1].Height // 106 -> 102
	nonSkippedBlockMetas[2].ParentHash = nonSkippedBlockMetas[1].Hash

	// Block 107 should point to block 106
	nonSkippedBlockMetas[3].ParentHeight = nonSkippedBlockMetas[2].Height // 107 -> 106
	nonSkippedBlockMetas[3].ParentHash = nonSkippedBlockMetas[2].Hash

	// Block 108 should point to block 107
	nonSkippedBlockMetas[4].ParentHeight = nonSkippedBlockMetas[3].Height // 108 -> 107
	nonSkippedBlockMetas[4].ParentHash = nonSkippedBlockMetas[3].Hash

	// Block 109 should point to block 108
	nonSkippedBlockMetas[5].ParentHeight = nonSkippedBlockMetas[4].Height // 109 -> 108
	nonSkippedBlockMetas[5].ParentHash = nonSkippedBlockMetas[4].Hash

	err := s.accessor.PersistBlockMetas(ctx, true, nonSkippedBlockMetas, nil)
	require.NoError(err)

	// Test case: chain normal growing case, [+0(skipped), +1]
	// Height 100 is skipped, height 101 is normal
	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents, numEvents+1, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+1, numEvents+2, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// Test case: chain normal growing case, +0(skipped), +1, [+2, +3(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+2, numEvents+3, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+3, numEvents+4, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// Test case: chain normal growing case, +0(skipped), +1, +2, +3(skipped), [+4(skipped), +5(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+4, numEvents+5, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+5, numEvents+6, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// Test case: rollback case, +6, +7, +8(skipped), [-8(skipped), -7]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+6, numEvents+8, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+7, numEvents+8, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// Test case: rollback case, +7(skipped), +8, [-8, -7(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+8, numEvents+9, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+8, numEvents+9, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// Test case: rollback case, +7(skipped), +8(skipped), [-8(skipped), -7(skipped)]
	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_ADDED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+8, numEvents+9, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	blockEvents = testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents+7, numEvents+8, s.tag, testutil.WithBlockSkipped())
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	// Verify that skipped blocks created block_metadata entries but not canonical_blocks entries
	s.verifySkippedBlockHandling(ctx, numEvents, s.tag)
}

// verifySkippedBlockHandling verifies that skipped blocks have block_metadata entries but not canonical_blocks entries
func (s *eventStorageTestSuite) verifySkippedBlockHandling(ctx context.Context, numEvents uint64, tag uint32) {
	require := testutil.Require(s.T())

	// Check that skipped heights have block_metadata entries with skipped=true
	skippedHeights := []uint64{numEvents, numEvents + 3, numEvents + 4, numEvents + 5}
	for _, height := range skippedHeights {
		var count int
		err := s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM block_metadata 
			WHERE tag = $1 AND height = $2 AND skipped = true AND hash IS NULL
		`, tag, height).Scan(&count)
		require.NoError(err)
		require.Greater(count, 0, "Expected skipped block metadata for height %d", height)

		// Verify that skipped blocks do NOT have canonical_blocks entries
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM canonical_blocks 
			WHERE tag = $1 AND height = $2
		`, tag, height).Scan(&count)
		require.NoError(err)
		require.Equal(0, count, "Skipped blocks should not have canonical entries for height %d", height)
	}

	// Check that non-skipped heights have both block_metadata and canonical_blocks entries
	nonSkippedHeights := []uint64{numEvents + 1, numEvents + 2, numEvents + 6, numEvents + 7, numEvents + 8, numEvents + 9}
	for _, height := range nonSkippedHeights {
		var count int
		// Should have block_metadata entry with skipped=false
		err := s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM block_metadata 
			WHERE tag = $1 AND height = $2 AND skipped = false AND hash IS NOT NULL
		`, tag, height).Scan(&count)
		require.NoError(err)
		require.Greater(count, 0, "Expected non-skipped block metadata for height %d", height)

		// Should have canonical_blocks entry
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM canonical_blocks 
			WHERE tag = $1 AND height = $2
		`, tag, height).Scan(&count)
		require.NoError(err)
		require.Greater(count, 0, "Expected canonical entry for non-skipped height %d", height)
	}
}

////////////////////////////////////////////////////////////

func (s *eventStorageTestSuite) TestGetFirstEventIdByBlockHeight() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	s.addEvents(s.eventTag, 0, numEvents, s.tag)

	// add the remove events again so for each height, there should be two events
	for i := int64(numEvents - 1); i >= 0; i-- {
		// Add block metadata for the removal event
		blockMetas := testutil.MakeBlockMetadatasFromStartHeight(uint64(i), 1, s.tag)
		ctx := context.TODO()
		err := s.accessor.PersistBlockMetas(ctx, true, blockMetas, nil)
		if err != nil {
			panic(err)
		}

		removeEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, uint64(i), uint64(i+1), s.tag)
		err = s.accessor.AddEvents(ctx, s.eventTag, removeEvents)
		if err != nil {
			panic(err)
		}
		eventId, err := s.accessor.GetFirstEventIdByBlockHeight(ctx, s.eventTag, uint64(i))
		if err != nil {
			panic(err)
		}
		require.Equal(i+model.EventIdStartValue, eventId)
	}
}

func (s *eventStorageTestSuite) TestGetFirstEventIdByBlockHeightNonDefaultEventTag() {
	require := testutil.Require(s.T())
	numEvents := uint64(100)
	eventTag := uint32(1)
	ctx := context.TODO()
	s.addEvents(eventTag, 0, numEvents, s.tag)

	// fetch event for blockHeight=0
	eventId, err := s.accessor.GetFirstEventIdByBlockHeight(ctx, eventTag, uint64(0))
	require.NoError(err)
	require.Equal(eventId, model.EventIdStartValue)

	// add the remove events again so for each height, there should be two events
	for i := int64(numEvents - 1); i >= 0; i-- {
		// Add block metadata for the removal event
		blockMetas := testutil.MakeBlockMetadatasFromStartHeight(uint64(i), 1, s.tag)
		err := s.accessor.PersistBlockMetas(ctx, true, blockMetas, nil)
		require.NoError(err)

		removeEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, uint64(i), uint64(i+1), s.tag)
		err = s.accessor.AddEvents(ctx, eventTag, removeEvents)
		require.NoError(err)
		eventId, err := s.accessor.GetFirstEventIdByBlockHeight(ctx, eventTag, uint64(i))
		require.NoError(err)
		require.Equal(i+model.EventIdStartValue, eventId)
	}
}

func (s *eventStorageTestSuite) TestGetEventByEventId() {
	const (
		eventId   = int64(10)
		numEvents = uint64(20)
	)

	require := testutil.Require(s.T())
	ctx := context.TODO()

	s.addEvents(s.eventTag, 0, numEvents, s.tag)

	event, err := s.accessor.GetEventByEventId(ctx, s.eventTag, eventId)
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

	s.addEvents(s.eventTag, 0, numEvents, s.tag)

	_, err := s.accessor.GetEventByEventId(ctx, s.eventTag, eventId)
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
	s.addEvents(s.eventTag, 0, numEvents, s.tag)

	// Add block metadata for the removal event
	blockMetas := testutil.MakeBlockMetadatasFromStartHeight(numEvents-1, 1, s.tag)
	err := s.accessor.PersistBlockMetas(ctx, true, blockMetas, nil)
	require.NoError(err)

	blockEvents := testutil.MakeBlockEvents(api.BlockchainEvent_BLOCK_REMOVED, numEvents-1, numEvents, s.tag)
	err = s.accessor.AddEvents(ctx, s.eventTag, blockEvents)
	require.NoError(err)

	events, err := s.accessor.GetEventsByBlockHeight(ctx, s.eventTag, blockHeight)
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
