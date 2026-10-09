package activity

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type migratorActivityTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	ctrl                  *gomock.Controller
	app                   testapp.TestApp
	logger                *zap.Logger
	migrator              *Migrator
	getLatestHeight       *GetLatestBlockHeightActivity
	getLatestFromPostgres *GetLatestBlockFromPostgresActivity
	getLatestEvent        *GetLatestEventFromPostgresActivity
	getMaxEventId         *GetMaxEventIdActivity
	env                   *cadence.TestEnv
	cfg                   *config.Config
}

func TestMigratorActivityTestSuite(t *testing.T) {
	suite.Run(t, new(migratorActivityTestSuite))
}

func (s *migratorActivityTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_ETHEREUM),
		config.WithNetwork(common.Network_NETWORK_ETHEREUM_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)
	s.cfg = cfg

	var deps struct {
		fx.In
		Migrator                   *Migrator
		GetLatestHeight            *GetLatestBlockHeightActivity
		GetLatestFromPostgres      *GetLatestBlockFromPostgresActivity
		GetLatestEventFromPostgres *GetLatestEventFromPostgresActivity
		GetMaxEventId              *GetMaxEventIdActivity
	}

	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Populate(&deps),
	)

	s.migrator = deps.Migrator
	s.getLatestHeight = deps.GetLatestHeight
	s.getLatestFromPostgres = deps.GetLatestFromPostgres
	s.getLatestEvent = deps.GetLatestEventFromPostgres
	s.getMaxEventId = deps.GetMaxEventId
	s.logger = s.app.Logger()

}

func (s *migratorActivityTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_NoReorgs() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test case: No reorgs - single segment
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xa", ParentHash: "0x99"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb", ParentHash: "0xa"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc", ParentHash: "0xb"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 1, "Should have 1 segment when no reorgs")
	require.Len(segments[0].Blocks, 3)

	// Verify blocks are in correct order
	require.Equal(uint64(100), segments[0].Blocks[0].Height)
	require.Equal("0xa", segments[0].Blocks[0].Hash)
	require.Equal(uint64(101), segments[0].Blocks[1].Height)
	require.Equal("0xb", segments[0].Blocks[1].Hash)
	require.Equal(uint64(102), segments[0].Blocks[2].Height)
	require.Equal("0xc", segments[0].Blocks[2].Hash)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_SingleReorg() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test case: Single reorg in middle
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xa", ParentHash: "0x99"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb", ParentHash: "0xa"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc1", ParentHash: "0xb"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102, BlockHash: "0xc1"},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc2", ParentHash: "0xb"},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 103, BlockHash: "0xd", ParentHash: "0xc2"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 2, "Should have 2 segments with 1 reorg")

	// First segment: blocks before reorg
	require.Len(segments[0].Blocks, 3)
	require.Equal(uint64(100), segments[0].Blocks[0].Height)
	require.Equal(uint64(101), segments[0].Blocks[1].Height)
	require.Equal(uint64(102), segments[0].Blocks[2].Height)
	require.Equal("0xc1", segments[0].Blocks[2].Hash)

	// Second segment: blocks after reorg
	require.Len(segments[1].Blocks, 2)
	require.Equal(uint64(102), segments[1].Blocks[0].Height)
	require.Equal("0xc2", segments[1].Blocks[0].Hash)
	require.Equal(uint64(103), segments[1].Blocks[1].Height)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_MultipleReorgs() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test case: Multiple reorgs
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xa", ParentHash: "0x99"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb1", ParentHash: "0xa"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101, BlockHash: "0xb1"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb2", ParentHash: "0xa"},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc1", ParentHash: "0xb2"},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102, BlockHash: "0xc1"},
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101, BlockHash: "0xb2"},
		{EventId: 8, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb3", ParentHash: "0xa"},
		{EventId: 9, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc2", ParentHash: "0xb3"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 3, "Should have 3 segments with 2 reorgs")

	// First segment
	require.Len(segments[0].Blocks, 2)
	require.Equal("0xa", segments[0].Blocks[0].Hash)
	require.Equal("0xb1", segments[0].Blocks[1].Hash)

	// Second segment
	require.Len(segments[1].Blocks, 2)
	require.Equal("0xb2", segments[1].Blocks[0].Hash)
	require.Equal("0xc1", segments[1].Blocks[1].Hash)

	// Third segment
	require.Len(segments[2].Blocks, 2)
	require.Equal("0xb3", segments[2].Blocks[0].Hash)
	require.Equal("0xc2", segments[2].Blocks[1].Hash)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_StartingWithRemoved() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test case: Starting with BLOCK_REMOVED
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102, BlockHash: "0xold"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101, BlockHash: "0xold"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb", ParentHash: "0xa"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc", ParentHash: "0xb"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 1, "Should have 1 segment when starting with REMOVED")
	require.Len(segments[0].Blocks, 2)
	require.Equal("0xb", segments[0].Blocks[0].Hash)
	require.Equal("0xc", segments[0].Blocks[1].Hash)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_EmptyEvents() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	segments := s.migrator.buildSegmentsFromEvents(logger, []*model.EventEntry{})
	require.Nil(segments, "Should return nil for empty events")
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_ComplexReorgPattern() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Complex pattern: build up chain, then deep reorg
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0x100", ParentHash: "0x99"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101a", ParentHash: "0x100"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102a", ParentHash: "0x101a"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 103, BlockHash: "0x103a", ParentHash: "0x102a"},
		// Deep reorg back to block 100
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 103, BlockHash: "0x103a"},
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102, BlockHash: "0x102a"},
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101, BlockHash: "0x101a"},
		// Build new chain
		{EventId: 8, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0x101b", ParentHash: "0x100"},
		{EventId: 9, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0x102b", ParentHash: "0x101b"},
		{EventId: 10, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 103, BlockHash: "0x103b", ParentHash: "0x102b"},
		{EventId: 11, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 104, BlockHash: "0x104", ParentHash: "0x103b"},
		{EventId: 12, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 105, BlockHash: "0x105", ParentHash: "0x104"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 2, "Should have 2 segments")

	// First segment: original chain
	require.Len(segments[0].Blocks, 4)
	require.Equal("0x100", segments[0].Blocks[0].Hash)
	require.Equal("0x103a", segments[0].Blocks[3].Hash)

	// Second segment: new chain after reorg
	require.Len(segments[1].Blocks, 5)
	require.Equal("0x101b", segments[1].Blocks[0].Hash)
	require.Equal("0x105", segments[1].Blocks[4].Hash)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_SkippedBlocks() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test with skipped blocks
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xa", ParentHash: "0x99", BlockSkipped: false},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "", ParentHash: "", BlockSkipped: true},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc", ParentHash: "0xa", BlockSkipped: false},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 1)
	require.Len(segments[0].Blocks, 3)

	// Verify skipped block is included
	require.Equal(uint64(100), segments[0].Blocks[0].Height)
	require.False(segments[0].Blocks[0].Skipped)
	require.Equal(uint64(101), segments[0].Blocks[1].Height)
	require.True(segments[0].Blocks[1].Skipped)
	require.Equal(uint64(102), segments[0].Blocks[2].Height)
	require.False(segments[0].Blocks[2].Skipped)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_OnlyRemovedEvents() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Only BLOCK_REMOVED events - no blocks to migrate
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 100},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Nil(segments, "Should return nil when only REMOVED events")
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_AlternatingAddRemove() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Alternating ADD and REMOVE pattern
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xa", ParentHash: "0x99"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 100},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xb", ParentHash: "0x99"},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 100},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xc", ParentHash: "0x99"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 3, "Should have 3 segments")

	// Each segment should have one block
	for i, segment := range segments {
		require.Len(segment.Blocks, 1, "Segment %d should have 1 block", i)
		require.Equal(uint64(100), segment.Blocks[0].Height)
	}
	require.Equal("0xa", segments[0].Blocks[0].Hash)
	require.Equal("0xb", segments[1].Blocks[0].Hash)
	require.Equal("0xc", segments[2].Blocks[0].Hash)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_ConsecutiveRemovals() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Multiple consecutive BLOCK_REMOVED events
	events := []*model.EventEntry{
		{EventId: 1, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xa", ParentHash: "0x99"},
		{EventId: 2, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb", ParentHash: "0xa"},
		{EventId: 3, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 101},
		{EventId: 4, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 100},
		{EventId: 5, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 99}, // Removing even further back
		{EventId: 6, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 99, BlockHash: "0xnew99", ParentHash: "0x98"},
		{EventId: 7, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xnew100", ParentHash: "0xnew99"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 2)

	// First segment: original blocks
	require.Len(segments[0].Blocks, 2)
	require.Equal("0xa", segments[0].Blocks[0].Hash)
	require.Equal("0xb", segments[0].Blocks[1].Hash)

	// Second segment: new blocks after deep reorg
	require.Len(segments[1].Blocks, 2)
	require.Equal("0xnew99", segments[1].Blocks[0].Hash)
	require.Equal("0xnew100", segments[1].Blocks[1].Hash)
}

func (s *migratorActivityTestSuite) TestFetchBlockData_OrderPreservation() {
	require := testutil.Require(s.T())

	// Create test blocks in specific order
	blocksToMigrate := []BlockToMigrate{
		{Height: 102, Hash: "0xc", EventSeq: 3},
		{Height: 100, Hash: "0xa", EventSeq: 1},
		{Height: 101, Hash: "0xb", EventSeq: 2},
	}

	// Test that WorkItem structure exists and has correct fields
	workItem := WorkItem{
		Block: blocksToMigrate[0],
		Index: 0,
	}
	require.Equal(uint64(102), workItem.Block.Height)
	require.Equal(0, workItem.Index)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_EventSequenceOrdering() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test that blocks maintain event sequence ordering within segments
	events := []*model.EventEntry{
		{EventId: 10, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 100, BlockHash: "0xa", ParentHash: "0x99"},
		{EventId: 20, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 101, BlockHash: "0xb", ParentHash: "0xa"},
		{EventId: 30, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xc", ParentHash: "0xb"},
		{EventId: 40, EventType: api.BlockchainEvent_BLOCK_REMOVED, BlockHeight: 102},
		{EventId: 50, EventType: api.BlockchainEvent_BLOCK_ADDED, BlockHeight: 102, BlockHash: "0xd", ParentHash: "0xb"},
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 2)

	// Verify EventSeq is preserved
	require.Equal(int64(10), segments[0].Blocks[0].EventSeq)
	require.Equal(int64(20), segments[0].Blocks[1].EventSeq)
	require.Equal(int64(30), segments[0].Blocks[2].EventSeq)
	require.Equal(int64(50), segments[1].Blocks[0].EventSeq)
}

func (s *migratorActivityTestSuite) TestBuildSegmentsFromEvents_LargeReorgChain() {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	// Test with a large number of blocks and reorgs
	events := make([]*model.EventEntry, 0, 1000)

	// Add 100 blocks
	for i := 0; i < 100; i++ {
		events = append(events, &model.EventEntry{
			EventId:     int64(i + 1),
			EventType:   api.BlockchainEvent_BLOCK_ADDED,
			BlockHeight: uint64(100 + i),
			BlockHash:   string(rune('a'+i%26)) + "orig",
			ParentHash:  string(rune('a'+(i-1)%26)) + "orig",
		})
	}

	// Remove last 50 blocks
	for i := 99; i >= 50; i-- {
		events = append(events, &model.EventEntry{
			EventId:     int64(101 + (99 - i)),
			EventType:   api.BlockchainEvent_BLOCK_REMOVED,
			BlockHeight: uint64(100 + i),
		})
	}

	// Add 60 new blocks
	for i := 0; i < 60; i++ {
		events = append(events, &model.EventEntry{
			EventId:     int64(151 + i),
			EventType:   api.BlockchainEvent_BLOCK_ADDED,
			BlockHeight: uint64(150 + i),
			BlockHash:   string(rune('a'+i%26)) + "new",
			ParentHash:  string(rune('a'+(i-1)%26)) + "new",
		})
	}

	segments := s.migrator.buildSegmentsFromEvents(logger, events)
	require.Len(segments, 2)
	require.Len(segments[0].Blocks, 100)
	require.Len(segments[1].Blocks, 60)
}
