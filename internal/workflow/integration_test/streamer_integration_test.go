package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type StreamerIntegrationTestSuite struct {
	backfillerDependentTestSuite
}

type streamerDependencies struct {
	fx.In
	Streamer    *workflow.Streamer
	MetaStorage metastorage.MetaStorage
}

func TestIntegrationStreamerTestSuite(t *testing.T) {
	suite.Run(t, new(StreamerIntegrationTestSuite))
}

func (s *StreamerIntegrationTestSuite) TestStreamerIntegration() {
	require := testutil.Require(s.T())

	streamerBatchSize := uint64(50)
	eventTag := uint32(0)
	cfg, err := config.New()
	require.NoError(err)

	tag := cfg.GetStableBlockTag()
	startHeight := cfg.Chain.BlockStartHeight
	endHeight := cfg.Chain.BlockStartHeight + streamerBatchSize
	s.backfillData(startHeight, endHeight, tag, common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET)

	streamerDeps := streamerDependencies{}
	streamerEnv := cadence.NewTestEnv(s)
	streamerEnv.SetTestTimeout(3 * time.Minute)

	app := testapp.New(
		s.T(),
		testapp.WithFunctional(),
		fx.Provide(func() metastorage.MetaStorage { return s.backfillDependencies.MetaStorage }),
		cadence.WithTestEnv(streamerEnv),
		testapp.WithConfig(cfg),
		workflow.Module,
		fx.Populate(&streamerDeps),
	)
	defer app.Close()

	_, err = streamerDeps.Streamer.Execute(context.Background(), &workflow.StreamerRequest{
		BatchSize:      streamerBatchSize,
		CheckpointSize: 1,
		EventTag:       eventTag,
	})

	require.NotNil(err)
	require.True(workflow.IsContinueAsNewError(err))

	maxEventId, err := streamerDeps.MetaStorage.GetMaxEventId(context.TODO(), eventTag)
	require.NoError(err)
	require.Equal(metastorage.EventIdStartValue+int64(streamerBatchSize)-1, maxEventId)
	events, err := streamerDeps.MetaStorage.GetEventsByEventIdRange(context.TODO(), eventTag, metastorage.EventIdStartValue, maxEventId)
	require.NoError(err)
	blocks, err := streamerDeps.MetaStorage.GetBlocksByHeightRange(context.TODO(), tag, startHeight, endHeight)
	require.NoError(err)
	for i, event := range events {
		expectedBlockEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, blocks[i])
		expectedEventId := int64(i) + metastorage.EventIdStartValue
		expectedEventDDBEntry := model.NewEventDDBEntry(eventTag, expectedEventId, expectedBlockEvent)
		require.Equal(expectedEventDDBEntry, event)
	}
}

func (s *StreamerIntegrationTestSuite) TestStreamerIntegration_NonDefaultEventTag() {
	require := testutil.Require(s.T())

	streamerBatchSize := uint64(5)
	eventTag := uint32(1)
	cfg, err := config.New()
	require.NoError(err)

	tag := cfg.GetStableBlockTag()
	startHeight := cfg.Chain.BlockStartHeight
	endHeight := cfg.Chain.BlockStartHeight + streamerBatchSize
	s.backfillData(startHeight, endHeight, tag, common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET)

	streamerDeps := streamerDependencies{}
	streamerEnv := cadence.NewTestEnv(s)
	streamerEnv.SetTestTimeout(3 * time.Minute)

	app := testapp.New(
		s.T(),
		testapp.WithFunctional(),
		fx.Provide(func() metastorage.MetaStorage { return s.backfillDependencies.MetaStorage }),
		cadence.WithTestEnv(streamerEnv),
		testapp.WithConfig(cfg),
		workflow.Module,
		fx.Populate(&streamerDeps),
	)
	defer app.Close()

	_, err = streamerDeps.Streamer.Execute(context.Background(), &workflow.StreamerRequest{
		BatchSize:      streamerBatchSize,
		CheckpointSize: 1,
		EventTag:       eventTag,
	})

	require.NotNil(err)
	require.True(workflow.IsContinueAsNewError(err))

	maxEventId, err := streamerDeps.MetaStorage.GetMaxEventId(context.TODO(), eventTag)
	require.NoError(err)
	require.Equal(metastorage.EventIdStartValue+int64(streamerBatchSize)-1, maxEventId)
	events, err := streamerDeps.MetaStorage.GetEventsByEventIdRange(context.TODO(), eventTag, metastorage.EventIdStartValue, maxEventId)
	require.NoError(err)
	blocks, err := streamerDeps.MetaStorage.GetBlocksByHeightRange(context.TODO(), tag, startHeight, endHeight)
	require.NoError(err)
	for i, event := range events {
		expectedBlockEvent := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, blocks[i])
		expectedEventId := int64(i) + metastorage.EventIdStartValue
		expectedEventDDBEntry := model.NewEventDDBEntry(eventTag, expectedEventId, expectedBlockEvent)
		require.Equal(expectedEventDDBEntry, event)
	}
}
