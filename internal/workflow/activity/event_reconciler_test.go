package activity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type EventReconcilerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env             *cadence.TestEnv
	ctrl            *gomock.Controller
	metaStorage     *metastoragemocks.MockMetaStorage
	app             testapp.TestApp
	eventReconciler *EventReconciler
}

func TestEventReconcilerLoaderTestSuite(t *testing.T) {
	suite.Run(t, new(EventReconcilerTestSuite))
}

func (s *EventReconcilerTestSuite) SetupTest() {
	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Provide(dlq.NewNop),
		fx.Populate(&s.eventReconciler),
	)
}

func (s *EventReconcilerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *EventReconcilerTestSuite) TestReconcilerWithoutReorgEvents_Success() {
	require := testutil.Require(s.T())

	var (
		tag                 uint32 = 2
		eventTag            uint32 = 2
		upgradeFromEventTag uint32 = 1
		upgradeFromBlockTag uint32 = 1
		startBlockHeight    uint64 = 123456
		endBlockHeight      uint64 = 123457
	)
	upgradeFromEvents := make([]*model.EventEntry, 2)
	upgradeFromEvents[0] = &model.EventEntry{
		EventId:      123789,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  startBlockHeight,
		BlockHash:    "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[1] = &model.EventEntry{
		EventId:      123790,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  endBlockHeight,
		BlockHash:    "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	block0 := &api.BlockMetadata{
		Tag:    tag,
		Height: 123456,
		Hash:   "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
	}
	block1 := &api.BlockMetadata{
		Tag:    tag,
		Height: 123457,
		Hash:   "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
	}
	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
			if height == startBlockHeight {
				return block0, nil
			}
			if height == startBlockHeight+1 {
				return block1, nil
			}

			return nil, nil
		}).Times(2)

	request := &EventReconcilerRequest{
		Tag:                 tag,
		EventTag:            eventTag,
		UpgradeFromEventTag: upgradeFromEventTag,
		UpgradeFromEvents:   upgradeFromEvents,
	}
	response, err := s.eventReconciler.Execute(s.env.BackgroundContext(), request)

	require.NoError(err)
	for _, updatedEvent := range response.Eventdata {
		require.Equal(tag, updatedEvent.Tag)
		require.Equal(eventTag, updatedEvent.EventTag)
	}
}

func (s *EventReconcilerTestSuite) TestReconcilerWithReorgEvents_Success() {
	require := testutil.Require(s.T())

	var (
		tag                 uint32 = 2
		eventTag            uint32 = 2
		upgradeFromEventTag uint32 = 1
		upgradeFromBlockTag uint32 = 1
	)
	upgradeFromEvents := make([]*model.EventEntry, 5)
	upgradeFromEvents[0] = &model.EventEntry{
		EventId:      1,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  1,
		BlockHash:    "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[1] = &model.EventEntry{
		EventId:      2,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  2,
		BlockHash:    "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[2] = &model.EventEntry{
		EventId:      3,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockHeight:  2,
		BlockHash:    "0x980b8257a59d438f50e229bc27609ec497f9c4c26bcbbc85c571feb1d617c4a9",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[3] = &model.EventEntry{
		EventId:      4,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  2,
		BlockHash:    "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[4] = &model.EventEntry{
		EventId:      5,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  3,
		BlockHash:    "0xaf63b64c85d6c44dd78159e983495106666ac96cd9a9e48716c210917de18fca",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	block1 := &api.BlockMetadata{
		Tag:    tag,
		Height: 1,
		Hash:   "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
	}
	block2 := &api.BlockMetadata{
		Tag:    tag,
		Height: 123457,
		Hash:   "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
	}
	block3 := &api.BlockMetadata{
		Tag:    tag,
		Height: 123457,
		Hash:   "0xaf63b64c85d6c44dd78159e983495106666ac96cd9a9e48716c210917de18fca",
	}

	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
			if height == 1 {
				return block1, nil
			}
			if height == 2 {
				return block2, nil
			}
			if height == 3 {
				return block3, nil
			}

			return nil, nil
		}).Times(5)

	request := &EventReconcilerRequest{
		Tag:                 tag,
		EventTag:            eventTag,
		UpgradeFromEventTag: upgradeFromEventTag,
		UpgradeFromEvents:   upgradeFromEvents,
	}
	response, err := s.eventReconciler.Execute(s.env.BackgroundContext(), request)

	require.NoError(err)
	require.Equal(5, len(response.Eventdata))
	for i, updatedEvent := range response.Eventdata {
		if i == 1 || i == 2 {
			require.Equal(upgradeFromBlockTag, updatedEvent.Tag)
			require.Equal(true, updatedEvent.BlockSkipped)
		} else {
			require.Equal(tag, updatedEvent.Tag)
		}
		require.Equal(eventTag, updatedEvent.EventTag)
		require.Equal(upgradeFromEvents[i].EventType, response.Eventdata[i].EventType)
	}
}

func (s *EventReconcilerTestSuite) TestReconcilerWithoutReorgEvents_BlockNotFound_Err() {
	require := testutil.Require(s.T())

	var (
		tag                 uint32 = 2
		eventTag            uint32 = 2
		upgradeFromEventTag uint32 = 1
		upgradeFromBlockTag uint32 = 1
		startBlockHeight    uint64 = 123456
		endBlockHeight      uint64 = 123457
	)
	upgradeFromEvents := make([]*model.EventEntry, 2)
	upgradeFromEvents[0] = &model.EventEntry{
		EventId:      123789,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  startBlockHeight,
		BlockHash:    "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[1] = &model.EventEntry{
		EventId:      123790,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  endBlockHeight,
		BlockHash:    "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, gomock.Any()).Return(nil, storage.ErrItemNotFound)

	request := &EventReconcilerRequest{
		Tag:                 tag,
		EventTag:            eventTag,
		UpgradeFromEventTag: upgradeFromEventTag,
		UpgradeFromEvents:   upgradeFromEvents,
	}
	response, err := s.eventReconciler.Execute(s.env.BackgroundContext(), request)

	require.Nil(response.Eventdata)
	require.Error(err)
}

func (s *EventReconcilerTestSuite) TestReconciler_WithFalsePositiveReorgEvents() {
	require := testutil.Require(s.T())

	var (
		tag                 uint32 = 2
		eventTag            uint32 = 2
		upgradeFromEventTag uint32 = 1
		upgradeFromBlockTag uint32 = 1
	)
	upgradeFromEvents := make([]*model.EventEntry, 5)
	upgradeFromEvents[0] = &model.EventEntry{
		EventId:      1,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  1,
		BlockHash:    "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[1] = &model.EventEntry{
		EventId:      2,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  2,
		BlockHash:    "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[2] = &model.EventEntry{
		EventId:      3,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockHeight:  2,
		BlockHash:    "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[3] = &model.EventEntry{
		EventId:      4,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  2,
		BlockHash:    "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[4] = &model.EventEntry{
		EventId:      5,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  3,
		BlockHash:    "0xaf63b64c85d6c44dd78159e983495106666ac96cd9a9e48716c210917de18fca",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	block1 := &api.BlockMetadata{
		Tag:    tag,
		Height: 1,
		Hash:   "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
	}
	block2 := &api.BlockMetadata{
		Tag:    tag,
		Height: 2,
		Hash:   "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
	}
	block3 := &api.BlockMetadata{
		Tag:    tag,
		Height: 3,
		Hash:   "0xaf63b64c85d6c44dd78159e983495106666ac96cd9a9e48716c210917de18fca",
	}

	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
			if height == 1 {
				return block1, nil
			}
			if height == 2 {
				return block2, nil
			}
			if height == 3 {
				return block3, nil
			}

			return nil, nil
		}).Times(5)

	request := &EventReconcilerRequest{
		Tag:                 tag,
		EventTag:            eventTag,
		UpgradeFromEventTag: upgradeFromEventTag,
		UpgradeFromEvents:   upgradeFromEvents,
	}
	response, err := s.eventReconciler.Execute(s.env.BackgroundContext(), request)

	require.NoError(err)
	require.Equal(5, len(response.Eventdata))
	for i, updatedEvent := range response.Eventdata {
		require.Equal(tag, updatedEvent.Tag)
		require.Equal(eventTag, updatedEvent.EventTag)
		require.Equal(upgradeFromEvents[i].EventType, response.Eventdata[i].EventType)
	}
}

func (s *EventReconcilerTestSuite) TestReconciler_FalsePositiveReorg_WithPreviousSkipped() {
	require := testutil.Require(s.T())

	var (
		tag                 uint32 = 2
		eventTag            uint32 = 2
		upgradeFromEventTag uint32 = 1
		upgradeFromBlockTag uint32 = 1
	)
	upgradeFromEvents := make([]*model.EventEntry, 5)
	upgradeFromEvents[0] = &model.EventEntry{
		EventId:      1,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  1,
		BlockHash:    "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[1] = &model.EventEntry{
		EventId:      2,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  2,
		BlockHash:    "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[2] = &model.EventEntry{
		EventId:      3,
		EventType:    api.BlockchainEvent_BLOCK_REMOVED,
		BlockHeight:  2,
		BlockHash:    "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: true,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[3] = &model.EventEntry{
		EventId:      4,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  2,
		BlockHash:    "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	upgradeFromEvents[4] = &model.EventEntry{
		EventId:      5,
		EventType:    api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:  3,
		BlockHash:    "0xaf63b64c85d6c44dd78159e983495106666ac96cd9a9e48716c210917de18fca",
		Tag:          upgradeFromBlockTag,
		BlockSkipped: false,
		EventTag:     upgradeFromEventTag,
	}
	block1 := &api.BlockMetadata{
		Tag:    tag,
		Height: 1,
		Hash:   "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
	}
	block2 := &api.BlockMetadata{
		Tag:    tag,
		Height: 2,
		Hash:   "0x0b2a3054e34ecd396fc7160a605b37087fccdf18feaa2fe76ee6d2950b7c8916",
	}
	block3 := &api.BlockMetadata{
		Tag:    tag,
		Height: 3,
		Hash:   "0xaf63b64c85d6c44dd78159e983495106666ac96cd9a9e48716c210917de18fca",
	}

	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, gomock.Any()).DoAndReturn(
		func(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
			if height == 1 {
				return block1, nil
			}
			if height == 2 {
				return block2, nil
			}
			if height == 3 {
				return block3, nil
			}

			return nil, nil
		}).Times(5)

	request := &EventReconcilerRequest{
		Tag:                 tag,
		EventTag:            eventTag,
		UpgradeFromEventTag: upgradeFromEventTag,
		UpgradeFromEvents:   upgradeFromEvents,
	}
	response, err := s.eventReconciler.Execute(s.env.BackgroundContext(), request)

	require.NoError(err)
	require.Equal(5, len(response.Eventdata))
	for i, updatedEvent := range response.Eventdata {
		require.Equal(tag, updatedEvent.Tag)
		require.Equal(eventTag, updatedEvent.EventTag)
		require.Equal(false, updatedEvent.BlockSkipped)
		require.Equal(upgradeFromEvents[i].EventType, response.Eventdata[i].EventType)
	}
}
