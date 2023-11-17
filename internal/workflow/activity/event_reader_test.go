package activity

import (
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

type EventReaderTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env         *cadence.TestEnv
	ctrl        *gomock.Controller
	metaStorage *metastoragemocks.MockMetaStorage
	app         testapp.TestApp
	eventReader *EventReader
}

func TestEventReaderTestSuite(t *testing.T) {
	suite.Run(t, new(EventReaderTestSuite))
}

func (s *EventReaderTestSuite) SetupTest() {
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
		fx.Populate(&s.eventReader),
	)
}

func (s *EventReaderTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *EventReaderTestSuite) TestEventReader_ReadRangeOfEvents_Success() {
	var (
		eventTag      uint32 = 1
		startSequence uint64 = 123456
		endSequence   uint64 = 123458
	)

	require := testutil.Require(s.T())

	events := make([]*model.EventEntry, 2)
	for i := startSequence; i < endSequence; i++ {
		events = append(events, NewEventDDBEntry(eventTag, int64(i)))
	}
	s.metaStorage.EXPECT().GetEventsByEventIdRange(gomock.Any(), eventTag, int64(startSequence), int64(endSequence)).Return(events, nil)
	request := &EventReaderRequest{
		EventTag:      eventTag,
		StartSequence: startSequence,
		EndSequence:   endSequence,
	}
	response, err := s.eventReader.Execute(s.env.BackgroundContext(), request)

	require.NoError(err)
	require.Equal(&EventReaderResponse{
		Eventdata: events,
	}, response)
}

func (s *EventReaderTestSuite) TestEventReader_ReadRangeOfEvents_NotFound() {
	var (
		eventTag      uint32 = 1
		startSequence uint64 = 123456
		endSequence   uint64 = 123457
	)

	require := testutil.Require(s.T())

	s.metaStorage.EXPECT().GetEventsByEventIdRange(gomock.Any(), eventTag, int64(startSequence), int64(endSequence)).Return(nil, storage.ErrItemNotFound)
	request := &EventReaderRequest{
		EventTag:      eventTag,
		StartSequence: startSequence,
		EndSequence:   endSequence,
	}
	response, err := s.eventReader.Execute(s.env.BackgroundContext(), request)

	require.NoError(err)
	require.Nil(response.Eventdata)
}

func (s *EventReaderTestSuite) TestEventReader_ReadRangeOfEvents_Error() {
	var (
		eventTag      uint32 = 1
		startSequence uint64 = 123456
		endSequence   uint64 = 123457
	)

	require := testutil.Require(s.T())

	s.metaStorage.EXPECT().GetEventsByEventIdRange(gomock.Any(), eventTag, int64(startSequence), int64(endSequence)).Return(nil, storage.ErrInvalidEventId)
	request := &EventReaderRequest{
		EventTag:      eventTag,
		StartSequence: startSequence,
		EndSequence:   endSequence,
	}
	_, err := s.eventReader.Execute(s.env.BackgroundContext(), request)

	require.Error(err)
}

func NewEventDDBEntry(eventTag uint32, eventId int64) *model.EventEntry {
	return &model.EventEntry{
		EventId:        eventId,
		EventType:      api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight:    123456,
		BlockHash:      "mock_hash",
		Tag:            1,
		ParentHash:     "mock_parent_hash",
		BlockSkipped:   false,
		EventTag:       eventTag,
		BlockTimestamp: 123456,
	}
}
