package activity

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

type EventLoaderTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env         *cadence.TestEnv
	ctrl        *gomock.Controller
	metaStorage *metastoragemocks.MockMetaStorage
	app         testapp.TestApp
	eventLoader *EventLoader
}

func TestEventLoaderTestSuite(t *testing.T) {
	suite.Run(t, new(EventLoaderTestSuite))
}

func (s *EventLoaderTestSuite) SetupTest() {
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
		fx.Populate(&s.eventLoader),
	)
}

func (s *EventLoaderTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *EventLoaderTestSuite) TestEventLoader_Success() {
	require := testutil.Require(s.T())

	eventTag := uint32(1)
	events := make([]*model.EventEntry, 2)
	for i := 0; i < 2; i++ {
		events[i] = NewEventDDBEntry(eventTag, int64(i))
	}
	s.metaStorage.EXPECT().AddEventEntries(gomock.Any(), eventTag, events).Return(nil)
	request := &EventLoaderRequest{
		EventTag: eventTag,
		Events:   events,
	}
	response, err := s.eventLoader.Execute(s.env.BackgroundContext(), request)

	require.NoError(err)
	require.NotNil(response)
}

func (s *EventLoaderTestSuite) TestEventLoader_Error() {
	require := testutil.Require(s.T())

	eventTag := uint32(1)
	var events []*model.EventEntry
	s.metaStorage.EXPECT().AddEventEntries(gomock.Any(), eventTag, gomock.Any()).Return(xerrors.Errorf("error loading events"))
	request := &EventLoaderRequest{
		EventTag: eventTag,
		Events:   events,
	}
	_, err := s.eventLoader.Execute(s.env.BackgroundContext(), request)

	require.Error(err)
}
