package activity

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type LoaderTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env         *cadence.TestEnv
	ctrl        *gomock.Controller
	metaStorage *metastoragemocks.MockMetaStorage
	app         testapp.TestApp
	loader      *Loader
}

func TestLoaderTestSuite(t *testing.T) {
	suite.Run(t, new(LoaderTestSuite))
}

func (s *LoaderTestSuite) SetupTest() {
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
		fx.Populate(&s.loader),
	)
}

func (s *LoaderTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *LoaderTestSuite) TestLoader() {
	require := testutil.Require(s.T())

	blocks := []*api.BlockMetadata{
		{Height: 1},
		{Height: 2},
		{Height: 3},
	}
	lastBlock := &api.BlockMetadata{
		Height: 0,
	}
	s.metaStorage.EXPECT().PersistBlockMetas(gomock.Any(), true, blocks, lastBlock).Return(nil)
	request := &LoaderRequest{
		Metadata:        blocks,
		LastBlock:       lastBlock,
		UpdateWatermark: true,
	}
	response, err := s.loader.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.NotNil(response)
}
