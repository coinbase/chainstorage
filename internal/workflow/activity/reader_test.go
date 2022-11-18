package activity

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type ReaderTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env         *cadence.TestEnv
	ctrl        *gomock.Controller
	metaStorage *metastoragemocks.MockMetaStorage
	app         testapp.TestApp
	reader      *Reader
}

func TestReaderTestSuite(t *testing.T) {
	suite.Run(t, new(ReaderTestSuite))
}

func (s *ReaderTestSuite) SetupTest() {
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
		fx.Populate(&s.reader),
	)
}

func (s *ReaderTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *ReaderTestSuite) TestReader() {
	const (
		tag    uint32 = 1
		height uint64 = 123456
		hash          = "0xabcd"
	)

	require := testutil.Require(s.T())

	metadata := &api.BlockMetadata{
		Tag:    tag,
		Hash:   hash,
		Height: height,
	}
	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(metadata, nil)
	request := &ReaderRequest{
		Tag:         tag,
		Height:      height,
		LatestBlock: false,
	}
	response, err := s.reader.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&ReaderResponse{
		Metadata: metadata,
	}, response)
}

func (s *ReaderTestSuite) TestReaderNotFound() {
	const (
		tag    uint32 = 1
		height uint64 = 123456
		hash          = "0xabcd"
	)

	require := testutil.Require(s.T())

	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(nil, storage.ErrItemNotFound)
	request := &ReaderRequest{
		Tag:         tag,
		Height:      height,
		LatestBlock: false,
	}
	response, err := s.reader.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Nil(response.Metadata)
}

func (s *ReaderTestSuite) TestReaderError() {
	const (
		tag    uint32 = 1
		height uint64 = 123456
	)

	require := testutil.Require(s.T())

	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(nil, storage.ErrInvalidHeight)
	request := &ReaderRequest{
		Tag:         tag,
		Height:      height,
		LatestBlock: false,
	}
	_, err := s.reader.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
}

func (s *ReaderTestSuite) TestReaderLatestBlock() {
	const (
		tag    uint32 = 1
		height uint64 = 13200000
		hash          = "0xabcd"
	)

	require := testutil.Require(s.T())

	metadata := &api.BlockMetadata{
		Tag:    tag,
		Hash:   hash,
		Height: height,
	}
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), tag).Return(metadata, nil)
	request := &ReaderRequest{
		Tag:         tag,
		Height:      height,
		LatestBlock: true,
	}
	response, err := s.reader.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&ReaderResponse{
		Metadata: metadata,
	}, response)
}
