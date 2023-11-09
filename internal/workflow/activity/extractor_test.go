package activity

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/pointer"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type ExtractorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env              *cadence.TestEnv
	ctrl             *gomock.Controller
	blockchainClient *clientmocks.MockClient
	metaStorage      *metastoragemocks.MockMetaStorage
	blobStorage      *blobstoragemocks.MockBlobStorage
	app              testapp.TestApp
	extractor        *Extractor
}

func TestExtractorTestSuite(t *testing.T) {
	suite.Run(t, new(ExtractorTestSuite))
}

func (s *ExtractorTestSuite) SetupTest() {
	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.blockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		fx.Provide(func() blobstorage.BlobStorage {
			return s.blobStorage
		}),
		fx.Provide(fx.Annotated{
			Name: "slave",
			Target: func() client.Client {
				return s.blockchainClient
			},
		}),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Provide(dlq.NewNop),
		fx.Populate(&s.extractor),
	)
}

func (s *ExtractorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *ExtractorTestSuite) TestSuccess() {
	const (
		tag       uint32 = 1
		height    uint64 = 123456
		hash             = "0xabcd"
		objectKey        = "foo/bar"
	)

	require := testutil.Require(s.T())

	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    tag,
			Hash:   hash,
			Height: height,
		},
	}
	s.blockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(block, nil)
	s.blobStorage.EXPECT().Upload(gomock.Any(), block, api.Compression_NONE).Return(objectKey, nil)

	response, err := s.extractor.Execute(s.env.BackgroundContext(), &ExtractorRequest{
		Tag:    tag,
		Height: height,
	})
	require.NoError(err)
	require.NotNil(response.Metadata)
	require.Equal(tag, response.Metadata.Tag)
	require.Equal(height, response.Metadata.Height)
	require.Equal(hash, response.Metadata.Hash)
	require.Equal(objectKey, response.Metadata.ObjectKeyMain)
}

func (s *ExtractorTestSuite) TestWithBestEffort() {
	const (
		tag       uint32 = 1
		height    uint64 = 123456
		hash             = "0xabcd"
		objectKey        = "foo/bar"
	)

	require := testutil.Require(s.T())

	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    tag,
			Hash:   hash,
			Height: height,
		},
	}
	s.blockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), tag, height, gomock.Any()).Return(block, nil)
	s.blobStorage.EXPECT().Upload(gomock.Any(), block, api.Compression_NONE).Return(objectKey, nil)
	response, err := s.extractor.Execute(s.env.BackgroundContext(), &ExtractorRequest{
		Tag:            tag,
		Height:         height,
		WithBestEffort: true,
	})

	require.NoError(err)
	require.NotNil(response.Metadata)
	require.Equal(tag, response.Metadata.Tag)
	require.Equal(height, response.Metadata.Height)
	require.Equal(hash, response.Metadata.Hash)
	require.Equal(objectKey, response.Metadata.ObjectKeyMain)
}

func (s *ExtractorTestSuite) TestUpgradeWithWrongTag() {
	const (
		oldTag uint32 = 2
		newTag uint32 = 1
		height uint64 = 123456
	)

	require := testutil.Require(s.T())

	_, err := s.extractor.Execute(s.env.BackgroundContext(), &ExtractorRequest{
		Tag:            newTag,
		Height:         height,
		UpgradeFromTag: pointer.Uint32(oldTag),
	})

	require.Error(err)
	require.Contains(err.Error(), "invalid UpgradeFromTag")
}

func (s *ExtractorTestSuite) TestUpgradeSuccess() {
	const (
		oldTag    uint32 = 0
		newTag    uint32 = 1
		height    uint64 = 123456
		hash             = "0xabcd"
		objectKey        = "foo/bar"
	)

	require := testutil.Require(s.T())

	metadata := &api.BlockMetadata{
		Tag:           oldTag,
		Hash:          hash,
		Height:        height,
		ObjectKeyMain: objectKey,
	}
	block := &api.Block{
		Metadata: metadata,
	}
	newBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:           newTag,
			Hash:          hash,
			Height:        height,
			ObjectKeyMain: objectKey,
		},
	}
	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), oldTag, height).Return(metadata, nil)
	s.blobStorage.EXPECT().Download(gomock.Any(), block.Metadata).Return(block, nil)
	s.blockchainClient.EXPECT().UpgradeBlock(gomock.Any(), block, newTag).Return(newBlock, nil)
	s.blobStorage.EXPECT().Upload(gomock.Any(), newBlock, api.Compression_NONE).Return(objectKey, nil)
	response, err := s.extractor.Execute(s.env.BackgroundContext(), &ExtractorRequest{
		Tag:            newTag,
		Height:         height,
		UpgradeFromTag: pointer.Uint32(oldTag),
	})

	require.NoError(err)
	require.NotNil(response.Metadata)
	require.Equal(newTag, response.Metadata.Tag)
	require.Equal(height, response.Metadata.Height)
	require.Equal(hash, response.Metadata.Hash)
	require.Equal(objectKey, response.Metadata.ObjectKeyMain)
}

func (s *ExtractorTestSuite) TestRehydrateSuccess() {
	const (
		oldTag    uint32 = 0
		newTag    uint32 = 1
		height    uint64 = 123456
		hash             = "0xabcd"
		objectKey        = "foo/bar"
	)

	require := testutil.Require(s.T())

	metadata := &api.BlockMetadata{
		Tag:           oldTag,
		Hash:          hash,
		Height:        height,
		ObjectKeyMain: objectKey,
	}
	block := &api.Block{
		Metadata: metadata,
	}
	newBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:           newTag,
			Hash:          hash,
			Height:        height,
			ObjectKeyMain: objectKey,
		},
	}
	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), oldTag, height).Return(metadata, nil)
	s.blobStorage.EXPECT().Download(gomock.Any(), block.Metadata).Return(block, nil)
	s.blobStorage.EXPECT().Upload(gomock.Any(), newBlock, api.Compression_NONE).Return(objectKey, nil)
	response, err := s.extractor.Execute(s.env.BackgroundContext(), &ExtractorRequest{
		Tag:              newTag,
		Height:           height,
		RehydrateFromTag: pointer.Uint32(oldTag),
	})

	require.NoError(err)
	require.NotNil(response.Metadata)
	require.Equal(newTag, response.Metadata.Tag)
	require.Equal(height, response.Metadata.Height)
	require.Equal(hash, response.Metadata.Hash)
	require.Equal(objectKey, response.Metadata.ObjectKeyMain)
}

func (s *ExtractorTestSuite) TestWithDataCompression() {
	const (
		tag       uint32 = 1
		height    uint64 = 123456
		hash             = "0xabcd"
		objectKey        = "foo/bar.gzip"
	)

	require := testutil.Require(s.T())

	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    tag,
			Hash:   hash,
			Height: height,
		},
	}
	s.blockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), tag, height, gomock.Any()).Return(block, nil)
	s.blobStorage.EXPECT().Upload(gomock.Any(), block, api.Compression_GZIP).Return(objectKey, nil)
	response, err := s.extractor.Execute(s.env.BackgroundContext(), &ExtractorRequest{
		Tag:             tag,
		Height:          height,
		DataCompression: api.Compression_GZIP,
	})

	require.NoError(err)
	require.NotNil(response.Metadata)
	require.Equal(tag, response.Metadata.Tag)
	require.Equal(height, response.Metadata.Height)
	require.Equal(hash, response.Metadata.Hash)
	require.Equal(objectKey, response.Metadata.ObjectKeyMain)
}
