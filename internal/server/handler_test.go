package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/consts"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	expectedBlockFile1 = &api.BlockFile{
		Tag:        0,
		Hash:       "hash1",
		ParentHash: "hash0",
		Height:     9000,
		FileUrl:    "http://endpoint/foo",
	}
	expectedBlockFile2 = &api.BlockFile{
		Tag:        0,
		Hash:       "hash2",
		ParentHash: "hash1",
		Height:     9001,
		FileUrl:    "http://endpoint/bar",
	}
)

type handlerTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	app                   testapp.TestApp
	awsClient             *client.Client
	metaStorage           *metastoragemocks.MockMetaStorage
	blobStorage           *blobstoragemocks.MockBlobStorage
	s3Client              *s3mocks.MockClient
	server                *Server
	config                *config.Config
	tagForTestEvents      uint32
	eventTagForTestEvents uint32
}

func TestHandlerSuite(t *testing.T) {
	suite.Run(t, new(handlerTestSuite))
}

func (s *handlerTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.awsClient = awstesting.NewClient(&aws.Config{
		Region: unit.Session.Config.Region,
	})
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.s3Client = s3mocks.NewMockClient(s.ctrl)
	s.app = testapp.New(
		s.T(),
		parser.Module,
		fx.Provide(func() metastorage.MetaStorage { return s.metaStorage }),
		fx.Provide(func() blobstorage.BlobStorage { return s.blobStorage }),
		fx.Provide(func() s3.Client { return s.s3Client }),
		fx.Provide(NewServer),
		fx.Populate(&s.server),
		fx.Populate(&s.config),
	)
	s.tagForTestEvents = 1
	s.eventTagForTestEvents = 0
}

func (s *handlerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *handlerTestSuite) TestGetLatestBlock_NotFound() {
	require := testutil.Require(s.T())
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			return nil, storage.ErrItemNotFound
		},
	)
	resp, err := s.server.GetLatestBlock(context.Background(), &api.GetLatestBlockRequest{Tag: uint32(0)})
	require.Nil(resp)
	s.verifyStatusCode(codes.NotFound, err)
}

func (s *handlerTestSuite) TestGetLatestBlock_DynamodbInternalErr() {
	require := testutil.Require(s.T())
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			return nil, fmt.Errorf("fail")
		},
	)
	resp, err := s.server.GetLatestBlock(context.Background(), &api.GetLatestBlockRequest{Tag: uint32(0)})
	require.Nil(resp)
	s.verifyStatusCode(codes.Internal, err)
}

func (s *handlerTestSuite) TestGetLatestBlockSuccess_StableTag() {
	require := testutil.Require(s.T())
	stableTag := s.app.Config().GetStableBlockTag()
	expectedBlock := &api.BlockMetadata{
		Tag:           stableTag,
		Hash:          "hash1",
		ParentHash:    "hash0",
		Height:        9000,
		ObjectKeyMain: "foo",
	}
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			require.Equal(stableTag, tag)
			return expectedBlock, nil
		},
	)
	resp, err := s.server.GetLatestBlock(context.Background(), &api.GetLatestBlockRequest{})
	require.NotNil(resp)
	require.Equal(expectedBlock.Tag, resp.Tag)
	require.Equal(expectedBlock.Hash, resp.Hash)
	require.Equal(expectedBlock.ParentHash, resp.ParentHash)
	require.Equal(expectedBlock.Height, resp.Height)
	require.NoError(err)
}

func (s *handlerTestSuite) TestGetLatestBlockSuccess_LatestTag() {
	require := testutil.Require(s.T())
	expectedBlock := &api.BlockMetadata{
		Tag:           s.app.Config().GetLatestBlockTag(),
		Hash:          "hash1",
		ParentHash:    "hash0",
		Height:        9000,
		ObjectKeyMain: "foo",
	}
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			return expectedBlock, nil
		},
	)
	resp, err := s.server.GetLatestBlock(context.Background(), &api.GetLatestBlockRequest{Tag: s.app.Config().GetLatestBlockTag()})
	require.NotNil(resp)
	require.Equal(expectedBlock.Tag, resp.Tag)
	require.Equal(expectedBlock.Hash, resp.Hash)
	require.Equal(expectedBlock.ParentHash, resp.ParentHash)
	require.Equal(expectedBlock.Height, resp.Height)
	require.NoError(err)
}

func (s *handlerTestSuite) TestGetBlockFile() {
	const (
		height        uint64 = 13193825
		hash                 = "0xda5a0439434adf072394e0b94f78e56032c5409a2c58668995f306b171ff4ace"
		parentHash           = "0xba6a6c85739b50384625e10718524fb2c1fcf88858eabf6db9bd851902b53546"
		objectKeyMain        = "foo/bar"
	)

	require := testutil.Require(s.T())
	tag := s.app.Config().Chain.BlockTag.Stable
	expected := &api.BlockFile{
		Tag:        tag,
		Hash:       hash,
		ParentHash: parentHash,
		Height:     height,
		FileUrl:    "http://endpoint/foo/bar",
	}
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, height_ uint64, hash_ string) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(height, height_)
				require.Equal(hash, hash_)
				return &api.BlockMetadata{
					Tag:           tag,
					Hash:          hash,
					ParentHash:    parentHash,
					Height:        height,
					ObjectKeyMain: objectKeyMain,
				}, nil
			},
		),
		s.s3Client.EXPECT().GetObjectRequest(gomock.Any()).Times(1).DoAndReturn(
			func(req *awss3.GetObjectInput) (*request.Request, *awss3.GetObjectOutput) {
				require.Equal("example-chainstorage-ethereum-mainnet-dev", *req.Bucket)
				require.Equal(objectKeyMain, *req.Key)
				return s.newAwsPresignRequest("name", "GET", objectKeyMain), nil
			},
		),
	)

	resp, err := s.server.GetBlockFile(context.Background(), &api.GetBlockFileRequest{
		Height: height,
		Hash:   hash,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(expected, resp.File)
}

func (s *handlerTestSuite) TestGetBlockFile_Gzip() {
	const (
		height        uint64 = 13193825
		hash                 = "0xda5a0439434adf072394e0b94f78e56032c5409a2c58668995f306b171ff4ace"
		parentHash           = "0xba6a6c85739b50384625e10718524fb2c1fcf88858eabf6db9bd851902b53546"
		objectKeyMain        = "foo/bar.gzip"
	)

	require := testutil.Require(s.T())
	tag := s.app.Config().Chain.BlockTag.Stable
	expected := &api.BlockFile{
		Tag:         tag,
		Hash:        hash,
		ParentHash:  parentHash,
		Height:      height,
		FileUrl:     "http://endpoint/foo/bar.gzip",
		Compression: api.Compression_GZIP,
	}
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, height_ uint64, hash_ string) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(height, height_)
				require.Equal(hash, hash_)
				return &api.BlockMetadata{
					Tag:           tag,
					Hash:          hash,
					ParentHash:    parentHash,
					Height:        height,
					ObjectKeyMain: objectKeyMain,
				}, nil
			},
		),
		s.s3Client.EXPECT().GetObjectRequest(gomock.Any()).Times(1).DoAndReturn(
			func(req *awss3.GetObjectInput) (*request.Request, *awss3.GetObjectOutput) {
				require.Equal("example-chainstorage-ethereum-mainnet-dev", *req.Bucket)
				require.Equal(objectKeyMain, *req.Key)
				return s.newAwsPresignRequest("name", "GET", objectKeyMain), nil
			},
		),
	)

	resp, err := s.server.GetBlockFile(context.Background(), &api.GetBlockFileRequest{
		Height: height,
		Hash:   hash,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(expected, resp.File)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_InvalidRange() {
	require := testutil.Require(s.T())
	resp, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 1,
		EndHeight:   1,
	})
	require.Nil(resp)
	s.verifyStatusCode(codes.InvalidArgument, err)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_InvalidTag() {
	require := testutil.Require(s.T())
	resp, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		Tag:         s.app.Config().GetLatestBlockTag() + 1,
		StartHeight: 1,
		EndHeight:   1,
	})
	require.Nil(resp)
	s.verifyStatusCode(codes.InvalidArgument, err)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_MaxRangeExceeded() {
	require := testutil.Require(s.T())
	resp, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 0,
		EndHeight:   s.app.Config().Api.MaxNumBlockFiles + 1,
	})
	require.Nil(resp)
	s.verifyStatusCode(codes.InvalidArgument, err)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_NotFound() {
	require := testutil.Require(s.T())
	s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			require.Equal(uint64(1), startHeight)
			require.Equal(uint64(3), endHeight)
			return nil, nil
		},
	)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			return &api.BlockMetadata{
				Tag:           0,
				Hash:          "hash1",
				ParentHash:    "hash0",
				Height:        1000,
				ObjectKeyMain: "key",
			}, nil
		},
	)
	resp, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 1,
		EndHeight:   3,
	})
	require.NoError(err)
	require.Len(resp.GetFiles(), 0)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_GetBlocksByHeightRangeCancelled() {
	require := testutil.Require(s.T())
	s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			return nil, storage.ErrRequestCanceled
		},
	)
	_, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 1,
		EndHeight:   3,
	})
	require.Error(err)
	s.verifyStatusCode(codes.Canceled, err)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_GetLatestBlockCancelled() {
	require := testutil.Require(s.T())
	s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
			require.Equal(uint64(1), startHeight)
			require.Equal(uint64(3), endHeight)
			return []*api.BlockMetadata{
				{ObjectKeyMain: "foo"},
				{ObjectKeyMain: "bar"},
			}, nil
		},
	)
	s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			return nil, storage.ErrRequestCanceled
		},
	)
	_, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 1,
		EndHeight:   3,
	})
	require.Error(err)
	s.verifyStatusCode(codes.Canceled, err)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_PresignErr() {
	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
				require.Equal(uint64(1), startHeight)
				require.Equal(uint64(3), endHeight)
				return []*api.BlockMetadata{
					{ObjectKeyMain: "foo"},
					{ObjectKeyMain: "bar"},
				}, nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
				return &api.BlockMetadata{
					Tag:           0,
					Hash:          "hash1",
					ParentHash:    "hash0",
					Height:        1000,
					ObjectKeyMain: "key",
				}, nil
			},
		),
		s.s3Client.EXPECT().GetObjectRequest(gomock.Any()).Times(1).DoAndReturn(
			func(req *awss3.GetObjectInput) (*request.Request, *awss3.GetObjectOutput) {
				require.Equal("example-chainstorage-ethereum-mainnet-dev", *req.Bucket)
				require.Equal("foo", *req.Key)
				return s.awsClient.NewRequest(&request.Operation{
					Name:       "name",
					HTTPMethod: "GET",
					HTTPPath:   "/foo",
				}, &struct{}{}, &struct{}{}), nil
			},
		),
		s.s3Client.EXPECT().GetObjectRequest(gomock.Any()).Times(1).DoAndReturn(
			func(req *awss3.GetObjectInput) (*request.Request, *awss3.GetObjectOutput) {
				require.Equal("example-chainstorage-ethereum-mainnet-dev", *req.Bucket)
				require.Equal("bar", *req.Key)
				return s.newAwsPresignFailedRequest(), nil
			},
		),
	)

	resp, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 1,
		EndHeight:   3,
	})
	require.Nil(resp)
	s.verifyStatusCode(codes.Internal, err)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange() {
	require := testutil.Require(s.T())
	stableTag := s.app.Config().Chain.BlockTag.Stable
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
				require.Equal(stableTag, tag)
				require.Equal(uint64(9000), startHeight)
				require.Equal(uint64(9002), endHeight)
				return []*api.BlockMetadata{
					{Hash: "hash1", ParentHash: "hash0", Height: 9000, ObjectKeyMain: "foo"},
					{Hash: "hash2", ParentHash: "hash1", Height: 9001, ObjectKeyMain: "bar"},
				}, nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
				require.Equal(stableTag, tag)
				return &api.BlockMetadata{
					Tag:           0,
					Hash:          "hash3",
					ParentHash:    "hash4",
					Height:        100000,
					ObjectKeyMain: "key",
				}, nil
			},
		),
		s.s3Client.EXPECT().GetObjectRequest(gomock.Any()).Times(1).DoAndReturn(
			func(req *awss3.GetObjectInput) (*request.Request, *awss3.GetObjectOutput) {
				require.Equal("example-chainstorage-ethereum-mainnet-dev", *req.Bucket)
				require.Equal("foo", *req.Key)
				return s.newAwsPresignRequest("name", "GET", "/foo"), nil
			},
		),
		s.s3Client.EXPECT().GetObjectRequest(gomock.Any()).Times(1).DoAndReturn(
			func(req *awss3.GetObjectInput) (*request.Request, *awss3.GetObjectOutput) {
				require.Equal("example-chainstorage-ethereum-mainnet-dev", *req.Bucket)
				require.Equal("bar", *req.Key)
				return s.newAwsPresignRequest("name", "GET", "/bar"), nil
			},
		),
	)

	resp, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 9000,
		EndHeight:   9002,
	})
	require.NotNil(resp)
	require.Len(resp.GetFiles(), 2)
	require.Equal(expectedBlockFile1, resp.GetFiles()[0])
	require.Equal(expectedBlockFile2, resp.GetFiles()[1])
	require.NoError(err)
}

func (s *handlerTestSuite) TestGetBlockFilesByRange_ExceededLatest() {
	require := testutil.Require(s.T())
	latest := uint64(99)
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
				require.Equal(uint64(100), startHeight)
				require.Equal(uint64(101), endHeight)
				return []*api.BlockMetadata{
					{Hash: "hash1", ParentHash: "hash0", Height: 100, ObjectKeyMain: "foo"},
				}, nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
				return &api.BlockMetadata{
					Tag:           0,
					Hash:          "hash3",
					ParentHash:    "hash4",
					Height:        latest,
					ObjectKeyMain: "key",
				}, nil
			},
		),
	)

	resp, err := s.server.GetBlockFilesByRange(context.Background(), &api.GetBlockFilesByRangeRequest{
		StartHeight: 100,
		EndHeight:   101,
	})
	require.Nil(resp)
	s.verifyStatusCode(codes.FailedPrecondition, err)
}

func (s *handlerTestSuite) TestGetRawBlock() {
	const (
		height uint64 = 13193825
	)

	require := testutil.Require(s.T())
	tag := s.app.Config().Chain.BlockTag.Stable
	blockMetadata := testutil.MakeBlockMetadatasFromStartHeight(height, 1, tag)[0]
	block := testutil.MakeBlocksFromStartHeight(height, 1, tag)[0]
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, height uint64, hash string) (*api.BlockMetadata, error) {
				require.Equal(blockMetadata.Tag, tag)
				require.Equal(blockMetadata.Height, height)
				require.Equal(blockMetadata.Hash, hash)
				return blockMetadata, nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				require.Equal(blockMetadata.ObjectKeyMain, metadata.ObjectKeyMain)
				require.Equal(storage_utils.GetCompressionType(metadata.ObjectKeyMain), api.Compression_NONE)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetRawBlock(context.Background(), &api.GetRawBlockRequest{
		Height: blockMetadata.Height,
		Hash:   blockMetadata.Hash,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(block, resp.Block)
}

func (s *handlerTestSuite) TestGetRawBlock_Gzip() {
	const (
		height uint64 = 13193825
	)

	require := testutil.Require(s.T())
	tag := s.app.Config().Chain.BlockTag.Stable
	blockMetadata := testutil.MakeBlockMetadatasFromStartHeight(height, 1, tag, testutil.WithDataCompression(api.Compression_GZIP))[0]
	block := testutil.MakeBlocksFromStartHeight(height, 1, tag)[0]
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, height uint64, hash string) (*api.BlockMetadata, error) {
				require.Equal(blockMetadata.Tag, tag)
				require.Equal(blockMetadata.Height, height)
				require.Equal(blockMetadata.Hash, hash)
				return blockMetadata, nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				require.Equal(blockMetadata.ObjectKeyMain, metadata.ObjectKeyMain)
				require.Equal(storage_utils.GetCompressionType(metadata.ObjectKeyMain), api.Compression_GZIP)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetRawBlock(context.Background(), &api.GetRawBlockRequest{
		Height: blockMetadata.Height,
		Hash:   blockMetadata.Hash,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(block, resp.Block)
}

func (s *handlerTestSuite) TestGetRawBlocksByRange_StableTag() {
	const (
		startHeight uint64 = 9000
		endHeight   uint64 = 9006
		numBlocks          = int(endHeight - startHeight)
	)

	tag := s.app.Config().GetStableBlockTag()
	blockMetadatas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag)
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blocksByObjectKey := make(map[string]*api.Block, len(blocks))
	for i, block := range blockMetadatas {
		blocksByObjectKey[block.ObjectKeyMain] = blocks[i]
	}

	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, startHeight_, endHeight_ uint64) ([]*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(startHeight, startHeight_)
				require.Equal(endHeight, endHeight_)
				return testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag), nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				return testutil.MakeBlockMetadata(10000, tag), nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(numBlocks).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				block, ok := blocksByObjectKey[metadata.ObjectKeyMain]
				require.True(ok)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetRawBlocksByRange(context.Background(), &api.GetRawBlocksByRangeRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Len(resp.Blocks, numBlocks)
	for i := 0; i < numBlocks; i++ {
		require.Equal(blockMetadatas[i], resp.Blocks[i].Metadata)
	}
}

func (s *handlerTestSuite) TestGetRawBlocksByRange_LatestTag() {
	const (
		startHeight uint64 = 9000
		endHeight   uint64 = 9006
		numBlocks          = int(endHeight - startHeight)
	)

	tag := s.app.Config().GetLatestBlockTag()
	blockMetadatas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag)
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blocksByObjectKey := make(map[string]*api.Block, len(blocks))
	for i, block := range blockMetadatas {
		blocksByObjectKey[block.ObjectKeyMain] = blocks[i]
	}

	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, startHeight_, endHeight_ uint64) ([]*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(startHeight, startHeight_)
				require.Equal(endHeight, endHeight_)
				return testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag), nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				return testutil.MakeBlockMetadata(10000, tag), nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(numBlocks).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				block, ok := blocksByObjectKey[metadata.ObjectKeyMain]
				require.True(ok)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetRawBlocksByRange(context.Background(), &api.GetRawBlocksByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Len(resp.Blocks, numBlocks)
	for i := 0; i < numBlocks; i++ {
		require.Equal(blockMetadatas[i], resp.Blocks[i].Metadata)
	}
}

func (s *handlerTestSuite) TestGetRawBlocksByRange_DownloadError() {
	const (
		startHeight  uint64 = 9000
		endHeight    uint64 = 9037
		failedHeight uint64 = 9035
		numBlocks           = int(endHeight - startHeight)
	)

	tag := s.app.Config().GetLatestBlockTag()
	blockMetadatas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag)
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blocksByObjectKey := make(map[string]*api.Block, len(blocks))
	for i, block := range blockMetadatas {
		blocksByObjectKey[block.ObjectKeyMain] = blocks[i]
	}

	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, startHeight_, endHeight_ uint64) ([]*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(startHeight, startHeight_)
				require.Equal(endHeight, endHeight_)
				return testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag), nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				return testutil.MakeBlockMetadata(10000, tag), nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				block, ok := blocksByObjectKey[metadata.ObjectKeyMain]
				require.True(ok)
				if block.Metadata.Height == failedHeight {
					return nil, fmt.Errorf("mock download error")
				}
				return block, nil
			},
		),
	)

	_, err := s.server.GetRawBlocksByRange(context.Background(), &api.GetRawBlocksByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	require.Error(err)
}

func (s *handlerTestSuite) TestGetRawBlocksByRange_DownloadCancelError() {
	const (
		startHeight uint64 = 9000
		endHeight   uint64 = 9037
		numBlocks          = int(endHeight - startHeight)
	)

	tag := s.app.Config().GetLatestBlockTag()

	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, startHeight_, endHeight_ uint64) ([]*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(startHeight, startHeight_)
				require.Equal(endHeight, endHeight_)
				return testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag), nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				return testutil.MakeBlockMetadata(10000, tag), nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(_ context.Context, _ *api.BlockMetadata) (*api.Block, error) {
				return nil, storage.ErrRequestCanceled
			},
		),
	)

	_, err := s.server.GetRawBlocksByRange(context.Background(), &api.GetRawBlocksByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	require.Error(err)
	s.verifyStatusCode(codes.Canceled, err)
}

func (s *handlerTestSuite) TestGetRawBlocksByRange_MaxRangeExceeded() {
	require := testutil.Require(s.T())
	resp, err := s.server.GetRawBlocksByRange(context.Background(), &api.GetRawBlocksByRangeRequest{
		Tag:         0,
		StartHeight: 0,
		EndHeight:   s.app.Config().Api.MaxNumBlocks + 1,
	})
	require.Nil(resp)
	s.verifyStatusCode(codes.InvalidArgument, err)
}

func (s *handlerTestSuite) TestGetNativeBlockByRange() {
	const (
		height uint64 = 9000
	)

	tag := s.app.Config().GetLatestBlockTag()
	blockMetadata := testutil.MakeBlockMetadatasFromStartHeight(height, 1, tag)[0]
	block := testutil.MakeBlocksFromStartHeight(height, 1, tag)[0]
	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, height uint64, hash string) (*api.BlockMetadata, error) {
				require.Equal(blockMetadata.Tag, tag)
				require.Equal(blockMetadata.Height, height)
				require.Equal(blockMetadata.Hash, hash)
				return blockMetadata, nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				require.Equal(blockMetadata.ObjectKeyMain, metadata.ObjectKeyMain)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetNativeBlock(context.Background(), &api.GetNativeBlockRequest{
		Tag:    blockMetadata.Tag,
		Height: blockMetadata.Height,
		Hash:   blockMetadata.Hash,
	})
	require.NoError(err)
	require.NotNil(resp)
	nativeBlock := resp.Block.GetEthereum()
	require.NotNil(nativeBlock)
	require.Equal(blockMetadata.Hash, nativeBlock.Header.Hash)
	require.Equal(blockMetadata.ParentHash, nativeBlock.Header.ParentHash)
	require.Equal(blockMetadata.Height, nativeBlock.Header.Number)
}

func (s *handlerTestSuite) TestGetNativeBlocksByRange() {
	const (
		startHeight uint64 = 9000
		endHeight   uint64 = 9043
		numBlocks          = int(endHeight - startHeight)
	)

	tag := s.app.Config().GetLatestBlockTag()
	blockMetadatas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag)
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blocksByObjectKey := make(map[string]*api.Block, len(blocks))
	for i, block := range blockMetadatas {
		blocksByObjectKey[block.ObjectKeyMain] = blocks[i]
	}

	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, startHeight_, endHeight_ uint64) ([]*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(startHeight, startHeight_)
				require.Equal(endHeight, endHeight_)
				return testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag), nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				return testutil.MakeBlockMetadata(10000, tag), nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(numBlocks).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				block, ok := blocksByObjectKey[metadata.ObjectKeyMain]
				require.True(ok)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetNativeBlocksByRange(context.Background(), &api.GetNativeBlocksByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Len(resp.Blocks, numBlocks)
	for i := 0; i < numBlocks; i++ {
		nativeBlock := resp.Blocks[i].GetEthereum()
		require.NotNil(nativeBlock)
		require.Equal(blockMetadatas[i].Hash, nativeBlock.Header.Hash)
		require.Equal(blockMetadatas[i].ParentHash, nativeBlock.Header.ParentHash)
		require.Equal(blockMetadatas[i].Height, nativeBlock.Header.Number)
	}
}

func (s *handlerTestSuite) TestGetRosettaBlock() {
	const (
		height uint64 = 9000
	)

	tag := s.app.Config().GetLatestBlockTag()
	blockMetadata := testutil.MakeBlockMetadatasFromStartHeight(height, 1, tag)[0]
	block := testutil.MakeBlocksFromStartHeight(height, 1, tag)[0]
	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, height uint64, hash string) (*api.BlockMetadata, error) {
				require.Equal(blockMetadata.Tag, tag)
				require.Equal(blockMetadata.Height, height)
				require.Equal("", hash)
				return blockMetadata, nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				require.Equal(blockMetadata.ObjectKeyMain, metadata.ObjectKeyMain)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetRosettaBlock(context.Background(), &api.GetRosettaBlockRequest{
		Tag:    blockMetadata.Tag,
		Height: blockMetadata.Height,
	})
	require.NoError(err)
	require.NotNil(resp)
	blockIdentifier := resp.Block.Block.BlockIdentifier
	require.Equal(blockMetadata.Hash, blockIdentifier.Hash)
	require.Equal(int64(blockMetadata.Height), blockIdentifier.Index)
}

func (s *handlerTestSuite) TestGetRosettaBlock_NotImplemented() {
	const (
		height uint64 = 9000
	)

	var server *Server
	app := testapp.New(
		s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET), // Solana RosettaParser is not implemented
		parser.Module,
		fx.Provide(func() metastorage.MetaStorage { return s.metaStorage }),
		fx.Provide(func() blobstorage.BlobStorage { return s.blobStorage }),
		fx.Provide(func() s3.Client { return s.s3Client }),
		fx.Provide(NewServer),
		fx.Populate(&server),
	)

	tag := app.Config().GetLatestBlockTag()
	blockMetadata := testutil.MakeBlockMetadatasFromStartHeight(height, 1, tag)[0]
	block := testutil.MakeBlocksFromStartHeight(height, 1, tag)[0]
	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag uint32, height uint64, hash string) (*api.BlockMetadata, error) {
				require.Equal(blockMetadata.Tag, tag)
				require.Equal(blockMetadata.Height, height)
				require.Equal("", hash)
				return blockMetadata, nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				require.Equal(blockMetadata.ObjectKeyMain, metadata.ObjectKeyMain)
				return block, nil
			},
		),
	)

	_, err := server.GetRosettaBlock(context.Background(), &api.GetRosettaBlockRequest{
		Tag:    blockMetadata.Tag,
		Height: blockMetadata.Height,
	})
	require.Error(err)
	s.verifyStatusCode(codes.Unimplemented, err)
}

func (s *handlerTestSuite) TestGetRosettaBlocksByRange() {
	const (
		startHeight uint64 = 9000
		endHeight   uint64 = 9050
		numBlocks          = int(endHeight - startHeight)
	)

	tag := s.app.Config().GetLatestBlockTag()
	blockMetadatas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag)
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blocksByObjectKey := make(map[string]*api.Block, len(blocks))
	for i, block := range blockMetadatas {
		blocksByObjectKey[block.ObjectKeyMain] = blocks[i]
	}

	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, startHeight_, endHeight_ uint64) ([]*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(startHeight, startHeight_)
				require.Equal(endHeight, endHeight_)
				return testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag), nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				return testutil.MakeBlockMetadata(10000, tag), nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(numBlocks).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				block, ok := blocksByObjectKey[metadata.ObjectKeyMain]
				require.True(ok)
				return block, nil
			},
		),
	)

	resp, err := s.server.GetRosettaBlocksByRange(context.Background(), &api.GetRosettaBlocksByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	require.NoError(err)
	require.NotNil(resp)
	require.Len(resp.Blocks, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blockIdentifier := resp.Blocks[i].Block.BlockIdentifier
		require.Equal(blockMetadatas[i].Hash, blockIdentifier.Hash)
		require.Equal(int64(blockMetadatas[i].Height), blockIdentifier.Index)
	}
}

func (s *handlerTestSuite) TestGetRosettaBlocksByRange_NotImplemented() {
	const (
		startHeight uint64 = 9000
		endHeight   uint64 = 9050
		numBlocks          = int(endHeight - startHeight)
	)

	var server *Server
	app := testapp.New(
		s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET), // Solana RosettaParser is not implemented
		parser.Module,
		fx.Provide(func() metastorage.MetaStorage { return s.metaStorage }),
		fx.Provide(func() blobstorage.BlobStorage { return s.blobStorage }),
		fx.Provide(func() s3.Client { return s.s3Client }),
		fx.Provide(NewServer),
		fx.Populate(&server),
	)

	tag := app.Config().GetLatestBlockTag()
	blockMetadatas := testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag)
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blocksByObjectKey := make(map[string]*api.Block, len(blocks))
	for i, block := range blockMetadatas {
		blocksByObjectKey[block.ObjectKeyMain] = blocks[i]
	}

	require := testutil.Require(s.T())
	gomock.InOrder(
		s.metaStorage.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32, startHeight_, endHeight_ uint64) ([]*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				require.Equal(startHeight, startHeight_)
				require.Equal(endHeight, endHeight_)
				return testutil.MakeBlockMetadatasFromStartHeight(startHeight, numBlocks, tag), nil
			},
		),
		s.metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, tag_ uint32) (*api.BlockMetadata, error) {
				require.Equal(tag, tag_)
				return testutil.MakeBlockMetadata(10000, tag), nil
			},
		),
		s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).Times(numBlocks).DoAndReturn(
			func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
				block, ok := blocksByObjectKey[metadata.ObjectKeyMain]
				require.True(ok)
				return block, nil
			},
		),
	)

	_, err := server.GetRosettaBlocksByRange(context.Background(), &api.GetRosettaBlocksByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	require.Error(err)
	s.verifyStatusCode(codes.Unimplemented, err)
}

func (s *handlerTestSuite) newAwsPresignFailedRequest() *request.Request {
	return s.awsClient.NewRequest(&request.Operation{
		BeforePresignFn: func(r *request.Request) error { return fmt.Errorf("fail") },
	}, &struct{}{}, &struct{}{})
}

func (s *handlerTestSuite) newAwsPresignRequest(name, method, path string) *request.Request {
	return s.awsClient.NewRequest(&request.Operation{
		Name:       name,
		HTTPMethod: method,
		HTTPPath:   path,
	}, &struct{}{}, &struct{}{})
}

func (s *handlerTestSuite) TestStreamChainEvents_WithSequence() {
	require := testutil.Require(s.T())
	const (
		startEventId int64 = 100
		endEventId   int64 = 300
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		Sequence: strconv.FormatInt(startEventId-1, 10),
		EventTag: s.eventTagForTestEvents,
	}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(strconv.FormatInt(eventDDBEntry.EventId, 10), event.Sequence)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(s.eventTagForTestEvents, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEvents_WithSequenceNum() {
	require := testutil.Require(s.T())
	const (
		startEventId int64 = 100
		endEventId   int64 = 300
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		SequenceNum: startEventId - 1,
		EventTag:    s.eventTagForTestEvents,
	}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(s.eventTagForTestEvents, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEventsNonDefaultEventTag() {
	require := testutil.Require(s.T())
	s.config.Chain.EventTag.Latest = 2
	const (
		startEventId int64  = 100
		endEventId   int64  = 300
		eventTag     uint32 = 2
	)
	eventDDBEntries := s.setupMetaStorageForEvents(eventTag, startEventId, endEventId)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		SequenceNum: startEventId - 1,
		EventTag:    eventTag,
	}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(eventTag, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEventsInvalidEventTag() {
	// latest < eventTag
	s.config.Chain.EventTag.Latest = 1
	eventTag := uint32(2)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
		EventTag:                eventTag,
	}, mockServer)
	s.verifyStatusCode(codes.InvalidArgument, err)
}

func (s *handlerTestSuite) TestStreamChainEventsMultipleCalls() {
	require := testutil.Require(s.T())
	const (
		startEventId int64 = 100
		endEventId   int64 = 200
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		SequenceNum: startEventId - 1,
	}, mockServer)
	require.NoError(err)
	require.True(len(mockServer.events) > 0)

	ctx, cancel = context.WithCancel(context.Background())
	mockServer.ctx = ctx
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err = s.server.StreamChainEvents(&api.ChainEventsRequest{
		SequenceNum: mockServer.events[len(mockServer.events)-1].SequenceNum,
	}, mockServer)
	require.NoError(err)

	// make sure we still get all the events in the end
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(s.eventTagForTestEvents, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEventsEarliest() {
	require := testutil.Require(s.T())
	const (
		startEventId = metastorage.EventIdStartValue
		endEventId   = metastorage.EventIdStartValue + 100
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)
	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionEarliest,
	}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(s.eventTagForTestEvents, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEventsDefault() {
	require := testutil.Require(s.T())
	const (
		startEventId = metastorage.EventIdStartValue
		endEventId   = metastorage.EventIdStartValue + 100
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(s.eventTagForTestEvents, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEventsLatest() {
	require := testutil.Require(s.T())
	const (
		startEventId int64 = 100
		endEventId   int64 = 200
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTagForTestEvents).Times(1).Return(startEventId, nil)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
		EventTag:                s.eventTagForTestEvents,
	}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(s.eventTagForTestEvents, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEventsLatestNoEventAfter() {
	require := testutil.Require(s.T())
	const (
		startEventId int64 = 100
		endEventId   int64 = 200
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTagForTestEvents).Times(1).Return(endEventId, nil)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
	}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, 1)
	event := mockServer.events[0]
	eventDDBEntry := eventDDBEntries[len(eventDDBEntries)-1]
	require.NotNil(event)
	require.Equal(eventDDBEntry.EventId, event.SequenceNum)
	require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
	require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
	require.Equal(s.tagForTestEvents, event.Block.Tag)
	require.Equal(s.eventTagForTestEvents, event.EventTag)
}

func (s *handlerTestSuite) TestStreamChainEventsNoEventForTooLong() {
	require := testutil.Require(s.T())
	const (
		startEventId int64 = 100
		endEventId   int64 = 200
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTagForTestEvents).Times(1).Return(endEventId, nil)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	s.server.maxNoEventTime = time.Second
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.Aborted, err)
	require.Len(mockServer.events, 1)
	event := mockServer.events[0]
	eventDDBEntry := eventDDBEntries[len(eventDDBEntries)-1]
	require.NotNil(event)
	require.Equal(eventDDBEntry.EventId, event.SequenceNum)
	require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
	require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
	require.Equal(s.tagForTestEvents, event.Block.Tag)
	require.Equal(s.eventTagForTestEvents, event.EventTag)
}

func (s *handlerTestSuite) TestStreamChainEventsSpecificHeight() {
	require := testutil.Require(s.T())
	const (
		startEventId int64  = 100
		endEventId   int64  = 200
		startHeight  uint64 = 50
	)
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)
	s.metaStorage.EXPECT().GetFirstEventIdByBlockHeight(gomock.Any(), s.eventTagForTestEvents, startHeight).Times(1).Return(startEventId, nil)

	ctx, cancel := context.WithCancel(context.Background())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    ctx,
	}
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: strconv.FormatUint(startHeight, 10),
		EventTag:                uint32(0),
	}, mockServer)
	require.NoError(err)
	require.Len(mockServer.events, int(endEventId+1-startEventId))
	for i, event := range mockServer.events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(s.eventTagForTestEvents, event.EventTag)
	}
}

func (s *handlerTestSuite) TestStreamChainEventsInvalidInitialStreamPosition() {
	require := testutil.Require(s.T())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    context.Background(),
	}
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: "blah",
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.InvalidArgument, err)
	require.Len(mockServer.events, 0)
}

func (s *handlerTestSuite) TestStreamChainEventsNoEventHistory() {
	require := testutil.Require(s.T())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    context.Background(),
	}
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTagForTestEvents).Times(1).Return(int64(0), storage.ErrNoEventHistory)
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.InvalidArgument, err)
	require.Len(mockServer.events, 0)
}

func (s *handlerTestSuite) TestStreamChainEventsNoEventHistoryNonDefaultEventTag() {
	require := testutil.Require(s.T())
	eventTag := uint32(1)
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    context.Background(),
	}
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), eventTag).Times(1).Return(int64(0), storage.ErrNoEventHistory)
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
		EventTag:                eventTag,
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.InvalidArgument, err)
	require.Len(mockServer.events, 0)
}

func (s *handlerTestSuite) TestStreamChainEventsErrorOnGetMaxEventId() {
	require := testutil.Require(s.T())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    context.Background(),
	}
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTagForTestEvents).Times(1).Return(int64(0), xerrors.New("blah"))
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.Internal, err)
	require.Len(mockServer.events, 0)
}

func (s *handlerTestSuite) TestStreamChainEventsNoBlockFound() {
	require := testutil.Require(s.T())
	startHeight := uint64(50)
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    context.Background(),
	}
	s.metaStorage.EXPECT().GetFirstEventIdByBlockHeight(gomock.Any(), s.eventTagForTestEvents, startHeight).Times(1).Return(int64(0), storage.ErrItemNotFound)
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: strconv.FormatUint(startHeight, 10),
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.NotFound, err)
	require.Len(mockServer.events, 0)
}

func (s *handlerTestSuite) TestStreamChainEventsErrorOnGetFirstEventIdByBlockHeight() {
	require := testutil.Require(s.T())
	startHeight := uint64(50)
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    context.Background(),
	}
	s.metaStorage.EXPECT().GetFirstEventIdByBlockHeight(gomock.Any(), s.eventTagForTestEvents, startHeight).Return(int64(0), xerrors.New("blah"))
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		InitialPositionInStream: strconv.FormatUint(startHeight, 10),
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.Internal, err)
	require.Len(mockServer.events, 0)
}

func (s *handlerTestSuite) TestStreamChainEventsInvalidSequence() {
	require := testutil.Require(s.T())
	mockServer := &mockStreamChainEventsServer{
		events: make([]*api.BlockchainEvent, 0),
		ctx:    context.Background(),
	}
	err := s.server.StreamChainEvents(&api.ChainEventsRequest{
		Sequence: "blah",
	}, mockServer)
	require.Error(err)
	s.verifyStatusCode(codes.InvalidArgument, err)
	require.Len(mockServer.events, 0)
}

func (s *handlerTestSuite) TestStreamBackoff() {
	require := testutil.Require(s.T())

	// Assert the backoff is big enough for at least 20 attempts.
	backoff := s.server.newStreamingBackoff()
	for i := 0; i < 20; i++ {
		duration := backoff.NextBackOff()
		require.NotEqual(streamingBackoffStop, duration)
		require.Greater(duration, time.Duration(0))
	}
}

func (s *handlerTestSuite) TestStreamBackoff_Expired() {
	require := testutil.Require(s.T())

	s.server.maxNoEventTime = 50 * time.Millisecond
	backoff := s.server.newStreamingBackoff()
	time.Sleep(100 * time.Millisecond)
	duration := backoff.NextBackOff()
	require.Equal(streamingBackoffStop, duration)
}

func (s *handlerTestSuite) setupMetaStorageForEvents(eventTag uint32, startEventId int64, endEventId int64) []*model.EventDDBEntry {
	eventDDBEntries := testutil.MakeBlockEventDDBEntries(
		api.BlockchainEvent_BLOCK_ADDED,
		eventTag, endEventId, uint64(startEventId), uint64(endEventId+1), s.tagForTestEvents,
	)

	require := testutil.Require(s.T())
	s.metaStorage.EXPECT().GetEventsAfterEventId(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventDDBEntry, error) {
			require.Equal(s.config.Api.StreamingBatchSize, maxEvents)
			start := int(eventId + 1 - startEventId)
			if start >= len(eventDDBEntries) {
				return []*model.EventDDBEntry{}, nil
			}
			end := int(eventId + 1 - startEventId + int64(maxEvents))
			if end > len(eventDDBEntries) {
				end = len(eventDDBEntries)
			}
			return eventDDBEntries[start:end], nil
		},
	)
	return eventDDBEntries
}

func (s *handlerTestSuite) verifyStatusCode(code codes.Code, err error) {
	require := testutil.Require(s.T())
	// Simulate calling mapToGrpcError from the error interceptor.
	fullMethod := fmt.Sprintf("/%v/handlerTestSuite", consts.FullServiceName)
	err = s.server.mapToGrpcError(err, fullMethod, nil)
	require.Equal(code.String(), status.Code(err).String(), err.Error())
}

type mockStreamChainEventsServer struct {
	api.ChainStorage_StreamChainEventsServer
	events []*api.BlockchainEvent
	ctx    context.Context
}

func (m *mockStreamChainEventsServer) Send(res *api.ChainEventsResponse) error {
	m.events = append(m.events, res.Event)
	return nil
}

func (m *mockStreamChainEventsServer) Context() context.Context {
	return m.ctx
}

func (s *handlerTestSuite) TestGetChainEvents_WithSequence() {
	require := testutil.Require(s.T())
	lastSeenEventId := int64(99)
	startEventId := lastSeenEventId + 1
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 + 50
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		Sequence:     strconv.FormatInt(lastSeenEventId, 10),
		MaxNumEvents: uint64(maxNumEvents),
	})
	require.NoError(err)
	require.Len(resp.Events, int(maxNumEvents))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(strconv.FormatInt(eventDDBEntry.EventId, 10), event.Sequence)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
	}
}

func (s *handlerTestSuite) TestGetChainEvents_WithSequenceAndInitialPosition() {
	require := testutil.Require(s.T())
	lastSeenEventId := int64(99)
	startEventId := lastSeenEventId + 1
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 + 50
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	// Sequence should take precedence over InitialPositionInStream.
	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		Sequence:                strconv.FormatInt(lastSeenEventId, 10),
		InitialPositionInStream: InitialPositionEarliest,
		MaxNumEvents:            uint64(maxNumEvents),
	})
	require.NoError(err)
	require.Len(resp.Events, int(maxNumEvents))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(strconv.FormatInt(eventDDBEntry.EventId, 10), event.Sequence)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
	}
}

func (s *handlerTestSuite) TestGetChainEvents_WithSequenceNum() {
	require := testutil.Require(s.T())
	lastSeenEventId := int64(99)
	startEventId := lastSeenEventId + 1
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 + 50
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		SequenceNum:  lastSeenEventId,
		MaxNumEvents: uint64(maxNumEvents),
	})
	require.NoError(err)
	require.Len(resp.Events, int(maxNumEvents))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
	}
}

func (s *handlerTestSuite) TestGetChainEvents_WithSequenceNumAndInitialPosition() {
	require := testutil.Require(s.T())
	lastSeenEventId := int64(99)
	startEventId := metastorage.EventIdStartValue
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 + 50
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	// InitialPositionInStream should take precedence over Sequence.
	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		SequenceNum:             lastSeenEventId,
		InitialPositionInStream: InitialPositionEarliest,
		MaxNumEvents:            uint64(maxNumEvents),
	})
	require.NoError(err)
	require.Len(resp.Events, int(maxNumEvents))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
	}
}

func (s *handlerTestSuite) TestGetChainEventsNonDefaultEventTag() {
	require := testutil.Require(s.T())
	s.config.Chain.EventTag.Latest = 2
	eventTag := uint32(2)
	lastSeenEventId := int64(99)
	startEventId := lastSeenEventId + 1
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 + 50
	eventDDBEntries := s.setupMetaStorageForEvents(eventTag, startEventId, endEventId)

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		SequenceNum:  lastSeenEventId,
		MaxNumEvents: uint64(maxNumEvents),
		EventTag:     eventTag,
	})
	require.NoError(err)
	require.Len(resp.Events, int(maxNumEvents))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
		require.Equal(eventTag, event.EventTag)
	}
}

func (s *handlerTestSuite) TestGetChainEventsInvalidEventTag() {
	require := testutil.Require(s.T())
	// latest < eventTag
	s.config.Chain.EventTag.Latest = 1
	eventTag := uint32(2)
	lastSeenEventId := int64(99)
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		SequenceNum:  lastSeenEventId,
		MaxNumEvents: uint64(maxNumEvents),
		EventTag:     eventTag,
	})
	require.Nil(resp)
	s.verifyStatusCode(codes.InvalidArgument, err)
}

func (s *handlerTestSuite) TestGetChainEventsWithPositionEarliest() {
	require := testutil.Require(s.T())
	startEventId := metastorage.EventIdStartValue
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 + 50
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		InitialPositionInStream: InitialPositionEarliest,
		MaxNumEvents:            uint64(maxNumEvents),
	})
	require.NoError(err)
	require.Len(resp.Events, int(maxNumEvents))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
	}
}

func (s *handlerTestSuite) TestGetChainEventsWithPositionLatest() {
	require := testutil.Require(s.T())
	startEventId := int64(100)
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 + 50

	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTagForTestEvents).Return(startEventId, nil)

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
		MaxNumEvents:            uint64(maxNumEvents),
	})
	require.NoError(err)
	require.Len(resp.Events, int(maxNumEvents))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
	}
}

func (s *handlerTestSuite) TestGetChainEventsWithPositionLatestError() {
	require := testutil.Require(s.T())
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	SampleErr := xerrors.New("test error")
	s.metaStorage.EXPECT().GetMaxEventId(gomock.Any(), s.eventTagForTestEvents).Times(1).Return(int64(0), SampleErr)

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		InitialPositionInStream: InitialPositionLatest,
		MaxNumEvents:            uint64(maxNumEvents),
	})
	require.Error(err)
	require.Nil(resp)
	require.True(xerrors.Is(err, SampleErr))
}

func (s *handlerTestSuite) TestGetChainEventsNotEnoughEvents() {
	require := testutil.Require(s.T())
	lastSeenEventId := int64(100)
	startEventId := lastSeenEventId + 1
	maxNumEvents := int64(s.config.Api.StreamingBatchSize) // this is required to use setupMetaStorageForEvents
	endEventId := startEventId + maxNumEvents + 1 - 5
	eventDDBEntries := s.setupMetaStorageForEvents(s.eventTagForTestEvents, startEventId, endEventId)

	resp, err := s.server.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		SequenceNum:  lastSeenEventId,
		MaxNumEvents: uint64(maxNumEvents),
	})
	require.NoError(err)
	require.Len(resp.Events, int(endEventId-startEventId+1))
	for i, event := range resp.Events {
		eventDDBEntry := eventDDBEntries[i]
		require.NotNil(event)
		require.Equal(eventDDBEntry.EventId, event.SequenceNum)
		require.Equal(eventDDBEntry.BlockHash, event.Block.Hash)
		require.Equal(eventDDBEntry.BlockHeight, event.Block.Height)
		require.Equal(s.tagForTestEvents, event.Block.Tag)
	}
}

func (s *handlerTestSuite) TestGetChainMetadata() {
	require := testutil.Require(s.T())

	s.config.Chain = config.ChainConfig{
		BlockTag: config.BlockTagConfig{
			Latest: 3,
			Stable: 2,
		},
		EventTag: config.EventTagConfig{
			Latest: 4,
			Stable: 3,
		},
		BlockStartHeight:     3_000_000,
		IrreversibleDistance: 100,
		BlockTime:            13 * time.Second,
	}

	resp, err := s.server.GetChainMetadata(context.Background(), &api.GetChainMetadataRequest{})
	require.NoError(err)
	require.Equal(uint32(3), resp.LatestBlockTag)
	require.Equal(uint32(2), resp.StableBlockTag)
	require.Equal(uint32(4), resp.LatestEventTag)
	require.Equal(uint32(3), resp.StableEventTag)
	require.Equal(uint64(3_000_000), resp.BlockStartHeight)
	require.Equal(uint64(100), resp.IrreversibleDistance)
	require.Equal("13s", resp.BlockTime)
}

func (s *handlerTestSuite) TestGetVersionedChainEvent_WithFromSequence() {
	const (
		blockHeight  = uint64(50)
		fromEventTag = uint32(0)
		fromEventId  = int64(50)
		toEventTag   = uint32(1)
		toEventId    = int64(100)
	)
	require := testutil.Require(s.T())

	fromEvent := &model.EventDDBEntry{
		EventId:     fromEventId,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight: blockHeight,
		BlockHash:   "hash",
		ParentHash:  "parentHash",
		EventTag:    fromEventTag,
	}
	s.metaStorage.EXPECT().GetEventByEventId(gomock.Any(), fromEventTag, fromEventId).Times(1).DoAndReturn(
		func(ctx context.Context, eventTag uint32, eventId int64) (*model.EventDDBEntry, error) {
			return fromEvent, nil
		},
	)
	s.metaStorage.EXPECT().GetEventsByBlockHeight(gomock.Any(), toEventTag, fromEvent.BlockHeight).Times(1).DoAndReturn(
		func(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventDDBEntry, error) {
			return []*model.EventDDBEntry{
				{
					EventId:     toEventId,
					EventType:   api.BlockchainEvent_BLOCK_ADDED,
					BlockHeight: blockHeight,
					BlockHash:   "hash",
					ParentHash:  "parentHash",
					EventTag:    toEventTag,
				},
				{
					EventId:     toEventId - 1,
					EventType:   api.BlockchainEvent_BLOCK_ADDED,
					BlockHeight: blockHeight,
					BlockHash:   "fake-hash",
					ParentHash:  "fake-parentHash",
					EventTag:    toEventTag,
				},
				{
					EventId:     toEventId - 2,
					EventType:   api.BlockchainEvent_BLOCK_ADDED,
					BlockHeight: blockHeight,
					BlockHash:   "hash",
					ParentHash:  "parentHash",
					EventTag:    toEventTag,
				},
			}, nil
		},
	)

	resp, err := s.server.GetVersionedChainEvent(context.Background(), &api.GetVersionedChainEventRequest{
		FromEventTag: fromEventTag,
		FromSequence: strconv.FormatInt(fromEventId, 10),
		ToEventTag:   toEventTag,
	})
	require.NoError(err)
	require.Equal(toEventId, resp.Event.SequenceNum)
	require.Equal(toEventTag, resp.Event.EventTag)
	require.Equal(fromEvent.BlockHeight, resp.Event.Block.Height)
	require.Equal(fromEvent.BlockHash, resp.Event.Block.Hash)
	require.Equal(fromEvent.EventType, resp.Event.Type)
	require.Equal(fromEvent.Tag, resp.Event.Block.Tag)
}

func (s *handlerTestSuite) TestGetVersionedChainEvent_WithFromSequenceNum() {
	const (
		blockHeight  = uint64(50)
		fromEventTag = uint32(0)
		fromEventId  = int64(50)
		toEventTag   = uint32(1)
		toEventId    = int64(100)
	)
	require := testutil.Require(s.T())

	fromEvent := &model.EventDDBEntry{
		EventId:     fromEventId,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight: blockHeight,
		BlockHash:   "hash",
		ParentHash:  "parentHash",
		EventTag:    fromEventTag,
	}
	s.metaStorage.EXPECT().GetEventByEventId(gomock.Any(), fromEventTag, fromEventId).Times(1).DoAndReturn(
		func(ctx context.Context, eventTag uint32, eventId int64) (*model.EventDDBEntry, error) {
			return fromEvent, nil
		},
	)
	s.metaStorage.EXPECT().GetEventsByBlockHeight(gomock.Any(), toEventTag, fromEvent.BlockHeight).Times(1).DoAndReturn(
		func(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventDDBEntry, error) {
			return []*model.EventDDBEntry{
				{
					EventId:     toEventId,
					EventType:   api.BlockchainEvent_BLOCK_ADDED,
					BlockHeight: blockHeight,
					BlockHash:   "hash",
					ParentHash:  "parentHash",
					EventTag:    toEventTag,
				},
				{
					EventId:     toEventId - 1,
					EventType:   api.BlockchainEvent_BLOCK_ADDED,
					BlockHeight: blockHeight,
					BlockHash:   "fake-hash",
					ParentHash:  "fake-parentHash",
					EventTag:    toEventTag,
				},
				{
					EventId:     toEventId - 2,
					EventType:   api.BlockchainEvent_BLOCK_ADDED,
					BlockHeight: blockHeight,
					BlockHash:   "hash",
					ParentHash:  "parentHash",
					EventTag:    toEventTag,
				},
			}, nil
		},
	)

	resp, err := s.server.GetVersionedChainEvent(context.Background(), &api.GetVersionedChainEventRequest{
		FromEventTag:    fromEventTag,
		FromSequenceNum: fromEventId,
		ToEventTag:      toEventTag,
	})
	require.NoError(err)
	require.Equal(toEventId, resp.Event.SequenceNum)
	require.Equal(toEventTag, resp.Event.EventTag)
	require.Equal(fromEvent.BlockHeight, resp.Event.Block.Height)
	require.Equal(fromEvent.BlockHash, resp.Event.Block.Hash)
	require.Equal(fromEvent.EventType, resp.Event.Type)
	require.Equal(fromEvent.Tag, resp.Event.Block.Tag)
}

func (s *handlerTestSuite) TestGetVersionedChainEvent_NoMatchingEvent() {
	const (
		blockHeight  = uint64(50)
		fromEventTag = uint32(0)
		fromEventId  = int64(50)
		toEventTag   = uint32(1)
		toEventId    = int64(100)
	)
	require := testutil.Require(s.T())

	fromEvent := &model.EventDDBEntry{
		EventId:     fromEventId,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight: blockHeight,
		BlockHash:   "hash",
		ParentHash:  "parentHash",
		EventTag:    fromEventTag,
	}
	s.metaStorage.EXPECT().GetEventByEventId(gomock.Any(), fromEventTag, fromEventId).Times(1).DoAndReturn(
		func(ctx context.Context, eventTag uint32, eventId int64) (*model.EventDDBEntry, error) {
			return fromEvent, nil
		},
	)
	s.metaStorage.EXPECT().GetEventsByBlockHeight(gomock.Any(), toEventTag, fromEvent.BlockHeight).Times(1).DoAndReturn(
		func(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventDDBEntry, error) {
			return []*model.EventDDBEntry{
				{
					EventId:     toEventId,
					EventType:   api.BlockchainEvent_BLOCK_REMOVED,
					BlockHeight: blockHeight,
					BlockHash:   "hash",
					ParentHash:  "parentHash",
					EventTag:    toEventTag,
				},
			}, nil
		},
	)

	_, err := s.server.GetVersionedChainEvent(context.Background(), &api.GetVersionedChainEventRequest{
		FromEventTag:    fromEventTag,
		FromSequenceNum: fromEventId,
		ToEventTag:      toEventTag,
	})
	require.Error(err)
}
