package s3

import (
	"context"
	"io"
	"net/url"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBlobStorage_NoCompression(t *testing.T) {
	const expectedObjectKey = "BLOCKCHAIN_ETHEREUM/NETWORK_ETHEREUM_MAINNET/1/12345/0xabcde"
	const expectedObjectSize = int64(12432)

	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	downloader.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)

			return expectedObjectSize, nil
		})

	uploader := s3mocks.NewMockUploader(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *awss3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)
			require.NotNil(input.ContentMD5)
			require.NotEmpty(*input.ContentMD5)

			return &manager.UploadOutput{}, nil
		})
	client := s3mocks.NewMockClient(ctrl)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	require.NotNil(storage)
	objectKey, err := storage.Upload(context.Background(), &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 12345,
			Hash:   "0xabcde",
		},
	}, api.Compression_NONE)
	require.NoError(err)
	require.Equal(expectedObjectKey, objectKey)

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "0xabcde",
		ObjectKeyMain: objectKey,
	}
	block, err := storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.NotNil(block)
}

func TestBlobStorage_NoCompression_WithSidechain(t *testing.T) {
	const expectedObjectKey = "BLOCKCHAIN_ETHEREUM/NETWORK_ETHEREUM_MAINNET/SIDECHAIN_ETHEREUM_MAINNET_BEACON/1/12345/12345"
	const expectedObjectSize = int64(12432)

	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	downloader.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)

			return expectedObjectSize, nil
		})

	uploader := s3mocks.NewMockUploader(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *awss3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)
			require.NotNil(input.ContentMD5)
			require.NotEmpty(*input.ContentMD5)
			require.Equal(awss3types.ObjectCannedACLBucketOwnerFullControl, input.ACL)

			return &manager.UploadOutput{}, nil
		})
	client := s3mocks.NewMockClient(ctrl)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetworkSidechain(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET, api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	require.NotNil(storage)
	objectKey, err := storage.Upload(context.Background(), &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 12345,
			Hash:   "12345",
		},
	}, api.Compression_NONE)
	require.NoError(err)
	require.Equal(expectedObjectKey, objectKey)

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "12345",
		ObjectKeyMain: objectKey,
	}
	block, err := storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.NotNil(block)
}

func TestBlobStorage_NoCompression_SkippedBlock(t *testing.T) {
	require := testutil.Require(t)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return nil }),
		fx.Provide(func() s3.Uploader { return nil }),
		fx.Provide(func() s3.Client { return nil }),
		fx.Populate(&storage),
	)
	defer app.Close()

	metadata := &api.BlockMetadata{
		Tag:     1,
		Height:  12345,
		Skipped: true,
	}
	objectKey, err := storage.Upload(context.Background(), &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		SideChain:  api.SideChain_SIDECHAIN_NONE,
		Metadata:   metadata,
	}, api.Compression_NONE)
	require.NoError(err)
	require.Empty(objectKey)

	block, err := storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.NotNil(block)
	require.Equal(&api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		SideChain:  api.SideChain_SIDECHAIN_NONE,
		Metadata:   metadata,
	}, block)
}

func TestBlobStorage_DownloadErrRequestCanceled(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uploader := s3mocks.NewMockUploader(ctrl)
	downloader := s3mocks.NewMockDownloader(ctrl)
	client := s3mocks.NewMockClient(ctrl)

	// Setup mock to return context canceled error
	downloader.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
			return 0, context.Canceled
		})

	var blobStorage internal.BlobStorageCore
	app := testapp.New(
		t,
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&blobStorage),
	)
	defer app.Close()
	require.NotNil(blobStorage)

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "0xabcde",
		ObjectKeyMain: "some download key",
	}
	_, err := blobStorage.Download(context.Background(), metadata)
	require.Error(err)
	require.Equal(errors.ErrRequestCanceled, err)
}

func TestBlobStorage_RoutesMixedGenerationReads(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.Bucket = "legacy-blocks"
	cfg.AWS.BlockStorage.Generations = map[string]config.BlockStorageGenerationConfig{
		"v2": {Bucket: "v2-blocks"},
	}

	payload, err := proto.Marshal(&api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
	})
	require.NoError(err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	buckets := make([]string, 0, 2)
	var bucketsMu sync.Mutex
	downloader.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).Times(2).
		DoAndReturn(func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
			bucketsMu.Lock()
			buckets = append(buckets, aws.ToString(input.Bucket))
			bucketsMu.Unlock()
			written, err := writer.WriteAt(payload, 0)
			return int64(written), err
		})

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithConfig(cfg),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	metadatas := []*api.BlockMetadata{
		{
			Height:            100,
			ObjectKeyMain:     "shared-key",
			StorageGeneration: "",
		},
		{
			Height:            101,
			ObjectKeyMain:     "shared-key",
			StorageGeneration: "v2",
		},
	}
	blocks, err := storage.DownloadMany(context.Background(), metadatas)
	require.NoError(err)
	require.Len(blocks, 2)
	require.Same(metadatas[0], blocks[0].GetMetadata())
	require.Same(metadatas[1], blocks[1].GetMetadata())
	require.ElementsMatch([]string{"legacy-blocks", "v2-blocks"}, buckets)
}

func TestBlobStorage_RejectsUnresolvableGenerationBeforeS3(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.Bucket = "legacy-blocks"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	downloader.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithConfig(cfg),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	_, err = storage.Download(context.Background(), &api.BlockMetadata{
		Height:            100,
		ObjectKeyMain:     "key",
		StorageGeneration: "v2",
	})
	require.ErrorContains(err, "unconfigured storage generation \"v2\"")

	_, err = storage.Download(context.Background(), &api.BlockMetadata{
		Height:            101,
		ObjectKeyMain:     "key",
		StorageGeneration: "future",
	})
	require.ErrorContains(err, "unsupported block storage generation \"future\"")
}

func TestBlobStorage_WriteGenerationV2RoutesAndStampsNewSingleBlock(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.Bucket = "legacy-blocks"
	cfg.AWS.BlockStorage = config.BlockStorageConfig{
		WriteGeneration: "v2",
		Generations: map[string]config.BlockStorageGenerationConfig{
			"v2": {Bucket: "v2-blocks"},
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			require.Equal("v2-blocks", aws.ToString(input.Bucket))
			return &manager.UploadOutput{}, nil
		},
	)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithConfig(cfg),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	metadata := &api.BlockMetadata{Tag: 1, Height: 12345, Hash: "hash"}
	_, err = storage.Upload(context.Background(), &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   metadata,
	}, api.Compression_NONE)
	require.NoError(err)
	require.Equal("v2", metadata.GetStorageGeneration())
}

func TestBlobStorage_PreSignRoutesByGeneration(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.Bucket = "legacy-blocks"
	cfg.AWS.BlockStorage.Generations = map[string]config.BlockStorageGenerationConfig{
		"v2": {Bucket: "v2-blocks"},
	}
	client := awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	storage := &blobStorageImpl{config: cfg, client: client}

	fileURL, err := storage.PreSign(context.Background(), &api.BlockMetadata{
		ObjectKeyMain:     "key",
		StorageGeneration: "v2",
	})
	require.NoError(err)
	parsed, err := url.Parse(fileURL)
	require.NoError(err)
	require.Contains(parsed.Host, "v2-blocks")
}

// Silence unused import warning
var _ = aws.String
