package s3

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/awstesting"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
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
	downloader.EXPECT().DownloadWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput, opts ...jsonrpc.Option) (int64, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)

			return expectedObjectSize, nil
		})

	uploader := s3mocks.NewMockUploader(ctrl)
	uploader.EXPECT().UploadWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *s3manager.UploadInput, opts ...jsonrpc.Option) (*s3manager.UploadOutput, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)
			require.NotNil(input.ContentMD5)
			require.NotEmpty(*input.ContentMD5)

			return &s3manager.UploadOutput{}, nil
		})
	client := s3mocks.NewMockClient(ctrl)

	var storage internal.BlobStorage
	app := testapp.New(
		t,
		fx.Provide(New),
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
	downloader.EXPECT().DownloadWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput, opts ...jsonrpc.Option) (int64, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)

			return expectedObjectSize, nil
		})

	uploader := s3mocks.NewMockUploader(ctrl)
	uploader.EXPECT().UploadWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *s3manager.UploadInput, opts ...jsonrpc.Option) (*s3manager.UploadOutput, error) {
			require.NotNil(input.Bucket)
			require.NotEmpty(*input.Bucket)
			require.NotNil(input.Key)
			require.Equal(expectedObjectKey, *input.Key)
			require.NotNil(input.ContentMD5)
			require.NotEmpty(*input.ContentMD5)
			require.Equal(*input.ACL, bucketOwnerFullControl)

			return &s3manager.UploadOutput{}, nil
		})
	client := s3mocks.NewMockClient(ctrl)

	var storage internal.BlobStorage
	app := testapp.New(
		t,
		testapp.WithBlockchainNetworkSidechain(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET, api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON),
		fx.Provide(New),
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

	var storage internal.BlobStorage
	app := testapp.New(
		t,
		fx.Provide(New),
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
	downloader := s3manager.NewDownloader(unit.Session)
	client := s3mocks.NewMockClient(ctrl)

	var blobStorage internal.BlobStorage
	app := testapp.New(
		t,
		fx.Provide(New),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&blobStorage),
	)
	defer app.Close()
	require.NotNil(blobStorage)

	ctx := &awstesting.FakeContext{DoneCh: make(chan struct{})}
	ctx.Error = fmt.Errorf("context canceled")
	close(ctx.DoneCh)

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "0xabcde",
		ObjectKeyMain: "some download key",
	}
	_, err := blobStorage.Download(ctx, metadata)
	require.Error(err)
	require.Equal(errors.ErrRequestCanceled, err)
}
