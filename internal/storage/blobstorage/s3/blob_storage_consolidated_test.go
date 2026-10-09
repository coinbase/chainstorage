package s3

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBlobStorage_UploadConsolidated_NewObject(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)

	var uploadedKey string
	var uploadedLength int64
	var uploadedSHA string
	gomock.InOrder(
		client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				require.NotNil(input.Bucket)
				require.NotEmpty(*input.Bucket)
				require.NotNil(input.Key)
				require.True(strings.HasPrefix(*input.Key, "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=7/shard=00000000000000000000-00000000000000010000/00000000000000000100-00000000000000000102-"))
				return nil, &awss3types.NotFound{}
			}),
		client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, input *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
				require.NotNil(input.Bucket)
				require.NotEmpty(*input.Bucket)
				require.NotNil(input.Key)
				require.NotNil(input.Body)
				require.NotNil(input.ContentLength)
				require.NotNil(input.ContentMD5)
				seeker, ok := input.Body.(io.Seeker)
				require.True(ok)
				_, err := seeker.Seek(0, io.SeekStart)
				require.NoError(err)
				require.Empty(input.ChecksumAlgorithm)
				require.Nil(input.ChecksumCRC32)
				require.Nil(input.ChecksumCRC32C)
				require.Nil(input.ChecksumCRC64NVME)
				require.Nil(input.ChecksumSHA1)
				require.Nil(input.ChecksumSHA256)
				require.NotNil(input.IfNoneMatch)
				require.Equal("*", *input.IfNoneMatch)
				require.Equal(awss3types.ObjectCannedACLBucketOwnerFullControl, input.ACL)
				require.Equal("cscb", input.Metadata[cscbMetadataFormat])
				require.Equal("batch-chunked", input.Metadata[cscbMetadataCompressionScope])
				require.Equal("zstd", input.Metadata[cscbMetadataCodec])
				require.Equal("11", input.Metadata[cscbMetadataUncompressedLength])

				body, err := io.ReadAll(input.Body)
				require.NoError(err)
				require.Equal(int64(len(body)), *input.ContentLength)
				var options awss3.Options
				for _, opt := range opts {
					opt(&options)
				}
				require.Equal(aws.RequestChecksumCalculationWhenRequired, options.RequestChecksumCalculation)
				sum := sha256.Sum256(body)
				md5Sum := md5.Sum(body)
				uploadedKey = *input.Key
				uploadedLength = int64(len(body))
				uploadedSHA = fmt.Sprintf("%x", sum[:])
				require.Equal(uploadedSHA, input.Metadata[cscbMetadataSHA256])
				require.Contains(uploadedKey, uploadedSHA)
				require.Equal(base64.StdEncoding.EncodeToString(md5Sum[:]), *input.ContentMD5)
				return &awss3.PutObjectOutput{}, nil
			}),
		client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				require.Equal(uploadedKey, *input.Key)
				return &awss3.HeadObjectOutput{
					ContentLength: aws.Int64(uploadedLength),
					Metadata: map[string]string{
						cscbMetadataSHA256: uploadedSHA,
					},
				}, nil
			}),
	)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	var stages []string
	ctx := internal.WithConsolidatedUploadProgress(context.Background(), func(stage string, details ...any) {
		stages = append(stages, stage)
	})
	objectKey, placements, err := storage.UploadConsolidated(ctx, testS3Payloads())
	require.NoError(err)
	require.Equal(uploadedKey, objectKey)
	require.Len(placements, 2)
	require.Equal(uploadedKey, placements[0].ObjectKey)
	require.Equal(uint64(0), placements[0].ByteOffset)
	require.Equal(uint64(5), placements[0].ByteLength)
	require.Equal(uint64(5), placements[1].ByteOffset)
	require.Equal(uint64(6), placements[1].ByteLength)
	require.Equal("", placements[0].StorageGeneration)
	require.Equal("", placements[1].StorageGeneration)
	require.Contains(stages, "s3_encode_config_loaded")
	require.Contains(stages, "cscb_encode_started")
	require.Contains(stages, "cscb_block_encoded")
	require.Contains(stages, "cscb_chunk_flushed")
	require.Contains(stages, "cscb_materialize_started")
	require.Contains(stages, "s3_md5_started")
	require.Contains(stages, "s3_md5_finished")
	require.Contains(stages, "s3_put_started")
	require.Contains(stages, "s3_put_finished")
	require.Contains(stages, "s3_head_uploaded_started")
	require.Contains(stages, "s3_head_uploaded_finished")
}

func TestBlobStorage_UploadConsolidated_WriteGenerationV2RoutesAndStampsPlacements(t *testing.T) {
	require := testutil.Require(t)
	expected, err := cscb.Encode(context.Background(), testS3EncodeConfig(), testS3Payloads())
	require.NoError(err)
	defer expected.Close()

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
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
	client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			require.Equal("v2-blocks", aws.ToString(input.Bucket))
			return &awss3.HeadObjectOutput{
				ContentLength: aws.Int64(int64(expected.Length)),
				Metadata: map[string]string{
					cscbMetadataSHA256: expected.SHA256,
				},
			}, nil
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

	objectKey, placements, err := storage.UploadConsolidated(context.Background(), testS3Payloads())
	require.NoError(err)
	require.Equal(expected.Key, objectKey)
	require.Len(placements, 2)
	for _, placement := range placements {
		require.Equal("v2", placement.StorageGeneration)
	}
}

func TestBlobStorage_UploadConsolidated_AcceptsExistingExactObject(t *testing.T) {
	require := testutil.Require(t)

	expected, err := cscb.Encode(context.Background(), testS3EncodeConfig(), testS3Payloads())
	require.NoError(err)
	defer expected.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).Times(0)
	client.EXPECT().PutObject(gomock.Any(), gomock.Any()).Times(0)
	client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			require.Equal(expected.Key, *input.Key)
			return &awss3.HeadObjectOutput{
				ContentLength: aws.Int64(int64(expected.Length)),
				Metadata: map[string]string{
					cscbMetadataSHA256: expected.SHA256,
				},
			}, nil
		})

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	objectKey, placements, err := storage.UploadConsolidated(context.Background(), testS3Payloads())
	require.NoError(err)
	require.Equal(expected.Key, objectKey)
	require.Len(placements, 2)
	require.Equal(expected.Key, placements[0].ObjectKey)
}

func TestBlobStorage_UploadConsolidated_RejectsExistingDifferentObject(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	uploader.EXPECT().Upload(gomock.Any(), gomock.Any()).Times(0)
	client.EXPECT().PutObject(gomock.Any(), gomock.Any()).Times(0)
	client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			return &awss3.HeadObjectOutput{
				ContentLength: aws.Int64(1),
				Metadata: map[string]string{
					cscbMetadataSHA256: "different",
				},
			}, nil
		})

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	_, _, err := storage.UploadConsolidated(context.Background(), testS3Payloads())
	require.Error(err)
	require.Contains(err.Error(), "length mismatch")
}

func TestBlobStorage_UploadConsolidated_AcceptsConcurrentExactObject(t *testing.T) {
	require := testutil.Require(t)

	expected, err := cscb.Encode(context.Background(), testS3EncodeConfig(), testS3Payloads())
	require.NoError(err)
	defer expected.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	gomock.InOrder(
		client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
			Return(nil, &awss3types.NotFound{}),
		client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "object already exists"}),
		client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
				require.Equal(expected.Key, *input.Key)
				return &awss3.HeadObjectOutput{
					ContentLength: aws.Int64(int64(expected.Length)),
					Metadata: map[string]string{
						cscbMetadataSHA256: expected.SHA256,
					},
				}, nil
			}),
	)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	objectKey, placements, err := storage.UploadConsolidated(context.Background(), testS3Payloads())
	require.NoError(err)
	require.Equal(expected.Key, objectKey)
	require.Len(placements, 2)
}

func TestBlobStorage_DownloadConsolidatedBlock_RangeReadsChunk(t *testing.T) {
	require := testutil.Require(t)
	object, metadatas, expectedBlocks, objectData, index := testS3CSCBProtoObject(t)
	defer object.Close()

	targetMetadata := metadatas[1]
	_, targetChunk, err := index.LookupBlock(targetMetadata)
	require.NoError(err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	expectedRanges := map[string]int{
		"bytes=0-65535":          1,
		rangeHeader(targetChunk): 1,
	}
	expectRangeReads(t, client, objectData, expectedRanges)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	actual, err := storage.Download(context.Background(), targetMetadata)
	require.NoError(err)
	require.True(proto.Equal(expectedBlocks[1], actual))
	assertRangeReadsConsumed(t, expectedRanges)
}

func TestBlobStorage_DownloadManyConsolidatedBlocks_GroupsByObjectAndChunk(t *testing.T) {
	require := testutil.Require(t)
	object, metadatas, expectedBlocks, objectData, index := testS3CSCBProtoObject(t)
	defer object.Close()

	_, chunk0, err := index.LookupBlock(metadatas[0])
	require.NoError(err)
	_, chunk1, err := index.LookupBlock(metadatas[2])
	require.NoError(err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	expectedRanges := map[string]int{
		"bytes=0-65535":     1,
		rangeHeader(chunk0): 1,
		rangeHeader(chunk1): 1,
	}
	expectRangeReads(t, client, objectData, expectedRanges)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	actual, err := storage.DownloadMany(context.Background(), []*api.BlockMetadata{
		metadatas[2],
		metadatas[0],
		metadatas[1],
	})
	require.NoError(err)
	require.Len(actual, 3)
	require.True(proto.Equal(expectedBlocks[2], actual[0]))
	require.True(proto.Equal(expectedBlocks[0], actual[1]))
	require.True(proto.Equal(expectedBlocks[1], actual[2]))
	assertRangeReadsConsumed(t, expectedRanges)
}

func TestBlobStorage_DownloadManyConsolidatedBlocks_SeparatesSameKeyByGeneration(t *testing.T) {
	require := testutil.Require(t)
	object, metadatas, _, objectData, index := testS3CSCBProtoObject(t)
	defer object.Close()

	legacy := proto.Clone(metadatas[0]).(*api.BlockMetadata)
	legacy.StorageGeneration = ""
	v2 := proto.Clone(metadatas[0]).(*api.BlockMetadata)
	v2.StorageGeneration = "v2"
	_, chunk, err := index.LookupBlock(legacy)
	require.NoError(err)

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(err)
	cfg.AWS.Bucket = "legacy-blocks"
	cfg.AWS.BlockStorage.Generations = map[string]config.BlockStorageGenerationConfig{
		"v2": {Bucket: "v2-blocks"},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	calls := make(chan string, 4)
	client.EXPECT().GetObject(gomock.Any(), gomock.Any()).Times(4).DoAndReturn(
		func(ctx context.Context, input *awss3.GetObjectInput, opts ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			rangeValue := aws.ToString(input.Range)
			calls <- aws.ToString(input.Bucket) + "|" + rangeValue
			var start uint64
			var end uint64
			if _, err := fmt.Sscanf(rangeValue, "bytes=%d-%d", &start, &end); err != nil {
				return nil, err
			}
			if start >= uint64(len(objectData)) {
				return nil, fmt.Errorf("range start %d exceeds object length %d", start, len(objectData))
			}
			if end >= uint64(len(objectData)) {
				end = uint64(len(objectData)) - 1
			}
			return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(objectData[start : end+1]))}, nil
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

	blocks, err := storage.DownloadMany(context.Background(), []*api.BlockMetadata{legacy, v2})
	require.NoError(err)
	require.Len(blocks, 2)
	require.Equal("", blocks[0].GetMetadata().GetStorageGeneration())
	require.Equal("v2", blocks[1].GetMetadata().GetStorageGeneration())
	close(calls)
	actualCalls := make(map[string]int)
	for call := range calls {
		actualCalls[call]++
	}
	chunkRange := rangeHeader(chunk)
	require.Equal(map[string]int{
		"legacy-blocks|bytes=0-65535": 1,
		"legacy-blocks|" + chunkRange: 1,
		"v2-blocks|bytes=0-65535":     1,
		"v2-blocks|" + chunkRange:     1,
	}, actualCalls)
}

func TestBlobStorage_DownloadManyConsolidatedBlocks_AllowsDuplicateInputs(t *testing.T) {
	require := testutil.Require(t)
	object, metadatas, expectedBlocks, objectData, index := testS3CSCBProtoObject(t)
	defer object.Close()

	_, chunk, err := index.LookupBlock(metadatas[1])
	require.NoError(err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	expectedRanges := map[string]int{
		"bytes=0-65535":    1,
		rangeHeader(chunk): 1,
	}
	expectRangeReads(t, client, objectData, expectedRanges)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	actual, err := storage.DownloadMany(context.Background(), []*api.BlockMetadata{
		metadatas[1],
		metadatas[1],
	})
	require.NoError(err)
	require.Len(actual, 2)
	require.True(proto.Equal(expectedBlocks[1], actual[0]))
	require.True(proto.Equal(expectedBlocks[1], actual[1]))
	assertRangeReadsConsumed(t, expectedRanges)
}

func TestReadExpectedRangeBodyAllowsShortInitialRead(t *testing.T) {
	require := testutil.Require(t)

	actual, err := readExpectedRangeBody(bytes.NewReader([]byte("short")), 64*1024, true)
	require.NoError(err)
	require.Equal([]byte("short"), actual)
}

func TestReadExpectedRangeBodyRejectsShortNonInitialRead(t *testing.T) {
	require := testutil.Require(t)

	_, err := readExpectedRangeBody(bytes.NewReader([]byte("short")), 10, false)
	require.Error(err)
	require.Contains(err.Error(), "range body length mismatch")
}

func TestReadExpectedRangeBodyRejectsOversizedRead(t *testing.T) {
	require := testutil.Require(t)

	_, err := readExpectedRangeBody(bytes.NewReader([]byte("too-long")), 3, true)
	require.Error(err)
	require.Contains(err.Error(), "range body length mismatch")
}

func TestBlobStorage_DownloadConsolidatedMetadataWithZeroLengthUsesSingleBlockPath(t *testing.T) {
	require := testutil.Require(t)

	metadata := &api.BlockMetadata{
		Tag:           7,
		Height:        100,
		Hash:          "hash-100",
		ObjectKeyMain: "single-block/object",
		ObjectFormat:  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteLength:    0,
	}
	expectedBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata:   metadata,
	}
	payload, err := proto.Marshal(expectedBlock)
	require.NoError(err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downloader := s3mocks.NewMockDownloader(ctrl)
	downloader.EXPECT().Download(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
			require.Equal(metadata.ObjectKeyMain, aws.ToString(input.Key))
			written, err := writer.WriteAt(payload, 0)
			return int64(written), err
		})
	uploader := s3mocks.NewMockUploader(ctrl)
	client := s3mocks.NewMockClient(ctrl)
	client.EXPECT().GetObject(gomock.Any(), gomock.Any()).Times(0)

	var storage internal.BlobStorageCore
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(newBlobStorage),
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Provide(func() s3.Client { return client }),
		fx.Populate(&storage),
	)
	defer app.Close()

	actual, err := storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.True(proto.Equal(expectedBlock, actual))
}

func testS3EncodeConfig() cscb.EncodeConfig {
	return cscb.EncodeConfig{
		Blockchain:             common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:                common.Network_NETWORK_SOLANA_MAINNET,
		Codec:                  api.Compression_ZSTD,
		CodecLevel:             6,
		MaxBlocks:              1000,
		CompressionChunkBlocks: 10,
		MaxCompressedBytes:     2147483648,
		MaxUncompressedBytes:   137438953472,
		ShardSize:              10000,
	}
}

func testS3Payloads() []internal.ConsolidatedBlockPayload {
	return []internal.ConsolidatedBlockPayload{
		{
			Metadata: &api.BlockMetadata{
				Tag:    7,
				Height: 100,
				Hash:   "hash-100",
			},
			MetadataID:         1001,
			RawBlockPayload:    internal.BytesPayloadSource([]byte("alpha")),
			UncompressedLength: 5,
		},
		{
			Metadata: &api.BlockMetadata{
				Tag:    7,
				Height: 101,
				Hash:   "hash-101",
			},
			MetadataID:         1002,
			RawBlockPayload:    internal.BytesPayloadSource([]byte("bravo!")),
			UncompressedLength: 6,
		},
	}
}

func testS3CSCBProtoObject(t *testing.T) (*cscb.Object, []*api.BlockMetadata, []*api.Block, []byte, *cscb.Index) {
	t.Helper()
	require := testutil.Require(t)

	blocks := []*api.Block{
		{
			Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
			Network:    common.Network_NETWORK_SOLANA_MAINNET,
			Metadata: &api.BlockMetadata{
				Tag:    7,
				Height: 100,
				Hash:   "hash-100",
			},
		},
		{
			Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
			Network:    common.Network_NETWORK_SOLANA_MAINNET,
			Metadata: &api.BlockMetadata{
				Tag:    7,
				Height: 101,
				Hash:   "hash-101",
			},
		},
		{
			Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
			Network:    common.Network_NETWORK_SOLANA_MAINNET,
			Metadata: &api.BlockMetadata{
				Tag:    7,
				Height: 102,
				Hash:   "hash-102",
			},
		},
	}
	payloads := make([]internal.ConsolidatedBlockPayload, len(blocks))
	for i, block := range blocks {
		raw, err := proto.Marshal(block)
		require.NoError(err)
		payloads[i] = internal.ConsolidatedBlockPayload{
			Metadata:           block.Metadata,
			MetadataID:         int64(1000 + i),
			RawBlockPayload:    internal.BytesPayloadSource(raw),
			UncompressedLength: uint64(len(raw)),
		}
	}

	cfg := testS3EncodeConfig()
	cfg.CompressionChunkBlocks = 2
	object, err := cscb.Encode(context.Background(), cfg, payloads)
	require.NoError(err)

	data, ok := object.Bytes()
	require.True(ok)
	index, err := cscb.ParseIndex(data)
	require.NoError(err)

	metadatas := make([]*api.BlockMetadata, len(blocks))
	expectedBlocks := make([]*api.Block, len(blocks))
	for i, placement := range object.Placements {
		metadata := proto.Clone(blocks[i].Metadata).(*api.BlockMetadata)
		metadata.ObjectKeyMain = object.Key
		metadata.ObjectFormat = placement.ObjectFormat
		metadata.ByteOffset = placement.ByteOffset
		metadata.ByteLength = placement.ByteLength
		metadata.UncompressedLength = placement.UncompressedLength
		metadatas[i] = metadata

		expected := proto.Clone(blocks[i]).(*api.Block)
		expected.Metadata = metadata
		expectedBlocks[i] = expected
	}
	return object, metadatas, expectedBlocks, data, index
}

func expectRangeReads(t *testing.T, client *s3mocks.MockClient, objectData []byte, expectedRanges map[string]int) {
	t.Helper()
	require := testutil.Require(t)

	totalReads := 0
	for _, count := range expectedRanges {
		totalReads += count
	}
	client.EXPECT().GetObject(gomock.Any(), gomock.Any()).Times(totalReads).
		DoAndReturn(func(ctx context.Context, input *awss3.GetObjectInput, opts ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			rangeValue := aws.ToString(input.Range)
			require.Positive(expectedRanges[rangeValue], "unexpected range read %s", rangeValue)
			expectedRanges[rangeValue]--

			var start uint64
			var end uint64
			_, err := fmt.Sscanf(rangeValue, "bytes=%d-%d", &start, &end)
			require.NoError(err)
			require.Less(start, uint64(len(objectData)))
			if end >= uint64(len(objectData)) {
				end = uint64(len(objectData)) - 1
			}
			return &awss3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(objectData[start : end+1])),
			}, nil
		})
}

func assertRangeReadsConsumed(t *testing.T, expectedRanges map[string]int) {
	t.Helper()
	require := testutil.Require(t)
	for rangeValue, count := range expectedRanges {
		require.Zero(count, "range %s was not read expected number of times", rangeValue)
	}
}

func rangeHeader(chunk *cscb.ChunkDescriptor) string {
	return fmt.Sprintf("bytes=%d-%d", chunk.CompressedPayloadOffset, chunk.CompressedPayloadOffset+chunk.CompressedLength-1)
}
