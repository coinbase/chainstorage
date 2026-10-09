package retirement

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
)

func TestS3ObjectStore_ListObjectVersionsPaginatesAndFiltersExactKey(t *testing.T) {
	require := require.New(t)
	controller := gomock.NewController(t)
	client := s3mocks.NewMockClient(controller)
	store := NewS3ObjectStore(client)
	modified := time.Date(2026, 7, 13, 1, 2, 3, 0, time.UTC)

	first := client.EXPECT().ListObjectVersions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.ListObjectVersionsInput, options ...func(*awss3.Options)) (*awss3.ListObjectVersionsOutput, error) {
			require.Equal("bucket", aws.ToString(input.Bucket))
			require.Equal("single-block/key", aws.ToString(input.Prefix))
			require.Equal(int32(1000), aws.ToInt32(input.MaxKeys))
			require.Empty(aws.ToString(input.KeyMarker))
			require.Empty(aws.ToString(input.VersionIdMarker))
			return &awss3.ListObjectVersionsOutput{
				Versions: []awss3types.ObjectVersion{
					{Key: aws.String("single-block/key"), VersionId: aws.String("v2"), ETag: aws.String("etag-v2"), Size: aws.Int64(22), IsLatest: aws.Bool(true), LastModified: &modified},
					{Key: aws.String("single-block/key-sibling"), VersionId: aws.String("ignored"), ETag: aws.String("ignored"), Size: aws.Int64(999)},
				},
				DeleteMarkers: []awss3types.DeleteMarkerEntry{
					{Key: aws.String("single-block/key-sibling"), VersionId: aws.String("ignored-marker")},
				},
				IsTruncated:         aws.Bool(true),
				NextKeyMarker:       aws.String("single-block/key"),
				NextVersionIdMarker: aws.String("v2"),
			}, nil
		},
	)
	client.EXPECT().ListObjectVersions(gomock.Any(), gomock.Any()).After(first).DoAndReturn(
		func(ctx context.Context, input *awss3.ListObjectVersionsInput, options ...func(*awss3.Options)) (*awss3.ListObjectVersionsOutput, error) {
			require.Equal("single-block/key", aws.ToString(input.KeyMarker))
			require.Equal("v2", aws.ToString(input.VersionIdMarker))
			return &awss3.ListObjectVersionsOutput{
				Versions: []awss3types.ObjectVersion{
					{Key: aws.String("single-block/key"), VersionId: aws.String("v1"), ETag: aws.String("etag-v1"), Size: aws.Int64(11), IsLatest: aws.Bool(false)},
				},
				DeleteMarkers: []awss3types.DeleteMarkerEntry{
					{Key: aws.String("single-block/key"), VersionId: aws.String("marker-v3"), IsLatest: aws.Bool(false)},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	)

	topology, err := store.ListObjectVersions(context.Background(), "bucket", "single-block/key")
	require.NoError(err)
	require.Len(topology.Versions, 2)
	require.Equal("v2", topology.Versions[0].VersionID)
	require.Equal("etag-v2", topology.Versions[0].ETag)
	require.Equal(uint64(22), topology.Versions[0].Bytes)
	require.True(topology.Versions[0].IsLatest)
	require.Equal(modified, *topology.Versions[0].LastModified)
	require.Equal("v1", topology.Versions[1].VersionID)
	require.Len(topology.DeleteMarkers, 1)
	require.Equal("marker-v3", topology.DeleteMarkers[0].VersionID)
}

func TestS3ObjectStore_UsesPinnedVersionForHeadReadRangeAndDelete(t *testing.T) {
	require := require.New(t)
	controller := gomock.NewController(t)
	client := s3mocks.NewMockClient(controller)
	store := NewS3ObjectStore(client)

	client.EXPECT().HeadObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.HeadObjectInput, options ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
			require.Equal("bucket", aws.ToString(input.Bucket))
			require.Equal("consolidated/key", aws.ToString(input.Key))
			require.Equal("cscb-v1", aws.ToString(input.VersionId))
			return &awss3.HeadObjectOutput{
				ContentLength: aws.Int64(123),
				VersionId:     aws.String("cscb-v1"),
				ETag:          aws.String("cscb-etag"),
				Metadata:      map[string]string{"chainstorage-format": "cscb"},
				Expiration:    aws.String(`expiry-date="Mon, 20 Jul 2026 00:00:00 GMT", rule-id="expire-current"`),
			}, nil
		},
	)
	head, err := store.HeadObjectVersion(context.Background(), "bucket", "consolidated/key", "cscb-v1")
	require.NoError(err)
	require.True(head.Exists)
	require.Equal(uint64(123), head.Bytes)
	require.Equal("cscb-v1", head.VersionID)
	require.Equal("cscb-etag", head.ETag)
	require.NotEmpty(head.Expiration)

	client.EXPECT().GetObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.GetObjectInput, options ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			require.Equal("single-block-v1", aws.ToString(input.VersionId))
			require.Empty(aws.ToString(input.Range))
			return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte("single-block")))}, nil
		},
	)
	data, err := store.ReadObjectVersion(context.Background(), "bucket", "single-block/key", "single-block-v1")
	require.NoError(err)
	require.Equal([]byte("single-block"), data)

	client.EXPECT().GetObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.GetObjectInput, options ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
			require.Equal("cscb-v1", aws.ToString(input.VersionId))
			require.Equal("bytes=96-127", aws.ToString(input.Range))
			return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte("range")))}, nil
		},
	)
	data, err = store.ReadObjectVersionRange(context.Background(), "bucket", "consolidated/key", "cscb-v1", 96, 32)
	require.NoError(err)
	require.Equal([]byte("range"), data)

	client.EXPECT().DeleteObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *awss3.DeleteObjectInput, options ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
			require.Equal("bucket", aws.ToString(input.Bucket))
			require.Equal("single-block/key", aws.ToString(input.Key))
			require.Equal("single-block-v1", aws.ToString(input.VersionId))
			return &awss3.DeleteObjectOutput{}, nil
		},
	)
	require.NoError(store.DeleteObjectVersion(context.Background(), "bucket", "single-block/key", "single-block-v1"))
}

func TestS3ObjectStore_RejectsMutableOrEmptyVersionIDs(t *testing.T) {
	store := NewS3ObjectStore(nil)
	for _, versionID := range []string{"", "null"} {
		require.Error(t, store.DeleteObjectVersion(context.Background(), "bucket", "key", versionID))
		require.Error(t, func() error {
			_, err := store.HeadObjectVersion(context.Background(), "bucket", "key", versionID)
			return err
		}())
		require.Error(t, func() error {
			_, err := store.ReadObjectVersion(context.Background(), "bucket", "key", versionID)
			return err
		}())
	}
}
