package generationrehome

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
)

type S3ObjectStore struct {
	client s3.Client
}

func NewS3ObjectStore(client s3.Client) *S3ObjectStore {
	return &S3ObjectStore{client: client}
}

func (s *S3ObjectStore) InspectObjectRetentionSafety(ctx context.Context, bucket string, key string) error {
	if s == nil {
		return xerrors.New("s3 client is required to verify destination retention safety")
	}
	_, err := retirement.NewS3ObjectStore(s.client).InspectObjectRetentionSafety(ctx, bucket, key)
	return err
}

func (s *S3ObjectStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	return s.headObject(ctx, bucket, key, "")
}

func (s *S3ObjectStore) HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	if !immutableVersionID(versionID) {
		return ObjectHead{}, xerrors.New("an immutable non-null version id is required")
	}
	return s.headObject(ctx, bucket, key, versionID)
}

func (s *S3ObjectStore) headObject(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	if s.client == nil {
		return ObjectHead{}, xerrors.New("s3 client is required")
	}
	input := &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	output, err := s.client.HeadObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return ObjectHead{}, nil
		}
		return ObjectHead{}, xerrors.Errorf("failed to head object (bucket=%s key=%s version_id=%s): %w", bucket, key, versionID, err)
	}
	head := ObjectHead{
		Exists:    true,
		VersionID: aws.ToString(output.VersionId),
		ETag:      aws.ToString(output.ETag),
	}
	if output.ContentLength != nil && *output.ContentLength > 0 {
		head.Bytes = uint64(*output.ContentLength)
	}
	return head, nil
}

func isNotFound(err error) bool {
	var notFound *awss3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var noSuchKey *awss3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return true
		}
	}
	return false
}

var _ ObjectStore = (*S3ObjectStore)(nil)
