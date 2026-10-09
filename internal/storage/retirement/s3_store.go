package retirement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/s3"
)

type S3ObjectStore struct {
	client s3.Client
}

func NewS3ObjectStore(client s3.Client) *S3ObjectStore {
	return &S3ObjectStore{client: client}
}

func (s *S3ObjectStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	return s.headObject(ctx, bucket, key, "")
}

func (s *S3ObjectStore) HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	if !isImmutableVersionID(versionID) {
		return ObjectHead{}, xerrors.New("an immutable non-null version id is required to head an object version")
	}
	return s.headObject(ctx, bucket, key, versionID)
}

func (s *S3ObjectStore) headObject(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	input := &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}
	out, err := s.client.HeadObject(ctx, input)
	if err != nil {
		if isObjectNotFound(err) {
			return ObjectHead{}, nil
		}
		return ObjectHead{}, xerrors.Errorf("failed to head object (bucket=%s key=%s): %w", bucket, key, err)
	}
	head := ObjectHead{
		Exists:     true,
		VersionID:  aws.ToString(out.VersionId),
		ETag:       aws.ToString(out.ETag),
		Metadata:   out.Metadata,
		DeleteMark: aws.ToBool(out.DeleteMarker),
		Expiration: aws.ToString(out.Expiration),
	}
	if out.ContentLength != nil && *out.ContentLength > 0 {
		head.Bytes = uint64(*out.ContentLength)
	}
	return head, nil
}

func (s *S3ObjectStore) ListObjectVersions(ctx context.Context, bucket string, key string) (ObjectVersionTopology, error) {
	input := &awss3.ListObjectVersionsInput{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(key),
		MaxKeys: aws.Int32(1000),
	}
	var topology ObjectVersionTopology
	for {
		out, err := s.client.ListObjectVersions(ctx, input)
		if err != nil {
			return ObjectVersionTopology{}, xerrors.Errorf("failed to list object versions (bucket=%s key=%s): %w", bucket, key, err)
		}
		for _, version := range out.Versions {
			if aws.ToString(version.Key) != key {
				continue
			}
			item := ObjectVersion{
				VersionID:    aws.ToString(version.VersionId),
				ETag:         aws.ToString(version.ETag),
				IsLatest:     aws.ToBool(version.IsLatest),
				LastModified: cloneTime(version.LastModified),
			}
			if version.Size != nil && *version.Size > 0 {
				item.Bytes = uint64(*version.Size)
			}
			topology.Versions = append(topology.Versions, item)
		}
		for _, marker := range out.DeleteMarkers {
			if aws.ToString(marker.Key) != key {
				continue
			}
			topology.DeleteMarkers = append(topology.DeleteMarkers, ObjectDeleteMarker{
				VersionID:    aws.ToString(marker.VersionId),
				IsLatest:     aws.ToBool(marker.IsLatest),
				LastModified: cloneTime(marker.LastModified),
			})
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		nextKeyMarker := aws.ToString(out.NextKeyMarker)
		nextVersionIDMarker := aws.ToString(out.NextVersionIdMarker)
		if nextKeyMarker == "" || (nextKeyMarker == aws.ToString(input.KeyMarker) && nextVersionIDMarker == aws.ToString(input.VersionIdMarker)) {
			return ObjectVersionTopology{}, xerrors.Errorf("invalid object-version pagination cursor (bucket=%s key=%s)", bucket, key)
		}
		input.KeyMarker = aws.String(nextKeyMarker)
		input.VersionIdMarker = aws.String(nextVersionIDMarker)
	}
	return topology, nil
}

func (s *S3ObjectStore) ReadObjectVersion(ctx context.Context, bucket string, key string, versionID string) ([]byte, error) {
	return s.readObject(ctx, bucket, key, versionID, "")
}

func (s *S3ObjectStore) ReadObjectVersionRange(ctx context.Context, bucket string, key string, versionID string, offset uint64, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, xerrors.New("object range length must be positive")
	}
	if offset > ^uint64(0)-(length-1) {
		return nil, xerrors.Errorf("object range overflow: offset=%d length=%d", offset, length)
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	return s.readObject(ctx, bucket, key, versionID, rangeHeader)
}

func (s *S3ObjectStore) readObject(ctx context.Context, bucket string, key string, versionID string, rangeHeader string) ([]byte, error) {
	if !isImmutableVersionID(versionID) {
		return nil, xerrors.New("an immutable non-null version id is required to read an object version")
	}
	input := &awss3.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	}
	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}
	out, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("failed to read object version (bucket=%s key=%s version_id=%s range=%s): %w", bucket, key, versionID, rangeHeader, err)
	}
	if out.Body == nil {
		return nil, xerrors.Errorf("object version returned an empty body (bucket=%s key=%s version_id=%s)", bucket, key, versionID)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, xerrors.Errorf("failed to read object-version body (bucket=%s key=%s version_id=%s): %w", bucket, key, versionID, err)
	}
	return body, nil
}

func (s *S3ObjectStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error {
	if !isImmutableVersionID(versionID) {
		return xerrors.New("an immutable non-null version id is required for retirement delete")
	}
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return xerrors.Errorf("failed to delete object version (bucket=%s key=%s version_id=%s): %w", bucket, key, versionID, err)
	}
	return nil
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func isObjectNotFound(err error) bool {
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
