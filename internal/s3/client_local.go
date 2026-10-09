package s3

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

func resetLocalResources(params S3Params) error {
	buckets := []string{params.Config.AWS.Bucket}
	for _, location := range params.Config.AWS.BlockStorage.Generations {
		buckets = append(buckets, location.Bucket)
	}
	sort.Strings(buckets)
	for _, bucket := range buckets {
		if err := initBucket(params.Logger, bucket, params.AWSConfig, params.Config.AWS.IsResetLocal); err != nil {
			return err
		}
	}
	return nil
}

func initBucket(log *zap.Logger, bucket string, cfg aws.Config, reset bool) error {
	ctx := context.Background()
	s3Client := s3.NewFromConfig(cfg)
	log.Debug(fmt.Sprintf("Initializing bucket %s", bucket))
	if reset {
		err := deleteBucketIfExists(ctx, log, bucket, s3Client)
		if err != nil {
			return err
		}
	}

	return createBucketIfNotExists(ctx, bucket, s3Client)
}

func deleteBucketIfExists(ctx context.Context, log *zap.Logger, bucket string, s3Client Client) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := s3Client.HeadBucket(ctx, input)
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return xerrors.Errorf("failed to get bucket %s: %w", bucket, err)
	}

	log.Info(fmt.Sprintf("bucket %s exists, deleting files...", bucket))

	// List and delete all objects
	paginator := s3.NewListObjectsV2Paginator(s3Client.(*s3.Client), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return xerrors.Errorf("failed to list objects in bucket %s: %w", bucket, err)
		}

		if len(page.Contents) > 0 {
			objectIds := make([]types.ObjectIdentifier, len(page.Contents))
			for i, obj := range page.Contents {
				objectIds[i] = types.ObjectIdentifier{Key: obj.Key}
			}

			_, err = s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &types.Delete{Objects: objectIds},
			})
			if err != nil {
				return xerrors.Errorf("failed to delete objects under bucket %s: %w", bucket, err)
			}
		}
	}

	log.Info(fmt.Sprintf("deleting bucket %s...", bucket))
	deleteInput := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err = s3Client.DeleteBucket(ctx, deleteInput)
	if err != nil {
		return xerrors.Errorf("failed to delete bucket %s: %w", bucket, err)
	}
	return nil
}

func createBucketIfNotExists(ctx context.Context, bucket string, s3Client Client) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := s3Client.CreateBucket(ctx, input)
	if err != nil {
		var bucketExists *types.BucketAlreadyExists
		var bucketOwned *types.BucketAlreadyOwnedByYou
		if errors.As(err, &bucketExists) || errors.As(err, &bucketOwned) {
			return nil
		}

		return xerrors.Errorf("failed to create bucket %s: %w", bucket, err)
	}

	return nil
}
