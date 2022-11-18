package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

func resetLocalResources(params S3Params) error {
	return initBucket(params.Logger, params.Config.AWS.Bucket, params.Session, params.Config.AWS.IsResetLocal)
}

func initBucket(log *zap.Logger, bucket string, awsSession *session.Session, reset bool) error {
	s3Client := s3.New(awsSession)
	log.Debug(fmt.Sprintf("Initializing bucket %s", bucket))
	if reset {
		err := deleteBucketIfExists(log, bucket, s3Client)
		if err != nil {
			return err
		}
	}

	return createBucketIfNotExists(bucket, s3Client)
}

func deleteBucketIfExists(log *zap.Logger, bucket string, s3Client s3iface.S3API) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := s3Client.HeadBucket(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotFound" {
				return nil
			}
		}
		return xerrors.Errorf("failed to get bucket %s: %w", bucket, err)
	}

	log.Info(fmt.Sprintf("bucket %s exists, deleting files...", bucket))
	iter := s3manager.NewDeleteListIterator(s3Client, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	if err := s3manager.NewBatchDeleteWithClient(s3Client).Delete(aws.BackgroundContext(), iter); err != nil {
		return xerrors.Errorf("failed to delete object under bucket %s: %w", bucket, err)
	}

	log.Info(fmt.Sprintf("deleting bucket %s...", bucket))
	deleteInput := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err = s3Client.DeleteBucket(deleteInput)
	if err != nil {
		return xerrors.Errorf("failed to delete bucket %s: %w", bucket, err)
	}
	return nil
}

func createBucketIfNotExists(bucket string, s3Client s3iface.S3API) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := s3Client.CreateBucket(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == s3.ErrCodeBucketAlreadyExists {
				return nil
			}
		}

		return xerrors.Errorf("failed to create bucket %s: %w", bucket, err)
	}

	return nil
}
