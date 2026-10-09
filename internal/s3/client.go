package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	// Client interface for S3 operations
	Client interface {
		GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
		PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
		HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
		HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
		GetBucketPolicy(ctx context.Context, params *s3.GetBucketPolicyInput, optFns ...func(*s3.Options)) (*s3.GetBucketPolicyOutput, error)
		GetBucketLifecycleConfiguration(ctx context.Context, params *s3.GetBucketLifecycleConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error)
		CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
		DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
		ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
		ListObjectVersions(ctx context.Context, params *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
		DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
		DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	}

	// Downloader interface for S3 download operations
	Downloader interface {
		Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*manager.Downloader)) (n int64, err error)
	}

	// Uploader interface for S3 upload operations
	Uploader interface {
		Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
	}

	S3 struct {
		Config aws.Config
	}

	S3Params struct {
		fx.In
		fxparams.Params
		AWSConfig aws.Config
	}

	ClientParams struct {
		fx.In
		S3 *S3
	}
)

func NewS3(params S3Params) (*S3, error) {
	if params.Config.AWS.IsLocalStack {
		if err := resetLocalResources(params); err != nil {
			return nil, xerrors.Errorf("failed to prepare local resources for aws s3 session: %w", err)
		}
	}

	return &S3{
		Config: params.AWSConfig,
	}, nil
}

func NewUploader(params ClientParams) Uploader {
	client := s3.NewFromConfig(params.S3.Config)
	return manager.NewUploader(client)
}

func NewDownloader(params ClientParams) Downloader {
	client := s3.NewFromConfig(params.S3.Config)
	return manager.NewDownloader(client)
}

func NewClient(params ClientParams) Client {
	return s3.NewFromConfig(params.S3.Config)
}
