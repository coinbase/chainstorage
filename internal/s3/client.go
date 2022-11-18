package s3

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	otaws "github.com/opentracing-contrib/go-aws-sdk"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	Downloader = s3manageriface.DownloaderAPI
	Uploader   = s3manageriface.UploaderAPI
	Client     = s3iface.S3API

	S3 struct {
		Session *session.Session
	}

	S3Params struct {
		fx.In
		fxparams.Params
		Session *session.Session
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
		Session: params.Session,
	}, nil
}

func NewUploader(params ClientParams) Uploader {
	return s3manager.NewUploader(params.S3.Session)
}

func NewDownloader(params ClientParams) Downloader {
	return s3manager.NewDownloader(params.S3.Session)
}

func NewClient(params ClientParams) Client {
	s3Client := s3.New(params.S3.Session)
	// this is optional as we already have traces at session level
	// but since we have the client, we might as well add for extra information at client level
	otaws.AddOTHandlers(s3Client.Client)
	return s3Client
}
