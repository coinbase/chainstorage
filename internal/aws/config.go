package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	awstrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/aws/aws-sdk-go-v2/aws"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	ConfigParams struct {
		fx.In
		fxparams.Params
	}
)

func NewConfig(params ConfigParams) (aws.Config, error) {
	ctx := context.Background()

	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(params.Config.AWS.Region))
	opts = append(opts, config.WithRetryer(newCustomRetryer))

	if params.Config.AWS.IsLocalStack {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("THESE", "ARE", "IGNORED"),
		))
		opts = append(opts, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               "http://localstack:4566",
					HostnameImmutable: true,
				}, nil
			}),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, xerrors.Errorf("failed to load AWS config: %w", err)
	}

	// Add DataDog tracing middleware
	awstrace.AppendMiddleware(&cfg)

	return cfg, nil
}
