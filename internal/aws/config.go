package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	ConfigParams struct {
		fx.In
		fxparams.Params
	}
)

func NewConfig(params ConfigParams) *aws.Config {
	cfg := &aws.Config{
		Region:  aws.String(params.Config.AWS.Region),
		Retryer: newCustomRetryer(),
	}
	if params.Config.AWS.IsLocalStack {
		cfg.Credentials = credentials.NewStaticCredentials("THESE", "ARE", "IGNORED")
		cfg.S3ForcePathStyle = aws.Bool(true)
		cfg.Endpoint = aws.String("http://localhost:4566")
	}
	return cfg
}
