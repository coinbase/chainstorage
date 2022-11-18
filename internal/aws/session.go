package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	awstrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/aws/aws-sdk-go/aws"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	SessionParams struct {
		fx.In
		fxparams.Params
		AWSConfig *aws.Config
	}
)

func NewSession(params SessionParams) (*session.Session, error) {
	awsSession, err := session.NewSession(params.AWSConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to create AWS session: %w", err)
	}

	// wrap aws session for tracing requests and responses
	awsSession = awstrace.WrapSession(awsSession)
	return awsSession, nil
}
