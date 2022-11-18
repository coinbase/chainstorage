package aws

import (
	"errors"
	"net"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/require"
)

var errReadConnectionReset = awserr.New(
	request.ErrCodeRequestError,
	"send request failed",
	&net.OpError{
		Op:  "read",
		Err: errors.New("connection reset"),
	},
)

func TestDefaultRetryer(t *testing.T) {
	require := require.New(t)
	req := &request.Request{
		Error: errReadConnectionReset,
	}
	retryer := client.DefaultRetryer{NumMaxRetries: 3}
	require.False(retryer.ShouldRetry(req))
}

func TestCustomRetryer(t *testing.T) {
	require := require.New(t)
	req := &request.Request{
		Error: errReadConnectionReset,
	}
	retryer := newCustomRetryer()
	require.True(retryer.ShouldRetry(req))
}
