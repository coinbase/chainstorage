package aws

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

func TestConnectionResetRetryable_IsRetryable(t *testing.T) {
	require := require.New(t)

	retryable := connectionResetRetryable{}

	// Test connection reset error is retryable
	resetErr := errors.New("read: connection reset")
	require.Equal(aws.TrueTernary, retryable.IsErrorRetryable(resetErr))

	// Test other errors are unknown (let other retryables decide)
	otherErr := errors.New("some other error")
	require.Equal(aws.UnknownTernary, retryable.IsErrorRetryable(otherErr))

	// Test nil error
	require.Equal(aws.UnknownTernary, retryable.IsErrorRetryable(nil))
}

func TestNewCustomRetryer(t *testing.T) {
	require := require.New(t)

	retryer := newCustomRetryer()
	require.NotNil(retryer)
	require.Equal(maxRetries, retryer.MaxAttempts())
}
