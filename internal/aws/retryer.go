package aws

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
)

const (
	// Use the same configurations as the DynamoDB client.
	maxRetries    = 10
	minRetryDelay = 50 * time.Millisecond
	maxRetryDelay = 5 * time.Second
)

// newCustomRetryer creates a custom retryer for AWS SDK v2.
func newCustomRetryer() aws.Retryer {
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = maxRetries
		o.MaxBackoff = maxRetryDelay
		o.Retryables = append(o.Retryables, connectionResetRetryable{})
	})
}

// connectionResetRetryable implements retry.IsErrorRetryable for connection reset errors.
type connectionResetRetryable struct{}

// IsErrorRetryable returns true if the error is a read connection reset error.
//
// A read connection reset error is thrown when the SDK is unable to read the response of an underlying API request
// due to a connection reset. The default retryer does not treat this error as a retryable error since the SDK has no
// knowledge about whether the given operation is idempotent or whether it would be safe to retry.
//
// In this project all operations with S3 or DynamoDB (read, write, list) are considered idempotent,
// and therefore this error can be treated as retryable.
func (connectionResetRetryable) IsErrorRetryable(err error) aws.Ternary {
	if err != nil && strings.Contains(err.Error(), "read: connection reset") {
		return aws.TrueTernary
	}
	return aws.UnknownTernary
}
