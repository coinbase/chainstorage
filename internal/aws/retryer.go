package aws

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
)

type customRetryer struct {
	client.DefaultRetryer
}

const (
	// Use the same configurations as the DynamoDB client.
	// Ref: https://github.com/aws/aws-sdk-go/blob/6fcfde5b3429cb00ea9dfc16157299cc6ec0bcaf/service/dynamodb/customizations.go#L33
	maxRetries    = 10
	minRetryDelay = 50 * time.Millisecond
)

var _ request.Retryer = &customRetryer{}

func newCustomRetryer() request.Retryer {
	return &customRetryer{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries: maxRetries,
			MinRetryDelay: minRetryDelay,
		},
	}
}

// isErrReadConnectionReset returns true if the underlying error is a read connection reset error.
//
// A read connection reset error is thrown when the SDK is unable to read the response of an underlying API request
// due to a connection reset. The DefaultRetryer does not treat this error as a retryable error since the SDK has no
// knowledge about whether the given operation is idempotent or whether it would be safe to retry.
// Ref: https://github.com/aws/aws-sdk-go/pull/2926#issuecomment-553637658.
//
// In this project all operations with S3 or DynamoDB (read, write, list) are considered idempotent,
// and therefore this error can be treated as retryable.
func isErrReadConnectionReset(err error) bool {
	// The error string must match the one in
	// https://github.com/aws/aws-sdk-go/blob/main/aws/request/connection_reset_error.go.
	return err != nil && strings.Contains(err.Error(), "read: connection reset")
}

// ShouldRetry overrides the implementation defined in client.DefaultRetryer.
func (sr *customRetryer) ShouldRetry(r *request.Request) bool {
	return sr.DefaultRetryer.ShouldRetry(r) || isErrReadConnectionReset(r.Error)
}
