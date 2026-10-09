package activity

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// rateLimitedError reproduces the error chain that the jsonrpc client returns for an HTTP 429,
// as wrapped by safeGetBlock.
func rateLimitedError(endpoint string, height uint64) error {
	err := retry.RateLimit(xerrors.Errorf("received http error: %w", &jsonrpc.HTTPError{
		Code:     429,
		Response: "Too Many Requests",
	}))
	err = xerrors.Errorf("failed to make http request (method=eth_getBlockByNumber, params=[%v], endpoint=%v): %w", height, endpoint, err)
	return xerrors.Errorf("failed to get block from slave (height=%v, hash=%v): %w", height, "0xabcd", err)
}

// methodNotFoundError reproduces the error chain of a node that lost the debug_* namespace.
func methodNotFoundError(endpoint string, height uint64) error {
	err := xerrors.Errorf("received rpc error (method=debug_traceBlockByHash, endpoint=%v): %w", endpoint, &jsonrpc.RPCError{
		Code:    -32601,
		Message: "the method debug_traceBlockByHash does not exist/is not available",
	})
	return xerrors.Errorf("failed to get block from master (height=%v, hash=%v): %w", height, "0xabcd", err)
}

func makeBlockFailures(err func(height uint64) error, startHeight uint64, numBlocks uint64) []*blockFailure {
	failures := make([]*blockFailure, 0, numBlocks)
	for height := startHeight; height < startHeight+numBlocks; height++ {
		failures = append(failures, &blockFailure{
			metadata: &api.BlockMetadata{Tag: tag, Height: height},
			err:      err(height),
		})
	}
	return failures
}

func TestSummarizeBlockFailures_RateLimited(t *testing.T) {
	require := testutil.Require(t)

	failures := makeBlockFailures(func(height uint64) error {
		return rateLimitedError("nownodes-jsonrpc-slave", height)
	}, 50881296, 10)

	require.Equal(
		"10/10 RateLimitError (HTTPError 429) on endpoint=nownodes-jsonrpc-slave; heights=[50881296..50881305]",
		summarizeBlockFailures(failures),
	)
}

func TestSummarizeBlockFailures_MethodNotFound(t *testing.T) {
	require := testutil.Require(t)

	failures := makeBlockFailures(func(height uint64) error {
		return methodNotFoundError("nownodes-jsonrpc-master", height)
	}, 100, 2)

	require.Equal(
		"2/2 RPCError -32601: the method debug_traceBlockByHash does not exist/is not available on endpoint=nownodes-jsonrpc-master; heights=[100..101]",
		summarizeBlockFailures(failures),
	)
}

func TestSummarizeBlockFailures_MultipleClasses(t *testing.T) {
	require := testutil.Require(t)

	failures := makeBlockFailures(func(height uint64) error {
		return rateLimitedError("slave", height)
	}, 100, 3)
	failures = append(failures, makeBlockFailures(func(height uint64) error {
		return methodNotFoundError("master", height)
	}, 200, 1)...)

	// The groups are ordered by size so that the dominant cause leads the summary.
	require.Equal(
		"3/4 RateLimitError (HTTPError 429) on endpoint=slave; heights=[100..102] | "+
			"1/4 RPCError -32601: the method debug_traceBlockByHash does not exist/is not available on endpoint=master; heights=[200]",
		summarizeBlockFailures(failures),
	)
}

func TestSummarizeBlockFailures_GroupsUntypedErrorsByNormalizedMessage(t *testing.T) {
	require := testutil.Require(t)

	// The heights and hashes of the individual blocks must not split one cause into many groups.
	failures := makeBlockFailures(func(height uint64) error {
		return xerrors.Errorf("failed to get block from slave (height=%v, hash=%v): block validation failed", height, fmt.Sprintf("0x%x", height))
	}, 100, 4)

	require.Equal(
		"4/4 failed to get block from slave (height=*, hash=*): block validation failed; heights=[100..103]",
		summarizeBlockFailures(failures),
	)
}

func TestSummarizeBlockFailures_BoundsErrorClasses(t *testing.T) {
	require := testutil.Require(t)

	failures := makeBlockFailures(func(height uint64) error {
		return xerrors.Errorf("distinct failure %v", height)
	}, 100, uint64(maxErrorClasses)+3)

	summary := summarizeBlockFailures(failures)
	require.Contains(summary, "and 3 more error classes")
	require.Equal(maxErrorClasses, strings.Count(summary, "; heights="))
}

func TestSummarizeBlockFailures_BoundsHeights(t *testing.T) {
	require := testutil.Require(t)

	// Non-contiguous heights cannot be compressed into ranges, so the list itself must be bounded.
	failures := make([]*blockFailure, 0, maxHeightRanges+2)
	for i := 0; i < maxHeightRanges+2; i++ {
		height := uint64(100 + i*2)
		failures = append(failures, &blockFailure{
			metadata: &api.BlockMetadata{Tag: tag, Height: height},
			err:      rateLimitedError("slave", height),
		})
	}

	require.Contains(summarizeBlockFailures(failures), "(+2 more)")
}

func TestSummarizeBlockFailures_NoFailures(t *testing.T) {
	require := testutil.Require(t)

	require.Equal("no failures", summarizeBlockFailures(nil))
}

func TestErrorClass(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil",
			err:      nil,
			expected: "unknown error",
		},
		{
			name:     "rate limited",
			err:      rateLimitedError("slave", 100),
			expected: "RateLimitError (HTTPError 429)",
		},
		{
			name:     "http error",
			err:      xerrors.Errorf("received http error: %w", &jsonrpc.HTTPError{Code: 503, Response: "unavailable"}),
			expected: "HTTPError 503",
		},
		{
			name:     "restapi http error",
			err:      xerrors.Errorf("received http error: %w", &restapi.HTTPError{Code: 502, Response: "bad gateway"}),
			expected: "HTTPError 502",
		},
		{
			name:     "rpc error",
			err:      methodNotFoundError("master", 100),
			expected: "RPCError -32601: the method debug_traceBlockByHash does not exist/is not available",
		},
		{
			name:     "block not found",
			err:      xerrors.Errorf("block not found by heights [98, 101): %w", client.ErrBlockNotFound),
			expected: "ErrBlockNotFound",
		},
		{
			name:     "deadline exceeded",
			err:      xerrors.Errorf("failed to send http request: %w", context.DeadlineExceeded),
			expected: "DeadlineExceeded",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			require.Equal(test.expected, errorClass(test.err))
		})
	}
}

func TestEndpointOf(t *testing.T) {
	require := testutil.Require(t)

	require.Equal("nownodes-jsonrpc-slave", endpointOf(rateLimitedError("nownodes-jsonrpc-slave", 100)))
	require.Equal("", endpointOf(xerrors.New("no endpoint here")))
	require.Equal("", endpointOf(nil))
}

func TestFormatHeights(t *testing.T) {
	tests := []struct {
		name     string
		heights  []uint64
		expected string
	}{
		{
			name:     "single",
			heights:  []uint64{100},
			expected: "[100]",
		},
		{
			name:     "contiguous and unordered",
			heights:  []uint64{102, 100, 101},
			expected: "[100..102]",
		},
		{
			name:     "multiple ranges",
			heights:  []uint64{100, 101, 200, 300, 301},
			expected: "[100..101,200,300..301]",
		},
		{
			name:     "duplicates",
			heights:  []uint64{100, 100, 101},
			expected: "[100..101]",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			require.Equal(test.expected, formatHeights(test.heights))
		})
	}
}
