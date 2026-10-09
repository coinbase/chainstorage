package activity

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// blockFailure pairs a block that could not be fetched with the error explaining why.
	// The error used to be dropped at the fetch site, so the activity error named the failed
	// blocks but not the cause, which made endpoint outages look like block corruption.
	blockFailure struct {
		metadata *api.BlockMetadata
		err      error
	}

	// failureGroup collects the blocks that failed for the same reason on the same endpoint.
	failureGroup struct {
		class    string
		endpoint string
		heights  []uint64
	}
)

const (
	// maxErrorClasses bounds the summary when the blocks fail for many different reasons.
	maxErrorClasses = 5

	// maxHeightRanges bounds the height list of a single error class.
	maxHeightRanges = 10

	// maxMessageLength truncates the messages of errors that carry no recognized type.
	maxMessageLength = 160
)

var (
	// endpointRegexp extracts the endpoint name that the jsonrpc and restapi layers embed in their errors.
	endpointRegexp = regexp.MustCompile(`endpoint=([^\s,)]+)`)

	// heightRegexp and hashRegexp normalize the block-specific details out of unclassified messages so
	// that blocks failing for the same reason still collapse into a single group.
	heightRegexp = regexp.MustCompile(`height=[0-9]+`)
	hashRegexp   = regexp.MustCompile(`hash=[^\s,)]+`)
)

func blockMetadatasOf(failures []*blockFailure) []*api.BlockMetadata {
	metadatas := make([]*api.BlockMetadata, len(failures))
	for i, failure := range failures {
		metadatas[i] = failure.metadata
	}
	return metadatas
}

// summarizeBlockFailures renders the causes of the failed block fetches, deduplicated by error class
// and endpoint so that the output stays bounded when many blocks fail for one reason, e.g.
//
//	10/10 RateLimitError (HTTPError 429) on endpoint=nownodes-jsonrpc-slave; heights=[50881296..50881305]
func summarizeBlockFailures(failures []*blockFailure) string {
	if len(failures) == 0 {
		return "no failures"
	}

	groups := make(map[string]*failureGroup)
	for _, failure := range failures {
		class := errorClass(failure.err)
		endpoint := endpointOf(failure.err)
		key := class + "\x00" + endpoint
		group, ok := groups[key]
		if !ok {
			group = &failureGroup{class: class, endpoint: endpoint}
			groups[key] = group
		}
		group.heights = append(group.heights, failure.metadata.GetHeight())
	}

	sorted := make([]*failureGroup, 0, len(groups))
	for _, group := range groups {
		sorted = append(sorted, group)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if len(sorted[i].heights) != len(sorted[j].heights) {
			return len(sorted[i].heights) > len(sorted[j].heights)
		}
		if sorted[i].class != sorted[j].class {
			return sorted[i].class < sorted[j].class
		}
		return sorted[i].endpoint < sorted[j].endpoint
	})

	truncated := 0
	if len(sorted) > maxErrorClasses {
		truncated = len(sorted) - maxErrorClasses
		sorted = sorted[:maxErrorClasses]
	}

	summaries := make([]string, len(sorted))
	for i, group := range sorted {
		var endpoint string
		if group.endpoint != "" {
			endpoint = fmt.Sprintf(" on endpoint=%v", group.endpoint)
		}
		summaries[i] = fmt.Sprintf(
			"%d/%d %v%v; heights=%v",
			len(group.heights), len(failures), group.class, endpoint, formatHeights(group.heights),
		)
	}

	summary := strings.Join(summaries, " | ")
	if truncated > 0 {
		summary += fmt.Sprintf(" | and %d more error classes", truncated)
	}

	return summary
}

// errorClass returns a bounded, block-independent description of why a block fetch failed.
func errorClass(err error) string {
	if err == nil {
		return "unknown error"
	}

	cause := errorCause(err)

	// RateLimitError is the classification that matters most during an incident: it tells the operator
	// that the endpoint is throttling rather than that the block data is broken.
	var rateLimitErr *retry.RateLimitError
	if xerrors.As(err, &rateLimitErr) {
		return fmt.Sprintf("RateLimitError (%v)", cause)
	}

	return cause
}

func errorCause(err error) string {
	var jsonrpcHTTPErr *jsonrpc.HTTPError
	if xerrors.As(err, &jsonrpcHTTPErr) {
		return fmt.Sprintf("HTTPError %d", jsonrpcHTTPErr.Code)
	}

	var restapiHTTPErr *restapi.HTTPError
	if xerrors.As(err, &restapiHTTPErr) {
		return fmt.Sprintf("HTTPError %d", restapiHTTPErr.Code)
	}

	var rpcErr *jsonrpc.RPCError
	if xerrors.As(err, &rpcErr) {
		return fmt.Sprintf("RPCError %d: %v", rpcErr.Code, truncateErrorMessage(rpcErr.Message))
	}

	switch {
	case xerrors.Is(err, client.ErrBlockNotFound):
		return "ErrBlockNotFound"
	case xerrors.Is(err, context.DeadlineExceeded):
		return "DeadlineExceeded"
	case xerrors.Is(err, context.Canceled):
		return "Canceled"
	}

	return truncateErrorMessage(normalizeMessage(err.Error()))
}

// endpointOf recovers the endpoint name from the message that the jsonrpc and restapi layers format.
// The name is not carried by a typed error, so this is a best-effort lookup and returns "" when absent.
func endpointOf(err error) string {
	if err == nil {
		return ""
	}

	match := endpointRegexp.FindStringSubmatch(err.Error())
	if len(match) < 2 {
		return ""
	}

	return match[1]
}

// normalizeMessage removes the block-specific details so that unclassified errors still group together.
func normalizeMessage(message string) string {
	message = heightRegexp.ReplaceAllString(message, "height=*")
	message = hashRegexp.ReplaceAllString(message, "hash=*")
	return message
}

func truncateErrorMessage(message string) string {
	if len(message) <= maxMessageLength {
		return message
	}

	return message[:maxMessageLength] + "..."
}

// formatHeights compresses the heights into ranges, e.g. [50881296..50881305].
func formatHeights(heights []uint64) string {
	sorted := make([]uint64, len(heights))
	copy(sorted, heights)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	ranges := make([]string, 0, len(sorted))
	for i := 0; i < len(sorted); {
		j := i
		for j+1 < len(sorted) && (sorted[j+1] == sorted[j] || sorted[j+1] == sorted[j]+1) {
			j += 1
		}
		if sorted[i] == sorted[j] {
			ranges = append(ranges, fmt.Sprintf("%d", sorted[i]))
		} else {
			ranges = append(ranges, fmt.Sprintf("%d..%d", sorted[i], sorted[j]))
		}
		i = j + 1
	}

	truncated := 0
	if len(ranges) > maxHeightRanges {
		truncated = len(ranges) - maxHeightRanges
		ranges = ranges[:maxHeightRanges]
	}

	formatted := fmt.Sprintf("[%v]", strings.Join(ranges, ","))
	if truncated > 0 {
		formatted += fmt.Sprintf("(+%d more)", truncated)
	}

	return formatted
}
