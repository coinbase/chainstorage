package endpoints

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/ratelimiter"
)

type requestWeightContextKey struct{}

// WithRequestWeight annotates ctx with the number of provider-side requests
// the enclosed HTTP request represents, e.g. the number of calls inside a
// JSON-RPC batch request. The weight is only consumed by endpoints configured
// with rps_count_batch; all other endpoints keep charging one token per HTTP
// request.
func WithRequestWeight(ctx context.Context, weight int) context.Context {
	return context.WithValue(ctx, requestWeightContextKey{}, weight)
}

// RequestWeightFromContext returns the weight set by WithRequestWeight.
// It returns 1 when the weight is unset or non-positive, since every HTTP
// request costs at least one provider-side request.
func RequestWeightFromContext(ctx context.Context) int {
	weight, ok := ctx.Value(requestWeightContextKey{}).(int)
	if !ok || weight <= 0 {
		return 1
	}

	return weight
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(rt.headers) > 0 {
		for key, val := range rt.headers {
			req.Header.Set(key, val)
		}
	}
	weight := 1
	if rt.countBatch {
		weight = RequestWeightFromContext(req.Context())
	}
	err := rt.rateLimiter.WaitWeight(req.Context(), weight)
	if err != nil {
		return nil, xerrors.Errorf("failed to wait for rate limiting: %w", err)
	}
	res, err := rt.base.RoundTrip(req)
	return res, err
}

func WrapHTTPClient(client *http.Client, headers map[string]string, rps int, rpsCountBatch bool) *http.Client {
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	client.Transport = &roundTripper{
		base:        transport,
		headers:     headers,
		rateLimiter: ratelimiter.New(rps),
		countBatch:  rpsCountBatch,
	}

	return client
}

func getCookieNames(cookieStr string) string {
	cookies := strings.Split(cookieStr, ";")

	var sb strings.Builder
	for _, c := range cookies {
		cookie := strings.Split(c, "=")
		if len(cookie) != 2 {
			continue
		}
		sb.WriteString(fmt.Sprintf("%s;", cookie[0]))
	}
	result := strings.TrimSuffix(sb.String(), ";")
	return result
}
