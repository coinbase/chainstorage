package endpoints

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/coinbase/chainstorage/internal/utils/ratelimiter"
)

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(rt.headers) > 0 {
		for key, val := range rt.headers {
			req.Header.Set(key, val)
		}
	}
	err := rt.rateLimiter.WaitN(req.Context(), 1)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for rate limiting: %w", err)
	}
	res, err := rt.base.RoundTrip(req)
	return res, err
}

func WrapHTTPClient(client *http.Client, headers map[string]string, rps int) *http.Client {
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	client.Transport = &roundTripper{
		base:        transport,
		headers:     headers,
		rateLimiter: ratelimiter.New(rps),
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
