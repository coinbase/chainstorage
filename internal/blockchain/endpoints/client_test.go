package endpoints

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

type stubRoundTripper struct {
	calls int
}

func (s *stubRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.calls++
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
	}, nil
}

func TestRequestWeightFromContext(t *testing.T) {
	require := testutil.Require(t)

	// Unset and non-positive weights are clamped to 1.
	require.Equal(1, RequestWeightFromContext(context.Background()))
	require.Equal(1, RequestWeightFromContext(WithRequestWeight(context.Background(), 0)))
	require.Equal(1, RequestWeightFromContext(WithRequestWeight(context.Background(), -3)))
	require.Equal(7, RequestWeightFromContext(WithRequestWeight(context.Background(), 7)))
}

func TestRoundTripperRequestWeight(t *testing.T) {
	require := testutil.Require(t)

	newRequest := func(weight int) *http.Request {
		ctx := WithRequestWeight(context.Background(), weight)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost", nil)
		require.NoError(err)
		return req
	}

	// countBatch disabled: a weighted request charges a single token, so it
	// passes instantly even though the weight exceeds the burst.
	stub := &stubRoundTripper{}
	client := WrapHTTPClient(&http.Client{Transport: stub}, nil, 20, false)
	start := time.Now()
	resp, err := client.Transport.RoundTrip(newRequest(25))
	require.NoError(err)
	require.Equal(http.StatusOK, resp.StatusCode)
	require.True(time.Since(start) < 200*time.Millisecond, "elapsed=%v", time.Since(start))
	require.Equal(1, stub.calls)

	// countBatch enabled: the same request charges 25 tokens against a bucket
	// of 20, so at least 5 tokens must be waited for at 20 rps (>= 250ms).
	stub = &stubRoundTripper{}
	client = WrapHTTPClient(&http.Client{Transport: stub}, nil, 20, true)
	start = time.Now()
	resp, err = client.Transport.RoundTrip(newRequest(25))
	require.NoError(err)
	require.Equal(http.StatusOK, resp.StatusCode)
	require.True(time.Since(start) >= 200*time.Millisecond, "elapsed=%v", time.Since(start))
	require.Equal(1, stub.calls)

	// countBatch enabled without a weight in the context: defaults to 1.
	stub = &stubRoundTripper{}
	client = WrapHTTPClient(&http.Client{Transport: stub}, nil, 20, true)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://localhost", nil)
	require.NoError(err)
	start = time.Now()
	_, err = client.Transport.RoundTrip(req)
	require.NoError(err)
	require.True(time.Since(start) < 200*time.Millisecond, "elapsed=%v", time.Since(start))
}

func TestGetCookieNames(t *testing.T) {
	tests := []struct {
		cookies  string
		expected string
	}{
		{
			cookies:  "",
			expected: "",
		},
		{
			cookies:  "cookies",
			expected: "",
		},
		{
			cookies:  "c1;c2",
			expected: "",
		},
		{
			cookies:  "c1=1;c2",
			expected: "c1",
		},
		{
			cookies:  "c1=1234567;c2=",
			expected: "c1;c2",
		},
	}
	for _, test := range tests {
		t.Run(test.cookies, func(t *testing.T) {
			require := testutil.Require(t)

			actual := getCookieNames(test.cookies)
			require.Equal(test.expected, actual)
		})
	}
}
