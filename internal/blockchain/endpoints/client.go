package endpoints

import (
	"fmt"
	"net/http"
	"strings"
)

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(rt.headers) > 0 {
		for key, val := range rt.headers {
			req.Header.Set(key, val)
		}
	}

	res, err := rt.base.RoundTrip(req)
	return res, err
}

func WrapHTTPClient(client *http.Client, headers map[string]string) *http.Client {
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	client.Transport = &roundTripper{
		base:    transport,
		headers: headers,
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
