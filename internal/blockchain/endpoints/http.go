package endpoints

import (
	"crypto/x509"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/publicsuffix"
	tracehttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
)

type (
	ClientOption func(opts *clientOptions)

	clientOptions struct {
		timeout             time.Duration
		idleConnTimeout     time.Duration
		maxConns            int
		jar                 http.CookieJar
		stickySessionHeader *stickySessionHeader
		headers             map[string]string
		rootCAs             *x509.CertPool
	}

	// stickySessionJar implements passive cookie affinity.
	//
	// The LB takes a cookie that’s present in the cookies header and hashes on its value.
	// Ref: https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/route/v3/route_components.proto#envoy-v3-api-msg-config-route-v3-routeaction-hashpolicy-cookie
	stickySessionJar struct {
		url         string
		cookieKey   string
		cookieValue string
	}

	stickySessionHeader struct {
		headerKey   string
		headerValue string
	}

	roundTripper struct {
		base    http.RoundTripper
		logger  *zap.Logger
		headers map[string]string
	}
)

const (
	defaultTimeout         = 120 * time.Second
	defaultIdleConnTimeout = 30 * time.Second
	defaultMaxConns        = 120
)

var _ http.CookieJar = (*stickySessionJar)(nil)

func newHTTPClient(opts ...ClientOption) (*http.Client, error) {
	options := &clientOptions{
		timeout:         defaultTimeout,
		idleConnTimeout: defaultIdleConnTimeout,
		maxConns:        defaultMaxConns,
	}

	for _, opt := range opts {
		opt(options)
	}

	var httpClient *http.Client
	var err error
	if options.jar == nil && options.stickySessionHeader == nil {
		httpClient, err = newDefaultClient(options)
		if err != nil {
			return nil, err
		}
	} else {
		httpClient, err = newStickyClient(options)
		if err != nil {
			return nil, err
		}
	}

	return tracehttp.WrapClient(httpClient), nil
}

func newDefaultClient(options *clientOptions) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if options.rootCAs != nil {
		transport.TLSClientConfig.RootCAs = options.rootCAs
	}

	client := &http.Client{
		Timeout:   options.timeout,
		Transport: transport,
	}
	wrapClient := WrapHTTPClient(client, options.headers)
	return wrapClient, nil
}

func newStickyClient(options *clientOptions) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	// Override connection pool options.
	transport.IdleConnTimeout = options.idleConnTimeout
	transport.MaxIdleConns = options.maxConns
	transport.MaxIdleConnsPerHost = options.maxConns
	if options.rootCAs != nil {
		transport.TLSClientConfig.RootCAs = options.rootCAs
	}

	client := &http.Client{
		Timeout:   options.timeout,
		Transport: transport,
		Jar:       options.jar,
	}

	headers := make(map[string]string)
	stickySession := options.stickySessionHeader
	if stickySession != nil {
		headers[stickySession.headerKey] = stickySession.headerValue
	}

	for key, val := range options.headers {
		headers[key] = val
	}

	wrapClient := WrapHTTPClient(client, headers)
	return wrapClient, nil
}

// The HeaderHash method consistently maps a header value to a specific node.
func withStickySessionHeaderHash(key string, value string) ClientOption {
	return func(opts *clientOptions) {
		opts.stickySessionHeader = &stickySessionHeader{
			headerKey:   key,
			headerValue: value,
		}
	}
}

// The CookiePassive method persists the cookie value provided by the server.
func withStickySessionCookiePassive() ClientOption {
	return func(opts *clientOptions) {
		jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
		opts.jar = jar
	}
}

// The CookieHash method consistently maps a cookie value to a specific node.
func withStickySessionCookieHash(url string, key string, value string) ClientOption {
	return func(opts *clientOptions) {
		opts.jar = &stickySessionJar{
			url:         url,
			cookieKey:   key,
			cookieValue: value,
		}
	}
}

// The withHeaders method add customized headers to request.
func withHeaders(headers map[string]string) ClientOption {
	return func(opts *clientOptions) {
		opts.headers = headers
	}
}

func (j *stickySessionJar) SetCookies(_ *url.URL, _ []*http.Cookie) {
	// nop
}

func (j *stickySessionJar) Cookies(u *url.URL) []*http.Cookie {
	if !strings.HasPrefix(u.String(), j.url) {
		return nil
	}

	return []*http.Cookie{
		{
			Name:  j.cookieKey,
			Value: j.cookieValue,
		},
	}
}

func withOverrideRequestTimeout(timeout time.Duration) ClientOption {
	return func(opts *clientOptions) {
		opts.timeout = timeout
	}
}

// temporary fix cert issue for golang1.18 on MacOS
func withRootCAs(roots *x509.CertPool) ClientOption {
	return func(opts *clientOptions) {
		opts.rootCAs = roots
	}
}
