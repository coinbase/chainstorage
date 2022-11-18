package endpoints

import (
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
		base                http.RoundTripper
		logger              *zap.Logger
		stickySessionHeader *stickySessionHeader
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

	// Set to zero to enable HTTP/2. See https://github.com/golang/go/issues/14391
	transport.ExpectContinueTimeout = 0

	// Disable HTTP persistent connection to improve load balancing on the server side.
	// This is necessary since BisonTrails cluster uses classic load balancer.
	transport.DisableKeepAlives = true

	return &http.Client{
		Timeout:   options.timeout,
		Transport: transport,
	}, nil
}

func newStickyClient(options *clientOptions) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	// Set to zero to enable HTTP/2. See https://github.com/golang/go/issues/14391
	transport.ExpectContinueTimeout = 0

	// Override connection pool options.
	transport.IdleConnTimeout = options.idleConnTimeout
	transport.MaxIdleConns = options.maxConns
	transport.MaxIdleConnsPerHost = options.maxConns

	client := &http.Client{
		Timeout:   options.timeout,
		Transport: transport,
		Jar:       options.jar,
	}
	wrapClient := WrapHTTPClient(client, options.stickySessionHeader)
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

func withOverrideRequestTimeout(timeout int) ClientOption {
	return func(opts *clientOptions) {
		opts.timeout = time.Duration(timeout) * time.Second
	}
}
