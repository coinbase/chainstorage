package endpoints

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestHTTPClient_Default(t *testing.T) {
	require := testutil.Require(t)

	client, err := newHTTPClient()
	require.NoError(err)
	require.NotEmpty(client.Timeout)
	require.Nil(client.Jar)
}

func TestHTTPClient_StickySessionCookie(t *testing.T) {
	const (
		u     = "https://www.coinbase.com"
		key   = "hash_key"
		value = "foo_bar"
	)

	require := testutil.Require(t)

	client, err := newHTTPClient(withStickySessionCookieHash(u, key, value))
	require.NoError(err)
	require.NotEmpty(client.Timeout)

	jar := client.Jar
	require.NotNil(jar)

	pu, err := url.Parse("https://www.coinbase.com/block")
	require.NoError(err)
	expected := []*http.Cookie{
		{
			Name:  key,
			Value: value,
		},
	}
	actual := jar.Cookies(pu)
	require.Equal(1, len(actual))
	require.Equal(expected, actual)
}

func TestHTTPClient_ALBStickySession(t *testing.T) {
	require := testutil.Require(t)

	client, err := newHTTPClient(withStickySessionCookiePassive())
	require.NoError(err)
	require.NotEmpty(client.Timeout)

	require.NotNil(client.Jar)
	_, ok := client.Jar.(http.CookieJar)
	require.True(ok)
}

func TestHTTPClient_StickySessionHeader(t *testing.T) {
	require := testutil.Require(t)

	client, err := newHTTPClient(withStickySessionHeaderHash("key", "value"))
	require.NoError(err)
	require.NotEmpty(client.Timeout)
	require.Nil(client.Jar)
}

func TestHTTPClient_TimeoutOverride(t *testing.T) {
	require := testutil.Require(t)
	timeout := 1000 * time.Second
	client, err := newHTTPClient(withOverrideRequestTimeout(timeout))
	require.NoError(err)
	require.NotEmpty(client.Timeout)
	require.Equal(timeout, client.Timeout)
}
