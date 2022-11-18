package endpoints

import (
	"context"
	"fmt"
	"math"
	"net/http/cookiejar"
	"sort"
	"testing"

	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

const (
	largeNumPicks = 100000
	smallNumPicks = 10
)

func TestEndpointProvider(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpoints := make([]config.Endpoint, 5)
	totalWeights := uint32(0)
	for i := 0; i < len(endpoints); i++ {
		endpoint := config.Endpoint{
			Name:   fmt.Sprintf("name%d", i),
			Url:    fmt.Sprintf("url%d", i),
			Weight: ^uint8(0) - uint8(i*2),
		}
		totalWeights += uint32(endpoint.Weight)
		endpoints[i] = endpoint
	}
	endpointGroup := &config.EndpointGroup{
		Endpoints: endpoints,
	}

	ctx := context.Background()
	provider, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)
	allEndpoints := provider.GetAllEndpoints()
	require.Equal(len(endpoints), len(allEndpoints))
	require.Equal([]string{"name0", "name1", "name2", "name3", "name4"}, getActiveEndpoints(ctx, provider))

	pickStats := make(map[string]int)
	for i := 0; i < largeNumPicks; i++ {
		pick, err := provider.GetEndpoint(ctx)
		require.NoError(err)
		pickStats[pick.Config.Url] += 1
	}
	for _, endpoint := range endpoints {
		expectedPickProbability := float64(endpoint.Weight) / float64(totalWeights)
		actualPickedProbability := float64(pickStats[endpoint.Url]) * 1.0 / float64(largeNumPicks)
		require.True(
			math.Abs(expectedPickProbability-actualPickedProbability) < 0.01,
			"endpoint %v: expectedPickProbability:%3f, actualPickedProbability:%3f",
			endpoint,
			expectedPickProbability,
			actualPickedProbability,
		)
	}
}

func TestEndpointProvider_WithFailover(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "bar",
				Weight: 1,
			},
		},
	}

	ep, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)

	allEndpoints := ep.GetAllEndpoints()
	require.Equal(2, len(allEndpoints))
	require.Equal("foo", allEndpoints[0].Name)
	require.Equal("bar", allEndpoints[1].Name)

	ctx := context.Background()
	require.Equal([]string{"foo"}, getActiveEndpoints(ctx, ep))
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := ep.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("foo", endpoint.Name)
	}

	ctx, err = ep.WithFailoverContext(ctx)
	require.Equal([]string{"bar"}, getActiveEndpoints(ctx, ep))
	require.NoError(err)
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := ep.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("bar", endpoint.Name)
	}
}

func TestEndpointProvider_UseFailover(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "bar",
				Weight: 1,
			},
		},
		UseFailover: true,
	}

	ep, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)

	allEndpoints := ep.GetAllEndpoints()
	require.Equal(2, len(allEndpoints))
	require.Equal("bar", allEndpoints[0].Name)
	require.Equal("foo", allEndpoints[1].Name)

	ctx := context.Background()
	require.Equal([]string{"bar"}, getActiveEndpoints(ctx, ep))
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := ep.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("bar", endpoint.Name)
	}

	ctx, err = ep.WithFailoverContext(ctx)
	require.NoError(err)
	require.Equal([]string{"foo"}, getActiveEndpoints(ctx, ep))
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := ep.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("foo", endpoint.Name)
	}
}

func TestEndpointProvider_EmptyEndpoints(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)
	ctx := context.Background()

	ep, err := newEndpointProvider(logger, cfg, &config.EndpointGroup{}, "master")
	require.NoError(err)
	require.Equal([]string{}, getActiveEndpoints(ctx, ep))
	_, err = ep.GetEndpoint(ctx)
	require.Error(err)

	ep, err = newEndpointProvider(logger, cfg, &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		UseFailover: true,
	}, "master")
	require.NoError(err)
	require.Equal([]string{}, getActiveEndpoints(ctx, ep))
	_, err = ep.GetEndpoint(ctx)
	require.Error(err)

	ep, err = newEndpointProvider(logger, cfg, &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
	}, "master")
	require.NoError(err)
	_, err = ep.GetEndpoint(ctx)
	require.NoError(err)
	_, err = ep.WithFailoverContext(ctx)
	require.Equal([]string{"foo"}, getActiveEndpoints(ctx, ep))
	require.Error(err)
	require.True(xerrors.Is(err, ErrFailoverUnavailable))
}

func TestEndpointProvider_StickySessionCookieHash(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Url:    "https://www.coinbase.com",
				Weight: 1,
			},
		},
		StickySession: config.StickySessionConfig{
			CookieHash: "sync_session",
		},
	}
	provider, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)
	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	client := endpoint.Client
	require.NotNil(client.Jar)
	jar, ok := client.Jar.(*stickySessionJar)
	require.True(ok)
	require.Equal("sync_session", jar.cookieKey)
	require.Equal("chainstorage-ethereum-mainnet-local", jar.cookieValue)
}

func TestEndpointProvider_StickySessionHeaderHash(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Url:    "https://www.coinbase.com",
				Weight: 1,
			},
		},
		StickySession: config.StickySessionConfig{
			HeaderHash: "testHeader",
		},
	}
	provider, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)
	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	client := endpoint.Client
	require.NoError(err)
	require.Nil(client.Jar)
}

func TestEndpointProvider_StickySessionCookiePassive(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Url:    "https://www.coinbase.com",
				Weight: 1,
			},
		},
		StickySession: config.StickySessionConfig{
			CookiePassive: true,
		},
	}
	provider, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)
	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	client := endpoint.Client
	require.NoError(err)
	require.NotNil(client.Jar)
	_, ok := client.Jar.(*cookiejar.Jar)
	require.True(ok)
}

func TestEndpointProvider_StickySessionPrimary(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Url:    "https://www.coinbase.com",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Url:    "https://cloud.coinbase.com",
				Weight: 4,
			},
		},
		StickySession: config.StickySessionConfig{
			CookiePassive: true,
		},
		StickySessionFailover: config.StickySessionConfig{
			CookieHash: "sync_session",
		},
		UseFailover: false,
	}
	provider, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)

	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	require.Equal(config.Endpoint{
		Url:    "https://www.coinbase.com",
		Weight: 1,
	}, *endpoint.Config)

	client := endpoint.Client
	require.NoError(err)
	require.NotNil(client.Jar)
	_, ok := client.Jar.(*cookiejar.Jar)
	require.True(ok)
}

func TestEndpointProvider_StickySessionFailover(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Url:    "https://www.coinbase.com",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Url:    "https://cloud.coinbase.com",
				Weight: 4,
			},
		},
		StickySession: config.StickySessionConfig{
			CookiePassive: true,
		},
		StickySessionFailover: config.StickySessionConfig{
			CookieHash: "sync_session",
		},
		UseFailover: true,
	}
	provider, err := newEndpointProvider(logger, cfg, endpointGroup, "master")
	require.NoError(err)

	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	require.Equal(config.Endpoint{
		Url:    "https://cloud.coinbase.com",
		Weight: 4,
	}, *endpoint.Config)

	client := endpoint.Client
	require.NoError(err)
	require.NotNil(client.Jar)
	jar, ok := client.Jar.(*stickySessionJar)
	require.True(ok)
	require.Equal("https://cloud.coinbase.com", jar.url)
	require.Equal("sync_session", jar.cookieKey)
	require.Equal("chainstorage-ethereum-mainnet-local", jar.cookieValue)
}

func getActiveEndpoints(ctx context.Context, ep EndpointProvider) []string {
	endpoints := ep.GetActiveEndpoints(ctx)
	res := make([]string, len(endpoints))
	for i, endpoint := range endpoints {
		res[i] = endpoint.Name
	}

	sort.Strings(res)
	return res
}
