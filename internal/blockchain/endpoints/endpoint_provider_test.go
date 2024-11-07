package endpoints

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http/cookiejar"
	"sort"
	"testing"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

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
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
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

	ep, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
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

	ep, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
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

	ep, err := newEndpointProvider(logger, cfg, tally.NoopScope, &config.EndpointGroup{}, "master")
	require.NoError(err)
	require.Equal([]string{}, getActiveEndpoints(ctx, ep))
	_, err = ep.GetEndpoint(ctx)
	require.Error(err)

	ep, err = newEndpointProvider(logger, cfg, tally.NoopScope, &config.EndpointGroup{
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

	ep, err = newEndpointProvider(logger, cfg, tally.NoopScope, &config.EndpointGroup{
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
	require.True(errors.Is(err, ErrFailoverUnavailable))
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
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookieHash: "sync_session",
			},
		},
	}
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
	require.NoError(err)
	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	client := endpoint.Client
	require.NotNil(client.Jar)
	jar, ok := client.Jar.(*stickySessionJar)
	require.True(ok)
	require.Equal("sync_session", jar.cookieKey)
	//require.Equal("411cde94b897c4355f8abcc08c6c3b3aa438ac7269e251f37baa861a49f83678", jar.cookieValue)
	require.Equal("59007272a2c3194048d7336914efa19a4338b8fdc202942c58056f5d977901ac", jar.cookieValue)
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
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				HeaderHash: "testHeader",
			},
		},
	}
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
	require.NoError(err)
	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	client := endpoint.Client
	require.NoError(err)
	require.Nil(client.Jar)
}

func TestEndpointProvider_Timeout(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	cfg.Chain.Client.HttpTimeout = 10 * time.Second
	require.NoError(err)

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Url:    "https://www.coinbase.com",
				Weight: 1,
			},
		},
	}
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
	require.NoError(err)
	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	client := endpoint.Client
	require.Equal(10.0, client.Timeout.Seconds())
}

func TestEndpointProvider_DefaultTimeout(t *testing.T) {
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
	}
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
	require.NoError(err)
	endpoint, err := provider.GetEndpoint(context.Background())
	require.NoError(err)
	client := endpoint.Client
	require.Equal(120.0, client.Timeout.Seconds())
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
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookiePassive: true,
			},
		},
	}
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
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
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookiePassive: true,
			},
			Headers: map[string]string{
				"k1": "v1",
			},
		},
		EndpointConfigFailover: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookieHash: "sync_session",
			},
		},
		UseFailover: false,
	}
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
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
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookiePassive: true,
			},
			Headers: map[string]string{
				"k1": "v1",
				"k2": "v2",
			},
		},
		EndpointConfigFailover: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookieHash: "sync_session",
			},
			Headers: map[string]string{
				"k3": "v3",
				"k4": "v4",
			},
		},
		UseFailover: true,
	}
	provider, err := newEndpointProvider(logger, cfg, tally.NoopScope, endpointGroup, "master")
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
	//require.Equal("411cde94b897c4355f8abcc08c6c3b3aa438ac7269e251f37baa861a49f83678", jar.cookieValue)
	require.Equal("59007272a2c3194048d7336914efa19a4338b8fdc202942c58056f5d977901ac", jar.cookieValue)
}

func TestEndpointProviderResult(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	ctx := context.Background()

	numSlaves := 2
	cfg.Chain.Client = config.ClientConfig{
		Master: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: getTestEndpoints(1),
			},
		},
		Slave: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: getTestEndpoints(numSlaves),
			},
		},
		Validator: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: getTestEndpoints(1),
			},
		},
		Consensus: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: getTestEndpoints(1),
			},
		},
	}

	endpointProviderResult := struct {
		fx.In
		Master    EndpointProvider `name:"master"`
		Slave     EndpointProvider `name:"slave"`
		Validator EndpointProvider `name:"validator"`
		Consensus EndpointProvider `name:"consensus"`
	}{}
	app := fxtest.New(
		t,
		fx.Provide(func() *config.Config { return cfg }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() *zap.Logger { return zaptest.NewLogger(t) }),
		Module,
		fx.Populate(&endpointProviderResult),
	)
	app.RequireStart()
	defer app.RequireStop()

	require.NotNil(endpointProviderResult.Master)
	require.NotNil(endpointProviderResult.Slave)
	require.NotNil(endpointProviderResult.Validator)
	require.NotNil(endpointProviderResult.Consensus)

	masterEndpoints := endpointProviderResult.Master.GetAllEndpoints()
	require.Equal(1, len(masterEndpoints))
	require.Equal([]string{"name_0"}, getActiveEndpoints(ctx, endpointProviderResult.Master))

	slaveEndpoints := endpointProviderResult.Slave.GetAllEndpoints()
	require.Equal(2, len(slaveEndpoints))
	require.Equal([]string{"name_0", "name_1"}, getActiveEndpoints(ctx, endpointProviderResult.Slave))

	validatorEndpoints := endpointProviderResult.Validator.GetAllEndpoints()
	require.Equal(1, len(validatorEndpoints))
	require.Equal([]string{"name_0"}, getActiveEndpoints(ctx, endpointProviderResult.Validator))

	consensusEndpoints := endpointProviderResult.Consensus.GetAllEndpoints()
	require.Equal(1, len(consensusEndpoints))
	require.Equal([]string{"name_0"}, getActiveEndpoints(ctx, endpointProviderResult.Consensus))
}

func TestEndpointProviderResult_ConsensusDefault(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	ctx := context.Background()

	numSlaves := 2
	cfg.Chain.Client = config.ClientConfig{
		Master: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: getTestEndpoints(1),
			},
		},
		Slave: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: getTestEndpoints(numSlaves),
			},
		},
	}

	endpointProviderResult := struct {
		fx.In
		Master    EndpointProvider `name:"master"`
		Slave     EndpointProvider `name:"slave"`
		Validator EndpointProvider `name:"validator"`
		Consensus EndpointProvider `name:"consensus"`
	}{}
	app := fxtest.New(
		t,
		fx.Provide(func() *config.Config { return cfg }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() *zap.Logger { return zaptest.NewLogger(t) }),
		Module,
		fx.Populate(&endpointProviderResult),
	)
	app.RequireStart()
	defer app.RequireStop()

	require.NotNil(endpointProviderResult.Master)
	require.NotNil(endpointProviderResult.Slave)
	require.NotNil(endpointProviderResult.Validator)
	require.NotNil(endpointProviderResult.Consensus)

	masterEndpoints := endpointProviderResult.Master.GetAllEndpoints()
	require.Equal(1, len(masterEndpoints))
	require.Equal([]string{"name_0"}, getActiveEndpoints(ctx, endpointProviderResult.Master))

	slaveEndpoints := endpointProviderResult.Slave.GetAllEndpoints()
	require.Equal(2, len(slaveEndpoints))
	require.Equal([]string{"name_0", "name_1"}, getActiveEndpoints(ctx, endpointProviderResult.Slave))

	validatorEndpoints := endpointProviderResult.Validator.GetAllEndpoints()
	require.Equal(0, len(validatorEndpoints))

	consensusEndpoints := endpointProviderResult.Consensus.GetAllEndpoints()
	require.Equal(2, len(consensusEndpoints))
	require.Equal([]string{"name_0", "name_1"}, getActiveEndpoints(ctx, endpointProviderResult.Consensus))
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
