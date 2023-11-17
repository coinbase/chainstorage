package endpoints

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestRosettaEndpointProvider(t *testing.T) {
	require := testutil.Require(t)

	numSlaves := 5
	cfg, err := config.New()
	require.NoError(err)
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

	endpointProviders := struct {
		fx.In
		Master    RosettaEndpointProvider `name:"master"`
		Slave     RosettaEndpointProvider `name:"slave"`
		Validator RosettaEndpointProvider `name:"validator"`
		Consensus RosettaEndpointProvider `name:"consensus"`
	}{}
	app := fxtest.New(
		t,
		fx.Provide(func() *config.Config { return cfg }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() *zap.Logger { return zaptest.NewLogger(t) }),
		Module,
		fx.Populate(&endpointProviders),
	)
	app.RequireStart()
	defer app.RequireStop()

	require.NotNil(endpointProviders.Master)
	require.NotNil(endpointProviders.Slave)
	require.NotNil(endpointProviders.Validator)
	require.NotNil(endpointProviders.Consensus)

	masterEndpoint, masterApiClient, err := endpointProviders.Master.GetEndpoint(context.TODO())
	require.NoError(err)
	require.NotNil(masterEndpoint)
	require.NotNil(masterApiClient)
	require.Equal("baseUrl_0", masterEndpoint.Config.Url)
	require.Equal("baseUrl_0", masterApiClient.GetConfig().BasePath)
	require.Equal(userAgent, masterApiClient.GetConfig().UserAgent)

	basePaths := make([]string, 0, numSlaves)
	for i := 0; i < numSlaves; i++ {
		slaveEndpoint, slaveApiClient, err := endpointProviders.Slave.GetEndpoint(context.TODO())
		require.NoError(err)
		require.NotNil(slaveEndpoint)
		basePaths = append(basePaths, slaveApiClient.GetConfig().BasePath)
	}
	sort.Slice(basePaths, func(i, j int) bool {
		return strings.Compare(basePaths[i], basePaths[j]) < 0
	})
	for i := 0; i < numSlaves; i++ {
		require.Equal(fmt.Sprintf("baseUrl_%d", i), basePaths[i])
	}

	validatorEndpoint, validatorApiClient, err := endpointProviders.Validator.GetEndpoint(context.TODO())
	require.NoError(err)
	require.NotNil(validatorEndpoint)
	require.NotNil(validatorApiClient)
	require.Equal("baseUrl_0", validatorEndpoint.Config.Url)
	require.Equal("baseUrl_0", validatorApiClient.GetConfig().BasePath)
	require.Equal(userAgent, validatorApiClient.GetConfig().UserAgent)

	consensusEndpoint, consensusApiClient, err := endpointProviders.Consensus.GetEndpoint(context.TODO())
	require.NoError(err)
	require.NotNil(consensusEndpoint)
	require.NotNil(consensusApiClient)
	require.Equal("baseUrl_0", consensusEndpoint.Config.Url)
	require.Equal("baseUrl_0", consensusApiClient.GetConfig().BasePath)
	require.Equal(userAgent, consensusApiClient.GetConfig().UserAgent)
}

func TestRosettaEndpointProvider_ConsensusDefault(t *testing.T) {
	require := testutil.Require(t)

	numSlaves := 5
	cfg, err := config.New()
	require.NoError(err)
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

	endpointProviders := struct {
		fx.In
		Master    RosettaEndpointProvider `name:"master"`
		Slave     RosettaEndpointProvider `name:"slave"`
		Consensus RosettaEndpointProvider `name:"consensus"`
	}{}
	app := fxtest.New(
		t,
		fx.Provide(func() *config.Config { return cfg }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() *zap.Logger { return zaptest.NewLogger(t) }),
		Module,
		fx.Populate(&endpointProviders),
	)
	app.RequireStart()
	defer app.RequireStop()

	require.NotNil(endpointProviders.Master)
	require.NotNil(endpointProviders.Slave)
	require.NotNil(endpointProviders.Consensus)

	masterEndpoint, masterApiClient, err := endpointProviders.Master.GetEndpoint(context.TODO())
	require.NoError(err)
	require.NotNil(masterEndpoint)
	require.NotNil(masterApiClient)
	require.Equal("baseUrl_0", masterEndpoint.Config.Url)
	require.Equal("baseUrl_0", masterApiClient.GetConfig().BasePath)
	require.Equal(userAgent, masterApiClient.GetConfig().UserAgent)

	slaveBasePaths := make([]string, 0, numSlaves)
	for i := 0; i < numSlaves; i++ {
		slaveEndpoint, slaveApiClient, err := endpointProviders.Slave.GetEndpoint(context.TODO())
		require.NoError(err)
		require.NotNil(slaveEndpoint)
		slaveBasePaths = append(slaveBasePaths, slaveApiClient.GetConfig().BasePath)
	}
	sort.Slice(slaveBasePaths, func(i, j int) bool {
		return strings.Compare(slaveBasePaths[i], slaveBasePaths[j]) < 0
	})
	for i := 0; i < numSlaves; i++ {
		require.Equal(fmt.Sprintf("baseUrl_%d", i), slaveBasePaths[i])
	}

	consensusBasePaths := make([]string, 0, numSlaves)
	for i := 0; i < numSlaves; i++ {
		consensusEndpoint, consensusClient, err := endpointProviders.Consensus.GetEndpoint(context.TODO())
		require.NoError(err)
		require.NotNil(consensusEndpoint)
		consensusBasePaths = append(consensusBasePaths, consensusClient.GetConfig().BasePath)
	}
	sort.Slice(consensusBasePaths, func(i, j int) bool {
		return strings.Compare(consensusBasePaths[i], consensusBasePaths[j]) < 0
	})
	for i := 0; i < numSlaves; i++ {
		require.Equal(fmt.Sprintf("baseUrl_%d", i), consensusBasePaths[i])
	}
}

func getTestEndpoints(n int) []config.Endpoint {
	endpoints := make([]config.Endpoint, 0, n)
	for i := 0; i < n; i++ {
		endpoints = append(endpoints, config.Endpoint{
			Url:    fmt.Sprintf("baseUrl_%d", i),
			Name:   fmt.Sprintf("name_%d", i),
			Weight: uint8(1),
		})
	}
	return endpoints
}
