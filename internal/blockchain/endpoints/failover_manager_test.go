package endpoints

import (
	"context"
	"testing"

	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestFailoverManager(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	masterEndpointGroup := &config.EndpointGroup{
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

	master, err := newEndpointProvider(logger, cfg, masterEndpointGroup, masterEndpointGroupName)
	require.NoError(err)

	slaveEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "qux",
				Weight: 1,
			},
		},
	}

	slave, err := newEndpointProvider(logger, cfg, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master: master,
		Slave:  slave,
	})

	ctx := context.Background()
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := master.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("foo", endpoint.Name)

		endpoint, err = slave.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("baz", endpoint.Name)
	}

	ctx, err = mgr.WithFailoverContext(ctx)
	require.NoError(err)
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := master.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("bar", endpoint.Name)

		endpoint, err = slave.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("qux", endpoint.Name)
	}
}

func TestFailoverManager_RosettaEndpointProvider(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	masterEndpointGroup := &config.EndpointGroup{
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

	masterEndpointProvider, err := newEndpointProvider(logger, cfg, masterEndpointGroup, masterEndpointGroupName)
	require.NoError(err)

	master, err := newRosettaEndpointProvider(masterEndpointProvider)
	require.NoError(err)

	slaveEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "qux",
				Weight: 1,
			},
		},
	}

	slaveEndpointProvider, err := newEndpointProvider(logger, cfg, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	slave, err := newRosettaEndpointProvider(slaveEndpointProvider)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master: masterEndpointProvider,
		Slave:  slaveEndpointProvider,
	})

	ctx := context.Background()
	for i := 0; i < smallNumPicks; i++ {
		endpoint, _, err := master.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("foo", endpoint.Name)

		endpoint, _, err = slave.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("baz", endpoint.Name)
	}

	ctx, err = mgr.WithFailoverContext(ctx)
	require.NoError(err)
	for i := 0; i < smallNumPicks; i++ {
		endpoint, _, err := master.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("bar", endpoint.Name)

		endpoint, _, err = slave.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("qux", endpoint.Name)
	}
}

func TestFailoverManager_MasterUnavailable(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	masterEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
	}

	master, err := newEndpointProvider(logger, cfg, masterEndpointGroup, masterEndpointGroupName)
	require.NoError(err)

	slaveEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "qux",
				Weight: 1,
			},
		},
	}

	slave, err := newEndpointProvider(logger, cfg, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master: master,
		Slave:  slave,
	})

	ctx := context.Background()
	_, err = mgr.WithFailoverContext(ctx)
	require.Error(err)
	require.True(xerrors.Is(err, ErrFailoverUnavailable))
}

func TestFailoverManager_SlaveUnavailable(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	cfg, err := config.New()
	require.NoError(err)

	masterEndpointGroup := &config.EndpointGroup{
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

	master, err := newEndpointProvider(logger, cfg, masterEndpointGroup, masterEndpointGroupName)
	require.NoError(err)

	slaveEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
	}

	slave, err := newEndpointProvider(logger, cfg, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master: master,
		Slave:  slave,
	})

	ctx := context.Background()
	_, err = mgr.WithFailoverContext(ctx)
	require.Error(err)
	require.True(xerrors.Is(err, ErrFailoverUnavailable))
}
