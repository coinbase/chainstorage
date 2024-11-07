package endpoints

import (
	"context"
	"errors"
	"testing"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

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
				Name:   "master",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "masterFailover",
				Weight: 1,
			},
		},
	}

	master, err := newEndpointProvider(logger, cfg, tally.NoopScope, masterEndpointGroup, masterEndpointGroupName)
	require.NoError(err)

	slaveEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "slave",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "slaveFailover",
				Weight: 1,
			},
		},
	}

	slave, err := newEndpointProvider(logger, cfg, tally.NoopScope, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	validatorEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "validator",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "validatorFailover",
				Weight: 1,
			},
		},
	}

	validator, err := newEndpointProvider(logger, cfg, tally.NoopScope, validatorEndpointGroup, validatorEndpointGroupName)
	require.NoError(err)

	consensusEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "consensus",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "consensusFailover",
				Weight: 1,
			},
		},
	}

	consensus, err := newEndpointProvider(logger, cfg, tally.NoopScope, consensusEndpointGroup, consensusEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master:    master,
		Slave:     slave,
		Validator: validator,
		Consensus: consensus,
	})

	ctx := context.Background()
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := master.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("master", endpoint.Name)

		endpoint, err = slave.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("slave", endpoint.Name)

		endpoint, err = validator.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("validator", endpoint.Name)

		endpoint, err = consensus.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("consensus", endpoint.Name)
	}

	tests := []struct {
		name     string
		clusters ClusterIdentity
		result   []string
	}{
		{
			name:     "Master",
			clusters: MasterCluster,
			result:   []string{"masterFailover", "slave", "validator", "consensus"},
		},
		{
			name:     "Slave",
			clusters: SlaveCluster,
			result:   []string{"master", "slaveFailover", "validator", "consensus"},
		},
		{
			name:     "Validator",
			clusters: ValidatorCluster,
			result:   []string{"master", "slave", "validatorFailover", "consensus"},
		},
		{
			name:     "Consensus",
			clusters: ConsensusCluster,
			result:   []string{"master", "slave", "validator", "consensusFailover"},
		},
		{
			name:     "Master|Slave",
			clusters: MasterCluster | SlaveCluster,
			result:   []string{"masterFailover", "slaveFailover", "validator", "consensus"},
		},
		{
			name:     "Master|Validator",
			clusters: MasterCluster | ValidatorCluster,
			result:   []string{"masterFailover", "slave", "validatorFailover", "consensus"},
		},
		{
			name:     "Master|Consensus",
			clusters: MasterCluster | ConsensusCluster,
			result:   []string{"masterFailover", "slave", "validator", "consensusFailover"},
		},
		{
			name:     "Slave|Validator",
			clusters: SlaveCluster | ValidatorCluster,
			result:   []string{"master", "slaveFailover", "validatorFailover", "consensus"},
		},
		{
			name:     "Slave|Consensus",
			clusters: SlaveCluster | ConsensusCluster,
			result:   []string{"master", "slaveFailover", "validator", "consensusFailover"},
		},
		{
			name:     "Master|Slave|Validator|Consensus",
			clusters: MasterCluster | SlaveCluster | ValidatorCluster | ConsensusCluster,
			result:   []string{"masterFailover", "slaveFailover", "validatorFailover", "consensusFailover"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, err = mgr.WithFailoverContext(ctx, test.clusters)
			require.NoError(err)
			for i := 0; i < smallNumPicks; i++ {
				endpoint, err := master.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[0], endpoint.Name)

				endpoint, err = slave.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[1], endpoint.Name)

				endpoint, err = validator.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[2], endpoint.Name)

				endpoint, err = consensus.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[3], endpoint.Name)
			}
		})
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
				Name:   "master",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "masterFailover",
				Weight: 1,
			},
		},
	}

	masterEndpointProvider, err := newEndpointProvider(logger, cfg, tally.NoopScope, masterEndpointGroup, masterEndpointGroupName)
	require.NoError(err)

	master, err := newRosettaEndpointProvider(masterEndpointProvider)
	require.NoError(err)

	slaveEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "slave",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "slaveFailover",
				Weight: 1,
			},
		},
	}

	slaveEndpointProvider, err := newEndpointProvider(logger, cfg, tally.NoopScope, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	slave, err := newRosettaEndpointProvider(slaveEndpointProvider)
	require.NoError(err)

	validatorEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "validator",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "validatorFailover",
				Weight: 1,
			},
		},
	}

	validatorEndpointProvider, err := newEndpointProvider(logger, cfg, tally.NoopScope, validatorEndpointGroup, validatorEndpointGroupName)
	require.NoError(err)

	validator, err := newRosettaEndpointProvider(validatorEndpointProvider)
	require.NoError(err)

	consensusEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "consensus",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "consensusFailover",
				Weight: 1,
			},
		},
	}

	consensusEndpointProvider, err := newEndpointProvider(logger, cfg, tally.NoopScope, consensusEndpointGroup, consensusEndpointGroupName)
	require.NoError(err)

	consensus, err := newRosettaEndpointProvider(consensusEndpointProvider)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master:    masterEndpointProvider,
		Slave:     slaveEndpointProvider,
		Validator: validatorEndpointProvider,
		Consensus: consensusEndpointProvider,
	})

	ctx := context.Background()
	for i := 0; i < smallNumPicks; i++ {
		endpoint, _, err := master.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("master", endpoint.Name)

		endpoint, _, err = slave.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("slave", endpoint.Name)

		endpoint, _, err = validator.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("validator", endpoint.Name)

		endpoint, _, err = consensus.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("consensus", endpoint.Name)
	}

	tests := []struct {
		name     string
		clusters ClusterIdentity
		result   []string
	}{
		{
			name:     "Master",
			clusters: MasterCluster,
			result:   []string{"masterFailover", "slave", "validator", "consensus"},
		},
		{
			name:     "Slave",
			clusters: SlaveCluster,
			result:   []string{"master", "slaveFailover", "validator", "consensus"},
		},
		{
			name:     "Validator",
			clusters: ValidatorCluster,
			result:   []string{"master", "slave", "validatorFailover", "consensus"},
		},
		{
			name:     "Master|Slave",
			clusters: MasterCluster | SlaveCluster,
			result:   []string{"masterFailover", "slaveFailover", "validator", "consensus"},
		},
		{
			name:     "Master|Validator",
			clusters: MasterCluster | ValidatorCluster,
			result:   []string{"masterFailover", "slave", "validatorFailover", "consensus"},
		},
		{
			name:     "Master|Consensus",
			clusters: MasterCluster | ConsensusCluster,
			result:   []string{"masterFailover", "slave", "validator", "consensusFailover"},
		},
		{
			name:     "Slave|Validator",
			clusters: SlaveCluster | ValidatorCluster,
			result:   []string{"master", "slaveFailover", "validatorFailover", "consensus"},
		},
		{
			name:     "Slave|Consensus",
			clusters: SlaveCluster | ConsensusCluster,
			result:   []string{"master", "slaveFailover", "validator", "consensusFailover"},
		},
		{
			name:     "Master|Slave|Validator|Consensus",
			clusters: MasterCluster | SlaveCluster | ValidatorCluster | ConsensusCluster,
			result:   []string{"masterFailover", "slaveFailover", "validatorFailover", "consensusFailover"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, err = mgr.WithFailoverContext(ctx, test.clusters)
			require.NoError(err)
			for i := 0; i < smallNumPicks; i++ {
				endpoint, _, err := master.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[0], endpoint.Name)

				endpoint, _, err = slave.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[1], endpoint.Name)

				endpoint, _, err = validator.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[2], endpoint.Name)

				endpoint, _, err = consensus.GetEndpoint(ctx)
				require.NoError(err)
				require.Equal(test.result[3], endpoint.Name)
			}
		})
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

	master, err := newEndpointProvider(logger, cfg, tally.NoopScope, masterEndpointGroup, masterEndpointGroupName)
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

	slave, err := newEndpointProvider(logger, cfg, tally.NoopScope, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master: master,
		Slave:  slave,
	})

	ctx := context.Background()
	_, err = mgr.WithFailoverContext(ctx, MasterSlaveClusters)
	require.Error(err)
	require.True(errors.Is(err, ErrFailoverUnavailable))
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

	master, err := newEndpointProvider(logger, cfg, tally.NoopScope, masterEndpointGroup, masterEndpointGroupName)
	require.NoError(err)

	slaveEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
	}

	slave, err := newEndpointProvider(logger, cfg, tally.NoopScope, slaveEndpointGroup, slaveEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Master: master,
		Slave:  slave,
	})

	ctx := context.Background()
	_, err = mgr.WithFailoverContext(ctx, MasterSlaveClusters)
	require.Error(err)
	require.True(errors.Is(err, ErrFailoverUnavailable))
}
