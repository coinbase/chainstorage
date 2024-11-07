package endpoints

import (
	"context"
	"fmt"

	"go.uber.org/fx"
)

type ClusterIdentity int

const (
	MasterCluster ClusterIdentity = 1 << iota
	SlaveCluster
	ValidatorCluster
	ConsensusCluster
)

const (
	MasterSlaveClusters = MasterCluster | SlaveCluster
)

type (
	FailoverManager interface {
		WithFailoverContext(ctx context.Context, cluster ClusterIdentity) (context.Context, error)
	}

	FailoverManagerParams struct {
		fx.In
		Master    EndpointProvider `name:"master"`
		Slave     EndpointProvider `name:"slave"`
		Validator EndpointProvider `name:"validator"`
		Consensus EndpointProvider `name:"consensus"`
	}

	failoverManager struct {
		master    EndpointProvider
		slave     EndpointProvider
		validator EndpointProvider
		consensus EndpointProvider
	}
)

func NewFailoverManager(params FailoverManagerParams) FailoverManager {
	return &failoverManager{
		master:    params.Master,
		slave:     params.Slave,
		validator: params.Validator,
		consensus: params.Consensus,
	}
}

func (m *failoverManager) WithFailoverContext(ctx context.Context, cluster ClusterIdentity) (context.Context, error) {
	var err error
	if cluster&MasterCluster != 0 {
		ctx, err = m.master.WithFailoverContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to failover the master endpoint group: %w", err)
		}
	}
	if cluster&SlaveCluster != 0 {
		ctx, err = m.slave.WithFailoverContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to failover the slave endpoint group: %w", err)
		}
	}
	if cluster&ValidatorCluster != 0 {
		ctx, err = m.validator.WithFailoverContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to failover the validator endpoint group: %w", err)
		}
	}
	if cluster&ConsensusCluster != 0 {
		ctx, err = m.consensus.WithFailoverContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to failover the consensus endpoint group: %w", err)
		}
	}

	return ctx, nil
}
