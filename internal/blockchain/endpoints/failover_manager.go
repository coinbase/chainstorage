package endpoints

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

type (
	FailoverManager interface {
		WithFailoverContext(ctx context.Context) (context.Context, error)
	}

	FailoverManagerParams struct {
		fx.In
		Master EndpointProvider `name:"master"`
		Slave  EndpointProvider `name:"slave"`
	}

	failoverManager struct {
		master EndpointProvider
		slave  EndpointProvider
	}
)

func NewFailoverManager(params FailoverManagerParams) FailoverManager {
	return &failoverManager{
		master: params.Master,
		slave:  params.Slave,
	}
}

func (m *failoverManager) WithFailoverContext(ctx context.Context) (context.Context, error) {
	ctx, err := m.master.WithFailoverContext(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to failover the master endpoint group: %w", err)
	}

	ctx, err = m.slave.WithFailoverContext(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to failover the slave endpoint group: %w", err)
	}

	return ctx, nil
}
