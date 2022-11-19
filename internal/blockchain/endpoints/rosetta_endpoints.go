package endpoints

import (
	"context"

	rc "github.com/coinbase/rosetta-sdk-go/client"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/consts"
)

const (
	userAgent = consts.ServiceName
)

type (
	RosettaEndpointProvider interface {
		GetEndpoint(ctx context.Context) (*Endpoint, *rc.APIClient, error)
	}

	RosettaEndpointsParams struct {
		fx.In
		Master    EndpointProvider `name:"master"`
		Slave     EndpointProvider `name:"slave"`
		Validator EndpointProvider `name:"validator"`
	}

	RosettaEndpointsResult struct {
		fx.Out
		Master    RosettaEndpointProvider `name:"master"`
		Slave     RosettaEndpointProvider `name:"slave"`
		Validator RosettaEndpointProvider `name:"validator"`
	}

	rosettaEndpointProvider struct {
		endpointProvider EndpointProvider
		clients          map[*Endpoint]*rc.APIClient
	}
)

func NewRosettaEndpointProvider(params RosettaEndpointsParams) (RosettaEndpointsResult, error) {
	master, err := newRosettaEndpointProvider(params.Master)
	if err != nil {
		return RosettaEndpointsResult{}, xerrors.Errorf("failed to create master endpoint provider: %w", err)
	}

	slave, err := newRosettaEndpointProvider(params.Slave)
	if err != nil {
		return RosettaEndpointsResult{}, xerrors.Errorf("failed to create slave endpoint provider: %w", err)
	}

	validator, err := newRosettaEndpointProvider(params.Validator)
	if err != nil {
		return RosettaEndpointsResult{}, xerrors.Errorf("failed to create validator endpoint provider: %w", err)
	}

	return RosettaEndpointsResult{
		Master:    master,
		Slave:     slave,
		Validator: validator,
	}, nil
}

func newRosettaEndpointProvider(endpointProvider EndpointProvider) (RosettaEndpointProvider, error) {
	clients := make(map[*Endpoint]*rc.APIClient)
	for _, endpoint := range endpointProvider.GetAllEndpoints() {
		rosettaConfig := rc.NewConfiguration(endpoint.Config.Url, userAgent, endpoint.Client)
		rosettaApiClient := rc.NewAPIClient(rosettaConfig)
		clients[endpoint] = rosettaApiClient
	}
	return &rosettaEndpointProvider{
		endpointProvider: endpointProvider,
		clients:          clients,
	}, nil
}

func (p *rosettaEndpointProvider) GetEndpoint(ctx context.Context) (*Endpoint, *rc.APIClient, error) {
	endpoint, err := p.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to get rosetta endpoint: %w", err)
	}
	return endpoint, p.clients[endpoint], nil
}
