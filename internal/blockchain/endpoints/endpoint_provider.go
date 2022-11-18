package endpoints

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/consts"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/picker"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	// EndpointProvider encapsulates the client-side routing logic.
	// By default, the request is routed to one of the primary endpoints based on their weights.
	// When the failover context is seen, the request is instead routed to one of the secondary endpoints.
	// In most cases, the primary endpoints are referring to the `EndpointGroup.Endpoints` config,
	// while the secondary endpoints are referring to the `EndpointGroup.EndpointsFailover` config.
	// But when `EndpointGroup.UseFailover` is turned on, the two list are swapped.
	EndpointProvider interface {
		GetEndpoint(ctx context.Context) (*Endpoint, error)
		GetAllEndpoints() []*Endpoint
		GetActiveEndpoints(ctx context.Context) []*Endpoint
		WithFailoverContext(ctx context.Context) (context.Context, error)
		HasFailoverContext(ctx context.Context) bool
	}

	Endpoint struct {
		Name   string
		Config *config.Endpoint
		Client *http.Client
	}

	EndpointProviderParams struct {
		fx.In
		Config *config.Config
		Logger *zap.Logger
	}

	EndpointProviderResult struct {
		fx.Out
		Master    EndpointProvider `name:"master"`
		Slave     EndpointProvider `name:"slave"`
		Validator EndpointProvider `name:"validator"`
	}
)

type (
	endpointProvider struct {
		name               string
		endpointGroup      *config.EndpointGroup
		logger             *zap.Logger
		primaryPicker      picker.Picker
		primaryEndpoints   []*Endpoint
		secondaryPicker    picker.Picker
		secondaryEndpoints []*Endpoint
	}

	contextKey string
)

const (
	masterEndpointGroupName    = "master"
	slaveEndpointGroupName     = "slave"
	validatorEndpointGroupName = "validator"
	contextKeyFailover         = "failover:"
)

var (
	ErrFailoverUnavailable = xerrors.New("no endpoint is available for failover")
)

func NewEndpointProvider(params EndpointProviderParams) (EndpointProviderResult, error) {
	logger := log.WithPackage(params.Logger)

	master, err := newEndpointProvider(logger, params.Config, &params.Config.Chain.Client.Master.EndpointGroup, masterEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create master endpoint provider: %w", err)
	}

	slave, err := newEndpointProvider(logger, params.Config, &params.Config.Chain.Client.Slave.EndpointGroup, slaveEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create slave endpoint provider: %w", err)
	}

	validator, err := newEndpointProvider(logger, params.Config, &params.Config.Chain.Client.Validator.EndpointGroup, validatorEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create validator endpoint provider: %w", err)
	}

	return EndpointProviderResult{
		Master:    master,
		Slave:     slave,
		Validator: validator,
	}, nil
}

func newEndpointProvider(logger *zap.Logger, cfg *config.Config, endpointGroup *config.EndpointGroup, name string) (*endpointProvider, error) {
	primaryEndpoints, primaryPicker, err := newEndpoints(
		logger,
		cfg,
		endpointGroup.Endpoints,
		&endpointGroup.StickySession,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create primary endpoints: %w", err)
	}

	secondaryEndpoints, secondaryPicker, err := newEndpoints(
		logger,
		cfg,
		endpointGroup.EndpointsFailover,
		&endpointGroup.StickySessionFailover,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create secondary endpoints: %w", err)
	}

	if endpointGroup.UseFailover {
		logger.Warn("using failover endpoints", zap.String("endpoint_group", name))
		primaryEndpoints, secondaryEndpoints = secondaryEndpoints, primaryEndpoints
		primaryPicker, secondaryPicker = secondaryPicker, primaryPicker
	}

	return &endpointProvider{
		name:               name,
		endpointGroup:      endpointGroup,
		logger:             logger,
		primaryPicker:      primaryPicker,
		primaryEndpoints:   primaryEndpoints,
		secondaryPicker:    secondaryPicker,
		secondaryEndpoints: secondaryEndpoints,
	}, nil
}

func newEndpoints(
	logger *zap.Logger,
	cfg *config.Config,
	endpoints []config.Endpoint,
	stickySession *config.StickySessionConfig,
) ([]*Endpoint, picker.Picker, error) {
	res := make([]*Endpoint, len(endpoints))
	for i := range endpoints {
		endpoint, err := newEndpoint(cfg, logger, &endpoints[i], stickySession)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to create endpoint: %w", err)
		}

		res[i] = endpoint
	}

	// Shuffle the endpoints to prevent overloading the first endpoint right after a deployment.
	rand.Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})

	choices := make([]*picker.Choice, len(endpoints))
	for i, endpoint := range res {
		choices[i] = &picker.Choice{
			Item:   endpoint,
			Weight: int(endpoint.Config.Weight),
		}
	}

	picker := picker.New(choices)
	return res, picker, nil
}

func newEndpoint(
	cfg *config.Config,
	logger *zap.Logger,
	endpoint *config.Endpoint,
	stickySession *config.StickySessionConfig,
) (*Endpoint, error) {
	var opts []ClientOption
	if stickySession.Enabled() {
		if stickySession.CookiePassive {
			opts = append(opts, withStickySessionCookiePassive())
			logger.Info(
				"creating http client with sticky session",
				zap.String("endpoint", endpoint.Name),
				zap.String("type", "CookiePassive"),
			)
		} else if stickySession.HeaderHash != "" {
			stickySessionHeaderKey := stickySession.HeaderHash
			stickySessionHeaderValue := getStickySessionValue(cfg)
			opts = append(opts, withStickySessionHeaderHash(stickySessionHeaderKey, stickySessionHeaderValue))
			logger.Info(
				"creating http client with sticky session",
				zap.String("endpoint", endpoint.Name),
				zap.String("type", "HeaderHash"),
				zap.String("stickySessionHeaderKey", stickySessionHeaderKey),
				zap.String("stickySessionHeaderValue", stickySessionHeaderValue),
			)
		} else if stickySession.CookieHash != "" {
			stickySessionCookie := stickySession.CookieHash
			stickySessionValue := getStickySessionValue(cfg)
			logger.Info(
				"creating http client with sticky session",
				zap.String("endpoint", endpoint.Name),
				zap.String("type", "CookieHash"),
				zap.String("stickySessionCookie", stickySessionCookie),
				zap.String("stickySessionValue", stickySessionValue),
			)
			opts = append(opts, withStickySessionCookieHash(endpoint.Url, stickySessionCookie, stickySessionValue))
		} else {
			return nil, xerrors.Errorf("unknown sticky session type: %+v", stickySession)
		}
	}

	if cfg.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_OPTIMISM {
		opts = append(opts, withOverrideRequestTimeout(600))
	}

	client, err := newHTTPClient(opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create http client for %v: %w", endpoint.Name, err)
	}

	return &Endpoint{
		Name:   endpoint.Name,
		Config: endpoint,
		Client: client,
	}, nil
}

func getStickySessionValue(cfg *config.Config) string {
	return fmt.Sprintf("%v-%v-%v", consts.ServiceName, cfg.Chain.Network.GetName(), cfg.Env())
}

func (e *endpointProvider) GetEndpoint(ctx context.Context) (*Endpoint, error) {
	activeEndpoints, activePicker := e.getActiveEndpoints(ctx)
	if len(activeEndpoints) == 0 {
		return nil, xerrors.New("no endpoint is available")
	}

	pick := activePicker.Next().(*Endpoint)
	return pick, nil
}

func (e *endpointProvider) GetAllEndpoints() []*Endpoint {
	return append(e.primaryEndpoints, e.secondaryEndpoints...)
}

func (e *endpointProvider) GetActiveEndpoints(ctx context.Context) []*Endpoint {
	endpoints, _ := e.getActiveEndpoints(ctx)
	return endpoints
}

func (e *endpointProvider) WithFailoverContext(ctx context.Context) (context.Context, error) {
	if len(e.secondaryEndpoints) == 0 {
		return nil, ErrFailoverUnavailable
	}

	return context.WithValue(ctx, e.getFailoverContextKey(), struct{}{}), nil
}

func (e *endpointProvider) getActiveEndpoints(ctx context.Context) ([]*Endpoint, picker.Picker) {
	activeEndpoints := e.primaryEndpoints
	activePicker := e.primaryPicker
	if e.HasFailoverContext(ctx) {
		activeEndpoints = e.secondaryEndpoints
		activePicker = e.secondaryPicker
	}

	return activeEndpoints, activePicker
}

func (e *endpointProvider) HasFailoverContext(ctx context.Context) bool {
	return ctx.Value(e.getFailoverContextKey()) != nil
}

func (e *endpointProvider) getFailoverContextKey() contextKey {
	// Use different key for master and slave.
	return contextKey(contextKeyFailover + e.name)
}
