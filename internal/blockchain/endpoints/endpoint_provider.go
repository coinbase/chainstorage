package endpoints

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/consts"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/picker"
	"github.com/coinbase/chainstorage/internal/utils/utils"
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

		requestsCounter tally.Counter
	}

	EndpointProviderParams struct {
		fx.In
		Config *config.Config
		Logger *zap.Logger
		Scope  tally.Scope
	}

	EndpointProviderResult struct {
		fx.Out
		Master    EndpointProvider `name:"master"`
		Slave     EndpointProvider `name:"slave"`
		Validator EndpointProvider `name:"validator"`
		Consensus EndpointProvider `name:"consensus"`
	}
)

type (
	endpointProvider struct {
		name               string
		endpointGroup      *config.EndpointGroup
		logger             *zap.Logger
		scope              tally.Scope
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
	consensusEndpointGroupName = "consensus"
	contextKeyFailover         = "failover:"
)

var (
	ErrFailoverUnavailable = xerrors.New("no endpoint is available for failover")
)

func NewEndpointProvider(params EndpointProviderParams) (EndpointProviderResult, error) {
	logger := log.WithPackage(params.Logger)
	scope := params.Scope.SubScope("endpoints")

	master, err := newEndpointProvider(logger, params.Config, scope, &params.Config.Chain.Client.Master.EndpointGroup, masterEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create master endpoint provider: %w", err)
	}

	slave, err := newEndpointProvider(logger, params.Config, scope, &params.Config.Chain.Client.Slave.EndpointGroup, slaveEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create slave endpoint provider: %w", err)
	}

	validator, err := newEndpointProvider(logger, params.Config, scope, &params.Config.Chain.Client.Validator.EndpointGroup, validatorEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create validator endpoint provider: %w", err)
	}

	// Consensus client is set as Slave by default
	var consensus *endpointProvider
	if !params.Config.Chain.Client.Consensus.EndpointGroup.Empty() {
		consensus, err = newEndpointProvider(logger, params.Config, scope, &params.Config.Chain.Client.Consensus.EndpointGroup, consensusEndpointGroupName)
		if err != nil {
			return EndpointProviderResult{}, xerrors.Errorf("failed to create consensus endpoint provider: %w", err)
		}
	} else {
		consensus, err = newEndpointProvider(logger, params.Config, scope, &params.Config.Chain.Client.Slave.EndpointGroup, consensusEndpointGroupName)
		if err != nil {
			return EndpointProviderResult{}, xerrors.Errorf("failed to create consensus endpoint provider with slave endpoints: %w", err)
		}
	}

	return EndpointProviderResult{
		Master:    master,
		Slave:     slave,
		Validator: validator,
		Consensus: consensus,
	}, nil
}

func newEndpointProvider(logger *zap.Logger, cfg *config.Config, scope tally.Scope, endpointGroup *config.EndpointGroup, name string) (*endpointProvider, error) {
	primaryEndpoints, primaryPicker, err := newEndpoints(
		logger,
		cfg,
		scope,
		endpointGroup.Endpoints,
		&endpointGroup.EndpointConfig,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create primary endpoints: %w", err)
	}

	secondaryEndpoints, secondaryPicker, err := newEndpoints(
		logger,
		cfg,
		scope,
		endpointGroup.EndpointsFailover,
		&endpointGroup.EndpointConfigFailover,
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
		scope:              scope,
		primaryPicker:      primaryPicker,
		primaryEndpoints:   primaryEndpoints,
		secondaryPicker:    secondaryPicker,
		secondaryEndpoints: secondaryEndpoints,
	}, nil
}

func newEndpoints(
	logger *zap.Logger,
	cfg *config.Config,
	scope tally.Scope,
	endpoints []config.Endpoint,
	endpointConfig *config.EndpointConfig,
) ([]*Endpoint, picker.Picker, error) {
	res := make([]*Endpoint, len(endpoints))
	for i := range endpoints {
		endpoint, err := newEndpoint(cfg, logger, scope, &endpoints[i], endpointConfig)
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
	scope tally.Scope,
	endpoint *config.Endpoint,
	endpointConfig *config.EndpointConfig,
) (*Endpoint, error) {
	var opts []ClientOption

	//TODO: check if this is still needed
	//if cfg.Env() == config.EnvLocal {
	//	// Skip TLS verify due to go1.18 issue in MacOS https://github.com/golang/go/issues/51991
	//	opts = append(opts, withRootCAs(fix.Roots()))
	//}

	stickySession := endpointConfig.StickySession
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

	if len(endpointConfig.Headers) > 0 {
		opts = append(opts, withHeaders(endpointConfig.Headers))
	}

	if cfg.Chain.Client.HttpTimeout > 0 {
		opts = append(opts, withOverrideRequestTimeout(cfg.Chain.Client.HttpTimeout))
	}

	client, err := newHTTPClient(opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to create http client for %v: %w", endpoint.Name, err)
	}

	providerID := "unknown"
	if endpoint.ProviderID != "" {
		providerID = endpoint.ProviderID
	}

	return &Endpoint{
		Name:   endpoint.Name,
		Config: endpoint,
		Client: client,
		requestsCounter: scope.
			Tagged(map[string]string{
				"endpoint_name": endpoint.Name,
				"provider_id":   providerID,
			}).
			Counter("requests"),
	}, nil
}

func getStickySessionValue(cfg *config.Config) string {
	headerValue := fmt.Sprintf("%v-%v-%v", consts.ServiceName, cfg.Chain.Network.GetName(), cfg.Env())
	return utils.GenerateSha256HashString(headerValue)
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

func (e *Endpoint) IncRequestsCounter(n int64) {
	e.requestsCounter.Inc(n)
}
