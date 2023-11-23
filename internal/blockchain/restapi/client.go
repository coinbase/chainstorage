package restapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/utils/finalizer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
)

type (
	// Client defines the interface for REST API. Different from Json RPC, parts of the parameters will be embeded into the
	// URL. Therefore, different types of requests will query different URLs. The endpoint shared URL is stored
	// in Config while each request method provides the parameter URL.
	Client interface {
		// Call is used to send one request to REST API endpoint. The request input will be 'method' and 'requestBody'.
		// Note that 'requestBody' is mostly used for POST. The request response and error are returned.
		Call(ctx context.Context, method *RequestMethod, requestBody []byte) ([]byte, error)

		// REST API doesn't support batch requests.
	}

	HTTPClient interface {
		Do(req *http.Request) (*http.Response, error)
	}

	ClientParams struct {
		fx.In
		fxparams.Params
		Master     endpoints.EndpointProvider `name:"master"`
		Slave      endpoints.EndpointProvider `name:"slave"`
		Validator  endpoints.EndpointProvider `name:"validator"`
		Consensus  endpoints.EndpointProvider `name:"consensus"`
		HTTPClient HTTPClient                 `optional:"true"` // Injected by unit test.
	}

	ClientResult struct {
		fx.Out
		Master    Client `name:"master"`
		Slave     Client `name:"slave"`
		Validator Client `name:"validator"`
		Consensus Client `name:"consensus"`
	}

	HTTPError struct {
		Code     int
		Response string
	}

	// RequestMethod represents a REST method, including the parameter URL and timeout.
	// The 'Name' is just used for annotation.
	// For example, in Aptos, the 'ParamsPath' for block 1 will be: "/blocks/by_height/1?with_transactions=true".
	RequestMethod struct {
		Name       string
		ParamsPath string
		Timeout    time.Duration
	}
)

type (
	clientImpl struct {
		logger           *zap.Logger
		metrics          tally.Scope
		httpClient       HTTPClient
		retry            retry.Retry
		endpointProvider endpoints.EndpointProvider
	}
)

const (
	instrumentName = "restapi.request"
)

func New(params ClientParams) (ClientResult, error) {
	master, err := newClient(params, params.Master)
	if err != nil {
		return ClientResult{}, xerrors.Errorf("failed to create master client: %w", err)
	}

	slave, err := newClient(params, params.Slave)
	if err != nil {
		return ClientResult{}, xerrors.Errorf("failed to create slave client: %w", err)
	}

	validator, err := newClient(params, params.Validator)
	if err != nil {
		return ClientResult{}, xerrors.Errorf("failed to create validator client: %w", err)
	}

	consensus, err := newClient(params, params.Consensus)
	if err != nil {
		return ClientResult{}, xerrors.Errorf("failed to create consensus client: %w", err)
	}

	return ClientResult{
		Master:    master,
		Slave:     slave,
		Validator: validator,
		Consensus: consensus,
	}, nil
}

func newClient(params ClientParams, endpointProvider endpoints.EndpointProvider) (Client, error) {
	logger := log.WithPackage(params.Logger)

	clientRetry := newRetry(params, logger)

	return &clientImpl{
		logger:           logger,
		metrics:          params.Metrics.SubScope("restapi"),
		httpClient:       params.HTTPClient,
		retry:            clientRetry,
		endpointProvider: endpointProvider,
	}, nil
}

func newRetry(params ClientParams, logger *zap.Logger) retry.Retry {
	maxAttempts := params.Config.Chain.Client.Retry.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = retry.DefaultMaxAttempts
	}

	return retry.New(
		retry.WithLogger(logger),
		retry.WithMaxAttempts(maxAttempts),
	)
}

func (c *clientImpl) Call(ctx context.Context, method *RequestMethod, requestBody []byte) ([]byte, error) {
	endpoint, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get endpoint for request: %w", err)
	}

	endpoint.IncRequestsCounter(1)

	var response []byte
	if err := c.wrap(ctx, method, endpoint.Name, func(ctx context.Context) error {
		if response, err = c.makeHTTPRequest(ctx, method, requestBody, endpoint); err != nil {
			return xerrors.Errorf("failed to make http request (method=%v, requestBody=%v, endpoint=%v): %w", method, requestBody, endpoint.Name, err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return response, nil
}

func (c *clientImpl) makeHTTPRequest(ctx context.Context, method *RequestMethod, requestBody []byte, endpoint *endpoints.Endpoint) ([]byte, error) {
	// The full URL is the shared endpoint URL and the parameter URL.
	url := endpoint.Config.Url + method.ParamsPath
	user := endpoint.Config.User
	password := endpoint.Config.Password

	ctx, cancel := context.WithTimeout(ctx, method.Timeout)
	defer cancel()

	// TODO: will handle both GET and POST.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, bytes.NewReader(requestBody))
	if err != nil {
		err = c.sanitizedError(err)
		return nil, xerrors.Errorf("failed to create request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	if user != "" && password != "" {
		request.SetBasicAuth(user, password)
	}

	// Send the request.
	response, err := c.getHTTPClient(endpoint).Do(request)
	if err != nil {
		err = c.sanitizedError(err)
		return nil, retry.Retryable(xerrors.Errorf("failed to send http request: %w", err))
	}

	finalizer := finalizer.WithCloser(response.Body)
	defer finalizer.Finalize()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, retry.Retryable(xerrors.Errorf("failed to read http response: %w", err))
	}

	if response.StatusCode != http.StatusOK {
		errHTTP := xerrors.Errorf("received http error: %w", &HTTPError{
			Code:     response.StatusCode,
			Response: string(responseBody),
		})

		if response.StatusCode == http.StatusTooManyRequests {
			return nil, retry.RateLimit(errHTTP)
		}

		if response.StatusCode >= http.StatusInternalServerError {
			return nil, retry.Retryable(errHTTP)
		}
		return nil, errHTTP
	}

	return responseBody, finalizer.Close()
}

func (c *clientImpl) getHTTPClient(endpoint *endpoints.Endpoint) HTTPClient {
	if c.httpClient != nil {
		// Injected by unit test.
		return c.httpClient
	}

	return endpoint.Client
}

// wrap wraps the operation with metrics, logging, and retry.
func (c *clientImpl) wrap(ctx context.Context, method *RequestMethod, endpoint string, operation instrument.OperationFn) error {
	tags := map[string]string{
		"method":   method.Name,
		"endpoint": endpoint,
	}
	scope := c.metrics.Tagged(tags)

	logger := c.logger.With(
		zap.String("method", method.Name),
		zap.String("endpoint", endpoint),
		zap.Reflect("request", method),
	)
	call := instrument.New(
		scope,
		"request",
		instrument.WithTracer(instrumentName, tags),
		instrument.WithLogger(logger, instrumentName),
	).WithRetry(c.retry)
	return call.Instrument(ctx, operation)
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTPError %v: %v", e.Code, e.Response)
}

func (c *clientImpl) sanitizedError(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		// url.Error includes the url in the error message, which may contain the API key.
		err = uerr.Err
	}
	return err
}
