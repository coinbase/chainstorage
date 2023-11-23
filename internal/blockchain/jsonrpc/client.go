package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
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
	Client interface {
		Call(ctx context.Context, method *RequestMethod, params Params, opts ...Option) (*Response, error)
		BatchCall(ctx context.Context, method *RequestMethod, batchParams []Params, opts ...Option) ([]*Response, error)
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

	Request struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
		ID      uint   `json:"id"`
	}

	Response struct {
		JSONRPC string          `json:"jsonrpc"`
		Result  json.RawMessage `json:"result,omitempty"`
		Error   *RPCError       `json:"error,omitempty"`
		ID      uint            `json:"id"`
	}

	RPCError struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}

	HTTPError struct {
		Code     int
		Response string
	}

	RequestMethod struct {
		Name    string
		Timeout time.Duration
	}

	Params []any

	Option func(opts *options)

	options struct {
		allowsRPCError bool
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
	jsonrpcVersion = "2.0"

	instrumentName = "jsonrpc.request"
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

func WithAllowsRPCError() Option {
	return func(opts *options) {
		opts.allowsRPCError = true
	}
}

func newClient(params ClientParams, endpointProvider endpoints.EndpointProvider) (Client, error) {
	logger := log.WithPackage(params.Logger)

	clientRetry := newRetry(params, logger)

	return &clientImpl{
		logger:           logger,
		metrics:          params.Metrics.SubScope("jsonrpc"),
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

func (c *clientImpl) Call(ctx context.Context, method *RequestMethod, params Params, opts ...Option) (*Response, error) {
	var options options
	for _, opt := range opts {
		opt(&options)
	}

	endpoint, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get endpoint for request: %w", err)
	}

	endpoint.IncRequestsCounter(1)

	request := &Request{
		JSONRPC: jsonrpcVersion,
		Method:  method.Name,
		Params:  params,
		ID:      0,
	}

	var response *Response
	if err := c.wrap(ctx, method.Name, endpoint.Name, []Params{params}, func(ctx context.Context) error {
		response = new(Response)
		if err := c.makeHTTPRequest(ctx, method.Timeout, endpoint, request, response); err != nil {
			return xerrors.Errorf("failed to make http request (method=%v, params=%v, endpoint=%v): %w", method, params, endpoint.Name, err)
		}

		return nil
	}); err != nil && response.Error == nil {
		return nil, err
	}

	if response.Error != nil && !options.allowsRPCError {
		return nil, xerrors.Errorf("received rpc error (method=%v, params=%v, endpoint=%v): %w", method, params, endpoint.Name, response.Error)
	}

	return response, nil
}

func (c *clientImpl) BatchCall(ctx context.Context, method *RequestMethod, batchParams []Params, opts ...Option) ([]*Response, error) {
	var options options
	for _, opt := range opts {
		opt(&options)
	}

	endpoint, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get endpoint for request: %w", err)
	}

	endpoint.IncRequestsCounter(int64(len(batchParams)))

	batchRequests := make([]*Request, len(batchParams))
	for i, params := range batchParams {
		batchRequests[i] = &Request{
			JSONRPC: jsonrpcVersion,
			Method:  method.Name,
			Params:  params,
			ID:      uint(i),
		}
	}

	finalBatchResponses := make([]*Response, len(batchParams))
	if err := c.wrap(ctx, method.Name, endpoint.Name, batchParams, func(ctx context.Context) error {
		var batchResponses []Response
		if err := c.makeHTTPRequest(ctx, method.Timeout, endpoint, batchRequests, &batchResponses); err != nil {
			return xerrors.Errorf(
				"failed to make http request (method=%v, endpoint=%v): %w",
				method, endpoint.Name, err,
			)
		}

		if len(batchParams) != len(batchResponses) {
			return xerrors.Errorf(
				"received wrong number of responses (method=%v, endpoint=%v, want=%v, got=%v)",
				method, endpoint.Name, len(batchParams), len(batchResponses),
			)
		}

		// The responses may be out of order.
		// Reorder them to match `batchParams`.
		for i := range batchResponses {
			response := &batchResponses[i]

			id := int(response.ID)
			if id >= len(finalBatchResponses) {
				return xerrors.Errorf(
					"received unexpected response id (method=%v, endpoint=%v, id=%v)",
					method, endpoint.Name, id,
				)
			}

			if response.Error != nil {
				if options.allowsRPCError {
					finalBatchResponses[id] = response
					continue
				}

				return xerrors.Errorf(
					"received rpc error (method=%v, endpoint=%v): %w",
					method, endpoint.Name, response.Error,
				)
			}

			if IsNullOrEmpty(response.Result) {
				// Retry the batch call if any of the responses is null.
				return retry.Retryable(xerrors.Errorf(
					"received a null response (method=%v, endpoint=%v, index=%v, response=%v)",
					method, endpoint.Name, i, string(response.Result),
				))
			}

			finalBatchResponses[id] = response
		}

		return nil
	}); err != nil {
		return nil, err
	}

	for i := range finalBatchResponses {
		if finalBatchResponses[i] == nil {
			return nil, xerrors.Errorf(
				"missing response (method=%v, endpoint=%v, id=%v)",
				method, endpoint.Name, i,
			)
		}
	}

	return finalBatchResponses, nil
}

func (c *clientImpl) makeHTTPRequest(ctx context.Context, timeout time.Duration, endpoint *endpoints.Endpoint, data any, out any) error {
	url := endpoint.Config.Url
	user := endpoint.Config.User
	password := endpoint.Config.Password

	requestBody, err := json.Marshal(data)
	if err != nil {
		return xerrors.Errorf("failed to marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(requestBody))
	if err != nil {
		err = c.sanitizedError(err)
		return xerrors.Errorf("failed to create request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	if user != "" && password != "" {
		request.SetBasicAuth(user, password)
	}

	response, err := c.getHTTPClient(endpoint).Do(request)
	if err != nil {
		err = c.sanitizedError(err)
		return retry.Retryable(xerrors.Errorf("failed to send http request: %w", err))
	}

	finalizer := finalizer.WithCloser(response.Body)
	defer finalizer.Finalize()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return retry.Retryable(xerrors.Errorf("failed to read http response: %w", err))
	}

	if response.StatusCode != http.StatusOK {
		errHTTP := xerrors.Errorf("received http error: %w", &HTTPError{
			Code:     response.StatusCode,
			Response: string(responseBody),
		})

		// Unmarshal responseBody if possible in order to preserve the error code. silence error if there is any.
		// For bitcoin 500 cases, it still returns a deserializable response with non-empty error.
		// Note that if Response.Error field is successfully deserialized, Call returns RPCError instead of HTTPError.
		// See also:
		// - https://github.com/coinbase/chainstorage/blob/55da0742878e5ffbc788b43b0d3fdef417574542/internal/blockchain/jsonrpc/client.go#L193
		// - https://github.com/coinbase/chainstorage/blob/55da0742878e5ffbc788b43b0d3fdef417574542/internal/blockchain/jsonrpc/client_test.go#L210
		_ = json.Unmarshal(responseBody, out)

		if response.StatusCode == 429 {
			return retry.RateLimit(errHTTP)
		}

		if response.StatusCode >= 500 {
			return retry.Retryable(errHTTP)
		}
		return errHTTP
	}

	if err := json.Unmarshal(responseBody, out); err != nil {
		// Some upstream clients (e.g. Erigon client for ETH) return invalid JSON responses for otherwise retryable
		// errors such as execution timeouts.
		return retry.Retryable(xerrors.Errorf("failed to decode response %v: %w", string(responseBody), err))
	}

	return finalizer.Close()
}

func (c *clientImpl) getHTTPClient(endpoint *endpoints.Endpoint) HTTPClient {
	if c.httpClient != nil {
		// Injected by unit test.
		return c.httpClient
	}

	return endpoint.Client
}

// wrap wraps the operation with metrics, logging, and retry.
func (c *clientImpl) wrap(ctx context.Context, method string, endpoint string, params []Params, operation instrument.OperationFn) error {
	tags := map[string]string{
		"method":   method,
		"endpoint": endpoint,
	}
	scope := c.metrics.Tagged(tags)
	logger := c.logger.With(
		zap.String("method", method),
		zap.String("endpoint", endpoint),
		zap.Int("numParams", len(params)),
		zap.Reflect("params", c.shortenParams(params)),
	)
	instrument := instrument.New(
		scope,
		"request",
		instrument.WithTracer(instrumentName, tags),
		instrument.WithLogger(logger, instrumentName),
	).WithRetry(c.retry)
	return instrument.Instrument(ctx, operation)
}

func (c *clientImpl) shortenParams(params []Params) []Params {
	const maxParams = 10

	if len(params) <= maxParams {
		return params
	}

	return params[:maxParams]
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("RPCError %v: %v", e.Code, e.Message)
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTPError %v: %v", e.Code, e.Response)
}

func (r *Response) Unmarshal(out any) error {
	return json.Unmarshal(r.Result, out)
}

func IsNullOrEmpty(r json.RawMessage) bool {
	if len(r) == 0 ||
		bytes.Equal(r, []byte("{}")) ||
		bytes.Equal(r, []byte("null")) {
		return true
	}

	return false
}

func (c *clientImpl) sanitizedError(err error) error {
	var uerr *url.Error
	if xerrors.As(err, &uerr) {
		// url.Error includes the url in the error message, which may contain the API key.
		err = uerr.Err
	}
	return err
}
