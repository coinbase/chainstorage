package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/utils/consts"
	"github.com/coinbase/chainstorage/internal/utils/finalizer"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	restClient struct {
		logger     *zap.Logger
		httpClient *http.Client
		address    string
		authHeader string
		authToken  string
		retry      retry.Retry
	}
)

const (
	timeout             = time.Second * 10
	retryMultiplier     = 2
	retryMaxInterval    = time.Second
	retryMaxElapsedTime = time.Second * 60
)

var (
	_ Client = (*restClient)(nil)

	ErrNotImplemented = errors.New("not implemented")
)

func newRestClient(params Params) (Client, error) {
	logger := log.WithPackage(params.Logger)
	address := params.Config.SDK.ChainstorageAddress
	authHeader := params.Config.SDK.AuthHeader
	authToken := params.Config.SDK.AuthToken
	httpClient := &http.Client{
		Timeout: timeout,
	}

	client := &restClient{
		logger:     logger,
		httpClient: httpClient,
		address:    address,
		authHeader: authHeader,
		authToken:  authToken,
		retry: retry.New(
			retry.WithLogger(logger),
			retry.WithMaxAttempts(maxRetries),
			retry.WithBackoffFactory(func() retry.Backoff {
				return &backoff.ExponentialBackOff{
					InitialInterval:     backoffScalar,
					RandomizationFactor: backoffJitter,
					Multiplier:          retryMultiplier,
					MaxInterval:         retryMaxInterval,
					MaxElapsedTime:      retryMaxElapsedTime,
					Clock:               backoff.SystemClock,
				}
			}),
		),
	}

	return client, nil
}

func (c *restClient) GetLatestBlock(ctx context.Context, request *api.GetLatestBlockRequest, _ ...grpc.CallOption) (*api.GetLatestBlockResponse, error) {
	var response api.GetLatestBlockResponse
	if err := c.makeRequest(ctx, "GetLatestBlock", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetBlockFile(ctx context.Context, request *api.GetBlockFileRequest, _ ...grpc.CallOption) (*api.GetBlockFileResponse, error) {
	var response api.GetBlockFileResponse
	if err := c.makeRequest(ctx, "GetBlockFile", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetBlockFilesByRange(ctx context.Context, request *api.GetBlockFilesByRangeRequest, _ ...grpc.CallOption) (*api.GetBlockFilesByRangeResponse, error) {
	var response api.GetBlockFilesByRangeResponse
	if err := c.makeRequest(ctx, "GetBlockFilesByRange", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetRawBlock(ctx context.Context, request *api.GetRawBlockRequest, _ ...grpc.CallOption) (*api.GetRawBlockResponse, error) {
	var response api.GetRawBlockResponse
	if err := c.makeRequest(ctx, "GetRawBlock", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetRawBlocksByRange(ctx context.Context, request *api.GetRawBlocksByRangeRequest, _ ...grpc.CallOption) (*api.GetRawBlocksByRangeResponse, error) {
	var response api.GetRawBlocksByRangeResponse
	if err := c.makeRequest(ctx, "GetRawBlocksByRange", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetNativeBlock(ctx context.Context, request *api.GetNativeBlockRequest, _ ...grpc.CallOption) (*api.GetNativeBlockResponse, error) {
	var response api.GetNativeBlockResponse
	if err := c.makeRequest(ctx, "GetNativeBlock", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetNativeBlocksByRange(ctx context.Context, request *api.GetNativeBlocksByRangeRequest, _ ...grpc.CallOption) (*api.GetNativeBlocksByRangeResponse, error) {
	var response api.GetNativeBlocksByRangeResponse
	if err := c.makeRequest(ctx, "GetNativeBlocksByRange", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetRosettaBlock(ctx context.Context, request *api.GetRosettaBlockRequest, _ ...grpc.CallOption) (*api.GetRosettaBlockResponse, error) {
	var response api.GetRosettaBlockResponse
	if err := c.makeRequest(ctx, "GetRosettaBlock", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetRosettaBlocksByRange(ctx context.Context, request *api.GetRosettaBlocksByRangeRequest, _ ...grpc.CallOption) (*api.GetRosettaBlocksByRangeResponse, error) {
	var response api.GetRosettaBlocksByRangeResponse
	if err := c.makeRequest(ctx, "GetRosettaBlocksByRange", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) StreamChainEvents(ctx context.Context, request *api.ChainEventsRequest, _ ...grpc.CallOption) (api.ChainStorage_StreamChainEventsClient, error) {
	return nil, fmt.Errorf("streaming is not supported under restful mode: %w", ErrNotImplemented)
}

func (c *restClient) GetChainEvents(ctx context.Context, request *api.GetChainEventsRequest, _ ...grpc.CallOption) (*api.GetChainEventsResponse, error) {
	var response api.GetChainEventsResponse
	if err := c.makeRequest(ctx, "GetChainEvents", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetChainMetadata(ctx context.Context, request *api.GetChainMetadataRequest, _ ...grpc.CallOption) (*api.GetChainMetadataResponse, error) {
	var response api.GetChainMetadataResponse
	if err := c.makeRequest(ctx, "GetChainMetadata", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetVersionedChainEvent(ctx context.Context, request *api.GetVersionedChainEventRequest, _ ...grpc.CallOption) (*api.GetVersionedChainEventResponse, error) {
	var response api.GetVersionedChainEventResponse
	if err := c.makeRequest(ctx, "GetVersionedChainEvent", request, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetBlockByTransaction(ctx context.Context, in *api.GetBlockByTransactionRequest, opts ...grpc.CallOption) (*api.GetBlockByTransactionResponse, error) {
	var response api.GetBlockByTransactionResponse
	if err := c.makeRequest(ctx, "GetBlockByTransaction", in, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetNativeTransaction(ctx context.Context, in *api.GetNativeTransactionRequest, opts ...grpc.CallOption) (*api.GetNativeTransactionResponse, error) {
	var response api.GetNativeTransactionResponse
	if err := c.makeRequest(ctx, "GetNativeTransaction", in, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) GetVerifiedAccountState(ctx context.Context, in *api.GetVerifiedAccountStateRequest, opts ...grpc.CallOption) (*api.GetVerifiedAccountStateResponse, error) {
	var response api.GetVerifiedAccountStateResponse
	if err := c.makeRequest(ctx, "GetVerifiedAccountState", in, &response); err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	return &response, nil
}

func (c *restClient) makeRequest(ctx context.Context, method string, request proto.Message, response proto.Message) error {
	return c.retry.Retry(ctx, func(ctx context.Context) error {
		marshaler := protojson.MarshalOptions{}
		requestBody, err := marshaler.Marshal(request)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}

		url := c.getURL(method)
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(requestBody))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		httpRequest.Header.Set("Content-Type", "application/json")
		httpRequest.Header.Set("Accept", "application/json")
		if c.authHeader != "" && c.authToken != "" {
			httpRequest.Header.Set(c.authHeader, c.authToken)
		}

		c.logger.Debug(
			"making http request",
			zap.String("method", method),
			zap.String("url", url),
		)
		httpResponse, err := c.httpClient.Do(httpRequest)
		if err != nil {
			return retry.Retryable(fmt.Errorf("failed to send http request: %w", err))
		}

		finalizer := finalizer.WithCloser(httpResponse.Body)
		defer finalizer.Finalize()

		body, err := ioutil.ReadAll(httpResponse.Body)
		if err != nil {
			return retry.Retryable(fmt.Errorf("failed to read from http response: %w", err))
		}
		if statusCode := httpResponse.StatusCode; statusCode != http.StatusOK {
			if statusCode == 429 || statusCode >= 500 {
				return retry.Retryable(fmt.Errorf("received retryable status code %v: %v", statusCode, string(body)))
			}

			return fmt.Errorf("received non-retryable status code %v: %v", statusCode, string(body))
		}

		unmarshaler := protojson.UnmarshalOptions{
			DiscardUnknown: true,
		}

		proto.Reset(response)
		if err := unmarshaler.Unmarshal(body, response); err != nil {
			return retry.Retryable(fmt.Errorf("failed to decode response: %w", err))
		}

		return nil
	})
}

func (c *restClient) getURL(method string) string {
	base := strings.TrimRight(c.address, "/")
	return strings.Join([]string{
		base,
		consts.FullServiceName,
		method,
	}, "/")
}
