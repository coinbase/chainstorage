package sdk

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// timeoutableClient intercepts the request and enforces a timeout with exponential retries.
	timeoutableClient struct {
		client Client
		logger *zap.Logger

		shortTimeout  time.Duration // Used by APIs such as a simple lookup to the meta storage. Defaults to 2s.
		mediumTimeout time.Duration // Used by APIs such as downloading one object from the blob storage. Defaults to 5s.
		longTimeout   time.Duration // Used by APIs such as downloading multiple objects from the blob storage. Defaults to 11s.
	}
)

const (
	// The default timeout is loose to account for various retries. For example:
	// - On the client side, certain gRPC error codes are automatically retried by the gateway package.
	// - On the server side, certain storage errors are automatically retried by the storage package.
	//
	// If your use case is very time sensitive, you may override Config.ClientTimeout.
	defaultClientTimeout = 2 * time.Second
)

func WithTimeoutableClientInterceptor(client Client, logger *zap.Logger) Client {
	c := &timeoutableClient{
		client: client,
		logger: logger,
	}

	c.SetClientTimeout(defaultClientTimeout)
	return c
}

func (c *timeoutableClient) GetTag() uint32 {
	return c.client.GetTag()
}

func (c *timeoutableClient) SetTag(tag uint32) {
	c.client.SetTag(tag)
}

func (c *timeoutableClient) GetClientID() string {
	return c.client.GetClientID()
}

func (c *timeoutableClient) SetClientID(clientID string) {
	c.client.SetClientID(clientID)
}

func (c *timeoutableClient) SetClientTimeout(timeout time.Duration) {
	if timeout == 0 {
		timeout = defaultClientTimeout
	}

	c.shortTimeout = timeout
	c.mediumTimeout = timeout*2 + time.Second
	c.longTimeout = timeout*4 + time.Second*3
}

func (c *timeoutableClient) SetBlockValidation(blockValidation bool) {
	c.client.SetBlockValidation(blockValidation)
}

func (c *timeoutableClient) GetBlockValidation() bool {
	return c.client.GetBlockValidation()
}

func (c *timeoutableClient) GetLatestBlock(ctx context.Context) (uint64, error) {
	// No retry needed because this is a wrapper on GetLatestBlockWithTag.
	return c.client.GetLatestBlock(ctx)
}

func (c *timeoutableClient) GetLatestBlockWithTag(ctx context.Context, tag uint32) (uint64, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (uint64, error) {
		ctx, cancel := context.WithTimeout(ctx, c.shortTimeout)
		defer cancel()

		return c.client.GetLatestBlockWithTag(ctx, tag)
	})
}

func (c *timeoutableClient) GetBlock(ctx context.Context, height uint64, hash string) (*api.Block, error) {
	// No retry needed because this is a wrapper on GetBlockWithTag
	return c.client.GetBlock(ctx, height, hash)
}

func (c *timeoutableClient) GetBlockWithTag(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (*api.Block, error) {
		ctx, cancel := context.WithTimeout(ctx, c.mediumTimeout)
		defer cancel()

		return c.client.GetBlockWithTag(ctx, tag, height, hash)
	})
}

func (c *timeoutableClient) GetBlocksByRange(ctx context.Context, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	// No retry needed because this is a wrapper on GetBlocksByRangeWithTag
	return c.client.GetBlocksByRange(ctx, startHeight, endHeight)
}

func (c *timeoutableClient) GetBlocksByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.Block, error) {
		timeout := c.mediumTimeout
		if endHeight-startHeight > 2 {
			timeout = c.longTimeout
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		return c.client.GetBlocksByRangeWithTag(ctx, tag, startHeight, endHeight)
	})
}

func (c *timeoutableClient) GetBlockByTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.Block, error) {
		ctx, cancel := context.WithTimeout(ctx, c.mediumTimeout)
		defer cancel()

		return c.client.GetBlockByTransaction(ctx, tag, transactionHash)
	})
}

func (c *timeoutableClient) StreamChainEvents(ctx context.Context, cfg StreamingConfiguration) (<-chan *ChainEventResult, error) {
	// No timeout is implemented.
	return c.client.StreamChainEvents(ctx, cfg)
}

func (c *timeoutableClient) GetChainEvents(ctx context.Context, req *api.GetChainEventsRequest) ([]*api.BlockchainEvent, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.BlockchainEvent, error) {
		timeout := c.shortTimeout
		if req.MaxNumEvents > 10 {
			timeout = c.mediumTimeout
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		return c.client.GetChainEvents(ctx, req)
	})
}

func (c *timeoutableClient) GetChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (*api.GetChainMetadataResponse, error) {
		ctx, cancel := context.WithTimeout(ctx, c.shortTimeout)
		defer cancel()

		return c.client.GetChainMetadata(ctx, req)
	})
}

func (c *timeoutableClient) GetStaticChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
	// This function never fails.
	return c.client.GetStaticChainMetadata(ctx, req)
}

func intercept[T any](ctx context.Context, logger *zap.Logger, operation retry.OperationWithResultFn[T]) (T, error) {
	return retry.WrapWithResult(
		ctx,
		func(ctx context.Context) (T, error) {
			res, err := operation(ctx)
			if err != nil {
				if isRetryableError(err) {
					return res, retry.Retryable(err)
				}

				return res, err
			}

			return res, nil
		},
		retry.WithLogger(logger),
	)
}

func isRetryableError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var grpcErr gateway.GrpcError
	if errors.As(err, &grpcErr) && grpcErr.GRPCStatus().Code() == codes.DeadlineExceeded {
		return true
	}

	return false
}
