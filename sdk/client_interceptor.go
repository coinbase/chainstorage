package sdk

import (
	"context"
	"time"

	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"

	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// retryableClient intercepts the request and retries the DeadlineExceeded errors.
	retryableClient struct {
		client Client
		logger *zap.Logger
	}
)

func WithClientRetryInterceptor(client Client, logger *zap.Logger) Client {
	return &retryableClient{
		client: client,
		logger: logger,
	}
}

func (c *retryableClient) GetTag() uint32 {
	return c.client.GetTag()
}

func (c *retryableClient) SetTag(tag uint32) {
	c.client.SetTag(tag)
}

func (c *retryableClient) GetClientID() string {
	return c.client.GetClientID()
}

func (c *retryableClient) SetClientID(clientID string) {
	c.client.SetClientID(clientID)
}

func (c *retryableClient) SetClientTimeout(timeout time.Duration) {
	c.client.SetClientTimeout(timeout)
}

func (c *retryableClient) SetBlockValidation(blockValidation bool) {
	c.client.SetBlockValidation(blockValidation)
}

func (c *retryableClient) GetBlockValidation() bool {
	return c.client.GetBlockValidation()
}

func (c *retryableClient) GetLatestBlock(ctx context.Context) (uint64, error) {
	// No retry needed because this is a wrapper on GetLatestBlockWithTag.
	return c.client.GetLatestBlock(ctx)
}

func (c *retryableClient) GetLatestBlockWithTag(ctx context.Context, tag uint32) (uint64, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (uint64, error) {
		return c.client.GetLatestBlockWithTag(ctx, tag)
	})
}

func (c *retryableClient) GetBlock(ctx context.Context, height uint64, hash string) (*api.Block, error) {
	// No retry needed because this is a wrapper on GetBlockWithTag
	return c.client.GetBlock(ctx, height, hash)
}

func (c *retryableClient) GetBlockWithTag(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (*api.Block, error) {
		return c.client.GetBlockWithTag(ctx, tag, height, hash)
	})
}

func (c *retryableClient) GetBlocksByRange(ctx context.Context, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	// No retry needed because this is a wrapper on GetBlocksByRangeWithTag
	return c.client.GetBlocksByRange(ctx, startHeight, endHeight)
}

func (c *retryableClient) GetBlocksByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.Block, error) {
		return c.client.GetBlocksByRangeWithTag(ctx, tag, startHeight, endHeight)
	})
}

func (c *retryableClient) GetBlockByTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.Block, error) {
		return c.client.GetBlockByTransaction(ctx, tag, transactionHash)
	})
}

func (c *retryableClient) StreamChainEvents(ctx context.Context, cfg StreamingConfiguration) (<-chan *ChainEventResult, error) {
	// No timeout is implemented.
	return c.client.StreamChainEvents(ctx, cfg)
}

func (c *retryableClient) GetChainEvents(ctx context.Context, req *api.GetChainEventsRequest) ([]*api.BlockchainEvent, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.BlockchainEvent, error) {
		return c.client.GetChainEvents(ctx, req)
	})
}

func (c *retryableClient) GetChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (*api.GetChainMetadataResponse, error) {
		return c.client.GetChainMetadata(ctx, req)
	})
}

func (c *retryableClient) GetStaticChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
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
	if xerrors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var grpcErr gateway.GrpcError
	if xerrors.As(err, &grpcErr) && grpcErr.GRPCStatus().Code() == codes.DeadlineExceeded {
		return true
	}

	return false
}
