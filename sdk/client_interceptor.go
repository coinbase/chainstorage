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

func (c *timeoutableClient) GetBlockWithTagAndReadSource(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*api.Block, error) {
	readSourceClient, ok := c.client.(ReadSourceClient)
	if !ok {
		return nil, xerrors.Errorf("client %T does not support read source override", c.client)
	}
	if readSource != api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED {
		return c.getBlockWithTagAndReadSource(ctx, readSourceClient, tag, height, hash, readSource)
	}

	block, err := c.getBlockWithTagAndReadSource(ctx, readSourceClient, tag, height, hash, readSource)
	if err == nil {
		return block, nil
	}
	if ctx.Err() != nil {
		return nil, err
	}

	c.logger.Warn(
		"consolidated block read timed out or failed; retrying single-block",
		zap.Uint32("tag", tag),
		zap.Uint64("height", height),
		zap.String("hash", hash),
		zap.Error(err),
	)
	block, fallbackErr := c.getBlockWithTagAndReadSource(ctx, readSourceClient, tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
	if fallbackErr != nil {
		return nil, xerrors.Errorf("failed to read consolidated block and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
	}
	return block, nil
}

func (c *timeoutableClient) GetBlocksByRange(ctx context.Context, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	// No retry needed because this is a wrapper on GetBlocksByRangeWithTag
	return c.client.GetBlocksByRange(ctx, startHeight, endHeight)
}

func (c *timeoutableClient) GetBlocksByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.Block, error) {
		timeout := c.rangeReadTimeout(startHeight, endHeight)

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		return c.client.GetBlocksByRangeWithTag(ctx, tag, startHeight, endHeight)
	})
}

func (c *timeoutableClient) GetBlocksByRangeWithTagAndReadSource(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) ([]*api.Block, error) {
	readSourceClient, ok := c.client.(ReadSourceClient)
	if !ok {
		return nil, xerrors.Errorf("client %T does not support read source override", c.client)
	}
	if readSource != api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED {
		return c.getBlocksByRangeWithTagAndReadSource(ctx, readSourceClient, tag, startHeight, endHeight, readSource)
	}

	blocks, err := c.getBlocksByRangeWithTagAndReadSource(ctx, readSourceClient, tag, startHeight, endHeight, readSource)
	if err == nil {
		return blocks, nil
	}
	if ctx.Err() != nil {
		return nil, err
	}

	c.logger.Warn(
		"consolidated block range read timed out or failed; retrying single-block",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.Error(err),
	)
	blocks, fallbackErr := c.getBlocksByRangeWithTagAndReadSource(ctx, readSourceClient, tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
	if fallbackErr != nil {
		return nil, xerrors.Errorf("failed to read consolidated block range and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
	}
	return blocks, nil
}

func (c *timeoutableClient) rangeReadTimeout(startHeight uint64, endHeight uint64) time.Duration {
	effectiveEndHeight := endHeight
	if effectiveEndHeight == 0 {
		effectiveEndHeight = startHeight + 1
	}

	if effectiveEndHeight > startHeight && effectiveEndHeight-startHeight > 2 {
		return c.longTimeout
	}
	return c.mediumTimeout
}

func (c *timeoutableClient) getBlockWithTagAndReadSource(
	ctx context.Context,
	readSourceClient ReadSourceClient,
	tag uint32,
	height uint64,
	hash string,
	readSource api.BlockReadSource,
) (*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (*api.Block, error) {
		ctx, cancel := context.WithTimeout(ctx, c.mediumTimeout)
		defer cancel()

		return readSourceClient.GetBlockWithTagAndReadSource(ctx, tag, height, hash, readSource)
	})
}

func (c *timeoutableClient) getBlocksByRangeWithTagAndReadSource(
	ctx context.Context,
	readSourceClient ReadSourceClient,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	readSource api.BlockReadSource,
) ([]*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.Block, error) {
		ctx, cancel := context.WithTimeout(ctx, c.rangeReadTimeout(startHeight, endHeight))
		defer cancel()

		return readSourceClient.GetBlocksByRangeWithTagAndReadSource(ctx, tag, startHeight, endHeight, readSource)
	})
}

// OpenRawBlockPayload passes through to the wrapped client. No timeout
// wrapping: the returned reader lifetime is user-controlled.
func (c *timeoutableClient) OpenRawBlockPayload(ctx context.Context, height uint64, hash string) (*RawBlockPayload, error) {
	return c.client.OpenRawBlockPayload(ctx, height, hash)
}

// OpenRawBlockPayloadWithTag passes through to the wrapped client. No timeout
// wrapping: the returned reader lifetime is user-controlled.
func (c *timeoutableClient) OpenRawBlockPayloadWithTag(ctx context.Context, tag uint32, height uint64, hash string) (*RawBlockPayload, error) {
	return c.client.OpenRawBlockPayloadWithTag(ctx, tag, height, hash)
}

func (c *timeoutableClient) OpenRawBlockPayloadWithTagAndReadSource(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*RawBlockPayload, error) {
	rawPayloadClient, ok := c.client.(RawPayloadReadSourceClient)
	if !ok {
		return nil, xerrors.Errorf("client %T does not support raw payload read source override", c.client)
	}
	return rawPayloadClient.OpenRawBlockPayloadWithTagAndReadSource(ctx, tag, height, hash, readSource)
}

// OpenRawBlockPayloadsByRange passes through to the wrapped client. No timeout
// wrapping: iterator reads are user-controlled.
func (c *timeoutableClient) OpenRawBlockPayloadsByRange(ctx context.Context, startHeight uint64, endHeight uint64) (RawBlockPayloadIterator, error) {
	return c.client.OpenRawBlockPayloadsByRange(ctx, startHeight, endHeight)
}

// OpenRawBlockPayloadsByRangeWithTag passes through to the wrapped client. No
// timeout wrapping: iterator reads are user-controlled.
func (c *timeoutableClient) OpenRawBlockPayloadsByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (RawBlockPayloadIterator, error) {
	return c.client.OpenRawBlockPayloadsByRangeWithTag(ctx, tag, startHeight, endHeight)
}

func (c *timeoutableClient) OpenRawBlockPayloadsByRangeWithTagAndReadSource(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) (RawBlockPayloadIterator, error) {
	rawPayloadClient, ok := c.client.(RawPayloadReadSourceClient)
	if !ok {
		return nil, xerrors.Errorf("client %T does not support raw payload read source override", c.client)
	}
	return rawPayloadClient.OpenRawBlockPayloadsByRangeWithTagAndReadSource(ctx, tag, startHeight, endHeight, readSource)
}

func (c *timeoutableClient) GetBlockByTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) ([]*api.Block, error) {
		ctx, cancel := context.WithTimeout(ctx, c.mediumTimeout)
		defer cancel()

		return c.client.GetBlockByTransaction(ctx, tag, transactionHash)
	})
}

func (c *timeoutableClient) GetBlockByTimestamp(ctx context.Context, tag uint32, timestamp uint64) (*api.Block, error) {
	return intercept(ctx, c.logger, func(ctx context.Context) (*api.Block, error) {
		ctx, cancel := context.WithTimeout(ctx, c.mediumTimeout)
		defer cancel()

		return c.client.GetBlockByTimestamp(ctx, tag, timestamp)
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

// StreamNativeBlock passes through to the wrapped client. No timeout
// wrapping: the download + iterator lifetime is user-controlled and may
// intentionally exceed short/medium/long defaults.
func (c *timeoutableClient) StreamNativeBlock(ctx context.Context, tag uint32, height uint64, hash string, opts ...ParseOption) (NativeStreamedBlock, error) {
	return c.client.StreamNativeBlock(ctx, tag, height, hash, opts...)
}

// StreamNativeBlocksByRange passes through to the wrapped client for the
// same reason as StreamNativeBlock: the iterator's lifetime is
// user-controlled.
func (c *timeoutableClient) StreamNativeBlocksByRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, opts ...ParseOption) (NativeStreamedBlockIterator, error) {
	return c.client.StreamNativeBlocksByRange(ctx, tag, startHeight, endHeight, opts...)
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
