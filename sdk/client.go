package sdk

import (
	"context"

	"google.golang.org/grpc/codes"

	"github.com/go-playground/validator/v10"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Client interface {
		// GetTag returns the current tag used by the client.
		// It is zero by default and ChainStorage returns the latest and greatest version of the block.
		GetTag() uint32

		// SetTag sets the current tag which effectively pins the version to the specified tag.
		SetTag(tag uint32)

		// GetClientID returns the clientID which could be empty.
		GetClientID() string

		// SetClientID sets the clientID when initiate client.
		SetClientID(clientID string)

		// SetBlockValidation sets the blockValidation which by default is disabled.
		SetBlockValidation(blockValidation bool)

		// GetBlockValidation returns the blockValidation.
		GetBlockValidation() bool

		// GetLatestBlock returns the latest block height.
		// Deprecated: use GetLatestBlockWithTag instead.
		GetLatestBlock(ctx context.Context) (uint64, error)

		// GetLatestBlockWithTag returns the latest block height, tag is optional.
		// If tag is not provided, ChainStorage uses the stable tag to look up the latest height.
		GetLatestBlockWithTag(ctx context.Context, tag uint32) (uint64, error)

		// GetBlock returns the raw block. height is required and hash is optional.
		// If hash is not provided, ChainStorage returns the block on the canonical chain.
		// Deprecated: use GetBlockWithTag instead.
		GetBlock(ctx context.Context, height uint64, hash string) (*api.Block, error)

		// GetBlockWithTag returns the raw block. height is required, while tag and hash are optional.
		// If tag is not provided, ChainStorage uses the stable tag to look up the block.
		// If hash is not provided, ChainStorage returns the block on the canonical chain.
		// Note that while processing a BlockchainEvent, tag/height/hash must be provided,
		// because it is associated with a past event.
		GetBlockWithTag(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error)

		// GetBlocksByRange returns the raw blocks between [startHeight, endHeight).
		// endHeight is optional and defaults to startHeight + 1.
		GetBlocksByRange(ctx context.Context, startHeight uint64, endHeight uint64) ([]*api.Block, error)

		// GetBlocksByRangeWithTag returns the raw blocks between [startHeight, endHeight).
		// endHeight is optional and defaults to startHeight + 1.
		// tag is optional and defaults to stable tag.
		// Note: GetBlocksByRangeWithTag is not equivalent to the batch version of GetBlockWithTag since there is no way to specify the block hash,
		// and thus you may get back FailedPrecondition errors if it goes beyond current tip due to reorg, especially for streaming case.
		GetBlocksByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.Block, error)

		// OpenRawBlockPayload opens the validated, decompressed protobuf payload bytes for one raw block
		// without unmarshaling api.Block. height is required and hash is optional.
		OpenRawBlockPayload(ctx context.Context, height uint64, hash string) (*RawBlockPayload, error)

		// OpenRawBlockPayloadWithTag opens the validated, decompressed protobuf payload bytes for one raw block
		// without unmarshaling api.Block. tag and hash are optional.
		OpenRawBlockPayloadWithTag(ctx context.Context, tag uint32, height uint64, hash string) (*RawBlockPayload, error)

		// OpenRawBlockPayloadsByRange opens an iterator over raw block payloads between [startHeight, endHeight)
		// without unmarshaling api.Block. endHeight is optional and defaults to startHeight + 1.
		OpenRawBlockPayloadsByRange(ctx context.Context, startHeight uint64, endHeight uint64) (RawBlockPayloadIterator, error)

		// OpenRawBlockPayloadsByRangeWithTag opens an iterator over raw block payloads between [startHeight, endHeight)
		// without unmarshaling api.Block. tag is optional and defaults to stable tag.
		OpenRawBlockPayloadsByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (RawBlockPayloadIterator, error)

		// GetBlockByTransaction returns the raw block(s) where the transaction resides,
		// or an empty list if the transaction is not found.
		// In most networks a transaction belongs to a single block, but there are exceptions.
		// Note that this API is still experimental and may change at any time.
		GetBlockByTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*api.Block, error)

		// GetBlockByTimestamp returns the latest block before or at the given timestamp.
		// The timestamp should be a Unix timestamp (seconds since January 1, 1970 UTC).
		// If no block is found at or before the timestamp, it returns an error.
		GetBlockByTimestamp(ctx context.Context, tag uint32, timestamp uint64) (*api.Block, error)

		// StreamChainEvents streams raw blocks from ChainStorage.
		// The caller is responsible for keeping track of the sequence or sequence_num in BlockchainEvent.
		StreamChainEvents(ctx context.Context, cfg StreamingConfiguration) (<-chan *ChainEventResult, error)

		// GetChainEvents returns at most req.MaxNumEvents available chain events.
		// Returned size is not guaranteed. If no enough chain events, it will return as many as possible.
		// Either req.StartEventId or req.InitialPositionInStream should be provided.
		GetChainEvents(ctx context.Context, req *api.GetChainEventsRequest) ([]*api.BlockchainEvent, error)

		// GetChainMetadata returns chain metadata, e.g. LatestEventTag.
		GetChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error)

		// GetStaticChainMetadata returns the static chain metadata, getting from the Config, instead of querying the ChainStorage server.
		// This is useful if the caller needs a consistent snapshot of chain metadata during its current lifecycle.
		GetStaticChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error)

		// StreamNativeBlock downloads a block to a local disk spool
		// and returns a chain-agnostic NativeStreamedBlock. Callers
		// pick the chain-matching accessor (GetBitcoin, GetEthereum,
		// ...) to iterate. Accessors that don't match the configured
		// chain — or that correspond to chains whose streaming walker
		// hasn't landed yet — return nil; callers should fall back to
		// GetBlock + ParseNativeBlock in that case.
		//
		// The returned stream owns the disk spool. Callers MUST call
		// Close() when done. A runtime cleanup is wired as a safety
		// net but should not be relied on.
		StreamNativeBlock(ctx context.Context, tag uint32, height uint64, hash string, opts ...ParseOption) (NativeStreamedBlock, error)

		// StreamNativeBlocksByRange returns an iterator of
		// NativeStreamedBlocks over [startHeight, endHeight) in height
		// order (endHeight 0 means startHeight+1). Consecutive blocks
		// stored in the same consolidated chunk are decompressed once,
		// which is what a walk over Solana history needs: calling
		// StreamNativeBlock per slot re-reads each 25-block chunk for
		// every slot. opts (e.g. WithTransactionFilter) apply to every
		// block. The range is bounded by the server's MaxNumBlockFiles;
		// page longer walks. Callers MUST close every returned block and
		// the iterator.
		//
		// Blob reads are lazy (they happen in Next), so the DEFAULT ->
		// SINGLE_BLOCK read-source fallback here covers only the metadata
		// lookup, unlike StreamNativeBlock, whose fallback also covers the
		// download. A block whose consolidated object cannot be read
		// surfaces from Next as an error (never as io.EOF) after the
		// downloader's retries; the iterator is then finished.
		StreamNativeBlocksByRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, opts ...ParseOption) (NativeStreamedBlockIterator, error)
	}

	ReadSourceClient interface {
		// GetBlockWithTagAndReadSource returns the raw block using the requested blob read source.
		GetBlockWithTagAndReadSource(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*api.Block, error)

		// GetBlocksByRangeWithTagAndReadSource returns raw blocks using the requested blob read source.
		GetBlocksByRangeWithTagAndReadSource(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) ([]*api.Block, error)
	}

	RawPayloadReadSourceClient interface {
		// OpenRawBlockPayloadWithTagAndReadSource opens raw block payload bytes using the requested blob read source.
		OpenRawBlockPayloadWithTagAndReadSource(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*RawBlockPayload, error)

		// OpenRawBlockPayloadsByRangeWithTagAndReadSource opens raw block payload bytes using the requested blob read source.
		OpenRawBlockPayloadsByRangeWithTagAndReadSource(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) (RawBlockPayloadIterator, error)
	}

	ChainEventResult struct {
		BlockchainEvent *api.BlockchainEvent
		Block           *api.Block
		Error           error
	}

	clientImpl struct {
		logger          *zap.Logger
		config          *config.Config
		blockDownloader downloader.BlockDownloader
		client          gateway.Client
		parser          Parser
		clientID        string
		tag             uint32
		blockValidation bool
		retry           retry.Retry
		validate        *validator.Validate
	}

	clientParams struct {
		fx.In
		Logger          *zap.Logger
		Config          *config.Config
		BlockDownloader downloader.BlockDownloader
		Client          gateway.Client
		Parser          parser.Parser
	}
)

func newClient(params clientParams) (Client, error) {
	logger := log.WithPackage(params.Logger)
	var client Client = &clientImpl{
		logger:          logger,
		config:          params.Config,
		blockDownloader: params.BlockDownloader,
		client:          params.Client,
		parser:          params.Parser,
		tag:             0, // by default, let the server decide the tag.
		retry:           retry.New(),
		validate:        validator.New(),
	}

	client = WithTimeoutableClientInterceptor(client, logger)
	return client, nil
}

func (c *clientImpl) GetTag() uint32 {
	return c.tag
}

func (c *clientImpl) SetTag(tag uint32) {
	c.tag = tag
}

func (c *clientImpl) GetClientID() string {
	return c.clientID
}

func (c *clientImpl) SetClientID(clientID string) {
	c.clientID = clientID
}

func (c *clientImpl) SetBlockValidation(blockValidation bool) {
	c.blockValidation = blockValidation
}

func (c *clientImpl) GetBlockValidation() bool {
	return c.blockValidation
}

func (c *clientImpl) GetLatestBlock(ctx context.Context) (uint64, error) {
	return c.GetLatestBlockWithTag(ctx, c.tag)
}

func (c *clientImpl) GetLatestBlockWithTag(ctx context.Context, tag uint32) (uint64, error) {
	resp, err := c.client.GetLatestBlock(ctx, &api.GetLatestBlockRequest{
		Tag: tag,
	})
	if err != nil {
		return 0, xerrors.Errorf("failed to get latest block height (tag=%v): %w", tag, err)
	}
	return resp.Height, nil
}

func (c *clientImpl) GetBlock(ctx context.Context, height uint64, hash string) (*api.Block, error) {
	return c.GetBlockWithTag(ctx, c.tag, height, hash)
}

func (c *clientImpl) GetBlockWithTag(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error) {
	return c.GetBlockWithTagAndReadSource(ctx, tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
}

func (c *clientImpl) GetBlockWithTagAndReadSource(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*api.Block, error) {
	block, err := c.downloadBlock(ctx, tag, height, hash, readSource)
	if err == nil || !shouldRetrySingleBlockRead(readSource) {
		return block, err
	}

	c.logger.Warn(
		"preferred block download failed; retrying single-block",
		zap.Uint32("tag", tag),
		zap.Uint64("height", height),
		zap.String("hash", hash),
		zap.String("read_source", readSource.String()),
		zap.Error(err),
	)
	block, fallbackErr := c.downloadBlock(ctx, tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
	if fallbackErr != nil {
		return nil, xerrors.Errorf("failed to download consolidated block and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
	}
	return block, nil
}

func (c *clientImpl) GetBlocksByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	return c.GetBlocksByRangeWithTagAndReadSource(ctx, tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
}

func (c *clientImpl) GetBlocksByRangeWithTagAndReadSource(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) ([]*api.Block, error) {
	if endHeight == 0 {
		endHeight = startHeight + 1
	}

	blocks, err := c.downloadBlocksByRange(ctx, tag, startHeight, endHeight, readSource)
	if err == nil || !shouldRetrySingleBlockRead(readSource) {
		return blocks, err
	}

	c.logger.Warn(
		"preferred block range download failed; retrying single-block",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.String("read_source", readSource.String()),
		zap.Error(err),
	)
	blocks, fallbackErr := c.downloadBlocksByRange(ctx, tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
	if fallbackErr != nil {
		return nil, xerrors.Errorf("failed to download consolidated block range and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
	}
	return blocks, nil
}

func (c *clientImpl) downloadBlocksByRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) ([]*api.Block, error) {
	resp, err := c.client.GetBlockFilesByRange(ctx, &api.GetBlockFilesByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
		ReadSource:  readSource,
	})

	if err != nil {
		return nil, xerrors.Errorf("failed to get block file metadata (tag=%d, startHeight=%d, endHeight=%d): %w", tag, startHeight, endHeight, err)
	}
	if len(resp.GetFiles()) == 0 {
		return nil, xerrors.Errorf("no block file metadata found")
	}
	blockFiles := resp.GetFiles()

	for _, blockFile := range blockFiles {
		c.logger.Debug(
			"downloading block",
			zap.Uint32("tag", blockFile.Tag),
			zap.Uint64("height", blockFile.Height),
			zap.String("hash", blockFile.Hash),
		)
	}

	blocks, err := c.blockDownloader.DownloadMany(ctx, blockFiles)
	if err != nil {
		return nil, xerrors.Errorf("failed to download block files: %w", err)
	}
	if len(blocks) != len(blockFiles) {
		return nil, xerrors.Errorf("downloaded block count mismatch: got %d want %d", len(blocks), len(blockFiles))
	}

	for i := range blockFiles {
		if c.blockValidation {
			blockFile := blockFiles[i]
			err := c.validateBlock(ctx, blocks[i])
			if err != nil {
				return nil, xerrors.Errorf("failed to validate block (blockHeight=%v, blockHash=%v): %w", blockFile.Height, blockFile.Hash, err)
			}
		}
	}

	return blocks, nil
}

func (c *clientImpl) GetBlocksByRange(ctx context.Context, startHeight uint64, endHeight uint64) ([]*api.Block, error) {
	return c.GetBlocksByRangeWithTag(ctx, c.tag, startHeight, endHeight)
}

func (c *clientImpl) OpenRawBlockPayload(ctx context.Context, height uint64, hash string) (*RawBlockPayload, error) {
	return c.OpenRawBlockPayloadWithTag(ctx, c.tag, height, hash)
}

func (c *clientImpl) OpenRawBlockPayloadWithTag(ctx context.Context, tag uint32, height uint64, hash string) (*RawBlockPayload, error) {
	return c.OpenRawBlockPayloadWithTagAndReadSource(ctx, tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
}

func (c *clientImpl) OpenRawBlockPayloadWithTagAndReadSource(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*RawBlockPayload, error) {
	payload, err := c.openRawBlockPayload(ctx, tag, height, hash, readSource)
	if err == nil || !shouldRetrySingleBlockRead(readSource) {
		return payload, err
	}

	c.logger.Warn(
		"preferred raw block payload open failed; retrying single-block",
		zap.Uint32("tag", tag),
		zap.Uint64("height", height),
		zap.String("hash", hash),
		zap.String("read_source", readSource.String()),
		zap.Error(err),
	)
	payload, fallbackErr := c.openRawBlockPayload(ctx, tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
	if fallbackErr != nil {
		return nil, xerrors.Errorf("failed to open consolidated raw block payload and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
	}
	return payload, nil
}

func (c *clientImpl) openRawBlockPayload(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*RawBlockPayload, error) {
	blockFile, err := c.client.GetBlockFile(ctx, &api.GetBlockFileRequest{
		Tag:        tag,
		Height:     height,
		Hash:       hash,
		ReadSource: readSource,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to query block file (tag=%v, height=%v, hash=%v): %w", tag, height, hash, err)
	}

	payload, err := c.blockDownloader.OpenRawBlockPayload(ctx, blockFile.GetFile())
	if err != nil {
		return nil, xerrors.Errorf("failed to open raw block payload (blockFile={%+v}): %w", blockFile.GetFile(), err)
	}
	return wrapRawBlockPayload(payload), nil
}

func (c *clientImpl) OpenRawBlockPayloadsByRange(ctx context.Context, startHeight uint64, endHeight uint64) (RawBlockPayloadIterator, error) {
	return c.OpenRawBlockPayloadsByRangeWithTag(ctx, c.tag, startHeight, endHeight)
}

func (c *clientImpl) OpenRawBlockPayloadsByRangeWithTag(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (RawBlockPayloadIterator, error) {
	return c.OpenRawBlockPayloadsByRangeWithTagAndReadSource(ctx, tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
}

func (c *clientImpl) OpenRawBlockPayloadsByRangeWithTagAndReadSource(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) (RawBlockPayloadIterator, error) {
	if endHeight == 0 {
		endHeight = startHeight + 1
	}

	iter, err := c.openRawBlockPayloadsByRange(ctx, tag, startHeight, endHeight, readSource)
	if err == nil || !shouldRetrySingleBlockRead(readSource) {
		return iter, err
	}

	c.logger.Warn(
		"preferred raw block payload range open failed; retrying single-block",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.String("read_source", readSource.String()),
		zap.Error(err),
	)
	iter, fallbackErr := c.openRawBlockPayloadsByRange(ctx, tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
	if fallbackErr != nil {
		return nil, xerrors.Errorf("failed to open consolidated raw block payload range and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
	}
	return iter, nil
}

func (c *clientImpl) openRawBlockPayloadsByRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) (RawBlockPayloadIterator, error) {
	resp, err := c.client.GetBlockFilesByRange(ctx, &api.GetBlockFilesByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
		ReadSource:  readSource,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get block file metadata (tag=%d, startHeight=%d, endHeight=%d): %w", tag, startHeight, endHeight, err)
	}
	blockFiles := resp.GetFiles()
	if len(blockFiles) == 0 {
		return nil, xerrors.Errorf("no block file metadata found")
	}

	iter, err := c.blockDownloader.OpenRawBlockPayloads(ctx, blockFiles)
	if err != nil {
		return nil, xerrors.Errorf("failed to open raw block payload iterator: %w", err)
	}
	return &rawBlockPayloadIterator{inner: iter}, nil
}

func shouldRetrySingleBlockRead(readSource api.BlockReadSource) bool {
	switch readSource {
	case api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT,
		api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED:
		return true
	default:
		return false
	}
}

func (c *clientImpl) StreamChainEvents(ctx context.Context, cfg StreamingConfiguration) (<-chan *ChainEventResult, error) {
	if err := c.validate.Struct(cfg); err != nil {
		return nil, xerrors.Errorf("invalid config: %w", err)
	}

	// initiate streaming API call
	stream, err := c.client.StreamChainEvents(ctx, cfg.ChainEventsRequest)
	if err != nil {
		return nil, xerrors.Errorf("failed to call StreamChainEvents (cfg={%+v}): %w", cfg, err)
	}

	// defaults to 1 if not set
	if cfg.ChannelBufferCapacity == 0 {
		cfg.ChannelBufferCapacity = 1
	}

	// initiate channel
	ch := make(chan *ChainEventResult, cfg.ChannelBufferCapacity)

	// start streaming
	go c.streamBlocks(ctx, &cfg, stream, ch)

	return ch, nil
}

func (c *clientImpl) streamBlocks(
	ctx context.Context,
	cfg *StreamingConfiguration,
	stream api.ChainStorage_StreamChainEventsClient,
	ch chan *ChainEventResult,
) {
	defer close(ch)

	request := proto.Clone(cfg.ChainEventsRequest).(*api.ChainEventsRequest)
	for i := uint64(0); cfg.NumberOfEvents == 0 || i < cfg.NumberOfEvents; i++ {
		var event *api.BlockchainEvent
		if err := c.retry.Retry(ctx, func(ctx context.Context) error {
			resp, err := stream.Recv()
			if err != nil {
				if request.Sequence == "" && request.InitialPositionInStream != "" {
					// Fail fast if InitialPositionInStream is specified,
					// because we do not know how to reconnect the stream in this case.
					return err
				}

				if !c.isTransientStreamError(err) {
					// Fail fast if the error is NOT transient.
					return err
				}

				// In the event of a transient error,
				// reconnect the stream using the previous sequence.
				// Note that it is not safe to retry stream.Recv() without creating a new stream, because:
				// 1. the connection may already be broken at this point;
				// 2. stream.Recv() is not idempotent and retry may result in duplicate or missing events.
				c.logger.Info(
					"reconnecting stream",
					zap.Error(err),
					zap.Reflect("request", request),
				)

				newStream, newErr := c.client.StreamChainEvents(ctx, request)
				if newErr != nil {
					c.logger.Warn(
						"failed to reconnect stream",
						zap.Error(newErr),
						zap.Reflect("request", request),
					)
					return err
				}

				stream = newStream
				return retry.Retryable(err)
			}

			event = resp.GetEvent()
			return nil
		}); err != nil {
			c.sendBlockResult(ctx, ch, &ChainEventResult{
				Error: xerrors.Errorf("failed to receive from event stream (cfg={%+v}, request={%+v}): %w", cfg, request, err),
			})
			return
		}

		if event == nil {
			c.sendBlockResult(ctx, ch, &ChainEventResult{
				Error: xerrors.Errorf("received null event (cfg={%+v}, request={%+v})", cfg, request),
			})
			return
		}

		// block is omitted if EventOnly is specified.
		var block *api.Block
		if !cfg.EventOnly {
			var err error
			blockID := event.GetBlock()
			block, err = c.GetBlockWithTagAndReadSource(ctx, blockID.GetTag(), blockID.GetHeight(), blockID.GetHash(), api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
			if err != nil {
				c.sendBlockResult(ctx, ch, &ChainEventResult{
					Error: xerrors.Errorf("failed to download block (cfg={%+v}, request={%+v}, event={%+v}): %w", cfg, request, event, err),
				})
				return
			}
		}

		if ok := c.sendBlockResult(ctx, ch, &ChainEventResult{
			BlockchainEvent: event,
			Block:           block,
		}); !ok {
			return
		}

		request.Sequence = event.Sequence
		request.SequenceNum = event.SequenceNum
	}
}

func (c *clientImpl) sendBlockResult(
	ctx context.Context,
	ch chan *ChainEventResult,
	result *ChainEventResult,
) bool {
	select {
	case <-ctx.Done():
		// caller may have ended the context either deadline or cancel
		c.logger.Debug("sendBlockResult context done")
		return false
	case ch <- result:
		return true
	}
}

func (c *clientImpl) downloadBlock(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*api.Block, error) {
	c.logger.Debug(
		"downloading block",
		zap.Uint32("tag", tag),
		zap.Uint64("height", height),
		zap.String("hash", hash),
	)
	blockFile, err := c.client.GetBlockFile(ctx, &api.GetBlockFileRequest{
		Tag:        tag,
		Height:     height,
		Hash:       hash,
		ReadSource: readSource,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to query block file (tag=%v, height=%v, hash=%v): %w", tag, height, hash, err)
	}

	rawBlock, err := c.blockDownloader.Download(ctx, blockFile.File)
	if err != nil {
		return nil, xerrors.Errorf("failed download blockFile (blockFile={%+v}): %w", blockFile.File, err)
	}

	if c.blockValidation {
		err = c.validateBlock(ctx, rawBlock)
		if err != nil {
			return nil, xerrors.Errorf("failed to validate block (blockHeight=%v, blockHash=%v): %w", height, hash, err)
		}
	}

	return rawBlock, nil
}

func (c *clientImpl) isTransientStreamError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}

	// Typical errors include:
	// - "rpc error: code = Internal desc = unexpected EOF"
	//   This error occurs when ChainStorage is re-deployed.
	// - "rpc error: code = Internal desc = stream terminated by RST_STREAM with error code: INTERNAL_ERROR"
	//   This error occurs periodically, yet no error is emitted by the handler of StreamChainEvents.
	//   It is likely caused by networking issues or gRPC internal errors.
	// - "rpc error: code = Unavailable desc = closing transport due to: connection error: desc = \"error reading from server: EOF\", received prior goaway: code: NO_ERROR"
	return gateway.IsRetryableCode(s.Code())
}

func (c *clientImpl) GetChainEvents(ctx context.Context, req *api.GetChainEventsRequest) ([]*api.BlockchainEvent, error) {
	resp, err := c.client.GetChainEvents(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain events (req={%+v}): %w", req, err)
	}

	return resp.Events, nil
}

func (c *clientImpl) GetChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
	resp, err := c.client.GetChainMetadata(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain metadata (req={%+v}): %w", req, err)
	}

	return resp, nil
}

func (c *clientImpl) GetStaticChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
	return c.config.GetChainMetadataHelper(req)
}

func (c *clientImpl) GetBlockByTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*api.Block, error) {
	resp, err := c.client.GetBlockByTransaction(ctx, &api.GetBlockByTransactionRequest{
		Tag:             tag,
		TransactionHash: transactionHash,
	})
	if err != nil {
		var grpcErr gateway.GrpcError
		if xerrors.As(err, &grpcErr) && grpcErr.GRPCStatus().Code() == codes.NotFound {
			return []*api.Block{}, nil
		}

		return nil, xerrors.Errorf("failed to find blocks by transaction (%v): %w", transactionHash, err)
	}

	blockIds := resp.Blocks
	if len(blockIds) == 0 {
		return []*api.Block{}, nil
	}

	// Parallel download is unnecessary
	// because a transaction belongs to a single block in most cases.
	blocks := make([]*api.Block, len(blockIds))
	for i, blockId := range blockIds {
		block, err := c.GetBlockWithTagAndReadSource(ctx, blockId.Tag, blockId.Height, blockId.Hash, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
		if err != nil {
			return nil, xerrors.Errorf("failed to download block data: %w", err)
		}

		blocks[i] = block
	}

	return blocks, nil
}

func (c *clientImpl) GetBlockByTimestamp(ctx context.Context, tag uint32, timestamp uint64) (*api.Block, error) {
	resp, err := c.client.GetBlockByTimestamp(ctx, &api.GetBlockByTimestampRequest{
		Tag:       tag,
		Timestamp: timestamp,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get block by timestamp (tag=%v, timestamp=%v): %w", tag, timestamp, err)
	}

	// Download the block data using the metadata from the response
	block, err := c.GetBlockWithTagAndReadSource(ctx, resp.Tag, resp.Height, resp.Hash, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
	if err != nil {
		return nil, xerrors.Errorf("failed to download block data: %w", err)
	}

	return block, nil
}

// streamingParser is an unexported mirror of the internal parser's
// streaming method. The injected c.parser always implements this (it
// comes from the internal parser impl), but sdk.Parser is deliberately
// trimmed to exclude it — consumers stream via Client.StreamNativeBlock
// rather than holding a Parser + constructing a SpooledBlock themselves.
type streamingParser interface {
	ParseStreamNative(ctx context.Context, spooled *downloader.SpooledBlock, opts ...ParseOption) (NativeStreamedBlock, error)
}

func (c *clientImpl) StreamNativeBlock(
	ctx context.Context,
	tag uint32, height uint64, hash string,
	opts ...ParseOption,
) (NativeStreamedBlock, error) {
	spooled, err := c.downloadBlockStream(ctx, tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
	if err != nil {
		c.logger.Warn(
			"preferred native block stream download failed; retrying single-block",
			zap.Uint32("tag", tag),
			zap.Uint64("height", height),
			zap.String("hash", hash),
			zap.Error(err),
		)
		var fallbackErr error
		spooled, fallbackErr = c.downloadBlockStream(ctx, tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
		if fallbackErr != nil {
			return nil, xerrors.Errorf("failed to download preferred native block stream and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
		}
	}

	sp, ok := c.parser.(streamingParser)
	if !ok {
		_ = spooled.Close()
		return nil, xerrors.Errorf("parser %T does not support streaming", c.parser)
	}
	stream, err := sp.ParseStreamNative(ctx, spooled, opts...)
	if err != nil {
		_ = spooled.Close()
		return nil, xerrors.Errorf("failed to create native stream: %w", err)
	}
	return stream, nil
}

func (c *clientImpl) StreamNativeBlocksByRange(
	ctx context.Context,
	tag uint32, startHeight uint64, endHeight uint64,
	opts ...ParseOption,
) (NativeStreamedBlockIterator, error) {
	if endHeight == 0 {
		endHeight = startHeight + 1
	}
	sp, ok := c.parser.(streamingParser)
	if !ok {
		return nil, xerrors.Errorf("parser %T does not support streaming", c.parser)
	}

	inner, err := c.openSpooledBlocksByRange(ctx, tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT)
	if err != nil {
		c.logger.Warn(
			"preferred native block range open failed; retrying single-block",
			zap.Uint32("tag", tag),
			zap.Uint64("start_height", startHeight),
			zap.Uint64("end_height", endHeight),
			zap.Error(err),
		)
		var fallbackErr error
		inner, fallbackErr = c.openSpooledBlocksByRange(ctx, tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
		if fallbackErr != nil {
			return nil, xerrors.Errorf("failed to open preferred native block range and single-block fallback failed (originalErr=%v): %w", err, fallbackErr)
		}
	}
	return &nativeStreamedBlockIterator{inner: inner, parser: sp, opts: opts}, nil
}

func (c *clientImpl) openSpooledBlocksByRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) (downloader.SpooledBlockIterator, error) {
	resp, err := c.client.GetBlockFilesByRange(ctx, &api.GetBlockFilesByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
		ReadSource:  readSource,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get block file metadata (tag=%d, startHeight=%d, endHeight=%d): %w", tag, startHeight, endHeight, err)
	}
	blockFiles := resp.GetFiles()
	if len(blockFiles) == 0 {
		return nil, xerrors.Errorf("no block file metadata found")
	}
	inner, err := c.blockDownloader.OpenSpooledBlocks(ctx, blockFiles)
	if err != nil {
		return nil, xerrors.Errorf("failed to open spooled block iterator: %w", err)
	}
	return inner, nil
}

func (c *clientImpl) downloadBlockStream(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*downloader.SpooledBlock, error) {
	blockFileResp, err := c.client.GetBlockFile(ctx, &api.GetBlockFileRequest{
		Tag:        tag,
		Height:     height,
		Hash:       hash,
		ReadSource: readSource,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to query block file (tag=%v, height=%v, hash=%v): %w", tag, height, hash, err)
	}

	spooled, err := c.blockDownloader.DownloadStream(ctx, blockFileResp.GetFile())
	if err != nil {
		return nil, err
	}
	return spooled, nil
}

func (c *clientImpl) validateBlock(ctx context.Context, rawBlock *api.Block) error {
	hash := rawBlock.GetMetadata().GetHash()
	height := rawBlock.GetMetadata().GetHeight()

	nativeBlock, err := c.parser.ParseNativeBlock(ctx, rawBlock)
	if err != nil {
		return xerrors.Errorf("failed to parse native format for block (blockHeight=%v, blockHash=%v): %w", height, hash, err)
	}

	err = c.parser.ValidateBlock(ctx, nativeBlock)
	if err != nil {
		return xerrors.Errorf("failed to validate block (blockHeight=%v, blockHash=%v): %w", height, hash, err)
	}

	return nil
}
