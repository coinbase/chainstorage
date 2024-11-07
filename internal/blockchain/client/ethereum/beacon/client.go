package beacon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	parser "github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/beacon"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Client struct {
		config   *config.Config
		logger   *zap.Logger
		client   restapi.Client
		validate *validator.Validate
	}

	blockHeaderResultHolder struct {
		metadata *api.BlockMetadata
		rawJson  json.RawMessage // Store the raw message in blob storage.
	}
)

const (
	getBlockHeaderMethodName = "GetBlockHeader"
	getBlockMethodName       = "GetBlock"
	getBlockBlobsMethodName  = "GetBlockBlobs"

	getLatestBlockHeaderMethodPath = "/eth/v1/beacon/headers/head"
	getBlockHeaderMethodPath       = "/eth/v1/beacon/headers/%v"
	getBlockMethodPath             = "/eth/v2/beacon/blocks/%v"
	getBlockBlobsMethodPath        = "/eth/v1/beacon/blob_sidecars/%v"

	getBlockHeaderMethodTimeout = 15 * time.Second
	getBlockMethodTimeout       = 30 * time.Second
	getBlockBlobsMethodTimeout  = 30 * time.Second
)

var (
	// genesisBlockTimestamp is the timestamp of the genesis block of the Ethereum beacon chain.
	// It is used to calculate the timestamp of a given block, since block response does not include timestamp values.
	genesisBlockTimestamp = map[common.Network]int64{
		common.Network_NETWORK_ETHEREUM_HOLESKY: 1695902400,
		common.Network_NETWORK_ETHEREUM_MAINNET: 1606824023,
	}
)

var _ internal.Client = (*Client)(nil)

func NewClientFactory(params internal.RestapiClientParams) internal.ClientFactory {
	return internal.NewRestapiClientFactory(params, func(client restapi.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &Client{
			config:   params.Config,
			logger:   logger,
			client:   client,
			validate: validator.New(),
		}
	})
}

func (c *Client) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, fmt.Errorf("invalid height range range of [%d, %d)", from, to)
	}

	numBlocks := int(to - from)
	blockMetadatas := make([]*api.BlockMetadata, numBlocks)

	for i := 0; i < numBlocks; i++ {
		height := from + uint64(i)

		headerResult, err := c.getHeaderByHeight(ctx, tag, height)
		if err != nil {
			return nil, fmt.Errorf("failed to get block header (height=%d) in BatchGetBlockMetadata: %w", height, err)
		}

		blockMetadatas[i] = headerResult.metadata
	}

	return blockMetadatas, nil
}

func (c *Client) getHeaderByHeight(ctx context.Context, tag uint32, height uint64) (*blockHeaderResultHolder, error) {
	getBlockHeaderMethod := &restapi.RequestMethod{
		Name:       getBlockHeaderMethodName,
		ParamsPath: fmt.Sprintf(getBlockHeaderMethodPath, height),
		Timeout:    getBlockHeaderMethodTimeout,
	}

	result, err := c.getHeader(ctx, tag, height, getBlockHeaderMethod)
	if err != nil {
		return nil, fmt.Errorf("failed to get block header by height (height=%d): %w", height, err)
	}
	return result, nil
}

func (c *Client) getHeaderByHash(ctx context.Context, tag uint32, height uint64, hash string) (*blockHeaderResultHolder, error) {
	getBlockHeaderMethod := &restapi.RequestMethod{
		Name:       getBlockHeaderMethodName,
		ParamsPath: fmt.Sprintf(getBlockHeaderMethodPath, hash),
		Timeout:    getBlockHeaderMethodTimeout,
	}

	result, err := c.getHeader(ctx, tag, height, getBlockHeaderMethod)
	if err != nil {
		return nil, fmt.Errorf("failed to get block header by hash (height=%d, hash=%v): %w", height, hash, err)
	}
	return result, nil
}

func (c *Client) getHeader(ctx context.Context, tag uint32, height uint64, method *restapi.RequestMethod) (*blockHeaderResultHolder, error) {
	response, err := c.client.Call(ctx, method, nil)
	if err != nil {
		callErr := handleCallError(err)
		// Beacon client returns `NOT_FOUND` error when query a missed/orphaned block
		if !errors.Is(callErr, internal.ErrBlockNotFound) {
			return nil, fmt.Errorf("failed to get header for block=%d: %w", height, err)
		}

		blockTimestamp, err := c.getBlockTimestamp(height)
		if err != nil {
			return nil, fmt.Errorf("failed to get timestamp of block=%d: %w", height, err)
		}

		return &blockHeaderResultHolder{
			metadata: &api.BlockMetadata{
				Tag:       tag,
				Height:    height,
				Skipped:   true,
				Timestamp: blockTimestamp,
			},
			rawJson: nil,
		}, nil
	}

	var header parser.BlockHeader
	if err := json.Unmarshal(response, &header); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header result for block=%d: %w", height, err)
	}

	if err := c.validate.Struct(header); err != nil {
		return nil, fmt.Errorf("failed to parse block=%d header: %w", height, err)
	}

	metadata, err := c.parseHeader(tag, height, &header)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block header for height=%d: %w", height, err)
	}

	return &blockHeaderResultHolder{
		metadata: metadata,
		rawJson:  response,
	}, nil
}

func (c *Client) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)

	header, err := c.getHeaderByHeight(ctx, tag, height)
	if err != nil {
		return nil, fmt.Errorf("failed to get block header (height=%d) in GetBlockByHeight: %w", height, err)
	}

	// Skip the `getBlock` call if it is a skipped block and return `Blobdata` as `nil`.
	if header.metadata.Skipped {
		return &api.Block{
			Blockchain: c.config.Chain.Blockchain,
			Network:    c.config.Chain.Network,
			SideChain:  c.config.Chain.Sidechain,
			Metadata:   header.metadata,
			Blobdata:   nil,
		}, nil
	}

	if header.metadata.Height != height {
		return nil, fmt.Errorf("get inconsistent block heights, expected: %v, actual: %v", height, header.metadata.Height)
	}

	hash := header.metadata.Hash
	// Get the block data by hash.
	block, err := c.getBlock(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block (height=%d, hash=%v) in GetBlockByHeight: %w", height, hash, err)
	}

	// Get the blob data by hash.
	blobs, err := c.getBlobs(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block blobs (height=%d, hash=%v) in GetBlockByHeight: %w", height, hash, err)
	}

	return &api.Block{
		Blockchain: c.config.Chain.Blockchain,
		Network:    c.config.Chain.Network,
		SideChain:  c.config.Chain.Sidechain,
		Metadata:   header.metadata,
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: header.rawJson,
				Block:  block,
				Blobs:  blobs,
			},
		},
	}, nil
}

func (c *Client) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, ops ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, ops...)

	// When hash is empty, the block is skipped.
	// Return a skipped block data directly
	if hash == "" {
		blockTimestamp, err := c.getBlockTimestamp(height)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate timestamp of block=%d: %w", height, err)
		}

		return &api.Block{
			Blockchain: c.config.Chain.Blockchain,
			Network:    c.config.Chain.Network,
			SideChain:  c.config.Chain.Sidechain,
			Metadata: &api.BlockMetadata{
				Tag:       tag,
				Height:    height,
				Skipped:   true,
				Timestamp: blockTimestamp,
			},
			Blobdata: nil,
		}, nil
	}

	// Get the block header by hash.
	header, err := c.getHeaderByHash(ctx, tag, height, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block header (height=%d, hash=%v) in GetBlockByHash: %w", height, hash, err)
	}

	if header.metadata.Skipped {
		// Convert this error into internal.ErrBlockNotFound so that the syncer could fall back to the master client.
		return nil, fmt.Errorf("block header (height=%d, hash=%v) not found: %w", height, hash, internal.ErrBlockNotFound)
	}

	if header.metadata.Hash != hash {
		return nil, fmt.Errorf("get inconsistent block hashes, expected: %v, actual: %v", hash, header.metadata.Hash)
	}

	if header.metadata.Height != height {
		return nil, fmt.Errorf("get inconsistent block heights, expected: %v, actual: %v", height, header.metadata.Height)
	}

	// Get the block data by hash.
	block, err := c.getBlock(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block (height=%d, hash=%v) in GetBlockByHash: %w", height, hash, err)
	}

	blobs, err := c.getBlobs(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block blobs (height=%d, hash=%v) in GetBlockByHash: %w", height, hash, err)
	}

	return &api.Block{
		Blockchain: c.config.Chain.Blockchain,
		Network:    c.config.Chain.Network,
		SideChain:  c.config.Chain.Sidechain,
		Metadata:   header.metadata,
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: header.rawJson,
				Block:  block,
				Blobs:  blobs,
			},
		},
	}, nil
}

func (c *Client) GetLatestHeight(ctx context.Context) (uint64, error) {
	latestBlockHeaderRequest := &restapi.RequestMethod{
		Name:       getBlockHeaderMethodName,
		ParamsPath: getLatestBlockHeaderMethodPath,
		Timeout:    getBlockHeaderMethodTimeout,
	}

	response, err := c.client.Call(ctx, latestBlockHeaderRequest, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block header: %w", err)
	}

	var result parser.BlockHeader
	if err := json.Unmarshal(response, &result); err != nil {
		return 0, fmt.Errorf("failed to unmarshal beacon header result: %w", err)
	}

	if err := c.validate.Struct(result); err != nil {
		return 0, fmt.Errorf("failed to parse latest block header: %w", err)
	}

	blockHeader := result.Data.Header

	return blockHeader.Message.Slot.Value(), nil
}

func (c *Client) parseHeader(tag uint32, height uint64, header *parser.BlockHeader) (*api.BlockMetadata, error) {
	headerData := header.Data
	blockHeaderMessage := headerData.Header.Message

	slot := blockHeaderMessage.Slot.Value()
	if height != slot {
		return nil, fmt.Errorf("get inconsistent block heights, expected: %v, actual: %v", height, slot)
	}

	blockTimestamp, err := c.getBlockTimestamp(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get timestamp of block=%d: %w", height, err)
	}

	return &api.BlockMetadata{
		Tag:          tag,
		Hash:         headerData.Root,
		ParentHash:   blockHeaderMessage.ParentRoot,
		Height:       slot,
		ParentHeight: 0, // No parent height in header response
		Skipped:      false,
		Timestamp:    blockTimestamp,
	}, nil
}

func (c *Client) getBlock(ctx context.Context, hash string) (json.RawMessage, error) {
	if hash == "" {
		return nil, fmt.Errorf("unexpected empty block hash")
	}

	ethBeaconGetBlockMethod := &restapi.RequestMethod{
		Name:       getBlockMethodName,
		ParamsPath: fmt.Sprintf(getBlockMethodPath, hash),
		Timeout:    getBlockMethodTimeout,
	}

	response, err := retry.WrapWithResult(ctx, func(ctx context.Context) (json.RawMessage, error) {
		response, err := c.client.Call(ctx, ethBeaconGetBlockMethod, nil)
		if err != nil {
			callErr := handleCallError(err)
			if errors.Is(callErr, internal.ErrBlockNotFound) {
				return nil, retry.Retryable(fmt.Errorf("failed to get block of blockHash=%v: %w", hash, callErr))
			}

			return nil, fmt.Errorf("failed to get block of blockHash=%v: %w", hash, err)
		}

		return response, nil
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *Client) getBlobs(ctx context.Context, hash string) (json.RawMessage, error) {
	if hash == "" {
		return nil, fmt.Errorf("unexpected empty block hash")
	}

	ethBeaconGetBlockBlobsMethod := &restapi.RequestMethod{
		Name:       getBlockBlobsMethodName,
		ParamsPath: fmt.Sprintf(getBlockBlobsMethodPath, hash),
		Timeout:    getBlockBlobsMethodTimeout,
	}

	// Post Dencun upgrade, the response of blobs have following several cases:
	// 1. Normal blocks without blobs: the response is empty list (`{"data":[]}`)
	// 2. Normal blocks with blobs: the response is a list of blobs
	// 3. Blocks haven't been synced on nodes: the response is `NOT_FOUND` error
	// 4. Skipped blocks: the response is `NOT_FOUND` error
	response, err := retry.WrapWithResult(ctx, func(ctx context.Context) (json.RawMessage, error) {
		response, err := c.client.Call(ctx, ethBeaconGetBlockBlobsMethod, nil)
		if err != nil {
			callErr := handleCallError(err)
			if errors.Is(callErr, internal.ErrBlockNotFound) {
				return nil, retry.Retryable(fmt.Errorf("failed to get blob data of blockHash=%v: %w", hash, callErr))
			}

			return nil, fmt.Errorf("failed to get blob data of blockHash=%v: %w", hash, err)
		}

		return response, nil
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *Client) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	return nil, internal.ErrNotImplemented
}

func (c *Client) CanReprocess(tag uint32, height uint64) bool {
	return false
}

func (c *Client) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return nil, internal.ErrNotImplemented
}

func handleCallError(callErr error) error {
	var errHTTP *restapi.HTTPError
	if !errors.As(callErr, &errHTTP) {
		return callErr
	}

	if errHTTP.Code == http.StatusNotFound {
		return internal.ErrBlockNotFound
	}

	return callErr
}

func (c *Client) getBlockTimestamp(height uint64) (*timestamp.Timestamp, error) {
	network := c.config.Chain.Network
	blockTime := uint64(c.config.Chain.BlockTime.Seconds())
	genesis, ok := genesisBlockTimestamp[network]
	if !ok {
		return nil, fmt.Errorf("failed to get genesis block timestamp for network=%v", network)
	}

	timeDelta := height * blockTime
	if timeDelta > uint64(math.MaxInt64)-uint64(genesis) {
		return nil, fmt.Errorf("block timestamp overflow, network=%v, height=%v", network, height)
	}

	t := genesis + int64(blockTime)*int64(height)
	return utils.ToTimestamp(t), nil
}
