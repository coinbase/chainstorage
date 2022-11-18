package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	SolanaClientParams struct {
		fx.In
		fxparams.Params
		MasterClient    jsonrpc.Client `name:"master"`
		SlaveClient     jsonrpc.Client `name:"slave"`
		ValidatorClient jsonrpc.Client `name:"validator"`
	}

	solanaClientFactoryImpl struct {
		config          *config.Config
		logger          *zap.Logger
		metrics         tally.Scope
		masterClient    jsonrpc.Client
		slaveClient     jsonrpc.Client
		validatorClient jsonrpc.Client
	}

	solanaClientImpl struct {
		config *config.Config
		logger *zap.Logger
		client jsonrpc.Client
	}
)

const (
	solanaUnavailableParentHash = "11111111111111111111111111111111"

	solanaGenesisBlockHeight = uint64(0)

	solanaBatchSize = 25

	// Error code for JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE.
	// See https://github.com/solana-labs/solana/blob/d5961e9d9f005966f409fbddd40c3651591b27fb/client/src/rpc_custom_error.rs#L12
	solanaErrorCodeBlockNotAvailable = -32004

	// Error code for JSON_RPC_SERVER_ERROR_SLOT_SKIPPED.
	// See https://github.com/solana-labs/solana/blob/d5961e9d9f005966f409fbddd40c3651591b27fb/client/src/rpc_custom_error.rs#L15
	solanaErrorCodeSlotSkipped = -32007

	// Error code for JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED.
	// See https://github.com/solana-labs/solana/blob/d5961e9d9f005966f409fbddd40c3651591b27fb/client/src/rpc_custom_error.rs#L17
	solanaErrorCodeLongTermStorageSlotSkipped = -32009

	// maxSupportedTransactionVersion is used for setting the max transaction version to return in responses.
	// If the requested block contains a transaction with a higher version, an error with code -32015
	// will be returned. Per the comment here(https://github.com/solana-labs/solana/issues/25287#issuecomment-1129135910)
	// the current version is 0. We need to bump this up when the version changes and getBlock() fails with
	// code -32015.
	maxSupportedTransactionVersion = 0
)

var (
	// Returns the slot that has reached the given or default commitment level.
	solanaMethodGetSlot = &jsonrpc.RequestMethod{
		Name:    "getSlot",
		Timeout: 5 * time.Second,
	}

	// getConfirmedBlock is on deprecation path hence switching to getBlock
	// Documentation link: https://docs.solana.com/developing/clients/jsonrpc-api#getblock
	//                     https://docs.solana.com/developing/clients/jsonrpc-api#getconfirmedblock
	solanaMethodGetBlock = &jsonrpc.RequestMethod{
		Name:    "getBlock",
		Timeout: 15 * time.Second,
	}

	solanaMethodGetBlockBatchCall = &jsonrpc.RequestMethod{
		Name:    "getBlock",
		Timeout: 50 * time.Second,
	}

	// The node will query the most recent block that has been voted on by super-majority of the cluster.
	solanaGetSlotCommitment = map[string]string{
		"commitment": "finalized",
	}

	// This configuration disables transaction details and rewards.
	solanaGetBlockLitConfiguration = map[string]interface{}{
		"encoding":                       "json",
		"transactionDetails":             "none",
		"rewards":                        false,
		"maxSupportedTransactionVersion": maxSupportedTransactionVersion,
	}

	// This configuration defines the maxSupportedTransactionVersion for getBlock requests. Specifying this
	// Allows getting blocks with mix transaction versions, if this is not provided, and there are mix transaction
	// versions in the block, -32015 error will be returned
	solanaGetBlockConfiguration = map[string]int{
		"maxSupportedTransactionVersion": maxSupportedTransactionVersion,
	}

	// This map is to work around missing data in Solana's long term storage (due to solanaErrorCodeLongTermStorageSlotSkipped).
	// NOTE: the map is currently empty because the underlying issue (https://github.com/solana-labs/solana/issues/21350) has been fixed in Solana.
	solanaBlocksMissingInLongTermStorage = map[uint64]bool{}
)

func NewSolanaClientFactory(params SolanaClientParams) ClientFactory {
	return &solanaClientFactoryImpl{
		config:          params.Config,
		logger:          params.Logger,
		metrics:         params.Metrics,
		masterClient:    params.MasterClient,
		slaveClient:     params.SlaveClient,
		validatorClient: params.ValidatorClient,
	}
}

func (f *solanaClientFactoryImpl) Master() Client {
	return f.newClient(f.masterClient)
}

func (f *solanaClientFactoryImpl) Slave() Client {
	return f.newClient(f.slaveClient)
}

func (f *solanaClientFactoryImpl) Validator() Client {
	return f.newClient(f.validatorClient)
}

func (f *solanaClientFactoryImpl) newClient(client jsonrpc.Client) Client {
	logger := log.WithPackage(f.logger)
	return &solanaClientImpl{
		config: f.config,
		logger: logger,
		client: client,
	}
}

func (c *solanaClientImpl) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range range of [%d, %d)", from, to)
	}

	result := make([]*api.BlockMetadata, 0, to-from)
	for batchStart := from; batchStart < to; batchStart += solanaBatchSize {
		batchEnd := batchStart + solanaBatchSize
		if batchEnd > to {
			batchEnd = to
		}

		batch, err := c.batchGetBlockMetadata(ctx, tag, batchStart, batchEnd)
		if err != nil {
			return nil, xerrors.Errorf("failed to get block metadata in batch (batchStart=%v, batchEnd=%v): %w", batchStart, batchEnd, err)
		}

		result = append(result, batch...)
	}

	return result, nil
}

func (c *solanaClientImpl) batchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	numBlocks := int(to - from)
	batchParams := make([]jsonrpc.Params, numBlocks)
	for i := 0; i < numBlocks; i++ {
		height := from + uint64(i)
		batchParams[i] = jsonrpc.Params{
			height,
			solanaGetBlockLitConfiguration,
		}
	}

	responses, err := c.client.BatchCall(ctx, solanaMethodGetBlockBatchCall, batchParams, jsonrpc.WithAllowsRPCError())
	if err != nil {
		return nil, xerrors.Errorf("failed to call jsonrpc (from=%v, to=%v): %w", from, to, err)
	}

	blockMetadatas := make([]*api.BlockMetadata, len(responses))
	for i, response := range responses {
		height := from + uint64(i)
		if response.Error != nil {
			if !c.isSlotSkippedError(response.Error) {
				return nil, xerrors.Errorf("received rpc error (from=%v, to=%v): %+v", from, to, response.Error)
			}

			blockMetadatas[i] = &api.BlockMetadata{
				Tag:     tag,
				Height:  height,
				Skipped: true,
			}
			continue
		}

		header, err := c.parseHeader(response)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse header (height=%v): %w", height, err)
		}

		blockMetadatas[i] = &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			ParentHeight: header.ParentSlot,
			Hash:         header.BlockHash,
			ParentHash:   header.PreviousBlockHash,
			Timestamp:    utils.ToTimestamp(header.BlockTime),
		}
	}

	return blockMetadatas, nil
}

func (c *solanaClientImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, _ ...ClientOption) (*api.Block, error) {
	metadata, header, err := c.getBlock(ctx, tag, height)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block (height=%v): %w", height, err)
	}

	block := &api.Block{
		Blockchain: c.config.Chain.Blockchain,
		Network:    c.config.Chain.Network,
		Metadata:   metadata,
		Blobdata:   c.getBlobdata(header),
	}
	return block, nil
}

func (c *solanaClientImpl) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...ClientOption) (*api.Block, error) {
	// There is no API to query block by hash in solana.
	// Since orphaned blocks are marked as skipped slots, we can simply query the block by height.
	block, err := c.GetBlockByHeight(ctx, tag, height, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block by hash: %w", err)
	}

	// Double check if the above assumption is correct.
	if hash != block.Metadata.Hash {
		return nil, xerrors.Errorf("failed to get block by hash: got unexpected hash (expected=%v, actual=%v)", hash, block.Metadata.Hash)
	}

	return block, nil
}

func (c *solanaClientImpl) GetLatestHeight(ctx context.Context) (uint64, error) {
	params := jsonrpc.Params{
		solanaGetSlotCommitment,
	}

	response, err := c.client.Call(ctx, solanaMethodGetSlot, params)
	if err != nil {
		return 0, xerrors.Errorf("failed to call jsonprc: %w", err)
	}

	var slot uint64
	if err := response.Unmarshal(&slot); err != nil {
		return 0, xerrors.Errorf("failed to unmarshal slot: %w", err)
	}

	return slot, nil
}

func (c *solanaClientImpl) UpgradeBlock(_ context.Context, _ *api.Block, _ uint32) (*api.Block, error) {
	return nil, ErrNotImplemented
}

func (c *solanaClientImpl) CanReprocess(_ uint32, _ uint64) bool {
	return false
}

func (c *solanaClientImpl) isNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	var rpcerr *jsonrpc.RPCError
	if !xerrors.As(err, &rpcerr) {
		return false
	}

	return rpcerr.Code == solanaErrorCodeBlockNotAvailable
}

func (c *solanaClientImpl) isSlotSkippedError(err error) bool {
	if err == nil {
		return false
	}

	var rpcerr *jsonrpc.RPCError
	if !xerrors.As(err, &rpcerr) {
		return false
	}

	return rpcerr.Code == solanaErrorCodeSlotSkipped || rpcerr.Code == solanaErrorCodeLongTermStorageSlotSkipped
}

func (c *solanaClientImpl) getBlobdata(header json.RawMessage) *api.Block_Solana {
	if header == nil {
		return nil
	}

	return &api.Block_Solana{
		Solana: &api.SolanaBlobdata{
			Header: header,
		},
	}
}

func (c *solanaClientImpl) getBlock(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, json.RawMessage, error) {
	params := jsonrpc.Params{
		height,
		solanaGetBlockConfiguration,
	}

	response, err := c.client.Call(ctx, solanaMethodGetBlock, params)
	if err != nil {
		if c.isNotFoundError(err) {
			return nil, nil, ErrBlockNotFound
		}

		if c.isSlotSkippedError(err) {
			return &api.BlockMetadata{
				Tag:     tag,
				Height:  height,
				Skipped: true,
			}, json.RawMessage{}, nil
		}

		return nil, nil, xerrors.Errorf("failed to call jsonrpc: %w", err)
	}

	header, err := c.parseHeader(response)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	return &api.BlockMetadata{
		Tag:          tag,
		Height:       height,
		ParentHeight: header.ParentSlot,
		Hash:         header.BlockHash,
		ParentHash:   header.PreviousBlockHash,
		Timestamp:    utils.ToTimestamp(header.BlockTime),
	}, response.Result, nil
}

func (c *solanaClientImpl) parseHeader(response *jsonrpc.Response) (*parser.SolanaBlockLit, error) {
	var header parser.SolanaBlockLit
	if err := response.Unmarshal(&header); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block: %w", err)
	}

	if header.BlockHash == "" {
		return nil, xerrors.Errorf("block hash is empty: {%+v}", header)
	}

	if header.PreviousBlockHash == solanaUnavailableParentHash {
		// If the parent block is not available due to ledger cleanup,
		// this field will return "11111111111111111111111111111111".
		// Set PreviousBlockHash to empty to disable validation in ValidateChain.
		header.PreviousBlockHash = ""
	}

	if solanaBlocksMissingInLongTermStorage[header.ParentSlot] {
		// If the parent slot is missing, set PreviousBlockHash to empty to disable validation in ValidateChain.
		header.PreviousBlockHash = ""
	}

	return &header, nil
}
