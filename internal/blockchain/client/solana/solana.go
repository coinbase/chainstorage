package solana

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/solana"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	solanaClientImpl struct {
		config *config.Config
		logger *zap.Logger
		client jsonrpc.Client
	}

	getBlockResponse struct {
		blockMetadata       *api.BlockMetadata
		blobData            json.RawMessage
		transactionMetadata *api.TransactionMetadata
	}
)

const (
	solanaUnavailableParentHash = "11111111111111111111111111111111"

	solanaGenesisBlockHeight = uint64(0)

	solanaBatchSize = 5

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
	solanaGetBlockLitConfiguration = map[string]any{
		"encoding":                       "json",
		"transactionDetails":             "none",
		"rewards":                        false,
		"maxSupportedTransactionVersion": maxSupportedTransactionVersion,
	}

	// This configuration defines the maxSupportedTransactionVersion for getBlock requests. Specifying this
	// Allows getting blocks with mix transaction versions, if this is not provided, and there are mix transaction
	// versions in the block, -32015 error will be returned
	solanaGetBlockConfiguration = map[string]any{
		"maxSupportedTransactionVersion": maxSupportedTransactionVersion,
	}

	solanaGetBlockConfigurationV2 = map[string]any{
		"encoding":                       "jsonParsed",
		"transactionDetails":             "full",
		"rewards":                        true,
		"maxSupportedTransactionVersion": maxSupportedTransactionVersion,
	}

	// This map is to work around missing data in Solana's long term storage (due to solanaErrorCodeLongTermStorageSlotSkipped).
	// NOTE: the map is currently empty because the underlying issue (https://github.com/solana-labs/solana/issues/21350) has been fixed in Solana.
	solanaBlocksMissingInLongTermStorage = map[uint64]bool{}
)

func NewSolanaClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &solanaClientImpl{
			config: params.Config,
			logger: logger,
			client: client,
		}
	})
}

func (c *solanaClientImpl) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range of [%d, %d)", from, to)
	}

	result := make([]*api.BlockMetadata, to-from)
	group, ctx := syncgroup.New(ctx)
	for i := from; i < to; i += solanaBatchSize {
		batchStart := i
		batchEnd := batchStart + solanaBatchSize
		if batchEnd > to {
			batchEnd = to
		}

		group.Go(func() error {
			batch, err := c.batchGetBlockMetadata(ctx, tag, batchStart, batchEnd)
			if err != nil {
				return xerrors.Errorf("failed to get block metadata in batch (batchStart=%v, batchEnd=%v): %w", batchStart, batchEnd, err)
			}

			for j := range batch {
				result[batchStart-from+uint64(j)] = batch[j]
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("failed to finish group: %w", err)
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

		header, _, err := c.parseHeader(response)
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

func (c *solanaClientImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, _ ...internal.ClientOption) (*api.Block, error) {
	getBlockResponse, err := c.getBlock(ctx, tag, height)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block (height=%v): %w", height, err)
	}

	block := &api.Block{
		Blockchain:          c.config.Chain.Blockchain,
		Network:             c.config.Chain.Network,
		Metadata:            getBlockResponse.blockMetadata,
		Blobdata:            c.getBlobdata(getBlockResponse.blobData),
		TransactionMetadata: getBlockResponse.transactionMetadata,
	}
	return block, nil
}

func (c *solanaClientImpl) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
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
	return nil, internal.ErrNotImplemented
}

func (c *solanaClientImpl) CanReprocess(_ uint32, _ uint64) bool {
	return false
}

func (c *solanaClientImpl) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return nil, internal.ErrNotImplemented
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

func (c *solanaClientImpl) getBlock(ctx context.Context, tag uint32, height uint64) (*getBlockResponse, error) {
	cfg := solanaGetBlockConfigurationV2
	if tag < 2 {
		// TODO: remove this branch once v2 is fully rolled out.
		cfg = solanaGetBlockConfiguration
	}
	params := jsonrpc.Params{
		height,
		cfg,
	}

	response, err := c.client.Call(ctx, solanaMethodGetBlock, params)
	if err != nil {
		if c.isNotFoundError(err) {
			return nil, internal.ErrBlockNotFound
		}

		if c.isSlotSkippedError(err) {
			blockMetadata := &api.BlockMetadata{
				Tag:     tag,
				Height:  height,
				Skipped: true,
			}

			return &getBlockResponse{
				blockMetadata:       blockMetadata,
				blobData:            json.RawMessage{},
				transactionMetadata: nil,
			}, nil
		}

		return nil, xerrors.Errorf("failed to call jsonrpc: %w", err)
	}

	header, txnList, err := c.parseHeader(response)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	blockMetadata := &api.BlockMetadata{
		Tag:          tag,
		Height:       height,
		ParentHeight: header.ParentSlot,
		Hash:         header.BlockHash,
		ParentHash:   header.PreviousBlockHash,
		Timestamp:    utils.ToTimestamp(header.BlockTime),
	}

	transactionMetadata := &api.TransactionMetadata{
		Transactions: txnList,
	}

	return &getBlockResponse{
		blockMetadata:       blockMetadata,
		blobData:            response.Result,
		transactionMetadata: transactionMetadata,
	}, nil
}

func (c *solanaClientImpl) parseHeader(response *jsonrpc.Response) (*solana.SolanaBlockLit, []string, error) {
	var header solana.SolanaBlockLit
	if err := response.Unmarshal(&header); err != nil {
		return nil, nil, xerrors.Errorf("failed to unmarshal block: %w", err)
	}

	if header.BlockHash == "" {
		return nil, nil, xerrors.Errorf("block hash is empty: {%+v}", header)
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

	txnList := make([]string, len(header.Transactions))
	for i, txn := range header.Transactions {
		transactionID, err := solana.ValidateSolanaParsedTransactionId(txn.Payload.Signatures)
		if err != nil {
			return nil, nil, err
		}

		txnList[i] = transactionID
	}

	return &header, txnList, nil
}
