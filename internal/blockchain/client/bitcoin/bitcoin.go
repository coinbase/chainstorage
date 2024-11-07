package bitcoin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/bitcoin"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	bitcoinClient struct {
		config   *config.Config
		logger   *zap.Logger
		client   jsonrpc.Client
		validate *validator.Validate
	}

	bitcoinBlockHeaderResultHolder struct {
		header  *bitcoin.BitcoinBlockLit // Use the light version for faster parsing.
		rawJson json.RawMessage          // Store the raw message in blob storage.
	}
)

const (
	// If verbosity is 2, returns an Object with information about block ‘hash’ and information about each transaction.
	bitcoinBlockVerbosity = 2
	// If verbosity is 1, returns a json object without full transaction data
	bitcoinBlockMetadataVerbosity = 1

	// err code defined by bitcoin.
	// reference: https://github.com/bitcoin/bitcoin/blob/89d148c8c65b3e6b6a8fb8b722efb4b6a7d0a375/src/rpc/protocol.h#L23-L87
	bitcoinErrCodeInvalidAddressOrKey = -5
	bitcoinErrCodeInvalidParameter    = -8
	bitcoinErrMessageBlockNotFound    = "Block not found"
	bitcoinErrMessageBlockOutOfRange  = "Block height out of range"

	// batch size
	bitcoinGetInputTransactionsBatchSize = 100
)

var _ internal.Client = (*bitcoinClient)(nil)

var (
	bitcoinGetBlockByHashMethod = &jsonrpc.RequestMethod{
		Name:    "getblock",
		Timeout: time.Second * 10,
	}
	bitcoinGetBlockHashMethod = &jsonrpc.RequestMethod{
		Name:    "getblockhash",
		Timeout: time.Second * 5,
	}
	bitcoinGetRawTransactionMethod = &jsonrpc.RequestMethod{
		Name:    "getrawtransaction",
		Timeout: time.Second * 30,
	}
	bitcoinGetBlockCountMethod = &jsonrpc.RequestMethod{
		Name:    "getblockcount",
		Timeout: time.Second * 5,
	}
)

func NewBitcoinClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &bitcoinClient{
			config:   params.Config,
			logger:   logger,
			client:   client,
			validate: validator.New(),
		}
	})
}

func (b *bitcoinClient) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, fmt.Errorf("invalid height range range of [%d, %d)", from, to)
	}

	numBlocks := int(to - from)
	blockHashes, err := b.getBlockHashesByHeights(ctx, from, to)
	if err != nil {
		return nil, err
	}

	params := make([]jsonrpc.Params, len(blockHashes))
	for i, hash := range blockHashes {
		params[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	responses, err := b.client.BatchCall(ctx, bitcoinGetBlockByHashMethod, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get block for blockhashes: %w", err)
	}

	if len(responses) != numBlocks {
		return nil, fmt.Errorf("missing blocks in BatchCall to %s", bitcoinGetBlockByHashMethod.Name)
	}

	results := make([]*api.BlockMetadata, len(responses))
	for i, response := range responses {
		hash := blockHashes[i]
		height := from + uint64(i)
		headerResult, err := b.getBlockHeader(response)
		if err != nil {
			return nil, fmt.Errorf("failed to get block hash %s: %w", hash, err)
		}

		actualHash := headerResult.header.Hash.Value()
		if actualHash != hash {
			return nil, fmt.Errorf("failed to get block due to inconsistent hash values, expected: %s, actual: %s", hash, actualHash)
		}

		actualHeight := headerResult.header.Height.Value()
		if actualHeight != height {
			return nil, fmt.Errorf("failed to get block due to inconsistent height, expected: %v, actual: %v", height, actualHeight)
		}

		results[i] = &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			ParentHeight: internal.GetParentHeight(height),
			Hash:         hash,
			ParentHash:   headerResult.header.PreviousBlockHash.Value(),
			Timestamp:    utils.ToTimestamp(int64(headerResult.header.Time.Value())),
		}
	}
	return results, nil
}

func (b *bitcoinClient) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{height}

	response, err := b.client.Call(ctx, bitcoinGetBlockHashMethod, params)
	if err != nil {
		var rpcErr *jsonrpc.RPCError
		if errors.As(err, &rpcErr) &&
			rpcErr.Code == bitcoinErrCodeInvalidParameter &&
			rpcErr.Message == bitcoinErrMessageBlockOutOfRange {
			return nil, fmt.Errorf("block not found by height %v: %w", height, internal.ErrBlockNotFound)
		}
		return nil, fmt.Errorf("failed to make a call for block %v: %w", height, err)
	}

	var hash bitcoin.BitcoinHexString
	if err := response.Unmarshal(&hash); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block hash: %w", err)
	}

	block, err := b.GetBlockByHash(ctx, tag, height, hash.Value(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get block by hash: %s for height: %v, %w", hash, height, err)
	}

	actualHeight := block.Metadata.Height
	if actualHeight != height {
		return nil, fmt.Errorf("failed to get block due to inconsistent height values, expected: %v, actual: %v", height, actualHeight)
	}

	return block, nil
}

func (b *bitcoinClient) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{
		hash,
		bitcoinBlockVerbosity,
	}

	response, err := b.client.Call(ctx, bitcoinGetBlockByHashMethod, params)
	if err != nil {
		var rpcErr *jsonrpc.RPCError
		if errors.As(err, &rpcErr) &&
			rpcErr.Code == bitcoinErrCodeInvalidAddressOrKey &&
			rpcErr.Message == bitcoinErrMessageBlockNotFound {
			return nil, fmt.Errorf("block not found by hash %s: %w", hash, internal.ErrBlockNotFound)
		}
		return nil, fmt.Errorf("failed to make a call for block hash %s: %w", hash, err)
	}

	headerResult, err := b.getBlockHeader(response)
	if err != nil {
		return nil, fmt.Errorf("failed to get block hash %s: %w", hash, err)
	}

	actualHash := headerResult.header.Hash.Value()
	if actualHash != hash {
		return nil, fmt.Errorf("failed to get block due to inconsistent hash values, expected: %s, actual: %s", hash, actualHash)
	}

	return b.getBlockFromHeader(ctx, tag, headerResult)
}

func (b *bitcoinClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	params := jsonrpc.Params{}

	response, err := b.client.Call(ctx, bitcoinGetBlockCountMethod, params)
	if err != nil {
		return 0, fmt.Errorf("failed to get the height of the most-work fully-validated chain: %w", err)
	}

	var height uint64
	if err := response.Unmarshal(&height); err != nil {
		return 0, fmt.Errorf("failed to unmarshal latest height: %w", err)
	}
	return height, nil
}

func (b *bitcoinClient) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	return nil, internal.ErrNotImplemented
}

func (b *bitcoinClient) CanReprocess(tag uint32, height uint64) bool {
	return true
}

func (b *bitcoinClient) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return nil, internal.ErrNotImplemented
}

func (b *bitcoinClient) getBlockHeader(response *jsonrpc.Response) (*bitcoinBlockHeaderResultHolder, error) {
	var header bitcoin.BitcoinBlockLit
	if err := response.Unmarshal(&header); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block header: %w", err)
	}

	if err := b.validate.Struct(header); err != nil {
		return nil, fmt.Errorf("failed to validate block: %w", err)
	}

	return &bitcoinBlockHeaderResultHolder{
		header:  &header,
		rawJson: response.Result,
	}, nil
}

func (b *bitcoinClient) getBlockFromHeader(
	ctx context.Context,
	tag uint32,
	headerResult *bitcoinBlockHeaderResultHolder,
) (*api.Block, error) {
	blockHash := headerResult.header.Hash.Value()

	inputTransactionsData, err := b.getInputTransactions(ctx, headerResult.header)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous transactions for block %s: %w", blockHash, err)
	}

	inputTransactions := make([]*api.RepeatedBytes, len(inputTransactionsData))
	for i, data := range inputTransactionsData {
		inputTransactions[i] = &api.RepeatedBytes{Data: data}
	}

	block := &api.Block{
		Blockchain: b.config.Chain.Blockchain,
		Network:    b.config.Chain.Network,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Hash:         blockHash,
			ParentHash:   headerResult.header.PreviousBlockHash.Value(),
			Height:       headerResult.header.Height.Value(),
			ParentHeight: internal.GetParentHeight(headerResult.header.Height.Value()),
			Timestamp:    utils.ToTimestamp(int64(headerResult.header.Time.Value())),
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header:            headerResult.rawJson,
				InputTransactions: inputTransactions,
			},
		},
	}

	return block, nil
}

// get raw transaction data for input transactions of a block
// if a block contains transactions A (input txs: A1, A2), B (input txs: B1, B2), C (input txs: C1),
// the results will be:
// [[transaction_data_of_A1, transaction_data_of_A2], [transaction_data_of_B1, transaction_data_of_B2], [transaction_data_of_C1]]
// For individual transaction data in the results, they are serialized bytes of BitcoinInputTransactionLit
func (b *bitcoinClient) getInputTransactions(
	ctx context.Context,
	header *bitcoin.BitcoinBlockLit,
) ([][][]byte, error) {
	transactions := header.Transactions
	blockHash := header.Hash.Value()

	var inputTransactionIDs []string
	// TODO: dedupe for inputTransactionIDs
	for _, tx := range transactions {
		for _, input := range tx.Inputs {
			inputTransactionID := input.Identifier.Value()
			// coinbase transaction does not have txid
			if inputTransactionID != "" {
				inputTransactionIDs = append(inputTransactionIDs, inputTransactionID)
			}
		}
	}

	numTransactions := len(inputTransactionIDs)
	inputTransactionsMap := make(map[string][]byte, numTransactions)

	// batch of batchCalls to getrawtransaction in order to fetch input transaction data
	for batchStart := 0; batchStart < numTransactions; batchStart += bitcoinGetInputTransactionsBatchSize {
		batchEnd := batchStart + bitcoinGetInputTransactionsBatchSize
		if batchEnd > numTransactions {
			batchEnd = numTransactions
		}

		batchParams := make([]jsonrpc.Params, batchEnd-batchStart)
		for i, transactionID := range inputTransactionIDs[batchStart:batchEnd] {
			batchParams[i] = jsonrpc.Params{
				transactionID,
				true,
			}
		}

		batchResponses, err := b.client.BatchCall(ctx, bitcoinGetRawTransactionMethod, batchParams)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to call %s for subset of (blockHash=%s, startTransactionID=%v, batchSize=%v): %w",
				bitcoinGetRawTransactionMethod.Name,
				blockHash,
				inputTransactionIDs[batchStart],
				batchEnd-batchStart,
				err,
			)
		}

		for respIndex, resp := range batchResponses {
			transactionID := inputTransactionIDs[batchStart+respIndex]
			inputTransactionsMap[transactionID] = resp.Result
		}
	}

	results := make([][][]byte, len(transactions))
	for index, tx := range transactions {
		var inputTransactions [][]byte
		if index == 0 {
			// coinbase transaction
			inputTransactions = make([][]byte, 0)
		} else {
			inputTransactions = make([][]byte, len(tx.Inputs))

			for inputIndex, input := range tx.Inputs {
				inputID := input.Identifier.Value()
				rawTransaction, ok := inputTransactionsMap[inputID]
				if !ok {
					return nil, fmt.Errorf(
						"input transaction id not found in map (blockHash=%s, transactionID=%v, inputTransactionID=%v)",
						blockHash,
						tx.Identifier,
						inputID,
					)
				}

				data, err := b.processInputTransactionRawData(rawTransaction, input.Vout.Value())
				if err != nil {
					return nil, fmt.Errorf(
						"error processing input transaction data (blockHash=%s, transactionID=%v, inputTransactionID=%v): %w",
						blockHash,
						tx.Identifier,
						inputID,
						err,
					)
				}

				inputTransactions[inputIndex] = data
			}
		}
		results[index] = inputTransactions
	}
	return results, nil
}

func (b *bitcoinClient) getBlockHashesByHeights(ctx context.Context, from uint64, to uint64) ([]string, error) {
	numBlocks := int(to - from)
	params := make([]jsonrpc.Params, numBlocks)
	for i := 0; i < numBlocks; i++ {
		height := from + uint64(i)
		params[i] = jsonrpc.Params{height}
	}

	responses, err := b.client.BatchCall(ctx, bitcoinGetBlockHashMethod, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get block hashes for heights: %w", err)
	}

	if numBlocks != len(responses) {
		return nil, fmt.Errorf("missing block hashes in BatchCall to %s", bitcoinGetBlockHashMethod.Name)
	}

	blockHashes := make([]string, len(responses))
	for i, response := range responses {
		var hash bitcoin.BitcoinHexString
		if err := response.Unmarshal(&hash); err != nil {
			return nil, fmt.Errorf("failed to get block hash for request %v: %w", params[i], err)
		}
		blockHashes[i] = hash.Value()
	}
	return blockHashes, nil
}

func (b *bitcoinClient) processInputTransactionRawData(data json.RawMessage, voutIndex uint64) ([]byte, error) {
	var transaction bitcoin.BitcoinInputTransactionLit
	if err := json.Unmarshal(data, &transaction); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data into bitcoin input transaction lite: %w", err)
	}

	if err := b.validate.Struct(transaction); err != nil {
		return nil, fmt.Errorf("failed to validate bitcoin input transaction lite %+v: %w", transaction, err)
	}

	if voutIndex >= uint64(len(transaction.Vout)) {
		return nil, fmt.Errorf("vout index out of bound (index=%d, len=%d)", voutIndex, len(transaction.Vout))
	}

	var output *bitcoin.BitcoinTransactionOutput
	for _, o := range transaction.Vout {
		if voutIndex == uint64(o.N) {
			output = o
			break
		}
	}

	if output == nil {
		return nil, fmt.Errorf("vout not found for index: %d", voutIndex)
	}

	// ignore other outputs
	transaction.Vout = []*bitcoin.BitcoinTransactionOutput{output}

	result, err := json.Marshal(&transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input transaction data %v: %w", transaction, err)
	}
	return result, nil
}
