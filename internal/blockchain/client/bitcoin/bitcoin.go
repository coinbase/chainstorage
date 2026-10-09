package bitcoin

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/bitcoin"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// rpcMethods holds the RPC method definitions for a Bitcoin-family client.
	// Use newRPCMethods() for defaults, or pass rpcMethodsOverride to customize.
	rpcMethods struct {
		getBlockByHash    *jsonrpc.RequestMethod
		getBlockHash      *jsonrpc.RequestMethod
		getRawTransaction *jsonrpc.RequestMethod
		getBlockCount     *jsonrpc.RequestMethod
	}

	rpcMethodName string

	rpcMethodsOverride map[rpcMethodName]*jsonrpc.RequestMethod

	bitcoinClient struct {
		config                       *config.Config
		logger                       *zap.Logger
		client                       jsonrpc.Client
		validate                     *validator.Validate
		methods                      rpcMethods
		preserveRawInputTransactions bool
		// Optional hook to customize getrawtransaction RPC params.
		// Args: (txid, current block hash). Returns: RPC call params.
		// nil means default behavior: jsonrpc.Params{txid, 1}
		getRawTxParams func(txid string, blockHash string) jsonrpc.Params
	}

	bitcoinBlockHeaderResultHolder struct {
		header  *bitcoin.BitcoinBlockLit // Use the light version for faster parsing.
		rawJson json.RawMessage          // Store the raw message in blob storage.
	}

	// parsedInputTx holds a parsed input transaction with an O(1) vout lookup map.
	parsedInputTx struct {
		tx      *bitcoin.BitcoinInputTransactionLit
		voutMap map[uint64]*bitcoin.BitcoinTransactionOutput
	}
)

const (
	// If verbosity is 2, returns an Object with information about block ‘hash’ and information about each transaction.
	bitcoinBlockVerbosity = 2
	// If verbosity is 1, returns a json object without full transaction data
	bitcoinBlockMetadataVerbosity = 1

	// Maximum number of concurrent batch RPC calls when fetching input transactions.
	fetchInputTxConcurrency = 3

	// err code defined by bitcoin.
	// reference: https://github.com/bitcoin/bitcoin/blob/89d148c8c65b3e6b6a8fb8b722efb4b6a7d0a375/src/rpc/protocol.h#L23-L87
	bitcoinErrCodeInvalidAddressOrKey = -5
	bitcoinErrCodeInvalidParameter    = -8
	bitcoinErrMessageBlockNotFound    = "Block not found"
	bitcoinErrMessageBlockOutOfRange  = "Block height out of range"
)

const (
	rpcMethodGetBlockByHash    rpcMethodName = "getBlockByHash"
	rpcMethodGetBlockHash      rpcMethodName = "getBlockHash"
	rpcMethodGetRawTransaction rpcMethodName = "getRawTransaction"
	rpcMethodGetBlockCount     rpcMethodName = "getBlockCount"
)

var _ internal.Client = (*bitcoinClient)(nil)

// Default RPC method definitions shared across all Bitcoin-family chains.
var (
	defaultGetBlockByHash    = &jsonrpc.RequestMethod{Name: "getblock", Timeout: 10 * time.Second}
	defaultGetBlockHash      = &jsonrpc.RequestMethod{Name: "getblockhash", Timeout: 5 * time.Second}
	defaultGetRawTransaction = &jsonrpc.RequestMethod{Name: "getrawtransaction", Timeout: 30 * time.Second}
	defaultGetBlockCount     = &jsonrpc.RequestMethod{Name: "getblockcount", Timeout: 5 * time.Second}
)

func defaultRPCMethods() rpcMethods {
	return rpcMethods{
		getBlockByHash:    defaultGetBlockByHash,
		getBlockHash:      defaultGetBlockHash,
		getRawTransaction: defaultGetRawTransaction,
		getBlockCount:     defaultGetBlockCount,
	}
}

func newRPCMethods(overrides ...rpcMethodsOverride) rpcMethods {
	methods := defaultRPCMethods()
	for _, override := range overrides {
		for methodName, method := range override {
			if method == nil {
				continue
			}
			switch methodName {
			case rpcMethodGetBlockByHash:
				methods.getBlockByHash = method
			case rpcMethodGetBlockHash:
				methods.getBlockHash = method
			case rpcMethodGetRawTransaction:
				methods.getRawTransaction = method
			case rpcMethodGetBlockCount:
				methods.getBlockCount = method
			}
		}
	}

	return methods
}

// rpcMethodsOverrideFromConfig builds an rpcMethodsOverride from configurable
// timeout values in ClientConfig. Zero-value durations are skipped, preserving
// the compiled defaults.
func rpcMethodsOverrideFromConfig(cfg *config.Config) rpcMethodsOverride {
	override := make(rpcMethodsOverride)
	clientCfg := cfg.Chain.Client
	if clientCfg.RpcTimeoutGetBlock > 0 {
		override[rpcMethodGetBlockByHash] = &jsonrpc.RequestMethod{Name: defaultGetBlockByHash.Name, Timeout: clientCfg.RpcTimeoutGetBlock}
	}
	if clientCfg.RpcTimeoutGetRawTx > 0 {
		override[rpcMethodGetRawTransaction] = &jsonrpc.RequestMethod{Name: defaultGetRawTransaction.Name, Timeout: clientCfg.RpcTimeoutGetRawTx}
	}
	if clientCfg.RpcTimeoutGetBlockHash > 0 {
		override[rpcMethodGetBlockHash] = &jsonrpc.RequestMethod{Name: defaultGetBlockHash.Name, Timeout: clientCfg.RpcTimeoutGetBlockHash}
	}
	return override
}

func NewBitcoinClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &bitcoinClient{
			config:   params.Config,
			logger:   logger,
			client:   client,
			validate: validator.New(),
			methods:  newRPCMethods(rpcMethodsOverrideFromConfig(params.Config)),
		}
	})
}

func (b *bitcoinClient) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range range of [%d, %d)", from, to)
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

	responses, err := b.client.BatchCall(ctx, b.methods.getBlockByHash, params)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block for blockhashes: %w", err)
	}

	if len(responses) != numBlocks {
		return nil, xerrors.Errorf("missing blocks in BatchCall to %s", b.methods.getBlockByHash.Name)
	}

	results := make([]*api.BlockMetadata, len(responses))
	for i, response := range responses {
		hash := blockHashes[i]
		height := from + uint64(i)
		headerResult, err := b.getBlockHeader(response)
		if err != nil {
			return nil, xerrors.Errorf("failed to get block hash %s: %w", hash, err)
		}

		actualHash := headerResult.header.Hash.Value()
		if actualHash != hash {
			return nil, xerrors.Errorf("failed to get block due to inconsistent hash values, expected: %s, actual: %s", hash, actualHash)
		}

		actualHeight := headerResult.header.Height.Value()
		if actualHeight != height {
			return nil, xerrors.Errorf("failed to get block due to inconsistent height, expected: %v, actual: %v", height, actualHeight)
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

	response, err := b.client.Call(ctx, b.methods.getBlockHash, params)
	if err != nil {
		if isBlockHeightOutOfRange(err) {
			return nil, xerrors.Errorf("block not found by height %v: %w", height, internal.ErrBlockNotFound)
		}
		return nil, xerrors.Errorf("failed to make a call for block %v: %w", height, err)
	}

	var hash bitcoin.BitcoinHexString
	if err := response.Unmarshal(&hash); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block hash: %w", err)
	}

	block, err := b.GetBlockByHash(ctx, tag, height, hash.Value(), opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block by hash: %s for height: %v, %w", hash, height, err)
	}

	actualHeight := block.Metadata.Height
	if actualHeight != height {
		return nil, xerrors.Errorf("failed to get block due to inconsistent height values, expected: %v, actual: %v", height, actualHeight)
	}

	return block, nil
}

func (b *bitcoinClient) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{
		hash,
		bitcoinBlockVerbosity,
	}

	response, err := b.client.Call(ctx, b.methods.getBlockByHash, params)
	if err != nil {
		var rpcErr *jsonrpc.RPCError
		if xerrors.As(err, &rpcErr) &&
			rpcErr.Code == bitcoinErrCodeInvalidAddressOrKey &&
			rpcErr.Message == bitcoinErrMessageBlockNotFound {
			return nil, xerrors.Errorf("block not found by hash %s: %w", hash, internal.ErrBlockNotFound)
		}
		return nil, xerrors.Errorf("failed to make a call for block hash %s: %w", hash, err)
	}

	headerResult, err := b.getBlockHeader(response)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block hash %s: %w", hash, err)
	}

	actualHash := headerResult.header.Hash.Value()
	if actualHash != hash {
		return nil, xerrors.Errorf("failed to get block due to inconsistent hash values, expected: %s, actual: %s", hash, actualHash)
	}

	return b.getBlockFromHeader(ctx, tag, headerResult)
}

func (b *bitcoinClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	params := jsonrpc.Params{}

	response, err := b.client.Call(ctx, b.methods.getBlockCount, params)
	if err != nil {
		return 0, xerrors.Errorf("failed to get the height of the most-work fully-validated chain: %w", err)
	}

	var height uint64
	if err := response.Unmarshal(&height); err != nil {
		return 0, xerrors.Errorf("failed to unmarshal latest height: %w", err)
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
		return nil, xerrors.Errorf("failed to unmarshal block header: %w", err)
	}

	if err := b.validate.Struct(header); err != nil {
		return nil, xerrors.Errorf("failed to validate block: %w", err)
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

	inputTransactionsData, rawInputTransactionsMap, err := b.getInputTransactions(ctx, headerResult.header)
	if err != nil {
		return nil, xerrors.Errorf("failed to get previous transactions for block %s: %w", blockHash, err)
	}

	inputTransactions := make([]*api.RepeatedBytes, len(inputTransactionsData))
	for i, data := range inputTransactionsData {
		inputTransactions[i] = &api.RepeatedBytes{Data: data}
	}

	blobdata := &api.BitcoinBlobdata{
		Header:            headerResult.rawJson,
		InputTransactions: inputTransactions,
	}
	if b.preserveRawInputTransactions {
		blobdata.RawInputTransactions = rawInputTransactionsMap
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
			Bitcoin: blobdata,
		},
	}

	return block, nil
}

// getInputTransactions fetches and processes input transactions for a block.
// Returns per-transaction filtered input data and the raw transaction map.
//
// For a block with transactions A (inputs: A1, A2), B (inputs: B1, B2), C (inputs: C1),
// the results will be:
// [[data_of_A1, data_of_A2], [data_of_B1, data_of_B2], [data_of_C1]]
// Each entry is serialized bytes of BitcoinInputTransactionLit with a single filtered vout.
func (b *bitcoinClient) getInputTransactions(
	ctx context.Context,
	header *bitcoin.BitcoinBlockLit,
) ([][][]byte, map[string][]byte, error) {
	transactions := header.Transactions
	blockHash := header.Hash.Value()

	inputTransactionIDs := collectInputTransactionIDs(transactions)

	inputTransactionsMap, err := b.fetchInputTransactions(ctx, inputTransactionIDs, blockHash)
	if err != nil {
		return nil, nil, err
	}

	parsedCache, err := parseInputTransactions(inputTransactionsMap)
	if err != nil {
		return nil, nil, err
	}

	results, err := buildInputTransactionResults(transactions, parsedCache, blockHash)
	if err != nil {
		return nil, nil, err
	}

	return results, inputTransactionsMap, nil
}

// collectInputTransactionIDs extracts deduplicated input transaction IDs
// from all block transactions, preserving first-seen order.
func collectInputTransactionIDs(transactions []*bitcoin.BitcoinTransactionLit) []string {
	seen := make(map[string]bool)
	var ids []string
	for _, tx := range transactions {
		for _, input := range tx.Inputs {
			txID := input.Identifier.Value()
			// coinbase transaction does not have txid
			if txID != "" && !seen[txID] {
				seen[txID] = true
				ids = append(ids, txID)
			}
		}
	}
	return ids
}

// fetchInputTransactions fetches raw transaction data for the given IDs via
// concurrent batched getrawtransaction RPC calls. Returns a map of txid -> raw JSON.
func (b *bitcoinClient) fetchInputTransactions(
	ctx context.Context,
	inputTransactionIDs []string,
	blockHash string,
) (map[string][]byte, error) {
	opts := internal.OptionsFromContext(ctx)
	txBatchSize := b.config.Chain.Client.TxBatchSize
	numTransactions := len(inputTransactionIDs)
	opts.RecordHeartbeat(ctx, "fetchInputTx.begin", numTransactions)

	b.logger.Debug(
		"getting input transactions>>>",
		zap.Int("numTransactions", numTransactions),
		zap.Int("txBatchSize", txBatchSize),
	)

	// Calculate number of batches and pre-allocate per-batch result slices.
	numBatches := (numTransactions + txBatchSize - 1) / txBatchSize
	batchResults := make([][]*jsonrpc.Response, numBatches)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(fetchInputTxConcurrency)

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		batchStart := batchIdx * txBatchSize
		batchEnd := min(batchStart+txBatchSize, numTransactions)
		idx := batchIdx

		batchParams := make([]jsonrpc.Params, batchEnd-batchStart)
		for i, transactionID := range inputTransactionIDs[batchStart:batchEnd] {
			if b.getRawTxParams != nil {
				batchParams[i] = b.getRawTxParams(transactionID, blockHash)
			} else {
				batchParams[i] = jsonrpc.Params{transactionID, true}
			}
		}

		g.Go(func() error {
			responses, err := b.client.BatchCall(
				ctx,
				b.methods.getRawTransaction,
				batchParams,
				jsonrpc.WithOnAttempt(func(ctx context.Context, attempt int) {
					opts.RecordHeartbeat(ctx, "fetchInputTx.batch", idx, attempt)
				}),
			)
			if err != nil {
				return xerrors.Errorf(
					"failed to call %s for subset of (blockHash=%s, startTransactionID=%v, batchSize=%v): %w",
					b.methods.getRawTransaction.Name,
					blockHash,
					inputTransactionIDs[batchStart],
					batchEnd-batchStart,
					err,
				)
			}
			batchResults[idx] = responses
			opts.RecordHeartbeat(ctx, "fetchInputTx.batch.done", idx)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	opts.RecordHeartbeat(ctx, "fetchInputTx.done", numBatches)

	// Merge batch results into a single map.
	result := make(map[string][]byte, numTransactions)
	for batchIdx, responses := range batchResults {
		batchStart := batchIdx * txBatchSize
		for respIdx, resp := range responses {
			result[inputTransactionIDs[batchStart+respIdx]] = resp.Result
		}
	}

	return result, nil
}

// parseInputTransactions unmarshals, validates, and indexes raw input
// transactions. Each unique txid is parsed once and its vouts are indexed
// by N for O(1) lookup. Replaces reflection-based validator.Struct with
// direct checks to avoid per-call reflection overhead.
func parseInputTransactions(rawMap map[string][]byte) (map[string]*parsedInputTx, error) {
	parsed := make(map[string]*parsedInputTx, len(rawMap))
	for txID, rawData := range rawMap {
		var tx bitcoin.BitcoinInputTransactionLit
		if err := json.Unmarshal(rawData, &tx); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal input transaction %s: %w", txID, err)
		}
		if tx.TxId.Value() == "" {
			return nil, xerrors.Errorf("failed to validate input transaction %s: txid is required", txID)
		}
		if len(tx.Vout) == 0 {
			return nil, xerrors.Errorf("failed to validate input transaction %s: vout must have at least 1 element", txID)
		}
		vm := make(map[uint64]*bitcoin.BitcoinTransactionOutput, len(tx.Vout))
		for _, o := range tx.Vout {
			vm[o.N.Value()] = o
		}
		parsed[txID] = &parsedInputTx{tx: &tx, voutMap: vm}
	}
	return parsed, nil
}

// buildInputTransactionResults assembles per-vin filtered results with
// marshal caching to avoid redundant work for shared (txid, vout) pairs.
func buildInputTransactionResults(
	transactions []*bitcoin.BitcoinTransactionLit,
	parsedCache map[string]*parsedInputTx,
	blockHash string,
) ([][][]byte, error) {
	// Build results, caching marshalled (txid, voutIndex) pairs.
	marshalCache := make(map[string][]byte)
	results := make([][][]byte, len(transactions))
	for index, tx := range transactions {
		if index == 0 {
			// coinbase transaction
			results[index] = make([][]byte, 0)
			continue
		}

		inputTransactions := make([][]byte, len(tx.Inputs))
		for inputIndex, input := range tx.Inputs {
			inputID := input.Identifier.Value()
			voutIdx := input.Vout.Value()
			cacheKey := inputID + ":" + strconv.FormatUint(voutIdx, 10)

			if cached, ok := marshalCache[cacheKey]; ok {
				inputTransactions[inputIndex] = cached
				continue
			}

			parsed, ok := parsedCache[inputID]
			if !ok {
				return nil, xerrors.Errorf(
					"input transaction id not found in map (blockHash=%s, transactionID=%v, inputTransactionID=%v)",
					blockHash, tx.Identifier, inputID,
				)
			}

			output, ok := parsed.voutMap[voutIdx]
			if !ok {
				return nil, xerrors.Errorf(
					"vout not found (blockHash=%s, transactionID=%v, inputTransactionID=%v, voutIndex=%d)",
					blockHash, tx.Identifier, inputID, voutIdx,
				)
			}

			filtered := bitcoin.BitcoinInputTransactionLit{
				TxId: parsed.tx.TxId,
				Vout: []*bitcoin.BitcoinTransactionOutput{output},
			}
			data, err := json.Marshal(&filtered)
			if err != nil {
				return nil, xerrors.Errorf(
					"failed to marshal filtered input transaction (blockHash=%s, inputTransactionID=%v, voutIndex=%d): %w",
					blockHash, inputID, voutIdx, err,
				)
			}

			marshalCache[cacheKey] = data
			inputTransactions[inputIndex] = data
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

	responses, err := b.client.BatchCall(ctx, b.methods.getBlockHash, params)
	if err != nil {
		if isBlockHeightOutOfRange(err) {
			// The serving node's tip is below one of the requested heights. Surface ErrBlockNotFound,
			// as GetBlockByHeight does, so that callers can decide whether that is expected.
			return nil, xerrors.Errorf("block not found by heights [%v, %v): %v: %w", from, to, err, internal.ErrBlockNotFound)
		}
		return nil, xerrors.Errorf("failed to get block hashes for heights: %w", err)
	}

	if numBlocks != len(responses) {
		return nil, xerrors.Errorf("missing block hashes in BatchCall to %s", b.methods.getBlockHash.Name)
	}

	blockHashes := make([]string, len(responses))
	for i, response := range responses {
		var hash bitcoin.BitcoinHexString
		if err := response.Unmarshal(&hash); err != nil {
			return nil, xerrors.Errorf("failed to get block hash for request %v: %w", params[i], err)
		}
		blockHashes[i] = hash.Value()
	}
	return blockHashes, nil
}

// isBlockHeightOutOfRange reports whether err is the bitcoind response to getblockhash for a height
// above the serving node's current tip.
func isBlockHeightOutOfRange(err error) bool {
	var rpcErr *jsonrpc.RPCError
	return xerrors.As(err, &rpcErr) &&
		rpcErr.Code == bitcoinErrCodeInvalidParameter &&
		rpcErr.Message == bitcoinErrMessageBlockOutOfRange
}
