package ethereum

import (
	"context"
	"encoding/json"
	"math/big"
	"regexp"
	"time"

	geth "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/go-playground/validator/v10"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	EthereumClient struct {
		config          *config.Config
		logger          *zap.Logger
		client          jsonrpc.Client
		dlq             dlq.DLQ
		validate        *validator.Validate
		metrics         *ethereumClientMetrics
		nodeType        types.EthereumNodeType
		traceType       types.TraceType
		commitmentLevel types.CommitmentLevel
	}

	EthereumClientOption func(client *EthereumClient)

	ethereumClientMetrics struct {
		traceBlockSuccessCounter          tally.Counter
		traceBlockServerErrorCounter      tally.Counter
		traceBlockExecutionTimeoutCounter tally.Counter
		traceBlockFakeCounter             tally.Counter
		traceTransactionFakeCounter       tally.Counter
		traceTransactionSuccessCounter    tally.Counter
		traceTransactionIgnoredCounter    tally.Counter
		transactionReceiptFakeCounter     tally.Counter
	}

	ethereumResultHolder struct {
		Error  string          `json:"error"`
		Result json.RawMessage `json:"result,omitempty"`
	}

	ethereumBlockHeaderResultHolder struct {
		header  *ethereum.EthereumBlockLit // Use the light version for faster parsing.
		rawJson json.RawMessage            // Store the raw message in blob storage.
	}
)

const (
	erigonTraceBlockByHashTimeout      = "150s"
	ethTraceBlockByHashTimeout         = "50s"
	ethTraceBlockByHashOptimismTimeout = "600s"

	ethTraceTransactionTimeout                      = "25s"
	ethTraceTransactionOptimismTimeout              = "300s"
	ethTraceTransactionError                        = "failed to trace transaction"
	ethTraceTransactionMaxOpCount                   = 39000
	ethTraceTransactionUnknownOpCount               = -1
	ethParityTransactionTraceErrCode                = -32000
	ethParityTransactionTraceErrMessageFoundNoTrace = "found no trace"

	ethCallTracer            = "callTracer"
	ethOpCountTracer         = "opcountTracer"
	ethExecutionTimeoutError = "execution timeout"

	ethBlockTransactionReceiptBatchSize = 100

	ethNullAddress     = "0x0000000000000000000000000000000000000000"
	genesisBlockNumber = "0x0"

	arbitrumNITROUpgradeBlockNumber = 22_207_818

	ethereumBatchSize                = 5
	ethereumBatchMetadataParallelism = 10
)

const (
	subScope                  = "blockchain_client"
	traceBlockCounter         = "trace_block"
	traceTransactionCounter   = "trace_transaction"
	transactionReceiptCounter = "transaction_receipt"
	resultType                = "result_type"
	transaction_FAILED        = uint64(0)
	optimismWhitelistError    = "TypeError: cannot read property 'toString' of undefined    in server-side tracer function 'result'"
	optimismFakeTraceError    = "Creating fake trace for failed transaction."
	unfinalizedDataError      = "cannot query unfinalized data"
)

var ethBlacklistedRanges = map[common.Network][]types.BlockRange{
	common.Network_NETWORK_ETHEREUM_MAINNET: {
		{StartHeight: 1_431_916, EndHeight: 1_431_917},
		{StartHeight: 2_283_397, EndHeight: 2_463_000},
	},
	common.Network_NETWORK_ETHEREUM_GOERLI: {
		{StartHeight: 5_546_892, EndHeight: 5_546_958},
		{StartHeight: 5_562_000, EndHeight: 5_563_000},
	},
}

// All the supported methods are documented here: https://eth.wiki/json-rpc/API
var (
	// JSON RPC method to get an ethereum block by its number.
	ethGetBlockByNumberMethod = &jsonrpc.RequestMethod{
		Name:    "eth_getBlockByNumber",
		Timeout: time.Second * 15,
	}

	// JSON RPC method to get an ethereum block by its hash.
	ethGetBlockByHashMethod = &jsonrpc.RequestMethod{
		Name:    "eth_getBlockByHash",
		Timeout: time.Second * 15,
	}

	// JSON RPC method to get an ethereum transaction receipt.
	ethGetTransactionReceiptMethod = &jsonrpc.RequestMethod{
		Name:    "eth_getTransactionReceipt",
		Timeout: time.Second * 20,
	}

	// JSON RPC method to trace a block by its hash.
	ethTraceBlockByHashMethod = &jsonrpc.RequestMethod{
		Name:    "debug_traceBlockByHash",
		Timeout: time.Second * 60,
	}

	// JSON RPC method to trace a block by its hash - with increased timeout for Optimism
	ethTraceBlockByHashOptimismMethod = &jsonrpc.RequestMethod{
		Name:    "debug_traceBlockByHash",
		Timeout: time.Second * 600,
	}

	// The same as ethTraceBlockByHashMethod, but with a longer timeout since execution timeouts yield invalid results.
	erigonTraceBlockByHashMethod = &jsonrpc.RequestMethod{
		Name:    "debug_traceBlockByHash",
		Timeout: time.Second * 150,
	}

	// JSON RPC method to get the number of most recent block.
	ethBlockNumber = &jsonrpc.RequestMethod{
		Name:    "eth_blockNumber",
		Timeout: time.Second * 5,
	}

	// JSON RPC method to trace a transaction by its hash.
	ethTraceTransactionMethod = &jsonrpc.RequestMethod{
		Name:    "debug_traceTransaction",
		Timeout: time.Second * 30,
	}

	// JSON RPC method to trace a transaction by its hash - with increased timeout for Optimism
	ethTraceTransactionOptimismMethod = &jsonrpc.RequestMethod{
		Name:    "debug_traceTransaction",
		Timeout: time.Second * 300,
	}

	// JSON RPC method to get the number of uncles.
	ethGetUncleCountByBlockHash = &jsonrpc.RequestMethod{
		Name:    "eth_getUncleCountByBlockHash",
		Timeout: time.Second * 10,
	}

	// JSON RPC method to get an uncle block.
	ethGetUncleByBlockHashAndIndex = &jsonrpc.RequestMethod{
		Name:    "eth_getUncleByBlockHashAndIndex",
		Timeout: time.Second * 10,
	}

	// JSON RPC method from bor client to get author(validator) of a block
	ethBorGetAuthorMethod = &jsonrpc.RequestMethod{
		Name:    "bor_getAuthor",
		Timeout: time.Second * 5,
	}

	ethParityTraceBlockByNumber = map[common.Blockchain]*jsonrpc.RequestMethod{
		common.Blockchain_BLOCKCHAIN_ARBITRUM: {
			Name:    "arbtrace_block",
			Timeout: time.Second * 90,
		},
		common.Blockchain_BLOCKCHAIN_FANTOM: {
			Name:    "trace_block",
			Timeout: time.Second * 90,
		},
	}

	ethParityTraceTransaction = map[common.Blockchain]*jsonrpc.RequestMethod{
		common.Blockchain_BLOCKCHAIN_ARBITRUM: {
			Name:    "arbtrace_transaction",
			Timeout: time.Second * 60,
		},
		common.Blockchain_BLOCKCHAIN_FANTOM: {
			Name:    "trace_transaction",
			Timeout: time.Second * 60,
		},
	}

	// JSON RPC method to get the proof for an account.
	ethGetProofMethod = &jsonrpc.RequestMethod{
		Name:    "eth_getProof",
		Timeout: time.Second * 5,
	}
)

var (
	skippedBlockTxn = map[common.Network]map[uint64]string{
		common.Network_NETWORK_ARBITRUM_MAINNET: {
			uint64(15458950): "0xf135954c7b2a17c094f917fff69aa215fa9af86443e55f167e701e39afa5ff0f",
			uint64(4527955):  "0x1d76d3d13e9f8cc713d484b0de58edd279c4c62e46e963899aec28eb648b5800",
		},
	}

	blockNotFoundRegexp    = regexp.MustCompile(`block \w+ not found`)
	executionAbortedRegexp = regexp.MustCompile(`execution aborted`)
	requestTimedOutRegexp  = regexp.MustCompile(`request timed out`)
)

var (
	// A map from the ERC20 token contract address to the storage slot. This will be used when calculating the
	// storage parameter of eth_getProof.
	// For now, we only support USDC. Later, we can add more.
	erc20StorageIndex = map[string]uint64{
		// USDC contract address.
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": uint64(9),
	}
)

func NewEthereumClientFactory(params internal.JsonrpcClientParams, opts ...EthereumClientOption) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		result := &EthereumClient{
			config:          params.Config,
			logger:          logger,
			client:          client,
			dlq:             params.DLQ,
			validate:        validator.New(),
			metrics:         newEthereumClientMetrics(params.Metrics),
			nodeType:        types.EthereumNodeType_ARCHIVAL,
			traceType:       types.TraceType_GETH,
			commitmentLevel: types.CommitmentLevelLatest,
		}
		for _, opt := range opts {
			opt(result)
		}
		return result
	})
}

func WithEthereumNodeType(nodeType types.EthereumNodeType) EthereumClientOption {
	return func(client *EthereumClient) {
		client.nodeType = nodeType
	}
}

func WithEthereumTraceType(traceType types.TraceType) EthereumClientOption {
	return func(client *EthereumClient) {
		client.traceType = traceType
	}
}

func WithEthereumCommitmentLevel(commitmentLevel types.CommitmentLevel) EthereumClientOption {
	return func(client *EthereumClient) {
		client.commitmentLevel = commitmentLevel
	}
}

func newEthereumClientMetrics(scope tally.Scope) *ethereumClientMetrics {
	scope = scope.SubScope(subScope)

	traceBlockSuccessCounter := scope.
		Tagged(map[string]string{resultType: "success"}).
		Counter(traceBlockCounter)
	traceBlockServerErrorCounter := scope.
		Tagged(map[string]string{resultType: "server_error"}).
		Counter(traceBlockCounter)
	traceBlockExecutionTimeoutCounter := scope.
		Tagged(map[string]string{resultType: "execution_timeout"}).
		Counter(traceBlockCounter)
	traceBlockFakeCounter := scope.
		Tagged(map[string]string{resultType: "fake_trace"}).
		Counter(traceBlockCounter)

	traceTransactionSuccessCounter := scope.
		Tagged(map[string]string{resultType: "success"}).
		Counter(traceTransactionCounter)
	traceTransactionIgnoredCounter := scope.
		Tagged(map[string]string{resultType: "ignored"}).
		Counter(traceTransactionCounter)
	traceTransactionFakeCounter := scope.
		Tagged(map[string]string{resultType: "fake_trace"}).
		Counter(traceTransactionCounter)

	transactionReceiptFakeCounter := scope.Counter(transactionReceiptCounter)

	return &ethereumClientMetrics{
		traceBlockSuccessCounter:          traceBlockSuccessCounter,
		traceBlockServerErrorCounter:      traceBlockServerErrorCounter,
		traceBlockExecutionTimeoutCounter: traceBlockExecutionTimeoutCounter,
		traceTransactionSuccessCounter:    traceTransactionSuccessCounter,
		traceTransactionIgnoredCounter:    traceTransactionIgnoredCounter,
		traceBlockFakeCounter:             traceBlockFakeCounter,
		traceTransactionFakeCounter:       traceTransactionFakeCounter,
		transactionReceiptFakeCounter:     transactionReceiptFakeCounter,
	}
}

// BatchGetBlockMetadata gets block metadatas for the height range [from, to), where "from" is the start of the range inclusive and "to" is the end exclusive.
func (c *EthereumClient) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range of [%d, %d)", from, to)
	}

	result := make([]*api.BlockMetadata, to-from)
	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(ethereumBatchMetadataParallelism))
	for i := from; i < to; i += ethereumBatchSize {
		batchStart := i
		batchEnd := batchStart + ethereumBatchSize
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

func (c *EthereumClient) batchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range of mini batch [%d, %d)", from, to)
	}

	numBlocks := int(to - from)
	batchParams := make([]jsonrpc.Params, numBlocks)
	for i := 0; i < numBlocks; i++ {
		height := from + uint64(i)
		batchParams[i] = jsonrpc.Params{
			hexutil.EncodeUint64(height),
			false,
		}
	}

	responses, err := retry.WrapWithResult(ctx, func(ctx context.Context) ([]*jsonrpc.Response, error) {
		responses, err := c.client.BatchCall(ctx, ethGetBlockByNumberMethod, batchParams)
		if err != nil {
			var rpcErr *jsonrpc.RPCError
			if xerrors.As(err, &rpcErr) {
				// Retry all RPCError.
				// Note when a block does not found, it returns `null` in `result`.
				return nil, retry.Retryable(xerrors.Errorf("failed to get block metadata: %w", rpcErr))
			}

			return nil, xerrors.Errorf("failed to get block metadata: %w", err)
		}

		return responses, nil
	})
	if err != nil {
		return nil, err
	}

	blockMetadatas := make([]*api.BlockMetadata, len(responses))
	for i, response := range responses {
		height := from + uint64(i)
		headerResult, err := c.getBlockHeader(response)
		if err != nil {
			return nil, xerrors.Errorf("failed to get the header for block %v: %w", height, err)
		}
		actualHeight := headerResult.header.Number.Value()
		if height != actualHeight {
			return nil, xerrors.Errorf("failed to get block due to inconsistent heights, expected: %v, actual: %v", height, actualHeight)
		}
		blockMetadatas[i] = &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			ParentHeight: internal.GetParentHeight(height),
			Hash:         headerResult.header.Hash.Value(),
			ParentHash:   headerResult.header.ParentHash.Value(),
			Timestamp:    utils.ToTimestamp(int64(headerResult.header.Timestamp.Value())),
		}
	}

	return blockMetadatas, nil
}

func (c *EthereumClient) getBlockHeaderResult(ctx context.Context, height uint64, opts ...internal.ClientOption) (*ethereumBlockHeaderResultHolder, error) {
	params := jsonrpc.Params{
		hexutil.EncodeUint64(height),
		true,
	}

	response, err := c.client.Call(ctx, ethGetBlockByNumberMethod, params)
	if err != nil {
		return nil, xerrors.Errorf("failed to call %s for block %v: %w", ethGetBlockByNumberMethod.Name, height, err)
	}

	headerResult, err := c.getBlockHeader(response)
	if err != nil {
		return nil, xerrors.Errorf("failed to get header for block %v: %w", height, err)
	}
	actualHeight := headerResult.header.Number.Value()
	if height != actualHeight {
		return nil, xerrors.Errorf("failed to get block due to inconsistent heights, expected: %v, actual: %v", height, actualHeight)
	}

	return headerResult, nil
}

func (c *EthereumClient) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)

	headerResult, err := c.getBlockHeaderResult(ctx, height, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get header result for block %v: %w", height, err)
	}

	return c.getBlockFromHeader(ctx, tag, headerResult)
}

func (c *EthereumClient) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
	ctx = internal.ContextWithOptions(ctx, opts...)
	params := jsonrpc.Params{
		hash,
		true,
	}

	response, err := c.client.Call(ctx, ethGetBlockByHashMethod, params)
	if err != nil {
		var rpcErr *jsonrpc.RPCError
		if xerrors.As(err, &rpcErr) && rpcErr.Code == -32000 && rpcErr.Message == unfinalizedDataError {
			// Convert this special error into ErrBlockNotFound so that the syncer may fall back to the master client.
			return nil, xerrors.Errorf("failed to call %s for block hash %s: %v: %w", ethGetBlockByHashMethod.Name, hash, rpcErr, internal.ErrBlockNotFound)
		}

		return nil, xerrors.Errorf("failed to call %s for block hash %s: %w", ethGetBlockByHashMethod.Name, hash, err)
	}

	headerResult, err := c.getBlockHeader(response)

	if err != nil {
		return nil, xerrors.Errorf("failed to get block hash %s: %w", hash, err)
	}

	actualHash := headerResult.header.Hash.Value()

	if actualHash != hash {
		return nil, xerrors.Errorf("failed to get block due to inconsistent hash values, expected: %s, actual: %s", hash, actualHash)
	}

	return c.getBlockFromHeader(ctx, tag, headerResult)
}

func (c *EthereumClient) getBlockFromHeader(ctx context.Context, tag uint32, headerResult *ethereumBlockHeaderResultHolder) (*api.Block, error) {
	height := headerResult.header.Number.Value()
	hash := headerResult.header.Hash.Value()

	transactionReceipts, err := c.getBlockTransactionReceipts(ctx, headerResult.header)
	if err != nil {
		return nil, xerrors.Errorf("failed to fetch transaction receipts for block %v: %w", height, err)
	}

	transactionTraces, err := c.getBlockTraces(ctx, tag, headerResult.header)
	if err != nil {
		return nil, xerrors.Errorf("failed to fetch traces for block %v: %w", height, err)
	}

	uncles, err := c.getBlockUncles(ctx, hash, uint64(len(headerResult.header.Uncles)))
	if err != nil {
		return nil, xerrors.Errorf("failed to fetch block uncles (hash=%v): %w", hash, err)
	}

	block := &api.Block{
		Blockchain: c.config.Chain.Blockchain,
		Network:    c.config.Chain.Network,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Height:       headerResult.header.Number.Value(),
			ParentHeight: internal.GetParentHeight(headerResult.header.Number.Value()),
			Hash:         headerResult.header.Hash.Value(),
			ParentHash:   headerResult.header.ParentHash.Value(),
			Timestamp:    utils.ToTimestamp(int64(headerResult.header.Timestamp.Value())),
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              headerResult.rawJson,
				TransactionReceipts: transactionReceipts,
				TransactionTraces:   transactionTraces,
				Uncles:              uncles,
			},
		},
	}

	if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_POLYGON {
		author, err := c.getBorAuthor(ctx, headerResult.header.Hash.Value())
		if err != nil {
			return nil, xerrors.Errorf("failed to get block author: %w", err)
		}

		block.GetEthereum().ExtraData = &api.EthereumBlobdata_Polygon{
			Polygon: &api.PolygonExtraData{
				Author: author,
			},
		}
	}

	return block, nil
}

func (c *EthereumClient) getBlockHeader(response *jsonrpc.Response) (*ethereumBlockHeaderResultHolder, error) {
	var header ethereum.EthereumBlockLit
	if err := response.Unmarshal(&header); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block header: %w", err)
	}

	if header.Hash == "" {
		return nil, xerrors.Errorf("block not found: %w", internal.ErrBlockNotFound)
	}

	if err := c.validate.Struct(header); err != nil {
		return nil, xerrors.Errorf("failed to parse block: %w", err)
	}

	// There's a rare case that wrong(duplicate) txn gets included in the confirmed block for Arbitrum and Optimism,
	// so we need to skip the duplicate txn for further processing,
	// For Abitrum: see this duplicate txn(0xf135954c7b2a17c094f917fff69aa215fa9af86443e55f167e701e39afa5ff0f)
	// in block [15458950](https://arbiscan.io/txs?block=15458950), however it is actually in another block [15458948](https://arbiscan.io/txs?block=15458948)
	// Please also reference this ticket for more details: https://github.com/OffchainLabs/arbitrum/issues/2313
	if _, ok := skippedBlockTxn[c.config.Chain.Network][header.Number.Value()]; ok {
		return c.deleteDuplicateTxn(response, header)
	}

	return &ethereumBlockHeaderResultHolder{
		header:  &header,
		rawJson: response.Result,
	}, nil
}

func (c *EthereumClient) deleteDuplicateTxn(response *jsonrpc.Response, header ethereum.EthereumBlockLit) (*ethereumBlockHeaderResultHolder, error) {
	skippedTxnIndex := -1
	blockHeight := header.Number.Value()
	network := c.config.Chain.Network
	for i, txn := range header.Transactions {
		if txn.Hash.Value() == skippedBlockTxn[network][blockHeight] {
			skippedTxnIndex = i
			break
		}
	}
	if skippedTxnIndex == -1 {
		return nil, xerrors.Errorf("cannot find specific txn %s in block %v", skippedBlockTxn[network][blockHeight], blockHeight)
	}
	header.Transactions = append(header.Transactions[:skippedTxnIndex], header.Transactions[skippedTxnIndex+1:]...)
	var tmpBlock any
	if err := json.Unmarshal(response.Result, &tmpBlock); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block header for manipulating errored txn: %w", err)
	}
	block, ok := tmpBlock.(map[string]any)
	if !ok {
		return nil, xerrors.Errorf("failed to convert block(%+v) to map[string]any", block)
	}
	tmpTxs, ok := block["transactions"]
	if !ok {
		return nil, xerrors.Errorf("missing transactions field in block=(%+v)", block)
	}
	txs, ok := tmpTxs.([]any)
	if !ok {
		return nil, xerrors.Errorf("failed to convert inputTxs(%+v) to []any", txs)
	}
	var filteredTxs []any
	for _, rawTx := range txs {
		tx, ok := rawTx.(map[string]any)
		if !ok {
			txsHash, ok := rawTx.(string)
			if !ok {
				return nil, xerrors.Errorf("failed to convert transaction(%+v) to string", tx)
			}
			if txsHash == skippedBlockTxn[network][blockHeight] {
				continue
			}
		} else {
			txsHash, ok := tx["hash"]
			if !ok {
				return nil, xerrors.Errorf("missing hash field in transaction=(%+v)", tx)
			}
			if txsHash == skippedBlockTxn[network][blockHeight] {
				continue
			}
		}
		filteredTxs = append(filteredTxs, rawTx)
	}
	if len(filteredTxs) != len(txs)-1 {
		return nil, xerrors.Errorf("error finding specific txn %s in block %v", skippedBlockTxn[network][blockHeight], blockHeight)
	}
	block["transactions"] = filteredTxs
	result, err := json.Marshal(block)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal restructured block header: %w", err)
	}
	return &ethereumBlockHeaderResultHolder{
		header:  &header,
		rawJson: result,
	}, nil
}

// getBorAuthor Get author(validator) for polygon blocks.
//
// bor_getAuthor endpoint now accepts blockHash as the request input, see this issue for more details: https://github.com/maticnetwork/bor/issues/582
// blockHashOrNumber can be either a blockHash string or blockNumber string in hexadecimal.
func (c *EthereumClient) getBorAuthor(ctx context.Context, blockHashOrNumber string) (json.RawMessage, error) {
	if blockHashOrNumber == genesisBlockNumber {
		nilAddress, _ := json.Marshal(ethNullAddress)
		return nilAddress, nil
	}

	params := jsonrpc.Params{
		blockHashOrNumber,
	}

	response, err := retry.WrapWithResult(ctx, func(ctx context.Context) (*jsonrpc.Response, error) {
		response, err := c.client.Call(ctx, ethBorGetAuthorMethod, params)
		if err != nil {
			var rpcErr *jsonrpc.RPCError
			if xerrors.As(err, &rpcErr) && rpcErr.Code == -32000 && rpcErr.Message == "unknown block" {
				// The nodes may be temporarily out of sync and the block may not be available in the node we just queried.
				// Retry with a different node proactively.
				// If all the retry attempts fail, return ErrBlockNotFound so that syncer may fall back to the master node.
				return nil, retry.Retryable(xerrors.Errorf("got unknown block error while querying author: %v: %w", rpcErr, internal.ErrBlockNotFound))
			}

			return nil, xerrors.Errorf("failed to call %s to get block author(blockHashOrNumber:%v): %w", ethBorGetAuthorMethod.Name, blockHashOrNumber, err)
		}

		return response, nil
	})
	if err != nil {
		return nil, err
	}

	return response.Result, nil
}

func (c *EthereumClient) getBlockTransactionReceipts(ctx context.Context, block *ethereum.EthereumBlockLit) ([][]byte, error) {
	if len(block.Transactions) == 0 || !c.nodeType.ReceiptsEnabled() {
		return nil, nil
	}

	height := block.Number.Value()
	blockHash := block.Hash.Value()
	numTransactions := len(block.Transactions)
	responses := make([]*jsonrpc.Response, numTransactions)

	for batchStart := 0; batchStart < numTransactions; batchStart += ethBlockTransactionReceiptBatchSize {
		batchEnd := batchStart + ethBlockTransactionReceiptBatchSize
		if batchEnd > numTransactions {
			batchEnd = numTransactions
		}

		batchParams := make([]jsonrpc.Params, batchEnd-batchStart)
		for i, transaction := range block.Transactions[batchStart:batchEnd] {
			batchParams[i] = jsonrpc.Params{
				transaction.Hash,
			}
		}

		if err := retry.Wrap(ctx, func(ctx context.Context) error {
			batchResponses, err := c.client.BatchCall(ctx, ethGetTransactionReceiptMethod, batchParams)
			if err != nil {
				return xerrors.Errorf(
					"failed to call %s for subset of (height=%v, blockHash=%v, startIndex=%v): %w",
					ethGetTransactionReceiptMethod.Name, height, blockHash, batchStart, err,
				)
			}

			for i, resp := range batchResponses {
				transactionIndex := batchStart + i

				var receipt ethereum.EthereumTransactionReceiptLit
				if err := resp.Unmarshal(&receipt); err != nil {
					return xerrors.Errorf(
						"failed to unmarshal transaction receipt (height=%v, blockHash=%v, transactionIndex=%v, response=%v)",
						height, blockHash, transactionIndex, string(resp.Result),
					)
				}

				// Need to retry if the returned transaction belongs to an orphaned block.
				if receipt.BlockHash.Value() != blockHash {

					// Create a fake receipt for the unsuccessful optimism transaction.
					if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_OPTIMISM && len(block.Transactions) > 0 {
						c.metrics.transactionReceiptFakeCounter.Inc(1)
						status := ethereum.EthereumQuantity(transaction_FAILED)
						blockTransaction := block.Transactions[transactionIndex]
						fakeReceipt := ethereum.EthereumTransactionReceipt{
							TransactionHash: blockTransaction.Hash,
							BlockHash:       ethereum.EthereumHexString(blockHash),
							BlockNumber:     block.Number,
							Status:          &status,
						}
						resp.Result, err = json.Marshal(&fakeReceipt)
						if err != nil {
							return xerrors.Errorf(
								"failed to marshal fake transaction receipt for optimism (height=%v, blockHash=%v, error=%v)",
								height, blockHash, err,
							)
						}
					} else {
						// Return ErrBlockNotFound so that syncer may fall back to the master node.
						return retry.Retryable(xerrors.Errorf(
							"got transaction receipt from an orphaned block (height=%v, blockHash=%v, transactionIndex=%v, response=%v): %w",
							height, blockHash, transactionIndex, string(resp.Result), internal.ErrBlockNotFound,
						))
					}
				}

				responses[transactionIndex] = resp
			}

			return nil
		}); err != nil {
			return nil, err
		}
	}

	return c.extractResultsFromResponses(responses), nil
}

func (c *EthereumClient) getBlockTraces(ctx context.Context, tag uint32, block *ethereum.EthereumBlockLit) ([][]byte, error) {
	nodeType := c.nodeType
	traceType := c.traceType
	if len(block.Transactions) == 0 || !nodeType.TracesEnabled() {
		return nil, nil
	}

	options := internal.OptionsFromContext(ctx)
	bestEffort := options != nil && options.BestEffort
	height := block.Number.Value()
	if bestEffort || c.isBlacklisted(height) || c.traceTransactions() {
		// There was a Geth DOS attack in this block range.
		// Use a different implementation to detect and skip the malicious transactions.
		return c.getTransactionTraces(ctx, tag, block)
	}

	hash := block.Hash.Value()

	if traceType.ParityTraceEnabled() || c.isArbitrumParityTrace(block) {
		number := block.Number.Value()
		params := jsonrpc.Params{
			hexutil.EncodeUint64(number),
		}
		method := ethParityTraceBlockByNumber[c.config.Blockchain()]

		response, err := c.client.Call(ctx, method, params)
		if err != nil {
			c.metrics.traceBlockServerErrorCounter.Inc(1)
			return nil, xerrors.Errorf("failed to call %s (number=%v, hash=%v): %w", method.Name, number, hash, err)
		}

		var tmpResults []json.RawMessage
		if err := json.Unmarshal(response.Result, &tmpResults); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal batch results: %w", err)
		}

		// Result structure and objects of parity trace are different from geth, the result is already flattened which is a list of single
		// layer storing all tx traces. e.g. See API doc here: https://www.quicknode.com/docs/arbitrum/arbtrace_block
		// And response example: /internal/utils/fixtures/client/arbitrum/arb_gettracesresponse.json
		results := make([][]byte, len(tmpResults))
		for i, trace := range tmpResults {
			results[i] = trace
		}

		c.metrics.traceBlockSuccessCounter.Inc(1)
		return results, nil
	} else {
		call := ethTraceBlockByHashMethod
		timeout := ethTraceBlockByHashTimeout
		if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_OPTIMISM {
			call = ethTraceBlockByHashOptimismMethod
			timeout = ethTraceBlockByHashOptimismTimeout
		}
		if c.traceType.ErigonTraceEnabled() {
			call = erigonTraceBlockByHashMethod
			timeout = erigonTraceBlockByHashTimeout
		}
		params := jsonrpc.Params{
			hash,
			map[string]string{
				"tracer":  ethCallTracer,
				"timeout": timeout,
			},
		}

		results := make([][]byte, c.getNumTraces(block.Transactions))

		response, err := retry.WrapWithResult(ctx, func(ctx context.Context) (*jsonrpc.Response, error) {
			response, err := c.client.Call(ctx, call, params)
			if err != nil {
				var rpcErr *jsonrpc.RPCError
				if xerrors.As(err, &rpcErr) {
					if rpcErr.Code == -32000 &&
						(blockNotFoundRegexp.MatchString(rpcErr.Message) || rpcErr.Message == unfinalizedDataError) {
						// The nodes may be temporarily out of sync and the block may not be available in the node we just queried.
						// Retry with a different node proactively.
						// If all the retry attempts fail, return ErrBlockNotFound so that syncer may fall back to the master node.
						return nil, retry.Retryable(xerrors.Errorf("block is not traceable: %v: %w", rpcErr, internal.ErrBlockNotFound))
					}

					if rpcErr.Code == -32000 && executionAbortedRegexp.MatchString(rpcErr.Message) {
						// Retry "RPCError -32000: execution aborted (timeout = 15s)"
						// Ref: https://github.com/ethereum/go-ethereum/blob/eed7983c7c0b0e76f1121368ace3e7e0efeb202b/internal/ethapi/api.go#L1070
						return nil, retry.Retryable(xerrors.Errorf("execution aborted while tracing block: %w", rpcErr))
					}

					if rpcErr.Code == -32002 && requestTimedOutRegexp.MatchString(rpcErr.Message) {
						return nil, retry.Retryable(xerrors.Errorf("request timed out while tracing block: %w", rpcErr))
					}
				}

				return nil, xerrors.Errorf("failed to call %s (height=%v, hash=%v): %w", call.Name, height, hash, err)
			}

			return response, nil
		})
		if err != nil {
			c.metrics.traceBlockServerErrorCounter.Inc(1)
			return nil, err
		}

		var tmpResults []ethereumResultHolder
		if err := json.Unmarshal(response.Result, &tmpResults); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal batch results: %w", err)
		}

		if len(results) != len(tmpResults) {
			return nil, xerrors.Errorf("unexpected number of results: expected=%v actual=%v", len(results), len(tmpResults))
		}

		for i, result := range tmpResults {
			// It is expected that, after https://github.com/ledgerwatch/erigon/issues/4935 is fixed, Erigon block trace
			// format will fall back to the GETH format. When that happens is uncertain, but this check should maintain
			// forward compatibility and can be removed once the change is complete.
			if traceType.ErigonTraceEnabled() && jsonrpc.IsNullOrEmpty(result.Result) {
				var erigonResults []json.RawMessage
				if err := json.Unmarshal(response.Result, &erigonResults); err != nil {
					return nil, xerrors.Errorf("failed to unmarshal erigon results: %w", err)
				}

				if len(results) != len(erigonResults) {
					return nil, xerrors.Errorf("unexpected number of erigon results: expected=%v actual=%v", len(results), len(erigonResults))
				}

				for i, eResult := range erigonResults {
					if jsonrpc.IsNullOrEmpty(eResult) {
						return nil, xerrors.Errorf("received empty erigon trace result at index %v", i)
					}
					results[i] = eResult
				}
				break
			}

			if result.Error != "" {
				// If an Optimism transaction is failed with the following error: "Fail with error
				// 'deployer address not whitelisted:'", we need to create a fake trace
				// because the debug_traceBlockByHash will return null trace for that transaction.
				// e.g. For height=87673 (https://optimistic.etherscan.io/tx/87673), we get the following error from
				// debug_traceBlockByHash: "TypeError: cannot read property 'toString' of undefined in server-side
				// tracer function 'result'" (https://optimistic.etherscan.io/vmtrace?txhash=0xcf6e46a1f41e1678fba10590f9d092690c5e8fd2e85a3614715fb21caa74655d&type=gethtrace20)
				if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_OPTIMISM && result.Error == optimismWhitelistError {
					c.metrics.traceBlockFakeCounter.Inc(1)
					fakeTrace := ethereum.EthereumTransactionTrace{
						Error: optimismFakeTraceError,
					}
					byteTrace, err := json.Marshal(&fakeTrace)
					if err != nil {
						return nil, xerrors.Errorf("failed to marshal fake trace for optimism block %v: %w", height, err)
					}
					result.Result = byteTrace
					c.logger.Warn("generate fake trace for block",
						zap.Uint64("height", height),
					)
				} else {
					// Calling tracer is expensive and occasionally it may return an error of "execution timeout".
					// See https://github.com/ethereum/go-ethereum/blob/dd9c3225cf06dab0acf783fad671b4f601a4470e/eth/tracers/api.go#L808
					c.metrics.traceBlockExecutionTimeoutCounter.Inc(1)
					return nil, xerrors.Errorf("received partial result (height=%v, hash=%v, index=%v): %v", height, hash, i, result.Error)
				}
			}
			results[i] = result.Result
		}

		c.metrics.traceBlockSuccessCounter.Inc(1)
		return results, nil
	}
}

// Because of Ethereum DoS attacks,
// we need to skip those malicious transactions that slow down or even crash the geth nodes.
// The algorithm is inspired by https://tjayrush.medium.com/defeating-the-ethereum-ddos-attacks-d3d773a9a063
func (c *EthereumClient) getTransactionTraces(ctx context.Context, tag uint32, block *ethereum.EthereumBlockLit) ([][]byte, error) {
	if len(block.Transactions) == 0 {
		return nil, nil
	}

	traceError, err := json.Marshal(&ethereumResultHolder{Error: ethTraceTransactionError})
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal error result: %w", err)
	}

	height := block.Number.Value()
	blockHash := block.Hash.Value()
	traceType := c.traceType
	var ignoredTransactions []int
	var results [][]byte

	if traceType.ParityTraceEnabled() || c.isArbitrumParityTrace(block) {
		var parityResults [][]byte
		for i, transaction := range block.Transactions {
			transactionHash := transaction.Hash.Value()
			params := jsonrpc.Params{
				transactionHash,
			}
			method := ethParityTraceTransaction[c.config.Blockchain()]

			response, err := c.client.Call(ctx, method, params)
			if err != nil {
				if c.isServerError(err) {
					c.logger.Warn(
						"ignored transaction trace",
						zap.String("reason", "trace_transaction_timeout"),
						zap.Uint64("height", height),
						zap.String("transaction_hash", transactionHash),
						zap.Error(err),
					)
					parityResults = append(parityResults, traceError)
					ignoredTransactions = append(ignoredTransactions, i)
					continue
				}
				// special error handling for 'found no trace' error, e.g. an arbitrum transaction may contain 0 trace
				// See https://arbiscan.io/tx/0x9db5708abe7ddf85ff6155581746fa2658f09d3939f0f4cfd1761df99ec34b4b
				var rpcError *jsonrpc.RPCError
				if xerrors.As(err, &rpcError) &&
					rpcError.Code == ethParityTransactionTraceErrCode &&
					rpcError.Message == ethParityTransactionTraceErrMessageFoundNoTrace {
					continue
				}

				return nil, xerrors.Errorf("failed to call %s (transaction hash=%v): %w", method.Name, transactionHash, err)
			}

			var tmpResults []json.RawMessage
			if err := json.Unmarshal(response.Result, &tmpResults); err != nil {
				return nil, xerrors.Errorf("failed to unmarshal batch parityResults: %w", err)
			}
			for _, result := range tmpResults {
				parityResults = append(parityResults, result)
			}
		}
		results = parityResults
	} else {
		gethResults := make([][]byte, c.getNumTraces(block.Transactions))
		for i, transaction := range block.Transactions {
			if i >= len(gethResults) {
				break
			}
			transactionHash := transaction.Hash.Value()

			opCount := ethTraceTransactionUnknownOpCount
			if c.isBlacklisted(height) {
				// Skip the transaction if its op count is greater than the threshold.
				// Without this check, debug_traceTransaction below may cause the node to crash.
				// For example, 0xb1e822c280cd9a3f8e58abeb1ef8ddd4642787c555cdc6da54d4ce46f959daf2, which has 40528 ops, will be filtered out.
				opCount = c.traceTransactionOpCount(ctx, transactionHash, height)
				if opCount == ethTraceTransactionUnknownOpCount || opCount > ethTraceTransactionMaxOpCount {
					c.logger.Warn(
						"ignored transaction trace",
						zap.String("reason", "op_count"),
						zap.Uint64("height", height),
						zap.String("transaction_hash", transactionHash),
						zap.Int("op_count", opCount),
						zap.Int("max_op_count", ethTraceTransactionMaxOpCount),
					)
					gethResults[i] = traceError
					ignoredTransactions = append(ignoredTransactions, i)
					continue
				}
			}

			call := ethTraceTransactionMethod
			timeout := ethTraceTransactionTimeout
			if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_OPTIMISM {
				call = ethTraceTransactionOptimismMethod
				timeout = ethTraceTransactionOptimismTimeout
			}
			params := jsonrpc.Params{
				transactionHash,
				map[string]string{
					"tracer":  ethCallTracer,
					"timeout": timeout,
				},
			}

			response, err := retry.WrapWithResult(ctx, func(ctx context.Context) (*jsonrpc.Response, error) {
				response, err := c.client.Call(ctx, call, params)
				if err != nil {
					var rpcErr *jsonrpc.RPCError
					if xerrors.As(err, &rpcErr) && rpcErr.Code == -32000 && rpcErr.Message == "genesis is not traceable" {
						// The nodes may be temporarily out of sync and the block may not be available in the node we just queried.
						// Retry with a different node proactively.
						// If all the retry attempts fail, return ErrBlockNotFound so that syncer may fall back to the master node.
						return nil, retry.Retryable(xerrors.Errorf("transaction is not traceable: %v: %w", rpcErr, internal.ErrBlockNotFound))
					}

					return nil, xerrors.Errorf("failed to trace transaction %v: %w", transactionHash, err)
				}

				return response, nil
			})
			if err != nil {
				if c.isServerError(err) {
					c.logger.Warn(
						"ignored transaction trace",
						zap.String("reason", "trace_transaction_timeout"),
						zap.Uint64("height", height),
						zap.String("transaction_hash", transactionHash),
						zap.Int("op_count", opCount),
						zap.Error(err),
					)
					gethResults[i] = traceError
					ignoredTransactions = append(ignoredTransactions, i)
					continue
				}

				traceByte, processed, processErr := c.processFailedTransactionTrace(err)
				if processed {
					if processErr != nil {
						return nil, xerrors.Errorf("failed to process failed transaction trace %v: %w", transactionHash, processErr)
					} else {
						gethResults[i] = traceByte
						c.logger.Warn("processed failed transaction trace",
							zap.String("hash", transactionHash),
						)
						continue
					}
				}

				return nil, xerrors.Errorf("failed to call %v (hash=%v, height=%v): %w", call.Name, transactionHash, height, err)
			}

			gethResults[i] = response.Result
		}
		results = gethResults
	}

	numIgnored := len(ignoredTransactions)
	numSuccess := len(block.Transactions) - numIgnored
	c.metrics.traceTransactionSuccessCounter.Inc(int64(numSuccess))
	if numIgnored > 0 {
		c.metrics.traceTransactionIgnoredCounter.Inc(int64(numIgnored))
		if err := c.createFailedTransactionTraceDLQMessage(ctx, tag, height, blockHash, ignoredTransactions); err != nil {
			return nil, xerrors.Errorf("failed to send to dlq: %w", err)
		}
	}

	return results, nil
}

func (c *EthereumClient) isArbitrumParityTrace(block *ethereum.EthereumBlockLit) bool {
	// All arb_trace methods on the Arbitrum One chain should be called on blocks prior to 22207818 due to NITRO upgrade.
	// Reference - arbitrum NITRO: https://github.com/OffchainLabs/nitro
	// QuickNode arb endpoint guide: https://www.quicknode.com/docs/arbitrum/arbtrace_block and https://www.quicknode.com/docs/arbitrum/debug_traceBlockByNumber
	return c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_ARBITRUM && block.Number < arbitrumNITROUpgradeBlockNumber
}

func (c *EthereumClient) traceTransactionOpCount(ctx context.Context, hash string, height uint64) int {
	params := jsonrpc.Params{
		hash,
		map[string]string{
			"tracer":  ethOpCountTracer,
			"timeout": ethTraceTransactionTimeout,
		},
	}

	response, err := c.client.Call(ctx, ethTraceTransactionMethod, params)
	if err != nil {
		// Since opcount tracer is a very lightweight call,
		// if it is failing, it is highly likely that the transaction belongs to the DoS attack.
		c.logger.Warn(
			"failed to query op count",
			zap.Uint64("height", height),
			zap.String("hash", hash),
			zap.Error(err),
		)
		return ethTraceTransactionUnknownOpCount
	}

	var opCount int
	if err := response.Unmarshal(&opCount); err != nil {
		c.logger.Warn(
			"failed to unmarshal op count",
			zap.Uint64("height", height),
			zap.String("hash", hash),
			zap.Error(err),
		)
		return ethTraceTransactionUnknownOpCount
	}

	return opCount
}

func (c *EthereumClient) createFailedTransactionTraceDLQMessage(ctx context.Context, tag uint32, height uint64, hash string, ignoredTransactions []int) error {
	if err := c.dlq.SendMessage(ctx, &dlq.Message{
		Topic: dlq.FailedTransactionTraceTopic,
		Data: &dlq.FailedTransactionTraceData{
			Tag:                 tag,
			Height:              height,
			Hash:                hash,
			IgnoredTransactions: ignoredTransactions,
		},
	}); err != nil {
		return xerrors.Errorf("failed to send to dlq: %w", err)
	}

	return nil
}

func (c *EthereumClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	switch c.commitmentLevel {
	case types.CommitmentLevelSafe, types.CommitmentLevelFinalized:
		response, err := c.client.Call(ctx, ethGetBlockByNumberMethod, jsonrpc.Params{
			string(c.commitmentLevel),
			false,
		})
		if err != nil {
			return 0, xerrors.Errorf("failed to get block by commitment level %v: %w", c.commitmentLevel, err)
		}

		header, err := c.getBlockHeader(response)
		if err != nil {
			return 0, xerrors.Errorf("failed to get header for block %v: %w", c.commitmentLevel, err)
		}

		height := header.header.Number.Value()
		return height, nil

	default:
		response, err := c.client.Call(ctx, ethBlockNumber, nil)
		if err != nil {
			return 0, xerrors.Errorf("failed to call %s: %w", ethBlockNumber.Name, err)
		}

		var result ethereum.EthereumHexString
		if err := response.Unmarshal(&result); err != nil {
			return 0, xerrors.Errorf("failed to unmarshal result: %w", err)
		}

		height, err := hexutil.DecodeUint64(result.Value())
		if err != nil {
			return 0, xerrors.Errorf("failed to decode height: %w", err)
		}

		return height, nil
	}
}

func (c *EthereumClient) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	oldTag := block.Metadata.Tag
	if c.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_ETHEREUM &&
		c.config.Chain.Network == common.Network_NETWORK_ETHEREUM_MAINNET {
		if oldTag == 0 && newTag == 1 {
			// Merge in uncles to upgrade from 0 to 1.
			blobdata := block.GetEthereum()
			if blobdata == nil {
				return nil, xerrors.Errorf("empty blobdata")
			}

			hash := block.Metadata.Hash
			count, err := c.getBlockUncleCount(ctx, hash)
			if err != nil {
				return nil, xerrors.Errorf("failed to get block uncle count (hash=%v): %w", hash, err)
			}

			uncles, err := c.getBlockUncles(ctx, hash, count)
			if err != nil {
				return nil, xerrors.Errorf("failed to get block uncles (hash=%v, count=%v): %w", hash, count, err)
			}

			// Upgrade tag and merge in additional data.
			block.Metadata.Tag = newTag
			blobdata.Uncles = uncles
			return block, nil
		}

		if oldTag == 1 && newTag == 2 {
			// Update traces to upgrade from 1 to 2.
			blobdata := block.GetEthereum()
			if blobdata == nil {
				return nil, xerrors.Errorf("empty blobdata")
			}

			height := block.Metadata.Height
			originNumTransactions := len(blobdata.TransactionTraces)

			headerResult, err := c.getBlockHeaderResult(ctx, height)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch header result for block %v: %w", height, err)
			}

			transactionTraces, err := c.getBlockTraces(ctx, newTag, headerResult.header)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch traces for block %v: %w", height, err)
			}

			// Validate transactionTraces
			if len(transactionTraces) != originNumTransactions {
				return nil, xerrors.Errorf("unexpected number of transaction traces, expect=%v, actual=%v", originNumTransactions, len(transactionTraces))
			}

			// Upgrade tag and update traces data.
			block.Metadata.Tag = newTag
			blobdata.TransactionTraces = transactionTraces
			return block, nil
		}
	} else if c.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_POLYGON &&
		c.config.Chain.Network == common.Network_NETWORK_POLYGON_MAINNET {
		if oldTag == 2 && newTag == 2 {
			blobdata := block.GetEthereum()
			if blobdata == nil {
				return nil, xerrors.Errorf("empty blobdata")
			}

			height := block.Metadata.Height
			headerResult, err := c.getBlockHeaderResult(ctx, height)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch header result for block %v: %w", height, err)
			}

			// Update blobdata header
			blobdata.Header = headerResult.rawJson

			return block, nil
		}
	}

	return nil, internal.ErrNotImplemented
}

func (c *EthereumClient) getBlockUncleCount(ctx context.Context, hash string) (uint64, error) {
	response, err := c.client.Call(ctx, ethGetUncleCountByBlockHash, jsonrpc.Params{
		hash,
	})
	if err != nil {
		return 0, xerrors.Errorf("failed to call %v: %w", ethGetUncleCountByBlockHash.Name, err)
	}

	var result ethereum.EthereumHexString
	if err := response.Unmarshal(&result); err != nil {
		return 0, xerrors.Errorf("failed to unmarshal result: %w", err)
	}

	count, err := hexutil.DecodeUint64(result.Value())
	if err != nil {
		return 0, xerrors.Errorf("failed to decode count: %w", err)
	}

	return count, nil
}

func (c *EthereumClient) getBlockUncles(ctx context.Context, hash string, count uint64) ([][]byte, error) {
	if count == 0 || c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_ARBITRUM || c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_FANTOM {
		return nil, nil
	}

	batchParams := make([]jsonrpc.Params, count)
	for i := uint64(0); i < count; i++ {
		batchParams[i] = jsonrpc.Params{
			hash,
			hexutil.EncodeUint64(i),
		}
	}

	responses, err := c.client.BatchCall(ctx, ethGetUncleByBlockHashAndIndex, batchParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to call %v (hash=%v): %w", ethGetUncleByBlockHashAndIndex.Name, hash, err)
	}

	return c.extractResultsFromResponses(responses), nil
}

func (c *EthereumClient) extractResultsFromResponses(responses []*jsonrpc.Response) [][]byte {
	results := make([][]byte, len(responses))
	for i, response := range responses {
		results[i] = response.Result
	}

	return results
}

// isServerError checks if the error is potentially an unrecoverable error.
// Note that the implementation may result in false positives,
// e.g. the request may time out when server is overloaded.
func (c *EthereumClient) isServerError(err error) bool {
	var httpErr *jsonrpc.HTTPError
	if xerrors.As(err, &httpErr) && httpErr.Code >= 500 {
		return true
	}

	var rpcErr *jsonrpc.RPCError
	if xerrors.As(err, &rpcErr) && rpcErr.Code == -32000 && rpcErr.Message == ethExecutionTimeoutError {
		return true
	}

	if xerrors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return false
}

func (c *EthereumClient) processFailedTransactionTrace(err error) ([]byte, bool, error) {
	processed := false
	if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_OPTIMISM {
		// If an Optimism transaction is failed with the following error: "Fail with error
		// 'deployer address not whitelisted:'", we need to create a fake trace
		// e.g. For height=87673 (https://optimistic.etherscan.io/tx/87673), we get the following error from
		// debug_traceTransaction: "TypeError: cannot read property 'toString' of undefined in server-side
		// tracer function 'result'" (https://optimistic.etherscan.io/vmtrace?txhash=0xcf6e46a1f41e1678fba10590f9d092690c5e8fd2e85a3614715fb21caa74655d&type=gethtrace20)
		var rpcErr *jsonrpc.RPCError
		if xerrors.As(err, &rpcErr) && rpcErr.Code == -32000 && rpcErr.Message == optimismWhitelistError {
			byteTrace, processErr := json.Marshal(ethereum.EthereumTransactionTrace{Error: optimismFakeTraceError})
			processed = true
			if processErr != nil {
				return nil, processed, xerrors.Errorf("failed to marshal fake trace for optimism transaction: %w", processErr)
			} else {
				c.metrics.traceTransactionFakeCounter.Inc(1)
				return byteTrace, processed, nil
			}
		}
	}
	return nil, processed, nil
}

func (c *EthereumClient) CanReprocess(tag uint32, height uint64) bool {
	return !c.isBlacklisted(height)
}

func (c *EthereumClient) isBlacklisted(height uint64) bool {
	if c.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_ETHEREUM {
		for _, br := range ethBlacklistedRanges[c.config.Chain.Network] {
			if height >= br.StartHeight && height < br.EndHeight {
				return true
			}
		}
	}

	return false
}

func (c *EthereumClient) traceTransactions() bool {
	return false
}

func (c *EthereumClient) getNumTraces(transactions []*ethereum.EthereumTransactionLit) int {
	numTraces := len(transactions)
	if numTraces == 0 {
		return numTraces
	}
	if c.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_POLYGON &&
		transactions[len(transactions)-1].From == ethNullAddress &&
		transactions[len(transactions)-1].To == ethNullAddress {
		// Keep the result same as TraceBlockByHash
		// In this case the last transactions will be ignored in TraceBlockByHash
		// e.g., https://polygonscan.com/block/2304
		numTraces -= 1
	}

	return numTraces
}

func (c *EthereumClient) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	// eth_getProof supports both block height and block hash. To avoid the impact of re-org, we will call with block hash if it is provided.
	// Otherwise, we will query with block height.
	internalReq := req.Req
	heightOrHash := internalReq.Hash
	if heightOrHash == "" {
		heightOrHash = hexutil.EncodeUint64(internalReq.Height)
	}

	contractAddr := internalReq.GetEthereum().GetErc20Contract()
	var account string
	var params jsonrpc.Params

	// If the input contract addrss is empty, then the account would be the native token account address.
	// Otherwise, we need to query the token account proof.
	if contractAddr == "" {
		account = internalReq.Account
		params = jsonrpc.Params{
			account,
			[]string{}, // This is for the storage state. Not needed for native token.
			heightOrHash,
		}
	} else {
		account = contractAddr

		// Make sure the input erc20 token is supported.
		_, ok := erc20StorageIndex[account]
		if !ok {
			return nil, xerrors.Errorf("the input erc20 token(%s) is not supported yet", account)
		}

		// Need to remove the "0x" prefix.
		accountData, err := hexutil.Decode(internalReq.Account)
		if err != nil {
			return nil, xerrors.Errorf("failed to hexutil.Decode the account %s: %w", internalReq.Account, err)
		}
		// Get the target slot index for this erc20 tokent.
		slotIndex := big.NewInt(int64(erc20StorageIndex[account])).Bytes()

		// This is the way to calculate the storage parameter !
		storageKey := crypto.Keccak256(geth.LeftPadBytes(accountData, 32), geth.LeftPadBytes(slotIndex, 32))

		params = jsonrpc.Params{
			// Use the contract account.
			account,
			// Use the calculate stoarge key.
			// Note that, we need to convert the storage key to be hex string.
			[]string{hexutil.Bytes(storageKey).String()},
			heightOrHash,
		}
	}

	response, err := c.client.Call(ctx, ethGetProofMethod, params)
	// TODO: we may need to parse the error message, and retry if the error is block not found.
	if err != nil {
		return nil, xerrors.Errorf("failed to call %s for block height %d, block hash %s: %w", ethGetProofMethod.Name, internalReq.Height, internalReq.Hash, err)
	}

	return &api.GetAccountProofResponse{
		Response: &api.GetAccountProofResponse_Ethereum{
			Ethereum: &api.EthereumAccountStateProof{
				AccountProof: []byte(response.Result),
			},
		},
	}, nil
}
