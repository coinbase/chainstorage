package client

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/go-playground/validator/v10"
	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/types"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	EthereumClient struct {
		config    *config.Config
		logger    *zap.Logger
		client    jsonrpc.Client
		dlq       dlq.DLQ
		validate  *validator.Validate
		metrics   *ethereumClientMetrics
		nodeType  types.EthereumNodeType
		traceType types.TraceType
		retry     retry.Retry
	}

	EthereumClientParams struct {
		fx.In
		fxparams.Params
		MasterClient    jsonrpc.Client `name:"master"`
		SlaveClient     jsonrpc.Client `name:"slave"`
		ValidatorClient jsonrpc.Client `name:"validator"`
		DLQ             dlq.DLQ
	}

	EthereumClientFactoryOption func(factory *ethereumClientFactory)

	ethereumClientFactory struct {
		config          *config.Config
		logger          *zap.Logger
		metrics         tally.Scope
		dlq             dlq.DLQ
		masterClient    jsonrpc.Client
		slaveClient     jsonrpc.Client
		validatorClient jsonrpc.Client
		nodeType        types.EthereumNodeType
		traceType       types.TraceType
	}

	ethereumClientMetrics struct {
		traceBlockSuccessCounter          tally.Counter
		traceBlockServerErrorCounter      tally.Counter
		traceBlockExecutionTimeoutCounter tally.Counter
		traceBlockFakeCounter             tally.Counter
		traceTransactionSuccessCounter    tally.Counter
		traceTransactionIgnoredCounter    tally.Counter
		transactionReceiptFakeCounter     tally.Counter
	}

	ethereumResultHolder struct {
		Error  string          `json:"error"`
		Result json.RawMessage `json:"result,omitempty"`
	}

	ethereumBlockHeaderResultHolder struct {
		header  *parser.EthereumBlockLit // Use the light version for faster parsing.
		rawJson json.RawMessage          // Store the raw message in blob storage.
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
	genesisBlockNumber = uint64(0)
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
		Timeout: time.Second * 10,
	}

	ethGetBlockByHashMethod = &jsonrpc.RequestMethod{
		Name:    "eth_getBlockByHash",
		Timeout: time.Second * 10,
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
	}

	ethParityTraceTransaction = map[common.Blockchain]*jsonrpc.RequestMethod{
		common.Blockchain_BLOCKCHAIN_ARBITRUM: {
			Name:    "arbtrace_transaction",
			Timeout: time.Second * 60,
		},
	}
)

var (
	skippedBlockTxn = map[common.Network]map[uint64]string{
		common.Network_NETWORK_ARBITRUM_MAINNET: {
			uint64(15458950): "0xf135954c7b2a17c094f917fff69aa215fa9af86443e55f167e701e39afa5ff0f",
			uint64(4527955):  "0x1d76d3d13e9f8cc713d484b0de58edd279c4c62e46e963899aec28eb648b5800",
		},
	}

	polygonBlockNotFoundRegexp = regexp.MustCompile(`block \w+ not found`)
)

func NewEthereumClientFactory(params EthereumClientParams, opts ...EthereumClientFactoryOption) ClientFactory {
	factory := &ethereumClientFactory{
		config:          params.Config,
		logger:          params.Logger,
		metrics:         params.Metrics,
		dlq:             params.DLQ,
		masterClient:    params.MasterClient,
		slaveClient:     params.SlaveClient,
		validatorClient: params.ValidatorClient,
		nodeType:        types.EthereumNodeType_ARCHIVAL,
		traceType:       types.TraceType_GETH,
	}
	for _, opt := range opts {
		opt(factory)
	}

	return factory
}

func WithEthereumNodeType(nodeType types.EthereumNodeType) EthereumClientFactoryOption {
	return func(factory *ethereumClientFactory) {
		factory.nodeType = nodeType
	}
}

func WithEthereumTraceType(traceType types.TraceType) EthereumClientFactoryOption {
	return func(factory *ethereumClientFactory) {
		factory.traceType = traceType
	}
}

func (f *ethereumClientFactory) Master() Client {
	return f.newClient(f.masterClient)
}

func (f *ethereumClientFactory) Slave() Client {
	return f.newClient(f.slaveClient)
}

func (f *ethereumClientFactory) Validator() Client {
	return f.newClient(f.validatorClient)
}

func (f *ethereumClientFactory) newClient(client jsonrpc.Client) Client {
	logger := log.WithPackage(f.logger)
	return &EthereumClient{
		config:    f.config,
		logger:    logger,
		client:    client,
		dlq:       f.dlq,
		validate:  validator.New(),
		metrics:   newEthereumClientMetrics(f.metrics),
		nodeType:  f.nodeType,
		traceType: f.traceType,
		retry:     retry.New(retry.WithLogger(logger)),
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

	transactionReceiptFakeCounter := scope.Counter(transactionReceiptCounter)

	return &ethereumClientMetrics{
		traceBlockSuccessCounter:          traceBlockSuccessCounter,
		traceBlockServerErrorCounter:      traceBlockServerErrorCounter,
		traceBlockExecutionTimeoutCounter: traceBlockExecutionTimeoutCounter,
		traceTransactionSuccessCounter:    traceTransactionSuccessCounter,
		traceTransactionIgnoredCounter:    traceTransactionIgnoredCounter,
		traceBlockFakeCounter:             traceBlockFakeCounter,
		transactionReceiptFakeCounter:     transactionReceiptFakeCounter,
	}
}

// BatchGetBlockMetadata gets block metadatas for the height range [from, to), where "from" is the start of the range inclusive and "to" is the end exclusive.
func (c *EthereumClient) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range range of [%d, %d)", from, to)
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

	responses, err := c.client.BatchCall(ctx, ethGetBlockByNumberMethod, batchParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block metadata: %w", err)
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
			ParentHeight: getParentHeight(height),
			Hash:         headerResult.header.Hash.Value(),
			ParentHash:   headerResult.header.ParentHash.Value(),
			Timestamp:    utils.ToTimestamp(int64(headerResult.header.Timestamp.Value())),
		}
	}

	return blockMetadatas, nil
}

func (c *EthereumClient) getBlockHeaderResult(ctx context.Context, height uint64, opts ...ClientOption) (*ethereumBlockHeaderResultHolder, error) {
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

func (c *EthereumClient) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, opts ...ClientOption) (*api.Block, error) {
	ctx = contextWithOptions(ctx, opts...)

	headerResult, err := c.getBlockHeaderResult(ctx, height, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get header result for block %v: %w", height, err)
	}

	return c.getBlockFromHeader(ctx, tag, headerResult)
}

func (c *EthereumClient) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...ClientOption) (*api.Block, error) {
	ctx = contextWithOptions(ctx, opts...)
	params := jsonrpc.Params{
		hash,
		true,
	}

	response, err := c.client.Call(ctx, ethGetBlockByHashMethod, params)

	if err != nil {
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
			ParentHeight: getParentHeight(headerResult.header.Number.Value()),
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
		author, err := c.getBorAuthorByNumber(ctx, headerResult.header.Number.Value())
		if err != nil {
			return nil, xerrors.Errorf("failed to get block author: %w", err)
		}

		block.GetEthereum().ExtraData = &api.EthereumBlobdata_Polygon{
			Polygon: &api.PolygonBlobdata{
				Author: author,
			},
		}
	}

	return block, nil
}

func (c *EthereumClient) getBlockHeader(response *jsonrpc.Response) (*ethereumBlockHeaderResultHolder, error) {
	var header parser.EthereumBlockLit
	if err := response.Unmarshal(&header); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block header: %w", err)
	}

	if header.Hash == "" {
		return nil, xerrors.Errorf("block not found: %w", ErrBlockNotFound)
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

func (c *EthereumClient) deleteDuplicateTxn(response *jsonrpc.Response, header parser.EthereumBlockLit) (*ethereumBlockHeaderResultHolder, error) {
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
	var tmpBlock interface{}
	if err := json.Unmarshal(response.Result, &tmpBlock); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block header for manipulating errored txn: %w", err)
	}
	block, ok := tmpBlock.(map[string]interface{})
	if !ok {
		return nil, xerrors.Errorf("failed to convert block(%+v) to map[string]interface{}", block)
	}
	tmpTxs, ok := block["transactions"]
	if !ok {
		return nil, xerrors.Errorf("missing transactions field in block=(%+v)", block)
	}
	txs, ok := tmpTxs.([]interface{})
	if !ok {
		return nil, xerrors.Errorf("failed to convert inputTxs(%+v) to []interface{}", txs)
	}
	var filteredTxs []interface{}
	for _, rawTx := range txs {
		tx, ok := rawTx.(map[string]interface{})
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

func (c *EthereumClient) getBorAuthorByNumber(ctx context.Context, number uint64) (json.RawMessage, error) {
	if number == genesisBlockNumber {
		nilAddress, _ := json.Marshal(ethNullAddress)
		return nilAddress, nil
	}

	params := jsonrpc.Params{
		hexutil.EncodeUint64(number),
	}

	var response *jsonrpc.Response
	if err := c.retry.Retry(ctx, func(ctx context.Context) error {
		tmpResponse, tmpErr := c.client.Call(ctx, ethBorGetAuthorMethod, params)
		if tmpErr != nil {
			if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_POLYGON {
				var rpcErr *jsonrpc.RPCError
				if xerrors.As(tmpErr, &rpcErr) && rpcErr.Code == -32000 && rpcErr.Message == "unknown block" {
					// The nodes may be temporarily out of sync and the block may not be available in the node we just queried.
					// Retry with a different node.
					return retry.Retryable(xerrors.Errorf("got unknown block error while querying author: %w", rpcErr))
				}
			}

			return tmpErr
		}

		response = tmpResponse
		return nil
	}); err != nil {
		return nil, xerrors.Errorf("failed to call %s for block author %v: %w", ethBorGetAuthorMethod.Name, number, err)
	}

	return response.Result, nil
}

func (c *EthereumClient) getBlockTransactionReceipts(ctx context.Context, block *parser.EthereumBlockLit) ([][]byte, error) {
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

		if err := c.retry.Retry(ctx, func(ctx context.Context) error {
			batchResponses, err := c.client.BatchCall(ctx, ethGetTransactionReceiptMethod, batchParams)
			if err != nil {
				return xerrors.Errorf(
					"failed to call %s for subset of (height=%v, blockHash=%v, startIndex=%v): %w",
					ethGetTransactionReceiptMethod.Name, height, blockHash, batchStart, err,
				)
			}

			for i, resp := range batchResponses {
				transactionIndex := batchStart + i

				var receipt parser.EthereumTransactionReceiptLit
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
						status := parser.EthereumQuantity(transaction_FAILED)
						blockTransaction := block.Transactions[transactionIndex]
						fakeReceipt := parser.EthereumTransactionReceipt{
							TransactionHash: blockTransaction.Hash,
							BlockHash:       parser.EthereumHexString(blockHash),
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
						return retry.Retryable(xerrors.Errorf(
							"got transaction receipt from an orphaned block (height=%v, blockHash=%v, transactionIndex=%v, response=%v)",
							height, blockHash, transactionIndex, string(resp.Result),
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

func (c *EthereumClient) getBlockTraces(ctx context.Context, tag uint32, block *parser.EthereumBlockLit) ([][]byte, error) {
	nodeType := c.nodeType
	traceType := c.traceType
	if len(block.Transactions) == 0 || !nodeType.TracesEnabled() {
		return nil, nil
	}

	options := optionsFromContext(ctx)
	bestEffort := options != nil && options.bestEffort
	height := block.Number.Value()
	if bestEffort || c.isBlacklisted(height) {
		// There was a Geth DOS attack in this block range.
		// Use a different implementation to detect and skip the malicious transactions.
		return c.getTransactionTraces(ctx, tag, block)
	}

	hash := block.Hash.Value()
	if traceType.ParityTraceEnabled() {
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

		var response *jsonrpc.Response
		if err := c.retry.Retry(ctx, func(ctx context.Context) error {
			tmpResponse, tmpErr := c.client.Call(ctx, call, params)
			if tmpErr != nil {
				if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_POLYGON {
					var rpcErr *jsonrpc.RPCError
					if xerrors.As(tmpErr, &rpcErr) && rpcErr.Code == -32000 && polygonBlockNotFoundRegexp.MatchString(rpcErr.Message) {
						// The nodes may be temporarily out of sync and the block may not be available in the node we just queried.
						// Retry with a different node.
						return retry.Retryable(xerrors.Errorf("block is not traceable: %w", rpcErr))
					}
				}

				return tmpErr
			}

			response = tmpResponse
			return nil
		}); err != nil {
			c.metrics.traceBlockServerErrorCounter.Inc(1)
			return nil, xerrors.Errorf("failed to call %s (height=%v, hash=%v): %w", call.Name, height, hash, err)
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
					fakeTrace := parser.EthereumTransactionTrace{
						Error: optimismFakeTraceError,
					}
					byteTrace, err := json.Marshal(&fakeTrace)
					if err != nil {
						return nil, xerrors.Errorf("failed to marshal fake trace for optimism block %v: %w", height, err)
					}
					result.Result = byteTrace
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
func (c *EthereumClient) getTransactionTraces(ctx context.Context, tag uint32, block *parser.EthereumBlockLit) ([][]byte, error) {
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

	if traceType.ParityTraceEnabled() {
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
				// See https://etherscan.io/tx/0xb1e822c280cd9a3f8e58abeb1ef8ddd4642787c555cdc6da54d4ce46f959daf2/#internal
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

			// TODO: https://jira.coinbase-corp.com/browse/CDF-2360
			timeout := ethTraceTransactionTimeout
			if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_OPTIMISM {
				timeout = ethTraceTransactionOptimismTimeout
			}
			params := jsonrpc.Params{
				transactionHash,
				map[string]string{
					"tracer":  ethCallTracer,
					"timeout": timeout,
				},
			}

			var response *jsonrpc.Response
			err := c.retry.Retry(ctx, func(ctx context.Context) error {
				tmpResponse, tmpErr := c.client.Call(ctx, ethTraceTransactionMethod, params)
				if tmpErr != nil {
					if c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_POLYGON {
						var rpcErr *jsonrpc.RPCError
						if xerrors.As(tmpErr, &rpcErr) && rpcErr.Code == -32000 && rpcErr.Message == "genesis is not traceable" {
							// The nodes may be temporarily out of sync and the block may not be available in the node we just queried.
							// Retry with a different node.
							return retry.Retryable(xerrors.Errorf("transaction is not traceable: %w", rpcErr))
						}
					}

					return tmpErr
				}

				response = tmpResponse
				return nil
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

				return nil, xerrors.Errorf("failed to call %v (hash=%v, height=%v): %w", ethTraceTransactionMethod.Name, transactionHash, height, err)
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
	response, err := c.client.Call(ctx, ethBlockNumber, nil)
	if err != nil {
		return 0, xerrors.Errorf("failed to call %s: %w", ethBlockNumber.Name, err)
	}

	var result parser.EthereumHexString
	if err := response.Unmarshal(&result); err != nil {
		return 0, xerrors.Errorf("failed to unmarshal result: %w", err)
	}

	height, err := hexutil.DecodeUint64(result.Value())
	if err != nil {
		return 0, xerrors.Errorf("failed to decode height: %w", err)
	}

	return height, nil
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
		if oldTag == 1 && newTag == 2 {
			blobdata := block.GetEthereum()
			if blobdata == nil {
				return nil, xerrors.Errorf("empty blobdata")
			}

			height := block.Metadata.Height

			headerResult, err := c.getBlockHeaderResult(ctx, height)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch header result for block %v: %w", height, err)
			}

			author, err := c.getBorAuthorByNumber(ctx, headerResult.header.Number.Value())
			if err != nil {
				return nil, xerrors.Errorf("failed to get block author: %w", err)
			}

			// For Polygon prod - update tag, author and traces
			block.Metadata.Tag = newTag
			blobdata.ExtraData = &api.EthereumBlobdata_Polygon{
				Polygon: &api.PolygonBlobdata{
					Author: author,
				},
			}
			transactionTraces, err := c.getBlockTraces(ctx, newTag, headerResult.header)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch traces for block %v: %w", height, err)
			}

			// Validate transactionTraces
			numTracesExpected := c.getNumTraces(headerResult.header.Transactions)
			if len(transactionTraces) != numTracesExpected {
				return nil, xerrors.Errorf("unexpected number of transaction traces, expect=%v, actual=%v", numTracesExpected, len(transactionTraces))
			}

			blobdata.TransactionTraces = transactionTraces

			return block, nil
		}
	} else if c.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_ARBITRUM &&
		c.config.Chain.Network == common.Network_NETWORK_ARBITRUM_MAINNET {
		if oldTag == 1 && newTag == 1 {
			blobdata := block.GetEthereum()
			if blobdata == nil {
				return nil, xerrors.Errorf("empty blobdata")
			}

			height := block.Metadata.Height

			headerResult, err := c.getBlockHeaderResult(ctx, height)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch header result for block %v: %w", height, err)
			}

			transactionReceipts, err := c.getBlockTransactionReceipts(ctx, headerResult.header)
			if err != nil {
				return nil, xerrors.Errorf("failed to fetch transaction receipts for block %v: %w", height, err)
			}

			// Update header and receipts with block size and txn/txn receipt type data.
			blobdata.Header = headerResult.rawJson
			blobdata.TransactionReceipts = transactionReceipts

			return block, nil
		}
	}

	return nil, ErrNotImplemented
}

func (c *EthereumClient) getBlockUncleCount(ctx context.Context, hash string) (uint64, error) {
	response, err := c.client.Call(ctx, ethGetUncleCountByBlockHash, jsonrpc.Params{
		hash,
	})
	if err != nil {
		return 0, xerrors.Errorf("failed to call %v: %w", ethGetUncleCountByBlockHash.Name, err)
	}

	var result parser.EthereumHexString
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
	if count == 0 || c.config.Blockchain() == common.Blockchain_BLOCKCHAIN_ARBITRUM {
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

func (c *EthereumClient) getNumTraces(transactions []*parser.EthereumTransactionLit) int {
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
