package ethereum

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
)

type (
	TronClient struct {
		*EthereumClient
		additionalClient restapi.Client
	}

	TronClientParams struct {
		fx.In
		fxparams.Params
		MasterClient     jsonrpc.Client `name:"master"`
		SlaveClient      jsonrpc.Client `name:"slave"`
		ValidatorClient  jsonrpc.Client `name:"validator"`
		ConsensusClient  jsonrpc.Client `name:"consensus"`
		AdditionalClient restapi.Client `name:"additional"`
		DLQ              dlq.DLQ
	}

	tronApiClientFactory struct {
		masterClient    jsonrpc.Client
		slaveClient     jsonrpc.Client
		validatorClient jsonrpc.Client
		consensusClient jsonrpc.Client
		clientFactory   TronApiClientFactoryFn
	}

	TronApiClientFactoryFn func(client jsonrpc.Client) internal.Client
)

type tronBlockNumRequestData struct {
	Num uint64 `json:"num"`
}

var tronTxInfoMethod = &restapi.RequestMethod{
	Name:       "GetTransactionInfoByBlockNum",
	ParamsPath: "/wallet/gettransactioninfobyblocknum", // No parameter URls
	Timeout:    6 * time.Second,
	HTTPMethod: http.MethodPost,
}

var tronBlockTxMethod = &restapi.RequestMethod{
	Name:       "GetBlockByNum",
	ParamsPath: "/wallet/getblockbynum",
	Timeout:    6 * time.Second,
	HTTPMethod: http.MethodPost,
}

func NewTronApiClientFactory(params TronClientParams, clientFactory TronApiClientFactoryFn) internal.ClientFactory {
	return &tronApiClientFactory{
		masterClient:    params.MasterClient,
		slaveClient:     params.SlaveClient,
		validatorClient: params.ValidatorClient,
		consensusClient: params.ConsensusClient,
		clientFactory:   clientFactory,
	}
}

func (f *tronApiClientFactory) Master() internal.Client {
	return f.clientFactory(f.masterClient)
}

func (f *tronApiClientFactory) Slave() internal.Client {
	return f.clientFactory(f.slaveClient)

}

func (f *tronApiClientFactory) Validator() internal.Client {
	return f.clientFactory(f.validatorClient)

}

func (f *tronApiClientFactory) Consensus() internal.Client {
	return f.clientFactory(f.consensusClient)
}

// Tron shares the same data schema as Ethereum since it is an EVM chain, but we retrive trace from another restapi Client which independent from the main jsonrpc client.
// So it need to create a new factory for TronClient and set the additionalClient to the restapi client.
func NewTronClientFactory(params TronClientParams) internal.ClientFactory {
	return NewTronApiClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		ethClient := &EthereumClient{
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
		result := &TronClient{
			EthereumClient:   ethClient,
			additionalClient: params.AdditionalClient,
		}
		result.tracer = result
		return result
	})
}

func (c *TronClient) makeTronHttpCall(ctx context.Context, httpMethod *restapi.RequestMethod, requestData tronBlockNumRequestData) ([]byte, error) {
	postData, err := json.Marshal(requestData)
	if err != nil {
		return nil, xerrors.Errorf("failed to Marshal Tron requestData: %w", err)
	}
	response, err := c.additionalClient.Call(ctx, httpMethod, postData)
	if err != nil {
		return nil, xerrors.Errorf("failed to call Tron API: %w", err)
	}

	// Validate that the response is valid JSON to detect truncated responses
	if len(response) > 0 && !json.Valid(response) {
		// Log the truncated response for debugging
		preview := string(response)
		if len(preview) > 500 {
			preview = preview[:500] + "..."
		}
		c.logger.Warn(
			"received invalid/truncated JSON response from Tron API",
			zap.String("method", httpMethod.Name),
			zap.Uint64("block_num", requestData.Num),
			zap.Int("response_length", len(response)),
			zap.String("response_preview", preview),
		)
		return nil, retry.Retryable(xerrors.Errorf("received invalid/truncated JSON response from %s (length=%d): response is not valid JSON", httpMethod.Name, len(response)))
	}

	return response, nil
}

func (c *TronClient) getBlockTxByNum(ctx context.Context, blockNumber uint64) ([]byte, error) {
	requestData := tronBlockNumRequestData{
		Num: blockNumber,
	}
	result, err := c.makeTronHttpCall(ctx, tronBlockTxMethod, requestData)
	if err != nil {
		return nil, xerrors.Errorf("failed to get Tron block: %w", err)
	}
	return result, nil
}

func (c *TronClient) getBlockTxInfoByNum(ctx context.Context, blockNumber uint64) ([]byte, error) {
	requestData := tronBlockNumRequestData{
		Num: blockNumber,
	}
	response, err := c.makeTronHttpCall(ctx, tronTxInfoMethod, requestData)
	if err != nil {
		return nil, xerrors.Errorf("failed to get Tron transaction info: %w", err)
	}
	return response, nil
}

func (c *TronClient) getBlockTraces(ctx context.Context, tag uint32, block *ethereum.EthereumBlockLit) ([][]byte, error) {
	blockNumber := block.Number.Value()

	// Get block transactions to extract types
	blockTxData, err := c.getBlockTxByNum(ctx, blockNumber)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block transactions: %w", err)
	}

	// Get transaction info
	txInfoResponse, err := c.getBlockTxInfoByNum(ctx, blockNumber)
	if err != nil {
		return nil, xerrors.Errorf("failed to get transaction info: %w", err)
	}

	// Parse block data to extract transaction types by txID
	txTypeMap, err := c.extractTransactionTypes(blockTxData)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract transaction types: %w", err)
	}

	// Merge txInfo with transaction types
	results, err := c.mergeTxInfoWithTypes(txInfoResponse, txTypeMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to merge txInfo with types: %w", err)
	}

	return results, nil
}

// mergeTxInfoWithTypes parses txInfo response and adds transaction types based on txID matching
func (c *TronClient) mergeTxInfoWithTypes(txInfoResponse []byte, txTypeMap map[string]string) ([][]byte, error) {
	// Parse txInfo response as array
	var txInfoArray []json.RawMessage
	if err := json.Unmarshal(txInfoResponse, &txInfoArray); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal TronTxInfo: %w", err)
	}

	// Merge each txInfo with its corresponding type
	results := make([][]byte, 0, len(txInfoArray))
	for _, txInfoBytes := range txInfoArray {
		// Parse txInfo as map to allow dynamic field addition
		var txInfo map[string]interface{}
		if err := json.Unmarshal(txInfoBytes, &txInfo); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal txInfo: %w", err)
		}

		// Extract txID from txInfo (every transaction must have txID)
		txID, ok := txInfo["id"].(string)
		if !ok {
			return nil, xerrors.Errorf("txInfo id is not a string or is missing: %+v", txInfo)
		}
		// Add transaction type if found
		if txType, exists := txTypeMap[txID]; exists {
			txInfo["type"] = txType
		}

		// Re-serialize the modified txInfo
		modifiedBytes, err := json.Marshal(txInfo)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal modified txInfo: %w", err)
		}

		results = append(results, modifiedBytes)
	}

	return results, nil
}

// extractTransactionTypes extracts transaction types from block data, indexed by txID
func (c *TronClient) extractTransactionTypes(blockTxData []byte) (map[string]string, error) {
	if len(blockTxData) == 0 {
		return make(map[string]string), nil
	}

	// Parse the block data
	var blockData map[string]interface{}
	if err := json.Unmarshal(blockTxData, &blockData); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block data: %w", err)
	}

	txTypeMap := make(map[string]string)

	// Extract transactions array
	transactions, ok := blockData["transactions"].([]any)
	if !ok {
		return txTypeMap, nil // No transactions in block
	}
	// Extract txID (every transaction must have txID)
	for _, tx := range transactions {
		txMap, ok := tx.(map[string]any)
		if !ok {
			return nil, xerrors.Errorf("failed to assert transaction as map[string]interface{}: %+v", tx)
		}

		txID, ok := txMap["txID"].(string)
		if !ok {
			return nil, xerrors.Errorf("transaction is missing txID or it's not a string: %+v", txMap)
		}

		rawDataVal, ok := txMap["raw_data"]
		if !ok {
			continue // Or return an error if raw_data is always expected
		}
		rawData, ok := rawDataVal.(map[string]any)
		if !ok {
			return nil, xerrors.Errorf("raw_data is not a map: %+v", rawDataVal)
		}

		contractsVal, ok := rawData["contract"]
		if !ok {
			continue
		}
		contracts, ok := contractsVal.([]any)
		if !ok || len(contracts) == 0 {
			continue
		}

		contract, ok := contracts[0].(map[string]any)
		if !ok {
			return nil, xerrors.Errorf("contract is not a map: %+v", contracts[0])
		}

		txType, ok := contract["type"].(string)
		if !ok {
			return nil, xerrors.Errorf("contract type is not a string: %+v", contract["type"])
		}

		txTypeMap[txID] = txType
	}

	return txTypeMap, nil
}
