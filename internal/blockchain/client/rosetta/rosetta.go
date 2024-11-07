package rosetta

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	rc "github.com/coinbase/rosetta-sdk-go/client"
	rt "github.com/coinbase/rosetta-sdk-go/types"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	rosettaClientFactory struct {
		config                    *config.Config
		logger                    *zap.Logger
		metrics                   tally.Scope
		masterEndpointProvider    endpoints.RosettaEndpointProvider
		slaveEndpointProvider     endpoints.RosettaEndpointProvider
		validatorEndpointProvider endpoints.RosettaEndpointProvider
		consensusEndpointProvider endpoints.RosettaEndpointProvider
		restApiParams             internal.RestapiClientParams
	}

	RosettaClientParams struct {
		fx.In
		fxparams.Params
		MasterEndpointProvider    endpoints.RosettaEndpointProvider `name:"master"`
		SlaveEndpointProvider     endpoints.RosettaEndpointProvider `name:"slave"`
		ValidatorEndpointProvider endpoints.RosettaEndpointProvider `name:"validator"`
		ConsensusEndpointProvider endpoints.RosettaEndpointProvider `name:"consensus"`
	}

	rosettaClientImpl struct {
		config           *config.Config
		logger           *zap.Logger
		metrics          tally.Scope
		retry            retry.Retry
		endpointProvider endpoints.RosettaEndpointProvider
	}

	rosettaClientWithRawBlockApiImpl struct {
		rosettaClientImpl
		restClient restapi.Client
	}
)

const (
	instrumentName                      = "rosetta.request"
	rosettaBatchMetadataParallelism     = 4
	rosettaQueryTransactionsParallelism = 20
)

var (
	rawBlockMethod = &restapi.RequestMethod{
		Name:       "RawBlock",
		ParamsPath: "/raw_block",
		Timeout:    5 * time.Second,
	}
)

func NewRosettaClientFactory(params RosettaClientParams, restApiParams internal.RestapiClientParams) internal.ClientFactory {
	return &rosettaClientFactory{
		config:                    params.Config,
		logger:                    params.Logger,
		metrics:                   params.Metrics,
		masterEndpointProvider:    params.MasterEndpointProvider,
		slaveEndpointProvider:     params.SlaveEndpointProvider,
		validatorEndpointProvider: params.ValidatorEndpointProvider,
		consensusEndpointProvider: params.ConsensusEndpointProvider,
		restApiParams:             restApiParams,
	}
}

func (f *rosettaClientFactory) Master() internal.Client {
	if f.config.Chain.Rosetta.EnableRawBlockApi {
		return f.getRosettaClientWithRawBlockApi(f.masterEndpointProvider, f.restApiParams.MasterClient)
	}

	return f.getRosettaClient(f.masterEndpointProvider)
}

func (f *rosettaClientFactory) Slave() internal.Client {
	if f.config.Chain.Rosetta.EnableRawBlockApi {
		return f.getRosettaClientWithRawBlockApi(f.slaveEndpointProvider, f.restApiParams.SlaveClient)
	}

	return f.getRosettaClient(f.slaveEndpointProvider)
}

func (f *rosettaClientFactory) Validator() internal.Client {
	if f.config.Chain.Rosetta.EnableRawBlockApi {
		return f.getRosettaClientWithRawBlockApi(f.validatorEndpointProvider, f.restApiParams.ValidatorClient)
	}

	return f.getRosettaClient(f.validatorEndpointProvider)
}

func (f *rosettaClientFactory) Consensus() internal.Client {
	if f.config.Chain.Rosetta.EnableRawBlockApi {
		return f.getRosettaClientWithRawBlockApi(f.consensusEndpointProvider, f.restApiParams.ConsensusClient)
	}

	return f.getRosettaClient(f.consensusEndpointProvider)
}

func (f *rosettaClientFactory) getRosettaClient(endpointProvider endpoints.RosettaEndpointProvider) internal.Client {
	logger := log.WithPackage(f.logger)
	return &rosettaClientImpl{
		config:           f.config,
		logger:           logger,
		metrics:          f.metrics.SubScope("rosetta"),
		retry:            retry.New(retry.WithLogger(logger)),
		endpointProvider: endpointProvider,
	}
}

func (f *rosettaClientFactory) getRosettaClientWithRawBlockApi(endpointProvider endpoints.RosettaEndpointProvider, restClient restapi.Client) internal.Client {
	logger := log.WithPackage(f.logger)
	return &rosettaClientWithRawBlockApiImpl{
		rosettaClientImpl: rosettaClientImpl{
			config:           f.config,
			logger:           logger,
			metrics:          f.metrics.SubScope("rosetta"),
			retry:            retry.New(retry.WithLogger(logger)),
			endpointProvider: endpointProvider,
		},
		restClient: restClient,
	}
}

func (c *rosettaClientImpl) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, fmt.Errorf("invalid height range of [%d, %d)", from, to)
	}

	numBlocks := int(to - from)
	blocks := make([]*api.BlockMetadata, numBlocks)

	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(rosettaBatchMetadataParallelism))
	for i := 0; i < numBlocks; i++ {
		index := i
		group.Go(func() error {
			height := from + uint64(index)
			block, err := c.getBlockByHeight(ctx, tag, int64(height), false)
			if err != nil {
				return fmt.Errorf("failed to get block for height %d: %w", height, err)
			}
			blocks[index] = block.Metadata
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("failed to get block metadata in parallel: %w", err)
	}
	return blocks, nil
}

func (c *rosettaClientImpl) GetBlockByHeight(
	ctx context.Context, tag uint32, height uint64, opts ...internal.ClientOption) (*api.Block, error) {
	return c.getBlockByHeight(ctx, tag, int64(height), true)
}

func (c *rosettaClientImpl) GetBlockByHash(
	ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
	req := c.getBlockRequest(&rt.PartialBlockIdentifier{Hash: &hash})
	endpoint, rosettaApiClient, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get rosetta api client %w", err)
	}
	endpoint.IncRequestsCounter(1)
	resp, err := c.queryBlock(ctx, req, rosettaApiClient, endpoint)
	if err != nil {
		return nil, err
	}
	actualHash := resp.Block.BlockIdentifier.Hash
	if strings.Compare(hash, actualHash) != 0 {
		return nil, fmt.Errorf(
			"inconsistent hash of returned rosetta block, expected: %s, actual: %s",
			hash,
			actualHash)
	}
	transactions, err := c.queryTransactions(ctx, resp, rosettaApiClient, endpoint)
	if err != nil {
		return nil, err
	}
	return c.getRawBlock(tag, resp, transactions)
}

func (c *rosettaClientImpl) GetLatestHeight(ctx context.Context) (uint64, error) {
	endpoint, rosettaApiClient, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get rosetta api client %w", err)
	}
	endpoint.IncRequestsCounter(1)
	resp, err := c.queryNetwork(ctx, rosettaApiClient, endpoint)
	if err != nil {
		return 0, err
	}
	return uint64(resp.CurrentBlockIdentifier.Index), nil
}

func (c *rosettaClientImpl) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	return nil, internal.ErrNotImplemented
}

func (c *rosettaClientImpl) CanReprocess(tag uint32, height uint64) bool {
	return false
}

func (c *rosettaClientImpl) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return nil, internal.ErrNotImplemented
}

func (c *rosettaClientImpl) getBlockByHeight(
	ctx context.Context, tag uint32, height int64, includeOtherTransactions bool) (*api.Block, error) {
	req := c.getBlockRequest(&rt.PartialBlockIdentifier{Index: &height})
	endpoint, rosettaApiClient, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get rosetta api client %w", err)
	}
	endpoint.IncRequestsCounter(1)
	resp, err := c.queryBlock(ctx, req, rosettaApiClient, endpoint)
	if err != nil {
		return nil, err
	}
	expectedHeight := *req.BlockIdentifier.Index
	actualHeight := resp.Block.BlockIdentifier.Index
	if expectedHeight != actualHeight {
		return nil, fmt.Errorf(
			"inconsistent height of returned rosetta block, expected: %d, actual: %d", expectedHeight, actualHeight)
	}
	var transactions []*rt.BlockTransactionResponse
	if includeOtherTransactions {
		transactions, err = c.queryTransactions(ctx, resp, rosettaApiClient, endpoint)
		if err != nil {
			return nil, err
		}
	}
	return c.getRawBlock(tag, resp, transactions)
}

func (c *rosettaClientImpl) getBlockRequest(blockIdentifier *rt.PartialBlockIdentifier) *rt.BlockRequest {
	return &rt.BlockRequest{
		NetworkIdentifier: &rt.NetworkIdentifier{
			Blockchain: c.config.Chain.Rosetta.Blockchain,
			Network:    c.config.Chain.Rosetta.Network,
		},
		BlockIdentifier: blockIdentifier,
	}
}

func (c *rosettaClientImpl) getTransactionRequest(
	blockIdentifier *rt.BlockIdentifier, transactionIdentifier *rt.TransactionIdentifier) *rt.BlockTransactionRequest {
	return &rt.BlockTransactionRequest{
		NetworkIdentifier: &rt.NetworkIdentifier{
			Blockchain: c.config.Chain.Rosetta.Blockchain,
			Network:    c.config.Chain.Rosetta.Network,
		},
		BlockIdentifier:       blockIdentifier,
		TransactionIdentifier: transactionIdentifier,
	}
}

func (c *rosettaClientImpl) queryBlock(
	ctx context.Context, req *rt.BlockRequest, rosettaApiClient *rc.APIClient, endpoint *endpoints.Endpoint) (*rt.BlockResponse, error) {
	var resp *rt.BlockResponse
	if err := c.wrap(ctx, "Block", endpoint.Name, req, func(ctx context.Context) error {
		var nodeErr *rt.Error
		var err error
		resp, nodeErr, err = rosettaApiClient.BlockAPI.Block(ctx, req)
		if nodeErr != nil {
			return c.mapRosettaError(nodeErr, fmt.Sprintf("failed to get rosetta block for request %+v", req))
		}
		if err != nil {
			return retry.Retryable(fmt.Errorf("failed to get rosetta block for request %+v: %w", req, err))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *rosettaClientImpl) queryTransactions(
	ctx context.Context, blockResp *rt.BlockResponse, rosettaApiClient *rc.APIClient, endpoint *endpoints.Endpoint) ([]*rt.BlockTransactionResponse, error) {
	transactions := make([]*rt.BlockTransactionResponse, len(blockResp.OtherTransactions))
	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(rosettaQueryTransactionsParallelism))
	for i := range blockResp.OtherTransactions {
		i := i
		group.Go(func() error {
			req := c.getTransactionRequest(blockResp.Block.BlockIdentifier, blockResp.OtherTransactions[i])
			resp, err := c.queryTransaction(ctx, req, rosettaApiClient, endpoint)
			if err != nil {
				return err
			}
			transactions[i] = resp
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	return transactions, nil
}

func (c *rosettaClientImpl) queryTransaction(
	ctx context.Context, req *rt.BlockTransactionRequest, rosettaApiClient *rc.APIClient, endpoint *endpoints.Endpoint) (*rt.BlockTransactionResponse, error) {
	var resp *rt.BlockTransactionResponse
	if err := c.wrap(ctx, "BlockTransaction", endpoint.Name, req, func(ctx context.Context) error {
		var nodeErr *rt.Error
		var err error
		resp, nodeErr, err = rosettaApiClient.BlockAPI.BlockTransaction(ctx, req)
		if nodeErr != nil {
			return c.mapRosettaError(nodeErr, fmt.Sprintf("failed to get transaction for request %+v", req))
		}
		if err != nil {
			return retry.Retryable(fmt.Errorf("failed to get transaction for request %+v: %w", req, err))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *rosettaClientImpl) queryNetwork(
	ctx context.Context, rosettaApiClient *rc.APIClient, endpoint *endpoints.Endpoint) (*rt.NetworkStatusResponse, error) {
	req := &rt.NetworkRequest{
		NetworkIdentifier: &rt.NetworkIdentifier{
			Blockchain: c.config.Chain.Rosetta.Blockchain,
			Network:    c.config.Chain.Rosetta.Network,
		},
	}
	var resp *rt.NetworkStatusResponse
	if err := c.wrap(ctx, "NetworkStatus", endpoint.Name, req, func(ctx context.Context) error {
		var nodeErr *rt.Error
		var err error
		resp, nodeErr, err = rosettaApiClient.NetworkAPI.NetworkStatus(ctx, req)
		if nodeErr != nil {
			return c.mapRosettaError(nodeErr, fmt.Sprintf("failed to get network status for request %+v", req))
		}
		if err != nil {
			return retry.Retryable(fmt.Errorf("failed to get network status for request %+v: %w", req, err))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *rosettaClientImpl) getRawBlock(
	tag uint32, resp *rt.BlockResponse, transactions []*rt.BlockTransactionResponse) (*api.Block, error) {
	header, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal rosetta block to bytes %w", err)
	}
	otherTransactions := make([][]byte, len(transactions))
	if len(transactions) > 0 {
		for i, tx := range transactions {
			txData, err := json.Marshal(tx)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal transaction %w", err)
			}
			otherTransactions[i] = txData
		}
	}
	rawBlock := &api.Block{
		Blockchain: c.config.Chain.Blockchain,
		Network:    c.config.Chain.Network,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Height:       uint64(resp.Block.BlockIdentifier.Index),
			ParentHeight: uint64(resp.Block.ParentBlockIdentifier.Index),
			Hash:         resp.Block.BlockIdentifier.Hash,
			ParentHash:   resp.Block.ParentBlockIdentifier.Hash,
			Timestamp:    utils.ToTimestamp(resp.Block.Timestamp / 1000), // resp.Block.BlockTimestamp is in milliseconds.
		},
		Blobdata: &api.Block_Rosetta{
			Rosetta: &api.RosettaBlobdata{
				Header:            header,
				OtherTransactions: otherTransactions,
			},
		},
	}
	return rawBlock, nil
}

// wrap wraps the operation with metrics, logging, and retry.
func (c *rosettaClientImpl) wrap(ctx context.Context, method string, endpoint string, request any, operation instrument.OperationFn) error {
	tags := map[string]string{
		"method":   method,
		"endpoint": endpoint,
	}
	scope := c.metrics.Tagged(tags)
	logger := c.logger.With(
		zap.String("method", method),
		zap.String("endpoint", endpoint),
		zap.Reflect("request", request),
	)
	instrument := instrument.New(
		scope,
		"request",
		instrument.WithTracer(instrumentName, tags),
		instrument.WithLogger(logger, instrumentName),
	).WithRetry(c.retry)
	return instrument.Instrument(ctx, operation)
}

// Update this whenever onboard new rosetta asset
func (c *rosettaClientImpl) mapRosettaError(error *rt.Error, message string) error {
	// Code is a network-specific error code according to the Rosetta spec.
	for _, c := range c.config.Chain.Rosetta.BlockNotFoundErrorCodes {
		if c == error.Code {
			return fmt.Errorf("%v: rosetta error %+v: %w", message, error, internal.ErrBlockNotFound)
		}
	}

	err := fmt.Errorf("%v: rosetta error %+v", message, error)
	if error.Retriable {
		return retry.Retryable(err)
	}

	return err
}

func (c *rosettaClientWithRawBlockApiImpl) GetBlockByHeight(
	ctx context.Context, tag uint32, height uint64, opts ...internal.ClientOption) (*api.Block, error) {

	// TODO Evaluate if block and raw_block calls should be parallelized
	block, err := c.rosettaClientImpl.GetBlockByHeight(ctx, tag, height, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get rosetta block by height: %w", err)
	}

	rawBlock, err := c.getRawBlockByHash(ctx, block.GetMetadata().Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get raw block: %w", err)
	}
	block.GetRosetta().RawBlock = rawBlock

	return block, nil
}

func (c *rosettaClientWithRawBlockApiImpl) GetBlockByHash(
	ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {

	// TODO Evaluate if block and raw_block calls should be parallelized
	block, err := c.rosettaClientImpl.GetBlockByHash(ctx, tag, height, hash, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get rosetta block by hash: %w", err)
	}

	rawBlock, err := c.getRawBlockByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get raw block: %w", err)
	}
	block.GetRosetta().RawBlock = rawBlock

	return block, nil
}

func (c *rosettaClientWithRawBlockApiImpl) getRawBlockByHash(ctx context.Context, hash string) ([]byte, error) {
	body, err := c.getBlockByHashRequest(hash)
	if err != nil {
		return nil, err
	}

	response, err := c.restClient.Call(ctx, rawBlockMethod, body)
	if err != nil {
		return nil, fmt.Errorf("failed to call restapi: %w", err)
	}

	return response, nil
}

func (c *rosettaClientWithRawBlockApiImpl) getBlockByHashRequest(hash string) ([]byte, error) {
	request := rt.BlockRequest{
		NetworkIdentifier: &rt.NetworkIdentifier{
			Blockchain: c.config.Chain.Rosetta.Blockchain,
			Network:    c.config.Chain.Rosetta.Network,
		},
		BlockIdentifier: &rt.PartialBlockIdentifier{
			Hash: &hash,
		},
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to create getBlockByHashRequest (%v): %w", request, err)
	}

	return body, nil
}
