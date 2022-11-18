package client

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	rc "github.com/coinbase/rosetta-sdk-go/client"
	rt "github.com/coinbase/rosetta-sdk-go/types"
	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
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
	}

	RosettaClientParams struct {
		fx.In
		fxparams.Params
		MasterEndpointProvider    endpoints.RosettaEndpointProvider `name:"master"`
		SlaveEndpointProvider     endpoints.RosettaEndpointProvider `name:"slave"`
		ValidatorEndpointProvider endpoints.RosettaEndpointProvider `name:"validator"`
	}

	rosettaClientImpl struct {
		config           *config.Config
		logger           *zap.Logger
		metrics          tally.Scope
		retry            retry.Retry
		endpointProvider endpoints.RosettaEndpointProvider
	}
)

const (
	instrumentName = "rosetta.request"
)

func NewRosettaClientFactory(params RosettaClientParams) ClientFactory {
	return &rosettaClientFactory{
		config:                    params.Config,
		logger:                    params.Logger,
		metrics:                   params.Metrics,
		masterEndpointProvider:    params.MasterEndpointProvider,
		slaveEndpointProvider:     params.SlaveEndpointProvider,
		validatorEndpointProvider: params.ValidatorEndpointProvider,
	}
}

func (f *rosettaClientFactory) Master() Client {
	return f.getRosettaClient(f.masterEndpointProvider)
}

func (f *rosettaClientFactory) Slave() Client {
	return f.getRosettaClient(f.slaveEndpointProvider)
}

func (f *rosettaClientFactory) Validator() Client {
	return f.getRosettaClient(f.validatorEndpointProvider)
}

func (f *rosettaClientFactory) getRosettaClient(endpointProvider endpoints.RosettaEndpointProvider) Client {
	logger := log.WithPackage(f.logger)
	return &rosettaClientImpl{
		config:           f.config,
		logger:           logger,
		metrics:          f.metrics.SubScope("rosetta"),
		retry:            retry.New(retry.WithLogger(logger)),
		endpointProvider: endpointProvider,
	}
}

func (c *rosettaClientImpl) BatchGetBlockMetadata(
	ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range range of [%d, %d)", from, to)
	}

	numBlocks := int(to - from)
	blocks := make([]*api.BlockMetadata, numBlocks)
	for i := 0; i < numBlocks; i++ {
		height := from + uint64(i)
		block, err := c.getBlockByHeight(ctx, tag, int64(height), false)
		if err != nil {
			return nil, xerrors.Errorf("failed to get block for height %d due to %w", height, err)
		}
		blocks[i] = block.Metadata
	}
	return blocks, nil
}

func (c *rosettaClientImpl) GetBlockByHeight(
	ctx context.Context, tag uint32, height uint64, opts ...ClientOption) (*api.Block, error) {
	return c.getBlockByHeight(ctx, tag, int64(height), true)
}

func (c *rosettaClientImpl) GetBlockByHash(
	ctx context.Context, tag uint32, height uint64, hash string, opts ...ClientOption) (*api.Block, error) {
	req := c.getBlockRequest(&rt.PartialBlockIdentifier{Hash: &hash})
	endpoint, rosettaApiClient, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get rosetta api client %w", err)
	}
	resp, err := c.queryBlock(ctx, req, rosettaApiClient, endpoint)
	if err != nil {
		return nil, err
	}
	actualHash := resp.Block.BlockIdentifier.Hash
	if strings.Compare(hash, actualHash) != 0 {
		return nil, xerrors.Errorf(
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
		return 0, xerrors.Errorf("failed to get rosetta api client %w", err)
	}
	resp, err := c.queryNetwork(ctx, rosettaApiClient, endpoint)
	if err != nil {
		return 0, err
	}
	return uint64(resp.CurrentBlockIdentifier.Index), nil
}

func (c *rosettaClientImpl) UpgradeBlock(ctx context.Context, block *api.Block, newTag uint32) (*api.Block, error) {
	return nil, ErrNotImplemented
}

func (c *rosettaClientImpl) CanReprocess(tag uint32, height uint64) bool {
	return false
}

func (c *rosettaClientImpl) getBlockByHeight(
	ctx context.Context, tag uint32, height int64, includeOtherTransactions bool) (*api.Block, error) {
	req := c.getBlockRequest(&rt.PartialBlockIdentifier{Index: &height})
	endpoint, rosettaApiClient, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get rosetta api client %w", err)
	}
	resp, err := c.queryBlock(ctx, req, rosettaApiClient, endpoint)
	if err != nil {
		return nil, err
	}
	expectedHeight := *req.BlockIdentifier.Index
	actualHeight := resp.Block.BlockIdentifier.Index
	if expectedHeight != actualHeight {
		return nil, xerrors.Errorf(
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
	if err := c.instrument(ctx, "Block", endpoint.Name, req, func(ctx context.Context) error {
		var nodeErr *rt.Error
		var err error
		resp, nodeErr, err = rosettaApiClient.BlockAPI.Block(ctx, req)
		if nodeErr != nil {
			return c.mapRosettaError(nodeErr, fmt.Sprintf("failed to get rosetta block for request %+v", req))
		}
		if err != nil {
			return retry.Retryable(xerrors.Errorf("failed to get rosetta block for request %+v: %w", req, err))
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
	for i := range blockResp.OtherTransactions {
		req := c.getTransactionRequest(blockResp.Block.BlockIdentifier, blockResp.OtherTransactions[i])
		resp, err := c.queryTransaction(ctx, req, rosettaApiClient, endpoint)
		if err != nil {
			return nil, err
		}
		transactions[i] = resp
	}
	return transactions, nil
}

func (c *rosettaClientImpl) queryTransaction(
	ctx context.Context, req *rt.BlockTransactionRequest, rosettaApiClient *rc.APIClient, endpoint *endpoints.Endpoint) (*rt.BlockTransactionResponse, error) {
	var resp *rt.BlockTransactionResponse
	if err := c.instrument(ctx, "BlockTransaction", endpoint.Name, req, func(ctx context.Context) error {
		var nodeErr *rt.Error
		var err error
		resp, nodeErr, err = rosettaApiClient.BlockAPI.BlockTransaction(ctx, req)
		if nodeErr != nil {
			return c.mapRosettaError(nodeErr, fmt.Sprintf("failed to get transaction for request %+v", req))
		}
		if err != nil {
			return retry.Retryable(xerrors.Errorf("failed to get transaction for request %+v: %w", req, err))
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
	if err := c.instrument(ctx, "NetworkStatus", endpoint.Name, req, func(ctx context.Context) error {
		var nodeErr *rt.Error
		var err error
		resp, nodeErr, err = rosettaApiClient.NetworkAPI.NetworkStatus(ctx, req)
		if nodeErr != nil {
			return c.mapRosettaError(nodeErr, fmt.Sprintf("failed to get network status for request %+v", req))
		}
		if err != nil {
			return retry.Retryable(xerrors.Errorf("failed to get network status for request %+v: %w", req, err))
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
		return nil, xerrors.Errorf("failed to marshal rosetta block to bytes %w", err)
	}
	otherTransactions := make([][]byte, len(transactions))
	if len(transactions) > 0 {
		for i, tx := range transactions {
			txData, err := json.Marshal(tx)
			if err != nil {
				return nil, xerrors.Errorf("failed to marshal transaction %w", err)
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

func (c *rosettaClientImpl) instrument(ctx context.Context, method string, endpoint string, request interface{}, fn instrument.OperationFn) error {
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
	call := instrument.NewCall(
		scope,
		"request",
		instrument.WithRetry(c.retry),
		instrument.WithTracer(instrumentName, tags),
		instrument.WithLogger(logger, instrumentName),
	)
	return call.Instrument(ctx, fn)
}

// Update this whenever onboard new rosetta asset
func (c *rosettaClientImpl) mapRosettaError(error *rt.Error, message string) error {
	// Code is a network-specific error code according to the Rosetta spec.
	for _, c := range c.config.Chain.Rosetta.BlockNotFoundErrorCodes {
		if c == error.Code {
			return xerrors.Errorf("%v: rosetta error %+v: %w", message, error, ErrBlockNotFound)
		}
	}

	err := xerrors.Errorf("%v: rosetta error %+v", message, error)
	if error.Retriable {
		return retry.Retryable(err)
	}

	return err
}
