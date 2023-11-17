package aptos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/aptos"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	aptosClientImpl struct {
		config   *config.Config
		logger   *zap.Logger
		client   restapi.Client
		validate *validator.Validate
	}

	// This is Aptos specific error response. The detail can be found here:
	// https://fullnode.mainnet.aptoslabs.com/v1/spec#/operations/get_block_by_height
	AptosError struct {
		Message     string `json:"message"`
		ErrorCode   string `json:"error_code"`
		VmErrorCode int    `json:"vm_error_code,omitempty"`
	}
)

const (
	// This is the returned error response when Aptos failed to find a target block.
	aptosErrorCodeBlockNotFound = "block_not_found"
)

func NewAptosClientFactory(params internal.RestapiClientParams) internal.ClientFactory {
	return internal.NewRestapiClientFactory(params, func(client restapi.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &aptosClientImpl{
			config:   params.Config,
			logger:   logger,
			client:   client,
			validate: validator.New(),
		}
	})
}

func (c *aptosClientImpl) BatchGetBlockMetadata(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
	if from >= to {
		return nil, xerrors.Errorf("invalid height range range of [%d, %d)", from, to)
	}

	numBlocks := int(to - from)
	blocks := make([]*api.BlockMetadata, numBlocks)
	// TODO: if the speed is not good enough, we can parallelize these requests.
	for i := 0; i < numBlocks; i++ {
		height := from + uint64(i)
		// Don't need to include transactions.
		metadata, _, err := c.getBlock(ctx, tag, height, false)
		if err != nil {
			return nil, xerrors.Errorf("failed to get block for height=%d due to: %w", height, err)
		}
		blocks[i] = metadata
	}

	return blocks, nil
}

func (c *aptosClientImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64, _ ...internal.ClientOption) (*api.Block, error) {
	metadata, header, err := c.getBlock(ctx, tag, height, true)
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

func (c *aptosClientImpl) GetBlockByHash(ctx context.Context, tag uint32, height uint64, hash string, opts ...internal.ClientOption) (*api.Block, error) {
	// Aptos doesn't have API to get a block by its hash. Because there is no reorg, we can query the block by its height.
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

func (c *aptosClientImpl) GetLatestHeight(ctx context.Context) (uint64, error) {
	// Get the full block, including all the transactions.
	method := c.getLedgerInfoMethod()

	// For the get ledger info, the requestBody is nil.
	response, err := c.client.Call(ctx, method, nil)
	if err != nil {
		return 0, xerrors.Errorf("failed to get latest block height: %w", handleCallError(err))
	}

	// Parse to get the latest block height
	var ledgerInfo aptos.AptosLedgerInfoLit
	if err := json.Unmarshal(response, &ledgerInfo); err != nil {
		return 0, xerrors.Errorf("failed to unmarshal ledger info: %w", err)
	}

	block_height, err := strconv.ParseUint(ledgerInfo.LatestBlockHeight, 10, 64)
	if err != nil {
		return 0, xerrors.Errorf("failed to parse ledgerInfo's block_height=%v: %w", ledgerInfo.LatestBlockHeight, err)
	}

	return block_height, nil
}

func (c *aptosClientImpl) UpgradeBlock(_ context.Context, _ *api.Block, _ uint32) (*api.Block, error) {
	return nil, internal.ErrNotImplemented
}

func (c *aptosClientImpl) CanReprocess(_ uint32, _ uint64) bool {
	return false
}

func (c *aptosClientImpl) GetAccountProof(ctx context.Context, req *api.GetVerifiedAccountStateRequest) (*api.GetAccountProofResponse, error) {
	return nil, internal.ErrNotImplemented
}

func (c *aptosClientImpl) getBlobdata(header []byte) *api.Block_Aptos {
	if header == nil {
		return nil
	}

	return &api.Block_Aptos{
		Aptos: &api.AptosBlobdata{
			Block: header,
		},
	}
}

// Given the block tag and height, return the block metadata, raw block and any error.
func (c *aptosClientImpl) getBlock(ctx context.Context, tag uint32, height uint64, withTransactions bool) (*api.BlockMetadata, []byte, error) {
	// Get the full block, including all the transactions.
	method := c.getBlockByHeightMethod(height, withTransactions)

	// For the get block request, the requestBody is nil.
	response, err := c.client.Call(ctx, method, nil)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to call restapi: %w", handleCallError(err))
	}

	header, err := c.parseHeader(response)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	time, err := strconv.ParseUint(header.BlockTime, 10, 64)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to parse header's timestamp=%v: %w", header.BlockTime, err)
	}

	return &api.BlockMetadata{
		Tag:          tag,
		Height:       height,
		ParentHeight: internal.GetParentHeight(height),
		Hash:         header.BlockHash,
		// TODO: note that Aptos's block doesn't have the parent hash. We need to reconsider whether this is required. If this is
		// required, we need to fetch the parent block to get the parent hash.
		ParentHash: "",
		// The input timestamp is in micro-seconds.
		Timestamp: utils.ToTimestamp(int64(time / 1000000)),
	}, response, nil
}

// Given the target block height and whether to include transactions or not, return the REST API request method.
// The parameters will be embedded into the URL.
func (c *aptosClientImpl) getBlockByHeightMethod(height uint64, withTransactions bool) *restapi.RequestMethod {
	url := fmt.Sprintf("/blocks/by_height/%d?with_transactions=%t", height, withTransactions)
	return &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: url,
		Timeout:    5 * time.Second,
	}
}

// Given the target block height and whether to include transactions or not, return the REST API request method.
// The parameters will be embedded into the URL.
func (c *aptosClientImpl) getLedgerInfoMethod() *restapi.RequestMethod {
	return &restapi.RequestMethod{
		Name:       "GetLedgerInfo",
		ParamsPath: "", // No parameter URls
		Timeout:    5 * time.Second,
	}
}

// Given the returned error of REST API, try to parse the error to Aptos specific error response.
func handleCallError(callErr error) error {
	var errHTTP *restapi.HTTPError
	if !errors.As(callErr, &errHTTP) {
		return callErr
	}

	errAptos := &AptosError{}
	if err := json.Unmarshal([]byte(errHTTP.Response), &errAptos); err != nil {
		return xerrors.Errorf("failed to unmarshal err (%v) with code %v to errAptos due to: %w", callErr, errHTTP.Code, err)
	}

	if errAptos.ErrorCode == aptosErrorCodeBlockNotFound {
		return internal.ErrBlockNotFound
	}

	return callErr
}

func (c *aptosClientImpl) parseHeader(response []byte) (*aptos.AptosBlockLit, error) {
	var header aptos.AptosBlockLit
	if err := json.Unmarshal(response, &header); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block: %w", err)
	}

	if header.BlockHash == "" {
		return nil, xerrors.Errorf("block hash is empty: {%+v}", header)
	}

	if err := c.validate.Struct(header); err != nil {
		return nil, xerrors.Errorf("failed to parse block lit: %w", err)
	}

	return &header, nil
}
