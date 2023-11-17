package ethereum

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	blockAuthorResponse = `"0xa863dd63d894f058a8f62c51e575d0364d3c885d1e2049da96a43d19f9e29939"`

	nilAddress = `"0x0000000000000000000000000000000000000000"`
)

type (
	polygonClientTestSuite struct {
		suite.Suite

		ctrl      *gomock.Controller
		testapp   testapp.TestApp
		rpcClient *jsonrpcmocks.MockClient
		client    internal.Client
	}
)

func TestPolygonClientTestSuite(t *testing.T) {
	suite.Run(t, new(polygonClientTestSuite))
}

func (s *polygonClientTestSuite) SetupTest() {
	require := testutil.Require(s.T())
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Blockchain = common.Blockchain_BLOCKCHAIN_POLYGON
	cfg.Chain.Network = common.Network_NETWORK_POLYGON_MAINNET

	var result internal.ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithConfig(cfg),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *polygonClientTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *polygonClientTestSuite) TestPolygonClient_New() {
	var result internal.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	s.NotNil(result.Master)
	s.NotNil(result.Slave)
	s.NotNil(result.Validator)
	s.NotNil(result.Consensus)
}

func (s *polygonClientTestSuite) TestPolygonTest_GetBlockByHeight() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethTraceTransactionMethod {
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethOpCountTracer {
					return &jsonrpc.Response{
						Result: []byte("123"),
					}, nil
				}

				if tracer == ethCallTracer {
					return &jsonrpc.Response{
						Result: []byte(fixtureTransactionTrace),
					}, nil
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)
			}

			if method == ethBorGetAuthorMethod {
				expectedParams := jsonrpc.Params{
					"0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				}
				require.Equal(expectedParams, params)

				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
	for _, transactionTrace := range blobdata.TransactionTraces {
		require.Equal(fixtureTransactionTrace, string(transactionTrace))
	}
}

func (s *polygonClientTestSuite) TestPolygonTest_GetBlockByHeight_TransactionWithNullAddress_WithBestEffort() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithNullAddressTransactions),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethTraceTransactionMethod {
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethOpCountTracer {
					return &jsonrpc.Response{
						Result: []byte("123"),
					}, nil
				}

				if tracer == ethCallTracer {
					return &jsonrpc.Response{
						Result: []byte(fixtureTransactionTrace),
					}, nil
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)
			}

			if method == ethBorGetAuthorMethod {
				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
	for _, transactionTrace := range blobdata.TransactionTraces {
		require.Equal(fixtureTransactionTrace, string(transactionTrace))
	}
}

func (s *polygonClientTestSuite) TestPolygonTest_GetBlockByHeight_TransactionWithNullAddress_WithoutBestEffort() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithNullAddressTransactions),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethTraceBlockByHashMethod {
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethCallTracer {
					return &jsonrpc.Response{
						Result: []byte(fixtureBlockTraceSize1),
					}, nil
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)
			}

			if method == ethBorGetAuthorMethod {
				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
	for _, transactionTrace := range blobdata.TransactionTraces {
		require.Equal(fixtureTransactionTrace, string(transactionTrace))
	}
}

func (s *polygonClientTestSuite) TestPolygonClient_UpgradeBlock_2_2_HeaderUpdated_Mainnet() {
	const (
		oldTag uint32 = 2
		newTag uint32 = 2
	)

	require := testutil.Require(s.T())

	existingBlockResult, err := fixtures.ReadFile("client/polygon/polygon_getblockbynumber.json")
	require.NoError(err)
	upgradeBlockResult, err := fixtures.ReadFile("client/polygon/polygon_getblockbynumber_upgrade_miner.json")
	require.NoError(err)

	blockResponse := &jsonrpc.Response{
		Result: upgradeBlockResult,
	}

	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, gomock.Any(),
	).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			return blockResponse, nil
		})

	inputBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    oldTag,
			Height: 11322000,
			Hash:   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header: existingBlockResult,
			},
		},
	}
	block, err := s.client.UpgradeBlock(context.Background(), inputBlock, newTag)
	require.NoError(err)
	require.Equal(newTag, block.Metadata.Tag)

	existingBlobdata := inputBlock.GetEthereum()
	require.NotNil(existingBlobdata)
	upgradedBlobdata := block.GetEthereum()
	require.NotNil(upgradedBlobdata)

	require.Equal(existingBlobdata.Header, upgradedBlobdata.Header)
}

func (s *polygonClientTestSuite) TestPolygonTest_RetryBorAuthor_ServerErr() {
	require := testutil.Require(s.T())

	retryAttempts := 0
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			switch method {
			case ethGetBlockByNumberMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureBlock),
				}, nil

			case ethTraceTransactionMethod:
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethOpCountTracer {
					return &jsonrpc.Response{
						Result: []byte("123"),
					}, nil
				}

				if tracer == ethCallTracer {
					return &jsonrpc.Response{
						Result: []byte(fixtureTransactionTrace),
					}, nil
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)

			case ethBorGetAuthorMethod:
				retryAttempts += 1
				if retryAttempts < retry.DefaultMaxAttempts {
					return nil, &jsonrpc.RPCError{
						Code:    -32000,
						Message: "unknown block",
					}
				}

				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil

			default:
				return nil, xerrors.Errorf("unknown method: %v", method)
			}
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
	for _, transactionTrace := range blobdata.TransactionTraces {
		require.Equal(fixtureTransactionTrace, string(transactionTrace))
	}
}

func (s *polygonClientTestSuite) TestRetryTraceBlock_NotFound() {
	require := testutil.Require(s.T())

	retryAttempts := 0
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			switch method {
			case ethGetBlockByHashMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureBlock),
				}, nil

			case ethBorGetAuthorMethod:
				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil

			case ethTraceBlockByHashMethod:
				retryAttempts += 1
				if retryAttempts == retry.DefaultMaxAttempts {
					return &jsonrpc.Response{
						Result: json.RawMessage(fixtureBlockTrace),
					}, nil
				}

				return nil, &jsonrpc.RPCError{
					Code:    -32000,
					Message: "block 0x5c1745dfaaeba631626fa427a6e4982d3545cdb5c8baaefddd4903a1ff3a6e79 not found",
				}

			default:
				return nil, xerrors.Errorf("unknown method: %v", method)
			}
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	_, err := s.client.GetBlockByHash(context.Background(), tag, ethereumHeight, ethereumHash)
	require.NoError(err)
}

func (s *polygonClientTestSuite) TestRetryTraceBlock_ExecutionAborted() {
	require := testutil.Require(s.T())

	retryAttempts := 0
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			switch method {
			case ethGetBlockByHashMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureBlock),
				}, nil

			case ethBorGetAuthorMethod:
				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil

			case ethTraceBlockByHashMethod:
				retryAttempts += 1
				if retryAttempts == retry.DefaultMaxAttempts {
					return &jsonrpc.Response{
						Result: json.RawMessage(fixtureBlockTrace),
					}, nil
				}

				return nil, &jsonrpc.RPCError{
					Code:    -32000,
					Message: "execution aborted (timeout = 15s)",
				}

			default:
				return nil, xerrors.Errorf("unknown method: %v", method)
			}
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	_, err := s.client.GetBlockByHash(context.Background(), tag, ethereumHeight, ethereumHash)
	require.NoError(err)
}

func (s *polygonClientTestSuite) TestRetryTraceBlock_RetryLimitExceeded() {
	require := testutil.Require(s.T())

	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			switch method {
			case ethGetBlockByHashMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureBlock),
				}, nil

			case ethBorGetAuthorMethod:
				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil

			case ethTraceBlockByHashMethod:
				return nil, &jsonrpc.RPCError{
					Code:    -32000,
					Message: "block 0x5c1745dfaaeba631626fa427a6e4982d3545cdb5c8baaefddd4903a1ff3a6e79 not found",
				}

			default:
				return nil, xerrors.Errorf("unknown method: %v", method)
			}
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	_, err := s.client.GetBlockByHash(context.Background(), tag, ethereumHeight, ethereumHash)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrBlockNotFound))
}

func (s *polygonClientTestSuite) TestRetryTraceTransaction() {
	require := testutil.Require(s.T())

	retryAttempts := 0
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			switch method {
			case ethGetBlockByNumberMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureBlock),
				}, nil

			case ethBorGetAuthorMethod:
				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil

			case ethTraceTransactionMethod:
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethOpCountTracer {
					return &jsonrpc.Response{
						Result: []byte("123"),
					}, nil
				}

				if tracer == ethCallTracer {
					retryAttempts += 1
					if retryAttempts < retry.DefaultMaxAttempts {
						return nil, &jsonrpc.RPCError{
							Code:    -32000,
							Message: "genesis is not traceable",
						}
					}

					return &jsonrpc.Response{
						Result: []byte(fixtureTransactionTrace),
					}, nil
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)

			default:
				return nil, xerrors.Errorf("unknown method: %v", method)
			}
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
	for _, transactionTrace := range blobdata.TransactionTraces {
		require.Equal(fixtureTransactionTrace, string(transactionTrace))
	}
}

func (s *polygonClientTestSuite) TestRetryTraceTransaction_RetryLimitExceeded() {
	require := testutil.Require(s.T())

	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			switch method {
			case ethGetBlockByNumberMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureBlock),
				}, nil

			case ethBorGetAuthorMethod:
				return &jsonrpc.Response{
					Result: []byte(blockAuthorResponse),
				}, nil

			case ethTraceTransactionMethod:
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethOpCountTracer {
					return &jsonrpc.Response{
						Result: []byte("123"),
					}, nil
				}

				if tracer == ethCallTracer {
					return nil, &jsonrpc.RPCError{
						Code:    -32000,
						Message: "genesis is not traceable",
					}
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)

			default:
				return nil, xerrors.Errorf("unknown method: %v", method)
			}
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	_, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, internal.WithBestEffort())
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrBlockNotFound))
}

func (s *polygonClientTestSuite) TestRetryBatchGetBlockMetadata() {
	require := testutil.Require(s.T())

	retryAttempts := 0

	batchResponse := []*jsonrpc.Response{
		{
			Result: json.RawMessage(fixtureBlock),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetBlockByNumberMethod, []jsonrpc.Params{
			{
				"0xacc290",
				false,
			},
		},
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params []jsonrpc.Params) ([]*jsonrpc.Response, error) {
			retryAttempts += 1
			if retryAttempts < retry.DefaultMaxAttempts {
				return nil, &jsonrpc.RPCError{
					Code:    -32603,
					Message: "request failed or timed out",
				}
			}
			return batchResponse, nil
		})

	blockMetadatas, err := s.client.BatchGetBlockMetadata(context.Background(), tag, 11322000, 11322001)
	require.NoError(err)
	require.NotNil(blockMetadatas)
	require.Equal(1, len(blockMetadatas))
	require.Equal(uint64(11322000), blockMetadatas[0].Height)
}
