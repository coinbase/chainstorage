package client

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	blockAuthorResponse = `"0xf0245f6251bef9447a08766b9da2b07b28ad80b0"`

	nilAddress = `"0x0000000000000000000000000000000000000000"`

	simpleGenesisBlock = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0x0",
		"parentHash": "0x9b863b8348e030fc6f2a566b7ad2914d4d9f39e93d0454e978e8509d3d14b91a",
		"timestamp": "0x5fbd2fb9"
	}
	`
)

type polygonClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testapp   testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    Client
}

func TestPolygonClientTestSuite(t *testing.T) {
	suite.Run(t, new(polygonClientTestSuite))
}

func (s *polygonClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET),
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
	var result ClientParams
	app := testapp.New(
		s.T(),
		Module,
		jsonrpc.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	s.NotNil(result.Master)
	s.NotNil(result.Slave)
	s.NotNil(result.Validator)
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

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, WithBestEffort())
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

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, WithBestEffort())
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

func (s *polygonClientTestSuite) TestEthereumClient_UpgradePolygonBlock_1_2_AuthorAndTracesAssigned() {
	const (
		oldTag uint32 = 1
		newTag uint32 = 2
	)

	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithNullAddressTransactions),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params([]interface{}{
			"0xacc290",
			true,
		}),
	).Return(blockResponse, nil)

	authorResponse := &jsonrpc.Response{
		Result: []byte(blockAuthorResponse),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethBorGetAuthorMethod, gomock.Any(),
	).Return(authorResponse, nil)

	traceResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockTraceSize1),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	originTraces := make([][]byte, 2)
	inputBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    oldTag,
			Height: 11322000,
			Hash:   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				TransactionTraces: originTraces,
			},
		},
	}
	block, err := s.client.UpgradeBlock(context.Background(), inputBlock, newTag)
	require.NoError(err)

	require.Equal(newTag, block.Metadata.Tag)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
	for _, transactionTrace := range blobdata.TransactionTraces {
		require.Equal(fixtureTransactionTrace, string(transactionTrace))
	}
	require.NotNil(blobdata.GetPolygon().Author)
}

func (s *polygonClientTestSuite) TestEthereumClient_UpgradePolygonBlock_1_2_GenesisBlock() {
	const (
		oldTag uint32 = 1
		newTag uint32 = 2
	)

	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(simpleGenesisBlock),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params([]interface{}{
			"0x0",
			true,
		}),
	).Return(blockResponse, nil)

	inputBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    oldTag,
			Height: 0,
			Hash:   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{},
		},
	}
	block, err := s.client.UpgradeBlock(context.Background(), inputBlock, newTag)
	require.NoError(err)

	require.Equal(newTag, block.Metadata.Tag)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.Equal(nilAddress, string(blobdata.GetPolygon().Author))
}

func (s *polygonClientTestSuite) TestEthereumClient_UpgradePolygonBlock_1_2_NoTransactions() {
	const (
		oldTag uint32 = 1
		newTag uint32 = 2
	)

	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithoutTransactions),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params([]interface{}{
			"0xacc290",
			true,
		}),
	).Return(blockResponse, nil)

	authorResponse := &jsonrpc.Response{
		Result: []byte(blockAuthorResponse),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethBorGetAuthorMethod, gomock.Any(),
	).Return(authorResponse, nil)

	originTraces := make([][]byte, 2)
	inputBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    oldTag,
			Height: 11322000,
			Hash:   "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				TransactionTraces: originTraces,
			},
		},
	}
	block, err := s.client.UpgradeBlock(context.Background(), inputBlock, newTag)
	require.NoError(err)

	require.Equal(newTag, block.Metadata.Tag)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.Nil(blobdata.TransactionTraces)
}

func (s *polygonClientTestSuite) TestPolygonTest_RetryBorAuthor() {
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

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, WithBestEffort())
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

func (s *polygonClientTestSuite) TestRetryTraceBlock() {
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

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, WithBestEffort())
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
