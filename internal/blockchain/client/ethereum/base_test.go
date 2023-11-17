package ethereum

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type baseClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testapp   testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    internal.Client
}

func TestBaseClientTestSuite(t *testing.T) {
	suite.Run(t, new(baseClientTestSuite))
}

func (s *baseClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BASE, common.Network_NETWORK_BASE_MAINNET),
		fx.Populate(&result),
	)

	s.NotNil(result.Master)
	s.NotNil(result.Slave)
	s.NotNil(result.Validator)
	s.NotNil(result.Consensus)

	s.client = result.Master
}

func (s *baseClientTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *baseClientTestSuite) TestBaseTest_GetLatestBlock() {
	require := testutil.Require(s.T())

	if baseCommitmentLevel == types.CommitmentLevelLatest {
		response := &jsonrpc.Response{
			Result: fixtures.MustReadFile("client/base/base_blocknumberresponse.json"),
		}
		s.rpcClient.EXPECT().
			Call(gomock.Any(), ethBlockNumber, nil).
			AnyTimes().
			Return(response, nil)

		height, err := s.client.GetLatestHeight(context.Background())
		require.NoError(err)
		require.Equal(uint64(0x5ea32), height)
	} else {
		response := &jsonrpc.Response{
			Result: fixtures.MustReadFile("client/base/base_getblockresponse.json"),
		}
		s.rpcClient.EXPECT().
			Call(gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{"safe", false}).
			AnyTimes().
			Return(response, nil)

		height, err := s.client.GetLatestHeight(context.Background())
		require.NoError(err)
		require.Equal(uint64(0x5ea32), height)
	}
}

func (s *baseClientTestSuite) TestBaseTest_GetBlockByHeight_WithBestEffort() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/base/base_getblockresponse.json"),
	}
	transactionResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/base/base_gettraceresponse.json"),
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
						Result: []byte("275"),
					}, nil
				}

				if tracer == ethCallTracer {
					return transactionResponse, nil
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{
			Result: fixtures.MustReadFile("client/base/base_gettransactionreceipt.json"),
		},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 387634, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
}

func (s *baseClientTestSuite) TestBaseTest_GetBlockByHeight_WithoutBestEffort() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/base/base_getblockresponse.json")
	require.NoError(err)
	transactionResult, err := fixtures.ReadFile("client/base/base_gettraceblockbyhashresponse.json")
	require.NoError(err)
	blockResponse := &jsonrpc.Response{
		Result: blockResult,
	}
	transactionResponse := &jsonrpc.Response{
		Result: transactionResult,
	}

	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			if method == ethGetBlockByHashMethod {
				return blockResponse, nil
			}

			if method == ethTraceBlockByHashMethod {
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethOpCountTracer {
					return &jsonrpc.Response{
						Result: []byte("275"),
					}, nil
				}

				if tracer == ethCallTracer {
					return transactionResponse, nil
				}

				return nil, xerrors.Errorf("unknown tracer: %v", tracer)
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{
			Result: fixtures.MustReadFile("client/base/base_gettransactionreceipt.json"),
		},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), tag, 387634, "0xcd2bbaef960686844a016bb301c60e5726d8e71db04ee19014ee3dfada7351b4")
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
}
