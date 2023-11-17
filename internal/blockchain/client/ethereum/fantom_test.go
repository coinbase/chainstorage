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
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	fixtureFantomReceipt = `{
		"blockHash": "0x00005a74000000fec7bf1f56c60d8fc0903da29d13b8e151c768c8630b999b4c",
		"blockNumber":"0xc5043f"
	}`
)

type fantomClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testapp   testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    internal.Client
}

func TestFantomClientTestSuite(t *testing.T) {
	suite.Run(t, new(fantomClientTestSuite))
}

func (s *fantomClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_FANTOM, common.Network_NETWORK_FANTOM_MAINNET),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *fantomClientTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *fantomClientTestSuite) TestFantomClient_New() {
	require := testutil.Require(s.T())

	var result internal.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_FANTOM, common.Network_NETWORK_FANTOM_MAINNET),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	require.NotNil(result.Master)
	require.NotNil(result.Slave)
	require.NotNil(result.Validator)
	require.NotNil(result.Consensus)
}

func (s *fantomClientTestSuite) TestFantomTest_GetBlockByHeight() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/fantom/fantom_getblockresponse.json")
	require.NoError(err)
	transactionResult, err := fixtures.ReadFile("client/fantom/fantom_gettracesresponse.json")
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
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethParityTraceTransaction[common.Blockchain_BLOCKCHAIN_FANTOM] {
				return transactionResponse, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureFantomReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 12911679, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(63, len(blobdata.TransactionTraces))
}

func (s *fantomClientTestSuite) TestFantomTest_GetBlockByHash_WithoutBestEffort() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/fantom/fantom_getblockresponse.json")
	require.NoError(err)
	transactionResult, err := fixtures.ReadFile("client/fantom/fantom_gettracesresponse.json")
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

			if method == ethParityTraceBlockByNumber[common.Blockchain_BLOCKCHAIN_FANTOM] {
				return transactionResponse, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureFantomReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), tag, 12911679, "0x00005a74000000fec7bf1f56c60d8fc0903da29d13b8e151c768c8630b999b4c")
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(63, len(blobdata.TransactionTraces))
}

func (s *fantomClientTestSuite) TestFantomTest_GetBlockByHeight_ServerError() {
	require := testutil.Require(s.T())

	rpcErr := &jsonrpc.RPCError{Code: ethParityTransactionTraceErrCode, Message: ethExecutionTimeoutError}
	traceError, _ := json.Marshal(&ethereumResultHolder{Error: ethTraceTransactionError})

	blockResult, err := fixtures.ReadFile("client/fantom/fantom_getblockresponse.json")
	require.NoError(err)
	blockResponse := &jsonrpc.Response{
		Result: blockResult,
	}

	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethParityTraceTransaction[common.Blockchain_BLOCKCHAIN_FANTOM] {
				return &jsonrpc.Response{}, rpcErr
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureFantomReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 12911679, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
	require.Equal(traceError, blobdata.TransactionTraces[0])
}
