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
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	fixtureArbitrumReceipt = `{
		"blockHash": "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141",
		"blockNumber":"0xacc290"
	}`
)

type arbitrumClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testapp   testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    internal.Client
}

func TestArbitrumClientTestSuite(t *testing.T) {
	suite.Run(t, new(arbitrumClientTestSuite))
}

func (s *arbitrumClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ARBITRUM, common.Network_NETWORK_ARBITRUM_MAINNET),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *arbitrumClientTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *arbitrumClientTestSuite) TestArbitrumClient_New() {
	var result internal.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ARBITRUM, common.Network_NETWORK_ARBITRUM_MAINNET),
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

func (s *arbitrumClientTestSuite) TestArbitrumTest_GetBlockByHeight() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/arbitrum/arb_getblockresponse.json")
	require.NoError(err)
	transactionResult, err := fixtures.ReadFile("client/arbitrum/arb_gettracesresponse.json")
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

			if method == ethParityTraceTransaction[common.Blockchain_BLOCKCHAIN_ARBITRUM] {
				return transactionResponse, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureArbitrumReceipt)},
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
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
}

func (s *arbitrumClientTestSuite) TestArbitrumTest_GetBlockByHash_WithoutBestEffort() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/arbitrum/arb_getblockresponse.json")
	require.NoError(err)
	transactionResult, err := fixtures.ReadFile("client/arbitrum/arb_gettracesresponse.json")
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

			if method == ethParityTraceBlockByNumber[common.Blockchain_BLOCKCHAIN_ARBITRUM] {
				return transactionResponse, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureArbitrumReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), tag, 11322000, "0x16786a164dca1618e0cbee77563ddd0f78fdcf5c0db8a123ea5ca0713d28e141")
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
}

func (s *arbitrumClientTestSuite) TestArbitrumTest_GetBlockByHeight_ServerError() {
	require := testutil.Require(s.T())

	rpcErr := &jsonrpc.RPCError{Code: ethParityTransactionTraceErrCode, Message: ethExecutionTimeoutError}
	traceError, _ := json.Marshal(&ethereumResultHolder{Error: ethTraceTransactionError})

	blockResult, err := fixtures.ReadFile("client/arbitrum/arb_getblockresponse.json")
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

			if method == ethParityTraceTransaction[common.Blockchain_BLOCKCHAIN_ARBITRUM] {
				return &jsonrpc.Response{}, rpcErr
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureArbitrumReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
	require.Equal(traceError, blobdata.TransactionTraces[0])
}

func (s *arbitrumClientTestSuite) TestArbitrumTest_SkipTxnForSpecificBlocks() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/arbitrum/arb_skippedtxn_blockresponse.json")
	require.NoError(err)
	transactionResult, err := fixtures.ReadFile("client/arbitrum/arb_gettracesresponse.json")
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

			if method == ethParityTraceTransaction[common.Blockchain_BLOCKCHAIN_ARBITRUM] {
				return transactionResponse, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureArbitrumReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 15458950, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))

	var parsedBlock ethereum.EthereumBlock
	err = json.Unmarshal(blobdata.Header, &parsedBlock)
	require.NoError(err)
	require.Equal(1, len(parsedBlock.Transactions))
	require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", parsedBlock.Transactions[0].Hash.Value())
}

func (s *arbitrumClientTestSuite) TestArbitrumTest_BatchGetBlockMetadata_SkipTxnForSpecificBlocks() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/arbitrum/arb_skippedtxn_batchresponse.json")
	require.NoError(err)
	blockResponse := []*jsonrpc.Response{
		{
			Result: blockResult,
		},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetBlockByNumberMethod, gomock.Any(),
	).Return(blockResponse, nil)

	blocksMetadata, err := s.client.BatchGetBlockMetadata(context.Background(), tag, 15458950, 15458951)
	block := blocksMetadata[0]
	require.NoError(err)
	require.NotNil(block)
}

func (s *arbitrumClientTestSuite) TestArbitrum_GetTracesByDebugEndpoint_Success() {
	require := testutil.Require(s.T())

	blockResult, err := fixtures.ReadFile("client/arbitrum/arb_getblockresponse_22207818.json")
	require.NoError(err)
	transactionResult, err := fixtures.ReadFile("client/arbitrum/arb_gettracesresponse_22207818.json")
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

			if method == ethTraceBlockByHashMethod {
				return transactionResponse, nil
			}

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureArbitrumReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 22207818)
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
