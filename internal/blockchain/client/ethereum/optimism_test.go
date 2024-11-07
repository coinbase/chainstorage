package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	fixtureBlockOptimism = `
	{
		"hash": "0x57fc5e681f7c6fa5bdd78f90ed32a917d261e8816fca786d6e75458abda76a6d",
		"number": "0x16e3600",
		"parentHash": "0xbf398fdf0ddbb775c57cc7a06ae6ada42f01d6abf7605461c6afcaf129ebcbf4",
		"timestamp": "0x632a10c3",
		"transactions": [
			{
				"blockHash": "0x57fc5e681f7c6fa5bdd78f90ed32a917d261e8816fca786d6e75458abda76a6d",
				"hash": 	 "0x2ed22f4431cd505d859dfef6b3e60d7a89237e5216f8be326e78a7a2220efa56"
			}
		]
	}
	`

	fixtureReceiptOptimism = `{
		"blockHash": 	"0x57fc5e681f7c6fa5bdd78f90ed32a917d261e8816fca786d6e75458abda76a6d",
		"blockNumber": 	"0x16e3600",
		"status":		"0x1"
	}`

	fixtureBlockOptimismDuplicateTransaction = `
	{
		"hash": "0x09b353fbfa414ff7765e9af807f488110775d55cfeee7df9ef3ee47e2aa0e9b9",
		"number": "0x3d9",
		"parentHash": "0x18f8b5a404a63456d8cc527beb93f61eaa7b1d3b71c2d43f18ba94c4cb0b077c",
		"timestamp": "0x632a10c3",
		"transactions": [
			{
				"blockHash": "0x5572ca94f6ef220f754ee486190a15c43aadcdfb2371ed3be1cd2d20f6edd96f",
				"hash": 	 "0x9ed8f713b2cc6439657db52dcd2fdb9cc944915428f3c6e2a7703e242b259cb9"
			}
		]
	}
	`
	fixtureReceiptOptimismDuplicateTransaction = `{
		"blockHash": 	"0x5572ca94f6ef220f754ee486190a15c43aadcdfb2371ed3be1cd2d20f6edd96f",
		"blockNumber": 	"0xafec",
		"status":		"0x1"
	}`

	fixtureTraceOptimismNullResponse = `
	[
		{"error": "TypeError: cannot read property 'toString' of undefined    in server-side tracer function 'result'"}
	]
	`
	fixtureTraceOptimismDuplicateTransaction = `{"error":"Creating fake trace for failed transaction.","type":"","from":"","to":"","value":"0x0","gas":"0x0","gasUsed":"0x0","input":"","output":"","calls":null}`
)

type optimismClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testapp   testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    internal.Client
}

func TestOptimismClientTestSuite(t *testing.T) {
	suite.Run(t, new(optimismClientTestSuite))
}

func (s *optimismClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_OPTIMISM, common.Network_NETWORK_OPTIMISM_MAINNET),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *optimismClientTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *optimismClientTestSuite) TestOptimismClient_New() {
	var result internal.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_OPTIMISM, common.Network_NETWORK_OPTIMISM_MAINNET),
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

func (s *optimismClientTestSuite) TestOptimismTest_GetBlockByHeight_WithBestEffort() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockOptimism),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethTraceTransactionOptimismMethod {
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

				return nil, fmt.Errorf("unknown tracer: %v", tracer)
			}

			return nil, fmt.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceiptOptimism)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 24000000, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))
	for _, transactionTrace := range blobdata.TransactionTraces {
		require.Equal(fixtureTransactionTrace, string(transactionTrace))
	}
}

func (s *optimismClientTestSuite) TestOptimismTest_GetBlockByHeight_WithoutBestEffort() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockOptimism),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethTraceBlockByHashOptimismMethod {
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethCallTracer {
					return &jsonrpc.Response{
						Result: []byte(fixtureBlockTraceSize1),
					}, nil
				}

				return nil, fmt.Errorf("unknown tracer: %v", tracer)
			}

			return nil, fmt.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceiptOptimism)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 24000000)
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

// For Optimism, some blocks have duplicate transactions due to which we need to create fake receipts and
// traces for them.
func (s *optimismClientTestSuite) TestOptimismTest_FakeBlockTrace() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockOptimismDuplicateTransaction),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethTraceBlockByHashOptimismMethod {
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethCallTracer {
					return &jsonrpc.Response{
						Result: []byte(fixtureTraceOptimismNullResponse),
					}, nil
				}

				return nil, fmt.Errorf("unknown tracer: %v", tracer)
			}

			return nil, fmt.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceiptOptimismDuplicateTransaction)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 985)
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))

	// Check fake receipt
	var fullReceipt ethereum.EthereumTransactionReceipt
	receipt := blobdata.TransactionReceipts[0]
	err = json.Unmarshal(receipt, &fullReceipt)
	require.NoError(err)
	require.Equal(fullReceipt.BlockHash.Value(), "0x09b353fbfa414ff7765e9af807f488110775d55cfeee7df9ef3ee47e2aa0e9b9")
	require.Equal(fullReceipt.Status.Value(), uint64(0x0))
	require.Equal(fullReceipt.BlockNumber.Value(), uint64(0x3d9))

	// Check fake trace
	require.Equal(fixtureTraceOptimismDuplicateTransaction, string(blobdata.TransactionTraces[0]))
}

func (s *optimismClientTestSuite) TestOptimismTest_FakeTransactionTrace() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockOptimismDuplicateTransaction),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			if method == ethGetBlockByNumberMethod {
				return blockResponse, nil
			}

			if method == ethTraceTransactionOptimismMethod {
				opts := params[1].(map[string]string)
				tracer := opts["tracer"]
				if tracer == ethCallTracer {
					return nil, &jsonrpc.RPCError{Code: -32000, Message: optimismWhitelistError}
				}

				return nil, fmt.Errorf("unknown tracer: %v", tracer)
			}

			return nil, fmt.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceiptOptimismDuplicateTransaction)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 985, internal.WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(1, len(blobdata.TransactionTraces))

	// Check fake trace
	require.Equal(fixtureTraceOptimismDuplicateTransaction, string(blobdata.TransactionTraces[0]))
}
