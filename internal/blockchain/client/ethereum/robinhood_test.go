package ethereum

import (
	"context"
	"encoding/json"
	"testing"

	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	// Transaction hashes embedded in fixtureBlock.
	robinhoodTestTxHash0 = "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"
	robinhoodTestTxHash1 = "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991"
)

func newRobinhoodTestClient(t *testing.T, rpcClient *jsonrpcmocks.MockClient) (internal.Client, func()) {
	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ROBINHOOD, common.Network_NETWORK_ROBINHOOD_MAINNET),
		fx.Populate(&result),
	)
	return result.Master, app.Close
}

// A block containing a transaction terminated by ArbOS before EVM execution cannot be traced
// at the block level ("incorrect number of top-level calls"). The client should fall back to
// per-transaction tracing and substitute a fake trace for the poisoned transaction.
func TestRobinhoodClient_GetBlockByHeight_TopLevelCallsFallback(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{"0xacc290", true},
	).Return(&jsonrpc.Response{Result: json.RawMessage(fixtureBlock)}, nil)

	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return([]*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}, nil)

	rpcErr := &jsonrpc.RPCError{Code: -32000, Message: incorrectTopLevelCallsError}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(nil, rpcErr)

	rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceTransactionMethod, gomock.Any(),
	).Times(2).DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
		txHash, ok := params[0].(string)
		require.True(ok)
		switch txHash {
		case robinhoodTestTxHash0:
			return &jsonrpc.Response{Result: json.RawMessage(fixtureTransactionTrace)}, nil
		case robinhoodTestTxHash1:
			return nil, rpcErr
		default:
			t.Fatalf("unexpected transaction hash: %v", txHash)
			return nil, nil
		}
	})

	client, closeApp := newRobinhoodTestClient(t, rpcClient)
	defer closeApp()

	block, err := client.GetBlockByHeight(context.Background(), tag, ethereumHeight)
	require.NoError(err)
	require.NotNil(block)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.Equal(2, len(blobdata.TransactionTraces))

	// The healthy transaction keeps its real trace.
	require.Equal(json.RawMessage(fixtureTransactionTrace), json.RawMessage(blobdata.TransactionTraces[0]))

	// The poisoned transaction gets the fake trace, and its wire format is exactly
	// {"error": robinhoodFakeTraceError} so the parser sees a failed trace.
	expectedStub, err := json.Marshal(ethereum.EthereumTransactionTrace{Error: robinhoodFakeTraceError})
	require.NoError(err)
	require.Equal(expectedStub, blobdata.TransactionTraces[1])

	var stub map[string]any
	require.NoError(json.Unmarshal(blobdata.TransactionTraces[1], &stub))
	require.Equal(robinhoodFakeTraceError, stub["error"])
}

// Other block-level trace errors must still fail: the fallback only applies to the exact
// "incorrect number of top-level calls" error.
func TestRobinhoodClient_GetBlockByHeight_OtherTraceError_NoFallback(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{"0xacc290", true},
	).Return(&jsonrpc.Response{Result: json.RawMessage(fixtureBlock)}, nil)

	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return([]*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}, nil).AnyTimes()

	rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(nil, &jsonrpc.RPCError{Code: -32000, Message: "some other error"})

	client, closeApp := newRobinhoodTestClient(t, rpcClient)
	defer closeApp()

	_, err := client.GetBlockByHeight(context.Background(), tag, ethereumHeight)
	require.Error(err)
	require.Contains(err.Error(), "some other error")
}

// Non-Robinhood chains must not fall back: the same error on Ethereum still fails the block,
// and no per-transaction tracing is attempted.
func TestEthereumClient_GetBlockByHeight_TopLevelCallsError_NoFallback(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{"0xacc290", true},
	).Return(&jsonrpc.Response{Result: json.RawMessage(fixtureBlock)}, nil)

	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return([]*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}, nil).AnyTimes()

	// No expectation for ethTraceTransactionMethod: any per-transaction call would fail the test.
	rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(nil, &jsonrpc.RPCError{Code: -32000, Message: incorrectTopLevelCallsError})

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	_, err := result.Master.GetBlockByHeight(context.Background(), tag, ethereumHeight)
	require.Error(err)
	require.Contains(err.Error(), incorrectTopLevelCallsError)
}
