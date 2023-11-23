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
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	fixtureErigonBlockTrace = `
	[
		{"type": "CALL", "error": "trace_execution_error"},
		{"type": "CALL"}
	]
	`

	fixtureErigonEmptyTrace = `
	[
		{"type": "CALL"},
		{}
	]
	`
)

func TestBscClient_GetBlockByHeight(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	traceResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureErigonBlockTrace),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), erigonTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BSC, common.Network_NETWORK_BSC_MAINNET),
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_BSC, block.Blockchain)
	require.Equal(common.Network_NETWORK_BSC_MAINNET, block.Network)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(tag, metadata.Tag)
	require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", metadata.Hash)
	require.Equal("0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575", metadata.ParentHash)
	require.Equal(uint64(11322000), metadata.Height)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Nil(blobdata.Uncles)
}

func TestBscClient_GetBlockByHeightErigonDeprecation(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	traceResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockTrace),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), erigonTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BSC, common.Network_NETWORK_BSC_MAINNET),
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_BSC, block.Blockchain)
	require.Equal(common.Network_NETWORK_BSC_MAINNET, block.Network)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(tag, metadata.Tag)
	require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", metadata.Hash)
	require.Equal("0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575", metadata.ParentHash)
	require.Equal(uint64(11322000), metadata.Height)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Nil(blobdata.Uncles)
}

func TestBscClient_GetBlockByHeightTraceError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	traceResponse := &jsonrpc.Response{
		Error: &jsonrpc.RPCError{
			Message: "test error",
		},
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), erigonTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BSC, common.Network_NETWORK_BSC_MAINNET),
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	_, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.Equal(
		"failed to fetch traces for block 11322000: failed to unmarshal batch results: unexpected end of JSON input",
		err.Error(),
	)
}

func TestBscClient_GetBlockByHeightTraceEmptyTraceError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	traceResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureErigonEmptyTrace),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), erigonTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BSC, common.Network_NETWORK_BSC_MAINNET),
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	_, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.Equal(
		"failed to fetch traces for block 11322000: received empty erigon trace result at index 1",
		err.Error(),
	)
}
