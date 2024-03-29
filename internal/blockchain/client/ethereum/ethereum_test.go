package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"testing"

	geth "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"

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
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	tag                    = uint32(1)
	ethereumHeight         = uint64(11322000)
	ethereumParentHeight   = uint64(11321999)
	ethereumHash           = "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"
	ethereumParentHash     = "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575"
	ethereumBlockTimestamp = "2020-11-24T16:07:21Z"

	fixtureBlock = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0xacc290",
		"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		"timestamp": "0x5fbd2fb9",
		"transactions": [
			{"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"},
			{"hash": "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991"}
		]
	}
	`

	fixtureBlockWithTransactionsHashes = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0xacc290",
		"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		"timestamp": "0x5fbd2fb9",
		"transactions": [
			"0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
			"0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991"
		]
	}
	`

	fixtureBlock2WithTransactionsHashes = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016c",
		"number": "0xacc291",
		"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000576",
		"timestamp": "0x5fbd2fb9",
		"transactions": [
			"0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
			"0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991"
		]
	}
	`

	fixtureBlockWithNullAddressTransactions = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0xacc290",
		"parentHash": "0x9b863b8348e030fc6f2a566b7ad2914d4d9f39e93d0454e978e8509d3d14b91a",
		"timestamp": "0x5fbd2fb9",
		"transactions": [
			{
				"hash": "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
				"from": "0x0000000000000000000000000000000000000000",
				"to": "0x85f12f7946773cce68e9f5b9dbe4ab6f910b9d5c"
			},
			{
				"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				"from": "0x0000000000000000000000000000000000000000",
				"to": "0x0000000000000000000000000000000000000000"
			}
		]
	}
	`

	fixtureBlockZero = `
	{
		"hash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
		"parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
	}
	`

	fixtureBlockWithoutTransactions = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0xacc290",
		"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		"timestamp": "0x5fbd2fb9"
	}
	`

	fixtureBlockWithoutParentHash = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0xacc290",
		"timestamp": "0x5fbd2fb9"
	}
	`

	fixtureBlockWithEmptyHash = `
	{
		"hash": "",
		"number": "0xacc290",
		"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		"timestamp": "0x5fbd2fb9"
	}
	`

	fixtureBlockWithinDoSRange = `
	{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0x231860",
		"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		"timestamp": "0x5fbd2fb9",
		"transactions": [
			{"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"},
			{"hash": "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991"}
		]
	}
	`

	fixtureReceipt = `{
		"blockHash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"blockNumber":"0xacc290"
	}`
	fixtureNullReceipt     = "null"
	fixtureOrphanedReceipt = `{
		"blockHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		"blockNumber":"0xacc290"
	}`

	fixtureBlockTrace = `
	[
		{"result": {"type": "CALL"}},
		{"result": {"type": "CALL"}}
	]
	`

	fixtureBlockTraceSize1 = `
	[
		{"result": {"type": "CALL"}}
	]
	`

	fixtureTransactionTrace                     = `{"type": "CALL"}`
	fixtureTransactionTraceWithExecutionTimeout = `{"error":"execution timeout"}`
	fixtureTransactionTraceWithGasAsNumber      = `{"gas": 0, "failed": false, "returnValue": "", "structLogs": []}`

	fixtureTraceWithExecutionTimeout = `
	[
		{"result": {"type": "CALL"}},
		{"error": "execution timeout"}
	]
	`

	fixtureBlockNumber = `"0xacc290"`

	fixtureSingleUncleCount = `"0x1"`

	fixtureBlockWithUncles = `{
		"hash": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
		"number": "0xacc290",
		"parentHash": "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575",
		"timestamp": "0x5fbd2fb9",
		"uncles": [
			"0xc3dc6f1ac3d8eefca50d71c9f513051bf3d8eadf3a2dba858a3ad127d808f087"
		]
	}`

	fixtureUncle = `{
		"hash": "0xc3dc6f1ac3d8eefca50d71c9f513051bf3d8eadf3a2dba858a3ad127d808f087",
		"number": "0xa8bc02",
		"parentHash": "0xee0595fda331a8c1e89f0beb9f56c89299262cf4dc2c4e24c30a5e1ac96c4853"
	}`
)

func TestEthereumClient_New(t *testing.T) {
	require := testutil.Require(t)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		fx.Provide(parser.NewNop),
		fx.Provide(dlq.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	require.NotNil(result.Master)
	require.NotNil(result.Slave)
	require.NotNil(result.Validator)
	require.NotNil(result.Consensus)
}

func TestEthereumClient_GetBlockByHeight(t *testing.T) {
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
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHeight(context.Background(), tag, ethereumHeight)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, block.Network)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(tag, metadata.Tag)
	require.Equal(ethereumHash, metadata.Hash)
	require.Equal(ethereumParentHash, metadata.ParentHash)
	require.Equal(ethereumHeight, metadata.Height)
	require.Equal(ethereumParentHeight, metadata.ParentHeight)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp("2020-11-24T16:07:21Z"), metadata.Timestamp)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Nil(blobdata.Uncles)
}

func TestEthereumClient_GetBlockByHeightWithoutTransactions(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithoutTransactions),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, block.Network)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(tag, metadata.Tag)
	require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", metadata.Hash)
	require.Equal("0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575", metadata.ParentHash)
	require.Equal(uint64(11322000), metadata.Height)
	require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), metadata.Timestamp)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(0, len(blobdata.TransactionReceipts))
	require.Nil(blobdata.TransactionTraces)
}

func TestEthereumClient_GetBlockByHeightWithoutParentHash(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithoutParentHash),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	_, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.Error(err)
	require.Contains(err.Error(), "Field validation for 'ParentHash' failed")
}

func TestEthereumClient_GetBlockByHeightWithEmptyHash(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithEmptyHash),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	_, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrBlockNotFound))
}

func TestEthereumClient_GetBlockByHash_UnfinalizedDataError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	rpcClient.EXPECT().
		Call(gomock.Any(), ethGetBlockByHashMethod, jsonrpc.Params{
			ethereumHash,
			true,
		}).
		Return(nil, &jsonrpc.RPCError{
			Code:    -32000,
			Message: unfinalizedDataError,
		})

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	_, err := client.GetBlockByHash(context.Background(), tag, ethereumHeight, ethereumHash)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrBlockNotFound))
}

func TestEthereumClient_GetBlockByHeightWithBlockZero(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockZero),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0x0",
			true,
		},
	).Return(blockResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	_, err := client.GetBlockByHeight(context.Background(), tag, 0)
	require.NoError(err)
}

func TestEthereumClient_GetBlockByHeightWithBestEffort(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
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

			return nil, xerrors.Errorf("unknown method: %v", method)
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHeight(context.Background(), tag, 11322000, internal.WithBestEffort())
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

func TestEthereumClient_GetBlockByHeightWithUncles(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockWithUncles),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)

	uncleResponses := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureUncle)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetUncleByBlockHashAndIndex, gomock.Any(),
	).Return(uncleResponses, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHeight(context.Background(), tag, 11322000)
	require.NoError(err)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Uncles)
	require.Equal(1, len(blobdata.Uncles))
	require.Equal(fixtureUncle, string(blobdata.Uncles[0]))
}

func TestEthereumClient_UpgradeBlock(t *testing.T) {
	const (
		oldTag uint32 = 0
		newTag uint32 = 1
	)

	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	uncleCountResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureSingleUncleCount),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetUncleCountByBlockHash, gomock.Any(),
	).Return(uncleCountResponse, nil)

	uncleResponses := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureUncle)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetUncleByBlockHashAndIndex, gomock.Any(),
	).Return(uncleResponses, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	inputBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    oldTag,
			Height: 12345,
			Hash:   "0xabcde",
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{},
		},
	}
	block, err := client.UpgradeBlock(context.Background(), inputBlock, newTag)
	require.NoError(err)

	require.Equal(newTag, block.Metadata.Tag)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Uncles)
	require.Equal(1, len(blobdata.Uncles))
	require.Equal(fixtureUncle, string(blobdata.Uncles[0]))
}

func TestEthereumClient_UpgradeBlock_1_2(t *testing.T) {
	const (
		oldTag uint32 = 1
		newTag uint32 = 2
	)

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

	traceResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockTrace),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

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
	block, err := client.UpgradeBlock(context.Background(), inputBlock, newTag)
	require.NoError(err)

	require.Equal(newTag, block.Metadata.Tag)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
	require.NotEmpty(blobdata.TransactionTraces[0])
	require.NotEmpty(blobdata.TransactionTraces[1])
}

func TestEthereumClient_GetLatestHeight(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockNumber),
	}
	rpcClient.EXPECT().Call(gomock.Any(), ethBlockNumber, nil).Return(blockResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	latest, err := client.GetLatestHeight(context.Background())
	require.NoError(err)
	require.Equal(uint64(11322000), latest)
}

func TestEthereumClient_BatchGetBlockMetadata(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	batchResponse :=
		[]*jsonrpc.Response{
			{
				Result: json.RawMessage(fixtureBlockWithTransactionsHashes),
			},
			{
				Result: json.RawMessage(fixtureBlock2WithTransactionsHashes),
			},
		}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(),
		ethGetBlockByNumberMethod,
		[]jsonrpc.Params{
			{
				"0xacc290",
				false,
			},
			{
				"0xacc291",
				false,
			},
		}).AnyTimes().Return(batchResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	blockMetadatas, err := client.BatchGetBlockMetadata(context.Background(), tag, ethereumHeight, ethereumHeight+2)
	require.NoError(err)
	require.Equal(2, len(blockMetadatas))

	metadata0 := blockMetadatas[0]
	require.NotNil(metadata0)
	require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", metadata0.Hash)
	require.Equal("0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575", metadata0.ParentHash)
	require.Equal(uint64(ethereumHeight), metadata0.Height)
	require.Equal(uint64(ethereumHeight-1), metadata0.ParentHeight)
	require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), metadata0.Timestamp)

	metadata1 := blockMetadatas[1]
	require.NotNil(metadata1)
	require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016c", metadata1.Hash)
	require.Equal("0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000576", metadata1.ParentHash)
	require.Equal(uint64(ethereumHeight+1), metadata1.Height)
	require.Equal(uint64(ethereumHeight), metadata1.ParentHeight)
	require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), metadata1.Timestamp)
}

func TestEthereumClient_BatchGetBlockMetadata_MiniBatch(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	tests := []struct {
		blocks         int
		batches        int
		blockResponses map[string]json.RawMessage
	}{
		{
			blocks:  ethereumBatchSize - 1,
			batches: 1,
			blockResponses: map[string]json.RawMessage{
				"0xacc290": json.RawMessage(`{"number": "0xacc290", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc291": json.RawMessage(`{"number": "0xacc291", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc292": json.RawMessage(`{"number": "0xacc292", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc293": json.RawMessage(`{"number": "0xacc293", "hash": "0xa", "parentHash": "0xb"}`),
			},
		},
		{
			blocks:  ethereumBatchSize,
			batches: 1,
			blockResponses: map[string]json.RawMessage{
				"0xacc290": json.RawMessage(`{"number": "0xacc290", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc291": json.RawMessage(`{"number": "0xacc291", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc292": json.RawMessage(`{"number": "0xacc292", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc293": json.RawMessage(`{"number": "0xacc293", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc294": json.RawMessage(`{"number": "0xacc294", "hash": "0xa", "parentHash": "0xb"}`),
			},
		},
		{
			blocks:  ethereumBatchSize + 1,
			batches: 2,
			blockResponses: map[string]json.RawMessage{
				"0xacc290": json.RawMessage(`{"number": "0xacc290", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc291": json.RawMessage(`{"number": "0xacc291", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc292": json.RawMessage(`{"number": "0xacc292", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc293": json.RawMessage(`{"number": "0xacc293", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc294": json.RawMessage(`{"number": "0xacc294", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc295": json.RawMessage(`{"number": "0xacc295", "hash": "0xa", "parentHash": "0xb"}`),
			},
		},
		{
			blocks:  ethereumBatchSize * 2,
			batches: 2,
			blockResponses: map[string]json.RawMessage{
				"0xacc290": json.RawMessage(`{"number": "0xacc290", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc291": json.RawMessage(`{"number": "0xacc291", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc292": json.RawMessage(`{"number": "0xacc292", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc293": json.RawMessage(`{"number": "0xacc293", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc294": json.RawMessage(`{"number": "0xacc294", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc295": json.RawMessage(`{"number": "0xacc295", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc296": json.RawMessage(`{"number": "0xacc296", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc297": json.RawMessage(`{"number": "0xacc297", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc298": json.RawMessage(`{"number": "0xacc298", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc299": json.RawMessage(`{"number": "0xacc299", "hash": "0xa", "parentHash": "0xb"}`),
			},
		},
		{
			blocks:  ethereumBatchSize*2 + 1,
			batches: 3,
			blockResponses: map[string]json.RawMessage{
				"0xacc290": json.RawMessage(`{"number": "0xacc290", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc291": json.RawMessage(`{"number": "0xacc291", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc292": json.RawMessage(`{"number": "0xacc292", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc293": json.RawMessage(`{"number": "0xacc293", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc294": json.RawMessage(`{"number": "0xacc294", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc295": json.RawMessage(`{"number": "0xacc295", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc296": json.RawMessage(`{"number": "0xacc296", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc297": json.RawMessage(`{"number": "0xacc297", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc298": json.RawMessage(`{"number": "0xacc298", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc299": json.RawMessage(`{"number": "0xacc299", "hash": "0xa", "parentHash": "0xb"}`),
				"0xacc29a": json.RawMessage(`{"number": "0xacc29a", "hash": "0xa", "parentHash": "0xb"}`),
			},
		},
	}
	for _, test := range tests {
		name := strconv.Itoa(test.blocks)
		t.Run(name, func(t *testing.T) {
			rpcClient.EXPECT().BatchCall(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, method *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, opts ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
					result := make([]*jsonrpc.Response, len(batchParams))
					for i, params := range batchParams {
						require.Equal(2, len(params))
						height := fmt.Sprintf("%s", params[0])
						result[i] = &jsonrpc.Response{Result: test.blockResponses[height]}
					}
					return result, nil
				}).Times(test.batches)

			metadatas, err := client.BatchGetBlockMetadata(context.Background(), tag, ethereumHeight, ethereumHeight+uint64(test.blocks))
			require.NoError(err)
			require.Equal(test.blocks, len(metadatas))
			for i, metadata := range metadatas {
				require.Equal(ethereumHeight+uint64(i), metadata.Height)
			}
		})
	}
}

func TestEthereumClient_GetBlockByHash(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}

	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByHashMethod, jsonrpc.Params{
			ethereumHash,
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
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHash(context.Background(), tag, ethereumHeight, ethereumHash)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, block.Network)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(ethereumHash, metadata.Hash)
	require.Equal(ethereumParentHash, metadata.ParentHash)
	require.Equal(ethereumHeight, metadata.Height)
	require.Equal(ethereumParentHeight, metadata.ParentHeight)
	require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), metadata.Timestamp)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
}

func TestEthereumClient_GetBlockByHashExecutionTimeoutError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rpcClient := jsonrpcmocks.NewMockClient(ctrl)
	rpcClient.EXPECT().Call(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			switch method {
			case ethGetBlockByHashMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureBlock),
				}, nil

			case ethTraceBlockByHashMethod:
				return &jsonrpc.Response{
					Result: json.RawMessage(fixtureTraceWithExecutionTimeout),
				}, nil

			default:
				return nil, xerrors.Errorf("unknown method: %v", method)
			}
		})

	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	_, err := client.GetBlockByHash(context.Background(), tag, ethereumHeight, ethereumHash)
	require.Error(err)
	require.Contains(err.Error(), "received partial result")
}

func TestEthereumClient_RetryOrphanedTransactionReceipts(t *testing.T) {
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

	attempts := 0
	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Times(retry.DefaultMaxAttempts).
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, opts ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
			// Return the correct receipt on the last retry attempt.
			attempts += 1
			if attempts < retry.DefaultMaxAttempts {
				return []*jsonrpc.Response{
					{Result: json.RawMessage(fixtureReceipt)},
					{Result: json.RawMessage(fixtureOrphanedReceipt)},
				}, nil
			}

			return []*jsonrpc.Response{
				{Result: json.RawMessage(fixtureReceipt)},
				{Result: json.RawMessage(fixtureReceipt)},
			}, nil
		})
	traceResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockTrace),
	}
	rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).Return(traceResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	block, err := client.GetBlockByHeight(context.Background(), tag, 0xacc290)
	require.NoError(err)
	require.NotNil(block)
	require.Equal(retry.DefaultMaxAttempts, attempts)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.Equal(fixtureReceipt, string(blobdata.TransactionReceipts[0]))
	require.Equal(fixtureReceipt, string(blobdata.TransactionReceipts[1]))
}

func TestEthereumClient_RetryOrphanedTransactionReceipts_RetryLimitExceeded(t *testing.T) {
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

	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Times(retry.DefaultMaxAttempts).
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, opts ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
			return []*jsonrpc.Response{
				{Result: json.RawMessage(fixtureReceipt)},
				{Result: json.RawMessage(fixtureOrphanedReceipt)},
			}, nil
		})

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	_, err := client.GetBlockByHeight(context.Background(), tag, 0xacc290)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrBlockNotFound))
}

func TestCanReprocess(t *testing.T) {
	tests := []struct {
		expected bool
		height   uint64
	}{
		{
			expected: true,
			height:   1000000,
		},
		{
			expected: false,
			height:   1431916,
		},
		{
			expected: true,
			height:   2000000,
		},
		{
			expected: false,
			height:   2283397,
		},
		{
			expected: false,
			height:   2462999,
		},
		{
			expected: true,
			height:   2463000,
		},
	}
	for _, test := range tests {
		name := strconv.Itoa(int(test.height))
		t.Run(name, func(t *testing.T) {
			require := testutil.Require(t)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			rpcClient := jsonrpcmocks.NewMockClient(ctrl)

			var result internal.ClientParams
			app := testapp.New(
				t,
				Module,
				testModule(rpcClient),
				fx.Populate(&result),
			)
			defer app.Close()

			actual := result.Master.CanReprocess(0, test.height)
			require.Equal(test.expected, actual)
		})
	}
}

func TestEthereumClient_GetAccountProof_WithHash(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	proofData := fixtures.MustReadFile("parser/ethereum/account_proof_block_17000000.json")
	proofResponse := &jsonrpc.Response{
		Result: json.RawMessage(proofData),
	}
	account := "0x8c8d7c46219d9205f056f28fee5950ad564d7465"

	rpcClient.EXPECT().Call(gomock.Any(), ethGetProofMethod, jsonrpc.Params{
		account,
		[]string{},
		ethereumHash,
	}).Return(proofResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	accountProof, err := client.GetAccountProof(context.Background(), &api.GetVerifiedAccountStateRequest{
		Req: &api.InternalGetVerifiedAccountStateRequest{
			Account: account,
			Height:  ethereumHeight,
			Hash:    ethereumHash,
		},
	})

	require.NoError(err)
	require.NotNil(accountProof)

	require.Equal(proofData, accountProof.GetEthereum().GetAccountProof())
}

func TestEthereumClient_GetAccountProof_WithoutHash(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	proofData := fixtures.MustReadFile("parser/ethereum/account_proof_block_17000000.json")
	proofResponse := &jsonrpc.Response{
		Result: json.RawMessage(proofData),
	}
	account := "0x8c8d7c46219d9205f056f28fee5950ad564d7465"

	heightString := hexutil.EncodeUint64(ethereumHeight)

	rpcClient.EXPECT().Call(gomock.Any(), ethGetProofMethod, jsonrpc.Params{
		account,
		[]string{},
		heightString,
	}).Return(proofResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	// The input blockHash is empty.
	accountProof, err := client.GetAccountProof(context.Background(), &api.GetVerifiedAccountStateRequest{
		Req: &api.InternalGetVerifiedAccountStateRequest{
			Account: account,
			Height:  ethereumHeight,
			Hash:    "",
		},
	})
	require.NoError(err)
	require.NotNil(accountProof)

	require.Equal(proofData, accountProof.GetEthereum().GetAccountProof())
}

func TestEthereumClient_GetAccountProof_Erc20(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	proofData := fixtures.MustReadFile("parser/ethereum/account_proof_erc20_block_17300000.json")
	proofResponse := &jsonrpc.Response{
		Result: json.RawMessage(proofData),
	}
	account := "0x467d543e5e4e41aeddf3b6d1997350dd9820a173"
	// USDC address
	contract := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

	// Calculate the storage slot index
	accountData, _ := hexutil.Decode(account)
	slotIndex := big.NewInt(int64(erc20StorageIndex[contract])).Bytes()
	storageKey := crypto.Keccak256(geth.LeftPadBytes(accountData, 32), geth.LeftPadBytes(slotIndex, 32))

	// Test the client parameters.
	rpcClient.EXPECT().Call(gomock.Any(), ethGetProofMethod, jsonrpc.Params{
		contract,
		[]string{hexutil.Bytes(storageKey).String()},
		ethereumHash,
	}).Return(proofResponse, nil)

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	accountProof, err := client.GetAccountProof(context.Background(), &api.GetVerifiedAccountStateRequest{
		Req: &api.InternalGetVerifiedAccountStateRequest{
			Account: account,
			Height:  ethereumHeight,
			Hash:    ethereumHash,
			ExtraInput: &api.InternalGetVerifiedAccountStateRequest_Ethereum{
				Ethereum: &api.EthereumExtraInput{
					Erc20Contract: contract,
				},
			},
		},
	})

	require.NoError(err)
	require.NotNil(accountProof)

	require.Equal(proofData, accountProof.GetEthereum().GetAccountProof())
}

func TestEthereumClient_GetAccountProof_Erc20_Failure(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	account := "0x467d543e5e4e41aeddf3b6d1997350dd9820a173"
	// An unsupported Erc20 token contract
	randomContract := "0x23486991c6218b36c1d19d4a2e9eb0ce3606eb48"

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)
	accountProof, err := client.GetAccountProof(context.Background(), &api.GetVerifiedAccountStateRequest{
		Req: &api.InternalGetVerifiedAccountStateRequest{
			Account: account,
			Height:  ethereumHeight,
			Hash:    ethereumHash,
			ExtraInput: &api.InternalGetVerifiedAccountStateRequest_Ethereum{
				Ethereum: &api.EthereumExtraInput{
					Erc20Contract: randomContract,
				},
			},
		},
	})

	require.Nil(accountProof)
	require.Contains(err.Error(), "the input erc20 token(0x23486991c6218b36c1d19d4a2e9eb0ce3606eb48) is not supported yet")
}

func TestEthereumClient_RetryTraceBlock_RequestTimedOut(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rpcClient := jsonrpcmocks.NewMockClient(ctrl)

	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	traceResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlockTrace),
	}
	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}

	rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByHashMethod, gomock.Any(),
	).Return(blockResponse, nil)

	rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	retryAttempts := 0
	rpcClient.EXPECT().Call(
		gomock.Any(), ethTraceBlockByHashMethod, gomock.Any(),
	).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params, opts ...jsonrpc.Option) (*jsonrpc.Response, error) {
			retryAttempts += 1
			if retryAttempts == retry.DefaultMaxAttempts {
				return traceResponse, nil
			}

			return nil, &jsonrpc.RPCError{
				Code:    -32002,
				Message: "request timed out",
			}
		})

	var result internal.ClientParams
	app := testapp.New(
		t,
		Module,
		testModule(rpcClient),
		fx.Populate(&result),
	)
	defer app.Close()

	client := result.Master
	require.NotNil(client)

	_, err := client.GetBlockByHash(context.Background(), tag, ethereumHeight, ethereumHash)
	require.NoError(err)
}

func testModule(client *jsonrpcmocks.MockClient) fx.Option {
	return fx.Options(
		internal.Module,
		restapi.Module,
		fx.Provide(fx.Annotated{
			Name:   "master",
			Target: func() jsonrpc.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "slave",
			Target: func() jsonrpc.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "validator",
			Target: func() jsonrpc.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "consensus",
			Target: func() jsonrpc.Client { return client },
		}),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
	)
}
