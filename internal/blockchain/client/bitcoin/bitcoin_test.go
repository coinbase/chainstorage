package bitcoin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	btcTag = uint32(1)

	btcFixtureBlockHash         = "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"
	btcFixturePreviousBlockHash = "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3"
	btcFixtureBlockHeight       = uint64(696402)
	btcFixtureBlockTime         = int64(1629306034)

	btcFixtureGetBlockCountResponse        = `697413`
	btcFixtureGetBlockHashResponse         = `"000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"`
	btcFixtureBatchGetBlockHashResponse1   = `"000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"`
	btcFixtureBatchGetBlockByHashResponse1 = `
{
  "hash": "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e",
  "height": 101,
  "tx": [],
  "previousblockhash": "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3",
  "time": 1629306034
}
`
	btcFixtureBatchGetBlockByHashResponse1WithoutHash = `
{
  "height": 101,
  "tx": [],
  "previousblockhash": "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3",
  "time": 1629306034
}
`
	btcFixtureBatchGetBlockByHashResponse1WithoutParentHash = `
{
  "hash": "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e",
  "height": 101,
  "tx": [],
  "time": 1629306034
}
`
	btcFixtureBatchGetBlockByHashResponse1WithInconsistentHash = `
{
  "hash": "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f",
  "height": 101,
  "tx": [],
  "previousblockhash": "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3",
  "time": 1629306034,
}
`
	btcFixtureBatchGetBlockByHashResponse1WithInconsistentHeight = `
{
  "hash": "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e",
  "height": 1010,
  "tx": [],
  "previousblockhash": "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3",
  "time": 1629306034
}
`
	btcFixtureBatchGetBlockHashHash1       = "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e"
	btcFixtureBatchGetBlockHashResponse2   = `"222000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0f222"`
	btcFixtureBatchGetBlockByHashResponse2 = `
{
  "hash": "222000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0f222",
  "height": 102,
  "tx": [],
  "previousblockhash": "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec3",
  "time": 1629306034
}
`
	btcFixtureBatchGetBlockHashHash2       = "222000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0f222"
	btcFixtureBatchGetBlockHashResponse3   = `"111000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0f111"`
	btcFixtureBatchGetBlockByHashResponse3 = `
{
  "hash": "111000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0f111",
  "height": 103,
  "tx": [],
  "previousblockhash": "0000000000000000000bbc2c027a9f9a9144f5368d1e02091bddd0307b058ec2",
  "time": 1629306034
}
`
	btcFixtureBatchGetBlockHashHash3 = "111000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0f111"

	btcFixtureGetGenesisBlockResponse = `
{
  "hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
  "previousblockhash":"",
  "height":0,
  "tx":[{"txid":"4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b","vin":[{"txid":""}]}],
  "time": 1629306034
}
`
	btcFixtureGenesisBlockHashResponse = `"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"`
	btcFixtureGenesisBlockHash         = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
)

var (
	btcFixtureInputTransactionIDs = []string{
		"15737c23e02136a7821e966d4eb1a9020f9b673ef2d42aa2634cf5a34f656e02",
		"e37e2f43693a5a8987c19fb8766d822d5e6d5da2f266882e709e05b4808c7d63",
		"09521bcb529be1fe63fd28aff8e0b121756371e612289866fdd948f0ca1321c4",
	}
)

type bitcoinClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testapp   testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    internal.Client
}

func TestRpcMethodsOverrideFromConfig(t *testing.T) {
	t.Run("NoOverrides", func(t *testing.T) {
		require := testutil.Require(t)
		cfg := &config.Config{}
		methods := newRPCMethods(rpcMethodsOverrideFromConfig(cfg))
		require.Equal(defaultGetBlockByHash.Timeout, methods.getBlockByHash.Timeout)
		require.Equal(defaultGetRawTransaction.Timeout, methods.getRawTransaction.Timeout)
		require.Equal(defaultGetBlockHash.Timeout, methods.getBlockHash.Timeout)
	})

	t.Run("OverrideGetRawTx", func(t *testing.T) {
		require := testutil.Require(t)
		cfg := &config.Config{}
		cfg.Chain.Client.RpcTimeoutGetRawTx = 60 * time.Second
		methods := newRPCMethods(rpcMethodsOverrideFromConfig(cfg))
		require.Equal(60*time.Second, methods.getRawTransaction.Timeout)
		require.Equal(defaultGetBlockByHash.Timeout, methods.getBlockByHash.Timeout)
		require.Equal(defaultGetBlockHash.Timeout, methods.getBlockHash.Timeout)
	})

	t.Run("OverrideAll", func(t *testing.T) {
		require := testutil.Require(t)
		cfg := &config.Config{}
		cfg.Chain.Client.RpcTimeoutGetBlock = 20 * time.Second
		cfg.Chain.Client.RpcTimeoutGetRawTx = 60 * time.Second
		cfg.Chain.Client.RpcTimeoutGetBlockHash = 10 * time.Second
		methods := newRPCMethods(rpcMethodsOverrideFromConfig(cfg))
		require.Equal(20*time.Second, methods.getBlockByHash.Timeout)
		require.Equal(60*time.Second, methods.getRawTransaction.Timeout)
		require.Equal(10*time.Second, methods.getBlockHash.Timeout)
	})
}

func TestBitcoinClientTestSuite(t *testing.T) {
	suite.Run(t, new(bitcoinClientTestSuite))
}

func (s *bitcoinClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *bitcoinClientTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_New() {
	var result internal.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
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

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHeight() {
	require := testutil.Require(s.T())

	getBlockHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(btcFixtureGetBlockHashResponse),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockHash, jsonrpc.Params{
			btcFixtureBlockHeight,
		},
	).Return(getBlockHashResponse, nil)

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: response,
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	resp1, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx1_resp.json")
	require.NoError(err)
	resp2, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx2_resp.json")
	require.NoError(err)
	resp3, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx3_resp.json")
	require.NoError(err)
	getRawTransactionResponse := []*jsonrpc.Response{
		{
			Result: resp1,
		},
		{
			Result: resp2,
		},
		{
			Result: resp3,
		},
	}
	var expectedParams []jsonrpc.Params
	for _, id := range btcFixtureInputTransactionIDs {
		expectedParams = append(
			expectedParams,
			jsonrpc.Params{id, true},
		)
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction, expectedParams, gomock.Any(),
	).Return(getRawTransactionResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), btcTag, btcFixtureBlockHeight)
	s.NoError(err)

	s.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, block.Blockchain)
	s.Equal(common.Network_NETWORK_BITCOIN_MAINNET, block.Network)
	s.Equal(btcTag, block.Metadata.Tag)
	s.Equal(btcFixtureBlockHash, block.Metadata.Hash)
	s.Equal(btcFixturePreviousBlockHash, block.Metadata.ParentHash)
	s.Equal(btcFixtureBlockHeight, block.Metadata.Height)
	s.Equal(btcFixtureBlockTime, block.Metadata.Timestamp.GetSeconds())

	blobdata := block.GetBitcoin()
	s.NotNil(blobdata)
	s.NotNil(blobdata.Header)
	s.NotNil(blobdata.InputTransactions)
	s.Equal(3, len(blobdata.InputTransactions))
	s.Equal(0, len(blobdata.InputTransactions[0].Data))
	s.Equal(1, len(blobdata.InputTransactions[1].Data))
	s.Equal(2, len(blobdata.InputTransactions[2].Data))

	data1, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx1_data.json")
	require.NoError(err)
	s.JSONEq(string(data1), string(blobdata.InputTransactions[1].Data[0]))

	data2, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx2_data.json")
	require.NoError(err)
	s.JSONEq(string(data2), string(blobdata.InputTransactions[2].Data[0]))

	data3, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx3_data.json")
	require.NoError(err)
	s.JSONEq(string(data3), string(blobdata.InputTransactions[2].Data[1]))
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetGenesisBlockByHeight() {
	getBlockHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(btcFixtureGenesisBlockHashResponse),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockHash, jsonrpc.Params{
			uint64(0),
		},
	).Return(getBlockHashResponse, nil)

	getBlockByHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(btcFixtureGetGenesisBlockResponse),
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureGenesisBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), btcTag, 0)
	s.NoError(err)

	s.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, block.Blockchain)
	s.Equal(common.Network_NETWORK_BITCOIN_MAINNET, block.Network)
	s.Equal(btcTag, block.Metadata.Tag)
	s.Equal(btcFixtureGenesisBlockHash, block.Metadata.Hash)
	s.Equal("", block.Metadata.ParentHash)
	s.Equal(uint64(0), block.Metadata.Height)
	s.Equal(btcFixtureBlockTime, block.Metadata.Timestamp.GetSeconds())

	blobdata := block.GetBitcoin()
	s.NotNil(blobdata)
	s.NotNil(blobdata.Header)
	s.Equal(1, len(blobdata.InputTransactions))
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHeight_BlockOutOfRange() {
	getBlockHashResponse := &jsonrpc.Response{
		Error: &jsonrpc.RPCError{
			Code:    -8,
			Message: "Block height out of range",
		},
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockHash, jsonrpc.Params{
			btcFixtureBlockHeight,
		},
	).Return(nil, getBlockHashResponse.Error)

	_, err := s.client.GetBlockByHeight(context.Background(), btcTag, btcFixtureBlockHeight)
	s.Error(err)
	s.True(xerrors.Is(err, internal.ErrBlockNotFound), err.Error())
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHeight_BlockNotFound() {
	getBlockHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(btcFixtureGetBlockHashResponse),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockHash, jsonrpc.Params{
			btcFixtureBlockHeight,
		},
	).Return(getBlockHashResponse, nil)

	getBlockByHashResponse := &jsonrpc.Response{
		Error: &jsonrpc.RPCError{
			Code:    -5,
			Message: "Block not found",
		},
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(nil, getBlockByHashResponse.Error)

	block, err := s.client.GetBlockByHeight(context.Background(), btcTag, btcFixtureBlockHeight)
	s.True(xerrors.Is(err, internal.ErrBlockNotFound))
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: response,
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	resp1, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx1_resp.json")
	require.NoError(err)
	resp2, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx2_resp.json")
	require.NoError(err)
	resp3, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx3_resp.json")
	require.NoError(err)
	getRawTransactionResponse := []*jsonrpc.Response{
		{
			Result: resp1,
		},
		{
			Result: resp2,
		},
		{
			Result: resp3,
		},
	}
	var expectedParams []jsonrpc.Params
	for _, id := range btcFixtureInputTransactionIDs {
		expectedParams = append(
			expectedParams,
			jsonrpc.Params{id, true},
		)
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction, expectedParams, gomock.Any(),
	).Return(getRawTransactionResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.NoError(err)

	s.Equal(common.Blockchain_BLOCKCHAIN_BITCOIN, block.Blockchain)
	s.Equal(common.Network_NETWORK_BITCOIN_MAINNET, block.Network)
	s.Equal(btcTag, block.Metadata.Tag)
	s.Equal(btcFixtureBlockHash, block.Metadata.Hash)
	s.Equal(btcFixturePreviousBlockHash, block.Metadata.ParentHash)
	s.Equal(btcFixtureBlockHeight, block.Metadata.Height)

	blobdata := block.GetBitcoin()
	s.NotNil(blobdata)
	s.NotNil(blobdata.Header)
	s.NotNil(blobdata.InputTransactions)
	s.Equal(3, len(blobdata.InputTransactions))
	s.Equal(0, len(blobdata.InputTransactions[0].Data))
	s.Equal(1, len(blobdata.InputTransactions[1].Data))
	s.Equal(2, len(blobdata.InputTransactions[2].Data))

	data1, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx1_data.json")
	require.NoError(err)
	s.JSONEq(string(data1), string(blobdata.InputTransactions[1].Data[0]))

	data2, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx2_data.json")
	require.NoError(err)
	s.JSONEq(string(data2), string(blobdata.InputTransactions[2].Data[0]))

	data3, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx3_data.json")
	require.NoError(err)
	s.JSONEq(string(data3), string(blobdata.InputTransactions[2].Data[1]))
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_BlockNotFound() {
	blockResponse := &jsonrpc.Response{
		Error: &jsonrpc.RPCError{
			Code:    -5,
			Message: "Block not found",
		},
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(nil, blockResponse.Error)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.True(xerrors.Is(err, internal.ErrBlockNotFound))
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_NoPreviousBlockHash() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse_withoutprevhash.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(response),
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_NoHeight() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse_withoutheight.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(response),
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_NoHash() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse_withouthash.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(response),
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_InconsistentHash() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse_withinconsistenthash.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: json.RawMessage(response),
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_GetInputTransactionRequestError() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: response,
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	var expectedParams []jsonrpc.Params
	for _, id := range btcFixtureInputTransactionIDs {
		expectedParams = append(
			expectedParams,
			jsonrpc.Params{id, true},
		)
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction, expectedParams, gomock.Any(),
	).Return(nil, xerrors.Errorf("error making http requests"))

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_GetRawTransactionResponseError() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: response,
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	resp1, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx1_resp.json")
	require.NoError(err)
	getRawTransactionResponse := []*jsonrpc.Response{
		{
			Result: resp1,
		},
		{
			Error: &jsonrpc.RPCError{
				Code:    -12345,
				Message: "error",
			},
		},
	}
	var expectedParams []jsonrpc.Params
	for _, id := range btcFixtureInputTransactionIDs {
		expectedParams = append(
			expectedParams,
			jsonrpc.Params{id, true},
		)
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction, expectedParams, gomock.Any(),
	).Return(getRawTransactionResponse, xerrors.Errorf("error making http requests"))

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_NoTransactions() {
	require := testutil.Require(s.T())

	resp, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse_withouttxs.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: resp,
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_InputTransactionNotFound() {
	require := testutil.Require(s.T())

	response, err := fixtures.ReadFile("client/bitcoin/btc_getblockresponse.json")
	require.NoError(err)
	getBlockByHashResponse := &jsonrpc.Response{
		Result: response,
		Error:  nil,
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{
			btcFixtureBlockHash,
			bitcoinBlockVerbosity,
		},
	).Return(getBlockByHashResponse, nil)

	resp2, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx2_resp.json")
	require.NoError(err)
	resp3, err := fixtures.ReadFile("client/bitcoin/btc_getinputtx3_resp.json")
	require.NoError(err)
	getRawTransactionResponse := []*jsonrpc.Response{
		{
			Result: resp2,
		},
		{
			Result: resp3,
		},
	}
	var expectedParams []jsonrpc.Params
	for _, id := range btcFixtureInputTransactionIDs {
		expectedParams = append(
			expectedParams,
			jsonrpc.Params{id, true},
		)
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction, expectedParams, gomock.Any(),
	).Return(getRawTransactionResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, btcFixtureBlockHeight, btcFixtureBlockHash)
	s.Error(err)
	s.Nil(block)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetLatestHeight() {
	getBlockCountResponse := &jsonrpc.Response{
		Result: json.RawMessage(btcFixtureGetBlockCountResponse),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockCount, jsonrpc.Params{},
	).Return(getBlockCountResponse, nil)

	height, err := s.client.GetLatestHeight(context.Background())
	s.NoError(err)
	s.Equal(uint64(697413), height)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetLatestHeight_ResponseError() {
	getBlockCountResponse := &jsonrpc.Response{
		Error: &jsonrpc.RPCError{
			Code:    -1234,
			Message: "Error",
		},
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockCount, jsonrpc.Params{},
	).Return(getBlockCountResponse, nil)

	height, err := s.client.GetLatestHeight(context.Background())
	s.Error(err)
	s.Equal(uint64(0), height)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_GetLatestHeight_CallError() {
	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockCount, jsonrpc.Params{},
	).Return(nil, xerrors.Errorf("error making http request"))

	height, err := s.client.GetLatestHeight(context.Background())
	s.Error(err)
	s.Equal(uint64(0), height)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	hashes := []string{
		btcFixtureBatchGetBlockHashHash1,
		btcFixtureBatchGetBlockHashHash2,
		btcFixtureBatchGetBlockHashHash3,
	}
	getBlocksParams := make([]jsonrpc.Params, len(hashes))
	for i, hash := range hashes {
		getBlocksParams[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	getBlocksResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockByHash, getBlocksParams,
	).Return(getBlocksResponses, nil)

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.NoError(err)
	s.NotNil(metadata)
	s.Equal(len(heights), len(metadata))

	for i, height := range heights {
		s.NotNil(metadata[i])
		s.Equal(height, metadata[i].Height)
		s.Equal(height-1, metadata[i].ParentHeight)
		s.Equal(hashes[i], metadata[i].Hash)
		s.Equal(btcTag, metadata[i].Tag)
		s.NotNil(metadata[i].ParentHash)
	}
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_GetBlockHashesError() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(nil, xerrors.Errorf("error making http request"))

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_BlockOutOfRange() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}

	rpcErr := &jsonrpc.RPCError{
		Code:    -8,
		Message: "Block height out of range",
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(nil, xerrors.Errorf("received rpc error (method=%v, endpoint=%v): %w", defaultGetBlockHash, "nownodes-master", rpcErr))

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
	s.True(xerrors.Is(err, internal.ErrBlockNotFound), err.Error())
	s.Contains(err.Error(), "Block height out of range")
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_GetBlocksError() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	hashes := []string{
		btcFixtureBatchGetBlockHashHash1,
		btcFixtureBatchGetBlockHashHash2,
		btcFixtureBatchGetBlockHashHash3,
	}
	getBlocksParams := make([]jsonrpc.Params, len(hashes))
	for i, hash := range hashes {
		getBlocksParams[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockByHash, getBlocksParams,
	).Return(nil, xerrors.Errorf("error getting blocks"))

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_MissingBlockHashes() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_MissingBlocks() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	hashes := []string{
		btcFixtureBatchGetBlockHashHash1,
		btcFixtureBatchGetBlockHashHash2,
		btcFixtureBatchGetBlockHashHash3,
	}
	getBlocksParams := make([]jsonrpc.Params, len(hashes))
	for i, hash := range hashes {
		getBlocksParams[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	getBlocksResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse2),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockByHash, getBlocksParams,
	).Return(getBlocksResponses, nil)

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_MissingBlockHashInMetadata() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	hashes := []string{
		btcFixtureBatchGetBlockHashHash1,
		btcFixtureBatchGetBlockHashHash2,
		btcFixtureBatchGetBlockHashHash3,
	}
	getBlocksParams := make([]jsonrpc.Params, len(hashes))
	for i, hash := range hashes {
		getBlocksParams[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	getBlocksResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse1WithoutHash),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockByHash, getBlocksParams,
	).Return(getBlocksResponses, nil)

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_MissingBlockParentHashInMetadata() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	hashes := []string{
		btcFixtureBatchGetBlockHashHash1,
		btcFixtureBatchGetBlockHashHash2,
		btcFixtureBatchGetBlockHashHash3,
	}
	getBlocksParams := make([]jsonrpc.Params, len(hashes))
	for i, hash := range hashes {
		getBlocksParams[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	getBlocksResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse1WithoutParentHash),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockByHash, getBlocksParams,
	).Return(getBlocksResponses, nil)

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_InconsistentHash() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	hashes := []string{
		btcFixtureBatchGetBlockHashHash1,
		btcFixtureBatchGetBlockHashHash2,
		btcFixtureBatchGetBlockHashHash3,
	}
	getBlocksParams := make([]jsonrpc.Params, len(hashes))
	for i, hash := range hashes {
		getBlocksParams[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	getBlocksResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse1WithInconsistentHash),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockByHash, getBlocksParams,
	).Return(getBlocksResponses, nil)

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

func (s *bitcoinClientTestSuite) TestBitcoinClient_BatchGetBlockMetadataByRange_InconsistentHeight() {
	heights := []uint64{uint64(101), uint64(102), uint64(103)}
	getBlockHashesParams := make([]jsonrpc.Params, len(heights))

	for i, height := range heights {
		getBlockHashesParams[i] = jsonrpc.Params{height}
	}
	getBlockHashesResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse1),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockHash, getBlockHashesParams,
	).Return(getBlockHashesResponses, nil)

	hashes := []string{
		btcFixtureBatchGetBlockHashHash1,
		btcFixtureBatchGetBlockHashHash2,
		btcFixtureBatchGetBlockHashHash3,
	}
	getBlocksParams := make([]jsonrpc.Params, len(hashes))
	for i, hash := range hashes {
		getBlocksParams[i] = jsonrpc.Params{
			hash,
			bitcoinBlockMetadataVerbosity,
		}
	}

	getBlocksResponses := []*jsonrpc.Response{
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse1WithInconsistentHeight),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse2),
		},
		{
			Result: json.RawMessage(btcFixtureBatchGetBlockByHashResponse3),
		},
	}

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetBlockByHash, getBlocksParams,
	).Return(getBlocksResponses, nil)

	metadata, err := s.client.BatchGetBlockMetadata(context.Background(), btcTag, 101, 104)
	s.Error(err)
	s.Nil(metadata)
}

// TestBitcoinClient_GetBlockByHash_SharedParentTxid verifies that when multiple
// inputs across different transactions reference the same parent txid (but different
// vout indices), the parent is only fetched once and each input gets the correct
// filtered vout in its output.
func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_SharedParentTxid() {
	blockHash := "00000000000000000001blockhash"
	parentTxA := "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111"
	parentTxB := "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222"

	// Block: coinbase + tx1 (input: parentA:0) + tx2 (inputs: parentA:1, parentB:0)
	blockJSON := json.RawMessage(`{
		"hash": "` + blockHash + `",
		"previousblockhash": "0000000000000000000000000000000000000000000000000000000000000000",
		"height": 100,
		"time": 1629306034,
		"tx": [
			{"txid": "coinbasetx", "vin": [{"coinbase": "00"}]},
			{"txid": "tx1", "vin": [{"txid": "` + parentTxA + `", "vout": 0}]},
			{"txid": "tx2", "vin": [{"txid": "` + parentTxA + `", "vout": 1}, {"txid": "` + parentTxB + `", "vout": 0}]}
		]
	}`)

	// parentA has 2 vouts
	parentARespJSON := json.RawMessage(`{
		"txid": "` + parentTxA + `",
		"vout": [
			{"value": 1.0, "n": 0, "scriptPubKey": {"asm": "a0", "hex": "a0", "type": "pubkeyhash", "address": "addrA0"}},
			{"value": 2.0, "n": 1, "scriptPubKey": {"asm": "a1", "hex": "a1", "type": "pubkeyhash", "address": "addrA1"}}
		]
	}`)
	// parentB has 1 vout
	parentBRespJSON := json.RawMessage(`{
		"txid": "` + parentTxB + `",
		"vout": [
			{"value": 3.0, "n": 0, "scriptPubKey": {"asm": "b0", "hex": "b0", "type": "pubkeyhash", "address": "addrB0"}}
		]
	}`)

	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{blockHash, bitcoinBlockVerbosity},
	).Return(&jsonrpc.Response{Result: blockJSON}, nil)

	// Only 2 unique parent txids should be fetched (parentA, parentB) in one batch
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction,
		[]jsonrpc.Params{{parentTxA, true}, {parentTxB, true}}, gomock.Any(),
	).Return([]*jsonrpc.Response{
		{Result: parentARespJSON},
		{Result: parentBRespJSON},
	}, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, 100, blockHash)
	s.NoError(err)

	blobdata := block.GetBitcoin()
	s.NotNil(blobdata)
	s.Equal(3, len(blobdata.InputTransactions))

	// tx[0] = coinbase -> empty
	s.Equal(0, len(blobdata.InputTransactions[0].Data))

	// tx[1] = 1 input (parentA:0) -> 1 entry with vout[0]
	s.Equal(1, len(blobdata.InputTransactions[1].Data))
	s.JSONEq(`{"txid":"`+parentTxA+`","vout":[{"value":1.0,"n":0,"scriptPubKey":{"asm":"a0","hex":"a0","reqSigs":0,"type":"pubkeyhash","addresses":null,"address":"addrA0"}}]}`,
		string(blobdata.InputTransactions[1].Data[0]))

	// tx[2] = 2 inputs (parentA:1, parentB:0)
	s.Equal(2, len(blobdata.InputTransactions[2].Data))
	s.JSONEq(`{"txid":"`+parentTxA+`","vout":[{"value":2.0,"n":1,"scriptPubKey":{"asm":"a1","hex":"a1","reqSigs":0,"type":"pubkeyhash","addresses":null,"address":"addrA1"}}]}`,
		string(blobdata.InputTransactions[2].Data[0]))
	s.JSONEq(`{"txid":"`+parentTxB+`","vout":[{"value":3.0,"n":0,"scriptPubKey":{"asm":"b0","hex":"b0","reqSigs":0,"type":"pubkeyhash","addresses":null,"address":"addrB0"}}]}`,
		string(blobdata.InputTransactions[2].Data[1]))
}

// TestBitcoinClient_GetBlockByHash_DuplicateTxidVout verifies that when two inputs
// in different transactions reference the exact same (txid, vout) pair, both get
// identical output data.
func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_DuplicateTxidVout() {
	blockHash := "00000000000000000002blockhash"
	parentTxA := "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111"

	blockJSON := json.RawMessage(`{
		"hash": "` + blockHash + `",
		"previousblockhash": "0000000000000000000000000000000000000000000000000000000000000000",
		"height": 200,
		"time": 1629306034,
		"tx": [
			{"txid": "coinbasetx", "vin": [{"coinbase": "00"}]},
			{"txid": "tx1", "vin": [{"txid": "` + parentTxA + `", "vout": 0}]},
			{"txid": "tx2", "vin": [{"txid": "` + parentTxA + `", "vout": 0}]}
		]
	}`)

	parentARespJSON := json.RawMessage(`{
		"txid": "` + parentTxA + `",
		"vout": [
			{"value": 5.0, "n": 0, "scriptPubKey": {"asm": "a0", "hex": "a0", "type": "pubkeyhash", "address": "addrA0"}}
		]
	}`)

	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{blockHash, bitcoinBlockVerbosity},
	).Return(&jsonrpc.Response{Result: blockJSON}, nil)

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction,
		[]jsonrpc.Params{{parentTxA, true}}, gomock.Any(),
	).Return([]*jsonrpc.Response{
		{Result: parentARespJSON},
	}, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, 200, blockHash)
	s.NoError(err)

	blobdata := block.GetBitcoin()
	s.Equal(3, len(blobdata.InputTransactions))
	s.Equal(0, len(blobdata.InputTransactions[0].Data))
	s.Equal(1, len(blobdata.InputTransactions[1].Data))
	s.Equal(1, len(blobdata.InputTransactions[2].Data))

	// Both should produce identical output
	s.JSONEq(string(blobdata.InputTransactions[1].Data[0]), string(blobdata.InputTransactions[2].Data[0]))
	s.JSONEq(`{"txid":"`+parentTxA+`","vout":[{"value":5.0,"n":0,"scriptPubKey":{"asm":"a0","hex":"a0","reqSigs":0,"type":"pubkeyhash","addresses":null,"address":"addrA0"}}]}`,
		string(blobdata.InputTransactions[1].Data[0]))
}

// TestBitcoinClient_GetBlockByHash_OutputShapeAndOrder verifies the strict contract:
// - results outer length == len(transactions)
// - coinbase (index 0) always has empty Data
// - each input maps to one entry with exactly one vout
// - entry order matches vin order within each transaction
func (s *bitcoinClientTestSuite) TestBitcoinClient_GetBlockByHash_OutputShapeAndOrder() {
	blockHash := "00000000000000000003blockhash"
	parentTxA := "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111"

	// tx2 has 3 inputs from the same parent, referencing vouts 2, 0, 1 (out of order)
	blockJSON := json.RawMessage(`{
		"hash": "` + blockHash + `",
		"previousblockhash": "0000000000000000000000000000000000000000000000000000000000000000",
		"height": 300,
		"time": 1629306034,
		"tx": [
			{"txid": "coinbasetx", "vin": [{"coinbase": "00"}]},
			{"txid": "tx2", "vin": [
				{"txid": "` + parentTxA + `", "vout": 2},
				{"txid": "` + parentTxA + `", "vout": 0},
				{"txid": "` + parentTxA + `", "vout": 1}
			]}
		]
	}`)

	parentARespJSON := json.RawMessage(`{
		"txid": "` + parentTxA + `",
		"vout": [
			{"value": 10.0, "n": 0, "scriptPubKey": {"asm": "v0", "hex": "v0", "type": "pubkeyhash", "address": "addr0"}},
			{"value": 20.0, "n": 1, "scriptPubKey": {"asm": "v1", "hex": "v1", "type": "pubkeyhash", "address": "addr1"}},
			{"value": 30.0, "n": 2, "scriptPubKey": {"asm": "v2", "hex": "v2", "type": "pubkeyhash", "address": "addr2"}}
		]
	}`)

	s.rpcClient.EXPECT().Call(
		gomock.Any(), defaultGetBlockByHash, jsonrpc.Params{blockHash, bitcoinBlockVerbosity},
	).Return(&jsonrpc.Response{Result: blockJSON}, nil)

	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), defaultGetRawTransaction,
		[]jsonrpc.Params{{parentTxA, true}}, gomock.Any(),
	).Return([]*jsonrpc.Response{
		{Result: parentARespJSON},
	}, nil)

	block, err := s.client.GetBlockByHash(context.Background(), btcTag, 300, blockHash)
	s.NoError(err)

	blobdata := block.GetBitcoin()
	s.Require().Equal(2, len(blobdata.InputTransactions))

	// coinbase
	s.Equal(0, len(blobdata.InputTransactions[0].Data))

	// tx2: 3 inputs, order must match vin order (vout 2, 0, 1)
	s.Require().Equal(3, len(blobdata.InputTransactions[1].Data))

	// Each entry must have exactly 1 vout
	for i, data := range blobdata.InputTransactions[1].Data {
		var parsed map[string]json.RawMessage
		s.NoError(json.Unmarshal(data, &parsed))
		var vouts []json.RawMessage
		s.NoError(json.Unmarshal(parsed["vout"], &vouts))
		s.Equal(1, len(vouts), "entry %d should have exactly 1 vout", i)
	}

	// Verify order: first entry is vout 2 (value 30), second is vout 0 (value 10), third is vout 1 (value 20)
	s.JSONEq(`{"txid":"`+parentTxA+`","vout":[{"value":30.0,"n":2,"scriptPubKey":{"asm":"v2","hex":"v2","reqSigs":0,"type":"pubkeyhash","addresses":null,"address":"addr2"}}]}`,
		string(blobdata.InputTransactions[1].Data[0]))
	s.JSONEq(`{"txid":"`+parentTxA+`","vout":[{"value":10.0,"n":0,"scriptPubKey":{"asm":"v0","hex":"v0","reqSigs":0,"type":"pubkeyhash","addresses":null,"address":"addr0"}}]}`,
		string(blobdata.InputTransactions[1].Data[1]))
	s.JSONEq(`{"txid":"`+parentTxA+`","vout":[{"value":20.0,"n":1,"scriptPubKey":{"asm":"v1","hex":"v1","reqSigs":0,"type":"pubkeyhash","addresses":null,"address":"addr1"}}]}`,
		string(blobdata.InputTransactions[1].Data[2]))
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
