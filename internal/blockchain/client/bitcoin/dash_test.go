package bitcoin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type dashClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	testapp   testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    internal.Client
}

func TestDashClientTestSuite(t *testing.T) {
	suite.Run(t, new(dashClientTestSuite))
}

func (s *dashClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.testapp = testapp.New(
		s.T(),
		Module,
		testModule(s.rpcClient),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_DASH, common.Network_NETWORK_DASH_MAINNET),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *dashClientTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *dashClientTestSuite) TestDashClient_GetBlockByHeight() {
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
		{Result: resp1},
		{Result: resp2},
		{Result: resp3},
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

	s.Equal(common.Blockchain_BLOCKCHAIN_DASH, block.Blockchain)
	s.Equal(common.Network_NETWORK_DASH_MAINNET, block.Network)
	s.Equal(btcTag, block.Metadata.Tag)
	s.Equal(btcFixtureBlockHash, block.Metadata.Hash)
	s.Equal(btcFixturePreviousBlockHash, block.Metadata.ParentHash)
	s.Equal(btcFixtureBlockHeight, block.Metadata.Height)
	s.Equal(btcFixtureBlockTime, block.Metadata.Timestamp.GetSeconds())

	blobdata := block.GetBitcoin()
	s.NotNil(blobdata)
	s.NotNil(blobdata.Header)

	// Verify processed input transactions (same as bitcoin)
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

	// Verify raw input transactions (Dash-specific: full raw JSON preserved)
	s.NotNil(blobdata.RawInputTransactions)
	s.Equal(3, len(blobdata.RawInputTransactions))
	// Each entry is the full getrawtransaction response keyed by txid
	s.JSONEq(string(resp1), string(blobdata.RawInputTransactions[btcFixtureInputTransactionIDs[0]]))
	s.JSONEq(string(resp2), string(blobdata.RawInputTransactions[btcFixtureInputTransactionIDs[1]]))
	s.JSONEq(string(resp3), string(blobdata.RawInputTransactions[btcFixtureInputTransactionIDs[2]]))
}

func (s *dashClientTestSuite) TestDashClient_GetGenesisBlockByHeight() {
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

	s.Equal(common.Blockchain_BLOCKCHAIN_DASH, block.Blockchain)
	s.Equal(common.Network_NETWORK_DASH_MAINNET, block.Network)

	blobdata := block.GetBitcoin()
	s.NotNil(blobdata)
	s.NotNil(blobdata.Header)
	s.Equal(1, len(blobdata.InputTransactions))
	s.Equal(0, len(blobdata.RawInputTransactions))
}

func (s *dashClientTestSuite) TestDashClient_GetBlockByHash() {
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
		{Result: resp1},
		{Result: resp2},
		{Result: resp3},
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

	s.Equal(common.Blockchain_BLOCKCHAIN_DASH, block.Blockchain)
	s.Equal(common.Network_NETWORK_DASH_MAINNET, block.Network)
	s.Equal(btcTag, block.Metadata.Tag)
	s.Equal(btcFixtureBlockHash, block.Metadata.Hash)

	blobdata := block.GetBitcoin()
	s.NotNil(blobdata)
	s.NotNil(blobdata.Header)
	s.NotNil(blobdata.InputTransactions)
	s.Equal(3, len(blobdata.InputTransactions))

	// Verify raw input transactions are preserved
	s.NotNil(blobdata.RawInputTransactions)
	s.Equal(3, len(blobdata.RawInputTransactions))
	s.JSONEq(string(resp1), string(blobdata.RawInputTransactions[btcFixtureInputTransactionIDs[0]]))
	s.JSONEq(string(resp2), string(blobdata.RawInputTransactions[btcFixtureInputTransactionIDs[1]]))
	s.JSONEq(string(resp3), string(blobdata.RawInputTransactions[btcFixtureInputTransactionIDs[2]]))
}

func (s *dashClientTestSuite) TestDashClient_GetLatestHeight() {
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
