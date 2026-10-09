package ethereum

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
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	restapimocks "github.com/coinbase/chainstorage/internal/blockchain/restapi/mocks"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	tronClientTestSuite struct {
		suite.Suite

		ctrl       *gomock.Controller
		app        testapp.TestApp
		rpcClient  *jsonrpcmocks.MockClient
		restClient *restapimocks.MockClient
		client     internal.Client
	}
)

const (
	tronTestTag = uint32(2)
	// tronTestHeight = uint64(10000)
	tronTestHeight             = ethereumHeight
	fixtureBlockTxInfoResponse = `
	[
		{
			"blockNumber": 11322000,
			"contractResult": [
				""
			],
			"blockTimeStamp": 1725466323000,
			"receipt": {
				"net_usage": 268
			},
			"id": "baa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"
		},
		{
			"id": "f5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
			"blockNumber": 11322000,
			"internal_transactions": [
				{
					"hash": "27b42aff06882822a0c84211121e5f98c06a9b074ee84a085c998397b8b2da3a",
					"caller_address": "4158baea0b354f7b333b3b1563c849e979ae4e2002",
					"transferTo_address": "41eed9e56a5cddaa15ef0c42984884a8afcf1bdebb",
					"callValueInfo": [
						{}
					],
					"note": "63616c6c"
				},
				{
					"hash": "3e2b8ca208f6c899afdc74b772a4504cdd6704bbeff6d34045351c9ad83f478d",
					"caller_address": "4158baea0b354f7b333b3b1563c849e979ae4e2002",
					"transferTo_address": "41f5a6eae2fb24b0bda6288e346982fc14e094c19a",
					"callValueInfo": [
						{
							"callValue": 405000000
						}
					],
					"note": "63616c6c"
				}
			]
		}
	]
	`
	fixtureBlockTxResponse = `
	{
		"blockID": "000000000408a36cd32a8e674045e96f895b7708b85fa5141f3c8fd92eb497a8",
		"block_header": {
			"raw_data": {
				"number": 67674988,
				"txTrieRoot": "08203d3094277ae2bfc9981bde29400a87ab5bc6b9aa807ce42d96a2de5ea109",
				"witness_address": "417f5e5aca5332ce5e18414d7f85bb62097cefa453",
				"parentHash": "000000000408a36b033d241520fd155cb0351a27bce043ddb7799ec2790ca1ee",
				"version": 31,
				"timestamp": 1733675346000
			},
			"witness_signature": "241ea0cb69f7e3dd1436c03c557f2b4a005f6ca315d968a11c1115324f795f2875883db4c91e74a0638c9100a4ced4196080fdaf6077881936636e6169d0fbb801"
		},
		"transactions": [
			{
				"ret": [
					{
						"contractRet": "SUCCESS"
					}
				],
				"signature": [
					"e93160d1df484382923db34f02ca196a7dbbd7342948214a739fdf4b96896cfc2ca4d9b706c2393c3f83d269f31901aee43ae8e50a04579b6636c03c321da8e000"
				],
				"txID": "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b",
				"raw_data": {
					"contract": [
						{
							"parameter": {
								"value": {
									"amount": 6,
									"owner_address": "4174d7980a2a3e48e3a863365542e92ab8d646e0aa",
									"to_address": "41c3d34597fb01e25d4d8e7ef34ccdd0f05bf85473"
								},
								"type_url": "type.googleapis.com/protocol.TransferContract"
							},
							"type": "TransferContract"
						}
					],
					"ref_block_bytes": "a358",
					"ref_block_hash": "c773a99ac91938c6",
					"expiration": 1733675400000,
					"timestamp": 1733675342465
				},
				"raw_data_hex": "0a02a3582208c773a99ac91938c640c0f6ecb8ba325a65080112610a2d747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e747261637412300a154174d7980a2a3e48e3a863365542e92ab8d646e0aa121541c3d34597fb01e25d4d8e7ef34ccdd0f05bf8547318067081b5e9b8ba32"
			},
			{
				"ret": [
					{
						"contractRet": "SUCCESS"
					}
				],
				"signature": [
					"27a73675483a50fa52972e7c5ffe33e931ffad1a037a79c3ff2ab97cf08315b557a723d3fc87d5a1e81165775fdfba4b14c7aab31ccd74d06d36bcdd4615f18a1c"
				],
				"txID": "f5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
				"raw_data": {
					"contract": [
						{
							"parameter": {
								"value": {
									"balance": 248167143,
									"receiver_address": "414c614b77e81392450d88f9e339481843abae0e34",
									"owner_address": "410611d6c1d784930f53979f06f10306251919e1ea"
								},
								"type_url": "type.googleapis.com/protocol.UnDelegateResourceContract"
							},
							"type": "UnDelegateResourceContract"
						}
					],
					"ref_block_bytes": "a343",
					"ref_block_hash": "92d31415885cc680",
					"expiration": 1733761442497,
					"fee_limit": 30000000,
					"timestamp": 1733675342497
				},
				"raw_data_hex": "0a02a343220892d31415885cc68040c1c5f0e1ba325a72083a126e0a37747970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e556e44656c65676174655265736f75726365436f6e747261637412330a15410611d6c1d784930f53979f06f10306251919e1ea18e7f5aa762215414c614b77e81392450d88f9e339481843abae0e3470a1b5e9b8ba3290018087a70e"
			}
		]
	}
	`
)

func TestTronClientTestSuite(t *testing.T) {
	suite.Run(t, new(tronClientTestSuite))
}

func (s *tronClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)
	s.restClient = restapimocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.app = testapp.New(
		s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_TRON, common.Network_NETWORK_TRON_MAINNET),
		Module,
		// jsonrpc.Module,
		// restapi.Module,
		testTronApiModule(s.rpcClient, s.restClient),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *tronClientTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func testTronApiModule(rpcClient *jsonrpcmocks.MockClient, restClient *restapimocks.MockClient) fx.Option {
	return fx.Options(
		internal.Module,
		fx.Provide(fx.Annotated{
			Name:   "master",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "slave",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "validator",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "consensus",
			Target: func() jsonrpc.Client { return rpcClient },
		}),
		fx.Provide(fx.Annotated{
			Name:   "additional",
			Target: func() restapi.Client { return restClient },
		}),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
	)
}

func (s *tronClientTestSuite) TestTronClient_New() {
	require := testutil.Require(s.T())

	var tronClientResult TronClientParams
	var clientResutl internal.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		jsonrpc.Module,
		restapi.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_TRON, common.Network_NETWORK_TRON_MAINNET),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
		fx.Populate(&tronClientResult),
		fx.Populate(&clientResutl),
	)
	defer app.Close()

	require.NotNil(s.client)
	require.NotNil(tronClientResult.AdditionalClient)
	s.NotNil(clientResutl.Master)
	s.NotNil(clientResutl.Slave)
	s.NotNil(clientResutl.Validator)
	s.NotNil(clientResutl.Consensus)
}

func (s *tronClientTestSuite) TestTronClient_GetBlockByHeight() {
	require := testutil.Require(s.T())
	// mock block jsonrpc request --------------------
	blockResponse := &jsonrpc.Response{
		Result: json.RawMessage(fixtureBlock),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, jsonrpc.Params{
			"0xacc290",
			true,
		},
	).Return(blockResponse, nil)
	// mock TxReceipt jsonrpc request --------------------
	receiptResponse := []*jsonrpc.Response{
		{Result: json.RawMessage(fixtureReceipt)},
		{Result: json.RawMessage(fixtureReceipt)},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	// mock BlockTxInfo restapi request --------------------
	blockTxInfoPostData := tronBlockNumRequestData{Num: ethereumHeight}
	postData, _ := json.Marshal(blockTxInfoPostData)
	txr := json.RawMessage(fixtureBlockTxInfoResponse)
	s.restClient.EXPECT().Call(gomock.Any(), tronTxInfoMethod, postData).Return(txr, nil)

	// mock BlockTx restapi request --------------------
	blockTxPostData := tronBlockNumRequestData{Num: ethereumHeight}
	postData, _ = json.Marshal(blockTxPostData)
	txr = json.RawMessage(fixtureBlockTxResponse)
	s.restClient.EXPECT().Call(gomock.Any(), tronBlockTxMethod, postData).Return(txr, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), tronTestTag, tronTestHeight)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_TRON, block.Blockchain)
	require.Equal(common.Network_NETWORK_TRON_MAINNET, block.Network)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(ethereumHash, metadata.Hash)
	require.Equal(ethereumParentHash, metadata.ParentHash)
	require.Equal(ethereumHeight, metadata.Height)
	require.Equal(ethereumParentHeight, metadata.ParentHeight)
	require.Equal(tronTestTag, metadata.Tag)

	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.Equal(2, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))
	require.NotNil(blobdata.TransactionTraces[0])
	require.NotNil(blobdata.TransactionTraces[1])
	require.Nil(blobdata.Uncles)
}

// TODO: add test case for TronClient.getBlockTraces
