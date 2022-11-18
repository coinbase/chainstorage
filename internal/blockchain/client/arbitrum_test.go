package client

import (
	"context"
	"encoding/json"
	"testing"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
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
	client    Client
}

func TestArbitrumClientTestSuite(t *testing.T) {
	suite.Run(t, new(arbitrumClientTestSuite))
}

func (s *arbitrumClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result ClientParams
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
	var result ClientParams
	app := testapp.New(
		s.T(),
		Module,
		jsonrpc.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ARBITRUM, common.Network_NETWORK_ARBITRUM_MAINNET),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	s.NotNil(result.Master)
	s.NotNil(result.Slave)
	s.NotNil(result.Validator)
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

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, WithBestEffort())
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

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 11322000, WithBestEffort())
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

	block, err := s.client.GetBlockByHeight(context.Background(), tag, 15458950, WithBestEffort())
	require.NoError(err)
	require.NotNil(block)
	blobdata := block.GetEthereum()
	require.NotNil(blobdata)
	require.NotNil(blobdata.Header)
	require.NotNil(blobdata.TransactionReceipts)
	require.Equal(1, len(blobdata.TransactionReceipts))
	require.NotNil(blobdata.TransactionTraces)
	require.Equal(2, len(blobdata.TransactionTraces))

	var parsedBlock parser.EthereumBlock
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

func (s *arbitrumClientTestSuite) TestEthereumClient_UpgradeArbitrumBlock_1_1_SizeAndTypeAssigned() {
	const (
		oldTag uint32 = 1
		newTag uint32 = 1
	)

	require := testutil.Require(s.T())

	existingBlockResult, err := fixtures.ReadFile("client/arbitrum/arb_getblockbynumber_existing.json")
	require.NoError(err)
	existingReceiptResponse, err := fixtures.ReadFile("client/arbitrum/arb_gettxnreceipt_existing.json")
	require.NoError(err)

	upgradeBlockResult, err := fixtures.ReadFile("client/arbitrum/arb_getblockbynumber_upgrade.json")
	require.NoError(err)
	upgradeReceiptResponse, err := fixtures.ReadFile("client/arbitrum/arb_gettxnreceipt_upgrade.json")
	require.NoError(err)

	blockResponse := &jsonrpc.Response{
		Result: upgradeBlockResult,
	}
	receiptResponse := []*jsonrpc.Response{
		{Result: upgradeReceiptResponse},
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(), ethGetBlockByNumberMethod, gomock.Any(),
	).Return(blockResponse, nil)
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(), ethGetTransactionReceiptMethod, gomock.Any(),
	).Return(receiptResponse, nil)

	inputBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:        oldTag,
			Height:     18413839,
			Hash:       "0x0d781f2570988488054e87aceda0d719cc9713629347a5e33228f34e52c89207",
			ParentHash: "0xa863dd63d894f058a8f62c51e575d0364d3c885d1e2049da96a43d19f9e29939",
		},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              existingBlockResult,
				TransactionReceipts: [][]byte{existingReceiptResponse},
			},
		},
	}

	block, err := s.client.UpgradeBlock(context.Background(), inputBlock, newTag)
	require.NoError(err)
	require.Equal(newTag, block.Metadata.Tag)

	expectedBlobData := block.GetEthereum()
	require.NotNil(expectedBlobData)
	existingBlobData := inputBlock.GetEthereum()
	require.NotNil(existingBlobData)

	var expectedParsedBlock parser.EthereumBlock
	err = json.Unmarshal(expectedBlobData.Header, &expectedParsedBlock)
	require.NoError(err)
	var existingParsedBlock parser.EthereumBlock
	err = json.Unmarshal(existingBlobData.Header, &existingParsedBlock)
	require.NoError(err)

	require.Equal(existingParsedBlock.Miner, expectedParsedBlock.Miner)
	require.Equal(existingParsedBlock.ParentHash, expectedParsedBlock.ParentHash)
	require.Equal(existingParsedBlock.Number, expectedParsedBlock.Number)
	require.Equal(existingParsedBlock.Difficulty, expectedParsedBlock.Difficulty)
	require.Equal(existingParsedBlock.GasUsed, expectedParsedBlock.GasUsed)
	require.Equal(existingParsedBlock.GasLimit, expectedParsedBlock.GasLimit)
	require.Equal(parser.EthereumQuantity(0x4ae), expectedParsedBlock.Size)
	require.Equal(parser.EthereumQuantity(0x78), expectedParsedBlock.Transactions[0].Type)

	var expectedParsedReceipt parser.EthereumTransactionReceipt
	err = json.Unmarshal(expectedBlobData.TransactionReceipts[0], &expectedParsedReceipt)
	require.NoError(err)
	var existingParsedReceipt parser.EthereumTransactionReceipt
	err = json.Unmarshal(existingBlobData.TransactionReceipts[0], &existingParsedReceipt)
	require.NoError(err)

	require.Equal(existingParsedReceipt.BlockNumber, expectedParsedReceipt.BlockNumber)
	require.Equal(existingParsedReceipt.GasUsed, expectedParsedReceipt.GasUsed)
	require.Equal(existingParsedReceipt.Status, expectedParsedReceipt.Status)
	require.Equal(existingParsedReceipt.TransactionHash, expectedParsedReceipt.TransactionHash)
	require.Equal(existingParsedReceipt.ContractAddress, expectedParsedReceipt.ContractAddress)
	require.Equal(existingParsedReceipt.EffectiveGasPrice, expectedParsedReceipt.EffectiveGasPrice)
	require.Equal(parser.EthereumQuantity(0x78), expectedParsedReceipt.Type)
}
