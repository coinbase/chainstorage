package solana

import (
	"context"
	"encoding/json"
	"strconv"
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

type solanaClientTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	app       testapp.TestApp
	rpcClient *jsonrpcmocks.MockClient
	client    internal.Client
}

const (
	solanaTag           = uint32(1)
	solanaTagV2         = uint32(2)
	solanaHeight        = uint64(100_000_000)
	solanaParentHeight  = uint64(99_999_999)
	solanaHash          = "GdY1gj7F8vq1nCy4dgCZK42WV19bkfQ4cp2e9evK18ry"
	solanaParentHash    = "7KpgQJdgXdPhzj69gCnyvyBiw9s6DZ5gmfNrhQr3XW1t"
	solanaTimestamp     = "2021-10-06T07:18:25Z"
	solanaHeight1       = uint64(100_000_001)
	solanaParentHeight1 = uint64(100_000_000)
	solanaHash1         = "7WB79XesnYuqmgcqBv3LLw7R2wzHyTho3ypXyLoQpnc4"
	solanaParentHash1   = "GdY1gj7F8vq1nCy4dgCZK42WV19bkfQ4cp2e9evK18ry"
)

func TestSolanaClientTestSuite(t *testing.T) {
	suite.Run(t, new(solanaClientTestSuite))
}

func (s *solanaClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.rpcClient = jsonrpcmocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.app = testapp.New(
		s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		Module,
		testModule(s.rpcClient),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *solanaClientTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *solanaClientTestSuite) TestBatchGetBlockMetadata() {
	require := testutil.Require(s.T())

	batchResponse :=
		[]*jsonrpc.Response{
			{
				Result: fixtures.MustReadFile("client/solana/block_header.json"),
			},
			{
				Result: fixtures.MustReadFile("client/solana/block_header1.json"),
			},
		}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(),
		solanaMethodGetBlockBatchCall,
		[]jsonrpc.Params{
			{
				solanaHeight,
				solanaGetBlockLitConfiguration,
			},
			{
				solanaHeight1,
				solanaGetBlockLitConfiguration,
			},
		},
		gomock.Any(),
	).AnyTimes().Return(batchResponse, nil)

	metadatas, err := s.client.BatchGetBlockMetadata(context.Background(), solanaTag, solanaHeight, solanaHeight+2)
	require.NoError(err)
	require.Equal(2, len(metadatas))

	metadata := metadatas[0]
	require.NotNil(metadata)
	require.Equal(solanaHeight, metadata.Height)
	require.Equal(solanaParentHeight, metadata.ParentHeight)
	require.Equal(solanaHash, metadata.Hash)
	require.Equal(solanaParentHash, metadata.ParentHash)
	require.Equal(testutil.MustTimestamp(solanaTimestamp), metadata.Timestamp)

	metadata = metadatas[1]
	require.NotNil(metadata)
	require.Equal(solanaHeight1, metadata.Height)
	require.Equal(solanaParentHeight1, metadata.ParentHeight)
	require.Equal(solanaHash1, metadata.Hash)
	require.Equal(solanaParentHash1, metadata.ParentHash)
	require.Equal(testutil.MustTimestamp(solanaTimestamp), metadata.Timestamp)
}

func (s *solanaClientTestSuite) TestBatchGetBlockMetadata_Skipped() {
	require := testutil.Require(s.T())

	batchResponse := []*jsonrpc.Response{
		{
			Result: fixtures.MustReadFile("client/solana/block_header.json"),
		},
		{
			Error: &jsonrpc.RPCError{Code: solanaErrorCodeSlotSkipped},
		},
	}
	s.rpcClient.EXPECT().BatchCall(
		gomock.Any(),
		solanaMethodGetBlockBatchCall,
		[]jsonrpc.Params{
			{
				solanaHeight,
				solanaGetBlockLitConfiguration,
			},
			{
				solanaHeight1,
				solanaGetBlockLitConfiguration,
			},
		},
		gomock.Any(),
	).AnyTimes().Return(batchResponse, nil)

	metadatas, err := s.client.BatchGetBlockMetadata(context.Background(), solanaTag, solanaHeight, solanaHeight+2)
	require.NoError(err)
	require.Equal(2, len(metadatas))

	metadata := metadatas[0]
	require.NotNil(metadata)
	require.Equal(solanaHeight, metadata.Height)
	require.Equal(solanaHash, metadata.Hash)
	require.Equal(solanaParentHash, metadata.ParentHash)
	require.False(metadata.Skipped)

	metadata = metadatas[1]
	require.NotNil(metadata)
	require.Equal(solanaHeight1, metadata.Height)
	require.Empty(metadata.Hash)
	require.Empty(metadata.ParentHash)
	require.True(metadata.Skipped)
}

func (s *solanaClientTestSuite) TestBatchGetBlockMetadata_MiniBatch() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block.json"),
	}

	tests := []struct {
		blocks  int
		batches int
	}{
		{
			blocks:  solanaBatchSize - 1,
			batches: 1,
		},
		{
			blocks:  solanaBatchSize,
			batches: 1,
		},
		{
			blocks:  solanaBatchSize + 1,
			batches: 2,
		},
		{
			blocks:  solanaBatchSize*4 + 1,
			batches: 5,
		},
		{
			blocks:  solanaBatchSize * 5,
			batches: 5,
		},
		{
			blocks:  solanaBatchSize*6 - 1,
			batches: 6,
		},
	}
	for _, test := range tests {
		name := strconv.Itoa(test.blocks)
		s.Run(name, func() {
			s.rpcClient.EXPECT().BatchCall(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, method *jsonrpc.RequestMethod, batchParams []jsonrpc.Params, opts ...jsonrpc.Option) ([]*jsonrpc.Response, error) {
					for _, params := range batchParams {
						require.Equal(2, len(params))
						require.Equal(solanaGetBlockLitConfiguration, params[1])
					}

					result := make([]*jsonrpc.Response, len(batchParams))
					for i := range result {
						result[i] = blockResponse
					}
					return result, nil
				}).Times(test.batches)

			metadatas, err := s.client.BatchGetBlockMetadata(context.Background(), solanaTag, solanaHeight, solanaHeight+uint64(test.blocks))
			require.NoError(err)
			require.Equal(test.blocks, len(metadatas))
		})
	}
}

func (s *solanaClientTestSuite) TestGetBlockByHeight() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block.json"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), solanaTag, solanaHeight)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(solanaTag, block.Metadata.Tag)
	require.Equal(solanaHeight, block.Metadata.Height)
	require.Equal(solanaParentHeight, block.Metadata.ParentHeight)
	require.Equal(solanaHash, block.Metadata.Hash)
	require.Equal(solanaParentHash, block.Metadata.ParentHash)
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(solanaTimestamp), block.Metadata.Timestamp)
	require.Less(0, len(block.GetSolana().GetHeader()))

	txnMetadata := block.GetTransactionMetadata()
	require.NotNil(txnMetadata)
	transactions := txnMetadata.GetTransactions()
	require.NotNil(transactions)
	require.Equal(10, len(transactions))
	require.Equal("2xRnwfAMxAvv5z2eiWC1YCR6bdcPj7ebPRTKFiFZuBHvWNc8QjM33W4Ev71T8C18g3yARJcHtMzC3VWTdASDybkU", transactions[0])
	require.Equal("37Zd6zGdAeusPm4TQkxLh5P5jUvjmz27kgCYjx6xdF8Wh4WQG61hzkWD8uw3RcVfxk9dbWccZnUiwxJZ7oNDsuoP", transactions[3])
	require.Equal("57VvLyUYMWpLb3n63LXNWZb1KQW1n9o6r9HGYuvi5BW7Futd9BMVwChyemyMawKUbU8n3J5vSaMwBEADgYQ29GTQ", transactions[9])
}

func (s *solanaClientTestSuite) TestGetBlockByHeightV2() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block_v2.json"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfigurationV2,
		},
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), solanaTagV2, solanaHeight)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(solanaTagV2, block.Metadata.Tag)
	require.Equal(solanaHeight, block.Metadata.Height)
	require.Equal(solanaParentHeight, block.Metadata.ParentHeight)
	require.Equal(solanaHash, block.Metadata.Hash)
	require.Equal(solanaParentHash, block.Metadata.ParentHash)
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(solanaTimestamp), block.Metadata.Timestamp)
	require.Less(0, len(block.GetSolana().GetHeader()))

	txnMetadata := block.GetTransactionMetadata()
	require.NotNil(txnMetadata)
	transactions := txnMetadata.GetTransactions()
	require.NotNil(transactions)
	require.Equal(10, len(transactions))
	require.Equal("2xRnwfAMxAvv5z2eiWC1YCR6bdcPj7ebPRTKFiFZuBHvWNc8QjM33W4Ev71T8C18g3yARJcHtMzC3VWTdASDybkU", transactions[0])
	require.Equal("37Zd6zGdAeusPm4TQkxLh5P5jUvjmz27kgCYjx6xdF8Wh4WQG61hzkWD8uw3RcVfxk9dbWccZnUiwxJZ7oNDsuoP", transactions[3])
	require.Equal("57VvLyUYMWpLb3n63LXNWZb1KQW1n9o6r9HGYuvi5BW7Futd9BMVwChyemyMawKUbU8n3J5vSaMwBEADgYQ29GTQ", transactions[9])
}

func (s *solanaClientTestSuite) TestGetBlockByHeight_NotFound() {
	require := testutil.Require(s.T())

	rpcerr := &jsonrpc.RPCError{Code: solanaErrorCodeBlockNotAvailable}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(nil, rpcerr)

	_, err := s.client.GetBlockByHeight(context.Background(), solanaTag, solanaHeight)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrBlockNotFound), err.Error())
}

func (s *solanaClientTestSuite) TestGetBlockByHeight_Skipped() {
	require := testutil.Require(s.T())

	rpcerr := &jsonrpc.RPCError{Code: solanaErrorCodeLongTermStorageSlotSkipped}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(nil, rpcerr)

	block, err := s.client.GetBlockByHeight(context.Background(), solanaTag, solanaHeight)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(solanaTag, block.Metadata.Tag)
	require.Equal(solanaHeight, block.Metadata.Height)
	require.True(block.Metadata.Skipped)
	require.Empty(block.Metadata.Hash)
	require.Empty(block.Metadata.ParentHash)
	require.True(block.Metadata.Skipped)
	require.Equal(0, len(block.GetSolana().GetHeader()))
}

func (s *solanaClientTestSuite) TestGetBlockByHeight_WithoutParentHash() {
	const (
		height = uint64(1_021_085)
		hash   = "2TLDT6Z3WJ5h5958BjdzMwmNGnVo3e4qcHyGBVgBPDm9"
	)

	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block_without_parent_hash.json"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			height,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), solanaTag, height)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(solanaTag, block.Metadata.Tag)
	require.Equal(height, block.Metadata.Height)
	require.Equal(height-1, block.Metadata.ParentHeight)
	require.Equal(hash, block.Metadata.Hash)
	// Note that 11111111111111111111111111111111 is converted into an empty parent hash.
	require.Empty(block.Metadata.ParentHash)
	require.False(block.Metadata.Skipped)
	require.Less(0, len(block.GetSolana().GetHeader()))
}

func (s *solanaClientTestSuite) TestGetBlockByHash() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block.json"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), solanaTag, solanaHeight, solanaHash)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(solanaTag, block.Metadata.Tag)
	require.Equal(solanaHeight, block.Metadata.Height)
	require.Equal(solanaParentHeight, block.Metadata.ParentHeight)
	require.Equal(solanaHash, block.Metadata.Hash)
	require.Equal(solanaParentHash, block.Metadata.ParentHash)
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(solanaTimestamp), block.Metadata.Timestamp)
	require.Less(0, len(block.GetSolana().GetHeader()))
}

func (s *solanaClientTestSuite) TestGetBlockByHash_UnexpectedHash() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block.json"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	_, err := s.client.GetBlockByHash(context.Background(), solanaTag, solanaHeight, solanaParentHash)
	require.Error(err)
}

func (s *solanaClientTestSuite) TestGetLatestBlock() {
	require := testutil.Require(s.T())

	response := &jsonrpc.Response{
		Result: json.RawMessage("103724191"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetSlot,
		jsonrpc.Params{
			solanaGetSlotCommitment,
		},
		gomock.Any(),
	).AnyTimes().Return(response, nil)

	height, err := s.client.GetLatestHeight(context.Background())
	require.NoError(err)
	require.Equal(uint64(103724191), height)
}

func (s *solanaClientTestSuite) TestGetBlockByHeight_TransactionListParsing_ErrNoSignatures() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block_transaction_no_signatures.json"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	_, err := s.client.GetBlockByHeight(context.Background(), solanaTag, solanaHeight)
	require.Error(err)
	require.ErrorContains(err, "signatures are empty")
}

func (s *solanaClientTestSuite) TestGetBlockByHeight_TransactionListParsing_ErrSignatureEmptyString() {
	require := testutil.Require(s.T())

	blockResponse := &jsonrpc.Response{
		Result: fixtures.MustReadFile("client/solana/block_transaction_signatures_empty_string.json"),
	}
	s.rpcClient.EXPECT().Call(
		gomock.Any(),
		solanaMethodGetBlock,
		jsonrpc.Params{
			solanaHeight,
			solanaGetBlockConfiguration,
		},
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	_, err := s.client.GetBlockByHeight(context.Background(), solanaTag, solanaHeight)
	require.Error(err)
	require.ErrorContains(err, "transaction id is empty")
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
