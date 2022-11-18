package integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	arbitrumTag            = uint32(1)
	arbitrumBlockHash      = "0xa863dd63d894f058a8f62c51e575d0364d3c885d1e2049da96a43d19f9e29939"
	arbitrumHeight         = uint64(18413838)
	arbitrumParentHash     = "0x8918819f5e3178b82cc958c524d0e96e64493e6dd28c0f40595ff7087a9c4d15"
	arbitrumBlockTimestamp = "2022-07-24T05:13:37Z"
)

type arbitrumIntegrationTestSuite struct {
	suite.Suite

	app    testapp.TestApp
	logger *zap.Logger
	client client.Client
	parser parser.Parser
}

func TestIntegrationArbitrumTestSuite(t *testing.T) {
	suite.Run(t, new(arbitrumIntegrationTestSuite))
}

func (s *arbitrumIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_ARBITRUM),
		config.WithNetwork(common.Network_NETWORK_ARBITRUM_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		testapp.WithConfig(cfg),
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)

	s.logger = s.app.Logger()
	s.client = deps.Client
	s.parser = deps.Parser
	require.NotNil(s.client)
	require.NotNil(s.parser)
}

func (s *arbitrumIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *arbitrumIntegrationTestSuite) TestArbitrumGetBlock() {
	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), arbitrumTag, arbitrumHeight, arbitrumBlockHash)
			},
		},
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), arbitrumTag, arbitrumHeight)

			},
		},
	}

	for _, test := range tests {
		s.app.Logger().Info("fetching block")
		require := testutil.Require(s.T())
		rawBlock, err := test.getBlock()
		require.NoError(err)

		s.Equal(common.Blockchain_BLOCKCHAIN_ARBITRUM, rawBlock.Blockchain)
		s.Equal(common.Network_NETWORK_ARBITRUM_MAINNET, rawBlock.Network)
		s.Equal(arbitrumTag, rawBlock.Metadata.Tag)
		s.Equal(arbitrumBlockHash, rawBlock.Metadata.Hash)
		s.Equal(arbitrumParentHash, rawBlock.Metadata.ParentHash)
		s.Equal(arbitrumHeight, rawBlock.Metadata.Height)
		s.Equal(testutil.MustTimestamp(arbitrumBlockTimestamp), rawBlock.Metadata.Timestamp)

		nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
		require.NoError(err)
		require.Equal(common.Blockchain_BLOCKCHAIN_ARBITRUM, nativeBlock.Blockchain)
		require.Equal(common.Network_NETWORK_ARBITRUM_MAINNET, nativeBlock.Network)
		require.Equal(arbitrumTag, nativeBlock.Tag)
		require.Equal(arbitrumBlockHash, nativeBlock.Hash)
		require.Equal(arbitrumParentHash, nativeBlock.ParentHash)
		require.Equal(arbitrumHeight, nativeBlock.Height)
		require.Equal(testutil.MustTimestamp(arbitrumBlockTimestamp), nativeBlock.Timestamp)
		require.Equal(uint64(2), nativeBlock.NumTransactions)

		block := nativeBlock.GetEthereum()
		require.NotNil(block)

		header := block.Header
		require.NotNil(header)

		require.Equal(arbitrumBlockHash, header.Hash)
		require.Equal(arbitrumParentHash, header.ParentHash)
		require.Equal(arbitrumHeight, header.Number)
		require.Equal(testutil.MustTimestamp(arbitrumBlockTimestamp), header.Timestamp)
		require.Equal("0x8db6c0e7f6b46ac1b6150ed3ab4bd630e0a2c1b2b89df18b0be31ae7dc739b1f", header.Transactions[1])
		require.Equal("0x6b1882ebba5872f007aef9f405e789e202e75d1d3d8b626fe605ac3c3eb3c58f", header.Transactions[0])
		require.Equal("0x0000000000000000", header.Nonce)
		require.Equal("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", header.Sha3Uncles)
		require.Equal("0x00200000000000000010000080000000000004000000000800000000040040000000000000000000000010040000000000004000000000000000000000200000000000010000000000000008000000200020000000000000000000000000000000000100800004000000000500000000000000000000000000000010000000000010000000000000000000000000000000000000000000084000004000000000020000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000004001000000000000000000010000000000000000000000000000000000000000000000000000000000000", header.LogsBloom)
		require.Equal("0x1d0caf3a27dcfe3be0ac134df1ff9e743bc0ef4c827f42fe37873f35e64e9c4b", header.TransactionsRoot)
		require.Equal("0x0000000000000000000000000000000000000000000000000000000000000000", header.StateRoot)
		require.Equal("0xdd1739075484cac25eee07f42028a6d55cdb2198181817933cc96d3fb4b79f14", header.ReceiptsRoot)
		require.Equal("0x0000000000000000000000000000000000000000", header.Miner)
		require.Equal(uint64(0), header.Difficulty)
		require.Equal("0", header.TotalDifficulty)
		require.Equal("0x", header.ExtraData)
		require.Equal(uint64(1646), header.Size)
		require.Equal(uint64(198372961), header.GasLimit)
		require.Equal(uint64(231031), header.GasUsed)
		require.Empty(header.Uncles)
		require.Nil(header.GetOptionalBaseFeePerGas())
		require.Equal(2, len(header.Transactions))

		// see https://arbiscan.io/tx/0x6b1882ebba5872f007aef9f405e789e202e75d1d3d8b626fe605ac3c3eb3c58f
		transactionIndex := uint64(0)
		transactionHash := "0x6b1882ebba5872f007aef9f405e789e202e75d1d3d8b626fe605ac3c3eb3c58f"
		transaction := block.Transactions[transactionIndex]
		transactionFrom := "0x4a4aa276871264d841b578d1ae7e9488f468cff4"
		transactionTo := "0xc2544a32872a91f4a553b404c6950e89de901fdb"
		s.app.Logger().Info("transaction:", zap.Reflect("transaction", transaction))
		require.Equal(transactionHash, transaction.Hash)
		require.Equal(arbitrumBlockHash, transaction.BlockHash)
		require.Equal(transactionIndex, transaction.Index)
		require.Equal(transactionFrom, transaction.From)
		require.Equal(transactionTo, transaction.To)
		require.Equal(uint64(1500000), transaction.Gas)
		require.Equal(uint64(100000000000), transaction.GasPrice)
		require.Equal("0", transaction.Value)
		require.Equal(uint64(120), transaction.Type)
		require.Nil(transaction.GetOptionalMaxFeePerGas())
		require.Nil(transaction.GetOptionalMaxPriorityFeePerGas())
		require.Nil(transaction.GetOptionalTransactionAccessList())
		require.Equal(testutil.MustTimestamp(arbitrumBlockTimestamp), transaction.BlockTimestamp)

		transactionReceipt := transaction.Receipt
		require.NotNil(transactionReceipt)
		s.app.Logger().Info("transaction receipt:", zap.Reflect("transaction_receipt", transactionReceipt))
		require.Equal(transactionHash, transactionReceipt.TransactionHash)
		require.Equal(transactionIndex, transactionReceipt.TransactionIndex)
		require.Equal(arbitrumBlockHash, transactionReceipt.BlockHash)
		require.Equal(arbitrumHeight, transactionReceipt.BlockNumber)
		require.Equal(transactionFrom, transactionReceipt.From)
		require.Equal(transactionTo, transactionReceipt.To)
		require.Equal(uint64(215537), transactionReceipt.CumulativeGasUsed)
		require.Equal(uint64(864597), transactionReceipt.GasUsed)
		require.Equal("0x00200000000000000010000080000000000004000000000800000000040040000000000000000000000010040000000000004000000000000000000000200000000000010000000000000008000000200020000000000000000000000000000000000100800004000000000500000000000000000000000000000010000000000010000000000000000000000000000000000000000000084000004000000000020000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000004001000000000000000000010000000000000000000000000000000000000000000000000000000000000", transactionReceipt.LogsBloom)
		require.Equal(uint64(1), transactionReceipt.GetStatus())
		require.Equal(uint64(120), transactionReceipt.Type)
		require.Equal(uint64(117342918), transactionReceipt.EffectiveGasPrice)

		// see https://arbiscan.io/tx/0x6b1882ebba5872f007aef9f405e789e202e75d1d3d8b626fe605ac3c3eb3c58f#eventlog
		require.Equal(5, len(transactionReceipt.Logs))
		eventLog := transactionReceipt.Logs[0]
		s.app.Logger().Info("event log:", zap.Reflect("event_log", eventLog))
		require.False(eventLog.Removed)
		require.Equal(uint64(0), eventLog.LogIndex)
		require.Equal(transactionHash, eventLog.TransactionHash)
		require.Equal(transactionIndex, eventLog.TransactionIndex)
		require.Equal(arbitrumBlockHash, eventLog.BlockHash)
		require.Equal(arbitrumHeight, eventLog.BlockNumber)
		require.Equal("0x17fc002b466eec40dae837fc4be5c67993ddbd6f", eventLog.Address)
		require.Equal("0x00000000000000000000000000000000000000000000001986ec02e52390bf81", eventLog.Data)
		require.Equal([]string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000004a4aa276871264d841b578d1ae7e9488f468cff4",
			"0x000000000000000000000000b771410e2b1d892c3469711824c44769528fdc89",
		}, eventLog.Topics)

		// see https://arbiscan.io/tx/0x8db6c0e7f6b46ac1b6150ed3ab4bd630e0a2c1b2b89df18b0be31ae7dc739b1f#internal
		transaction = block.Transactions[1]
		transactionHash = transaction.Hash
		transactionFlattenedTraces := transaction.FlattenedTraces
		require.Equal(7, len(transactionFlattenedTraces))
		s.app.Logger().Info("arbitrum transaction flattened traces:", zap.Reflect("transaction_flattened_traces", transactionFlattenedTraces))

		traceFrom := "0x13fd973dcd35d851d2ea641386da69800b01eb16"
		traceTo := "0xba1bfd85432905ff4a2e7f516b56b7485dbdc5f6"
		require.Equal("call", transactionFlattenedTraces[0].Type)
		require.Equal(traceFrom, transactionFlattenedTraces[0].From)
		require.Equal(traceTo, transactionFlattenedTraces[0].To)
		require.Equal("0", transactionFlattenedTraces[0].Value)
		require.Equal(uint64(1), transactionFlattenedTraces[0].Subtraces)
		require.Equal([]uint64{}, transactionFlattenedTraces[0].TraceAddress)
		require.Equal(arbitrumHeight, transactionFlattenedTraces[0].BlockNumber)
		require.Equal(arbitrumBlockHash, transactionFlattenedTraces[0].BlockHash)
		require.Equal(transactionHash, transactionFlattenedTraces[0].TransactionHash)
		require.Equal(uint64(1), transactionFlattenedTraces[0].TransactionIndex)
		require.Equal("call", transactionFlattenedTraces[0].CallType)
		require.Equal("call", transactionFlattenedTraces[0].TraceType)
		require.Equal("call_0x8db6c0e7f6b46ac1b6150ed3ab4bd630e0a2c1b2b89df18b0be31ae7dc739b1f", transactionFlattenedTraces[0].TraceId)
		require.Equal(uint64(1), transactionFlattenedTraces[0].Status)

		for i := 2; i < len(transactionFlattenedTraces); i++ {
			require.Equal("Revert", transactionFlattenedTraces[i].Error)
		}
	}
}

// See https://arbiscan.io/block/19164551 for more details on this errored transaction
func (s *arbitrumIntegrationTestSuite) TestArbitrumGetBlock_ErrorTraces() {
	const arbitrumErrorHeight = uint64(19164551)

	require := testutil.Require(s.T())

	rawBlock, err := s.client.GetBlockByHeight(context.Background(), arbitrumTag, arbitrumErrorHeight)
	require.NoError(err)
	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)
	block := nativeBlock.GetEthereum()
	require.NotNil(block)

	transaction := block.Transactions[0]
	transactionFlattenedTraces := transaction.FlattenedTraces

	for i := 0; i < len(transactionFlattenedTraces); i++ {
		require.Equal("Revert", transactionFlattenedTraces[i].Error)
	}
}

func (s *arbitrumIntegrationTestSuite) TestArbitrumGetBlock_NotFound() {
	const (
		heightNotFound = 99_999_999
		hashNotFound   = "0x0000000000000000000000000000000000000000000000000000000000000000"
	)

	require := testutil.Require(s.T())

	_, err := s.client.GetBlockByHeight(context.Background(), arbitrumTag, heightNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())

	_, err = s.client.GetBlockByHash(context.Background(), arbitrumTag, heightNotFound, hashNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
}
