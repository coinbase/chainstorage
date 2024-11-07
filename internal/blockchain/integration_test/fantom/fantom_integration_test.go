package fantom_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	fantomTag        uint32 = 1
	fantomHash              = "0x000147ed00000e30baa2b4d847f755d6d624cdfd7d4a85e83c5bbb295fdfd67f"
	fantomParentHash        = "0x000147ed00000e242ceda1595be6af3248398b721eb93551c7d84e0322d4a61e"
	fantomHeight     uint64 = 0x1E154DB
)

type fantomIntegrationTestSuite struct {
	suite.Suite

	app    testapp.TestApp
	logger *zap.Logger
	client client.Client
	parser parser.Parser
}

func TestIntegrationFantomTestSuite(t *testing.T) {
	suite.Run(t, new(fantomIntegrationTestSuite))
}

func (s *fantomIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_FANTOM),
		config.WithNetwork(common.Network_NETWORK_FANTOM_MAINNET),
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
		restapi.Module,
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

func (s *fantomIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *fantomIntegrationTestSuite) TestFantomGetBlock() {
	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), fantomTag, fantomHeight, fantomHash)
			},
		},
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), fantomTag, fantomHeight)

			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.app.Logger().Info("fetching block")
			require := testutil.Require(s.T())
			rawBlock, err := test.getBlock()
			require.NoError(err)

			s.Equal(common.Blockchain_BLOCKCHAIN_FANTOM, rawBlock.Blockchain)
			s.Equal(common.Network_NETWORK_FANTOM_MAINNET, rawBlock.Network)
			s.Equal(fantomTag, rawBlock.Metadata.Tag)
			s.Equal(fantomHash, rawBlock.Metadata.Hash)
			s.Equal(fantomParentHash, rawBlock.Metadata.ParentHash)
			s.Equal(fantomHeight, rawBlock.Metadata.Height)

			nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)
			require.Equal(common.Blockchain_BLOCKCHAIN_FANTOM, nativeBlock.Blockchain)
			require.Equal(common.Network_NETWORK_FANTOM_MAINNET, nativeBlock.Network)
			require.Equal(fantomTag, nativeBlock.Tag)
			require.Equal(fantomHash, nativeBlock.Hash)
			require.Equal(fantomParentHash, nativeBlock.ParentHash)
			require.Equal(fantomHeight, nativeBlock.Height)
			require.Equal(uint64(1), nativeBlock.NumTransactions)

			block := nativeBlock.GetEthereum()
			require.NotNil(block)

			header := block.Header
			require.NotNil(header)

			require.Equal(fantomHash, header.Hash)
			require.Equal(fantomParentHash, header.ParentHash)
			require.Equal(fantomHeight, header.Number)
			require.Equal("0x3bcecde51408aa8e6793903a09f3e16d0947cc0f6c51e7a663607ab13c5575f9", header.Transactions[0])
			require.Equal("0x0000000000000000", header.Nonce)
			require.Equal("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", header.Sha3Uncles)
			require.Equal("0x00000020000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000080000000000000000001000000000000000000000008000002000000000000010000000040000000000000000000020000000000040000020800000000000000000001000010000000040180000000000000000000000000000000000000000000000000000000000810000000000000020000000000000000000000000000000000800000000000000040000002000000000000000000000000000000000000000000000000000020000010040000000000000000000000000000000000000000000000000000000010", header.LogsBloom)
			require.Equal("0x0952151d52333d319b4b3516f5e2294b004346e45b5a9d5c5d066af6f2a636bf", header.TransactionsRoot)
			require.Equal("0x489705f5017c04f105b3ec708cb28ddccccdf55cc3b2147d71336f0347d4ebab", header.StateRoot)
			require.Equal("0x6f83439ceb036ac0bc7c8f542c00467304ff46afe2131ece9fd25a2786bd2fc4", header.ReceiptsRoot)
			require.Equal("0x0000000000000000000000000000000000000000", header.Miner)
			require.Equal(uint64(0), header.Difficulty)
			require.Equal("0", header.TotalDifficulty)
			require.Equal(uint64(727), header.Size)
			require.Equal(uint64(281474976710655), header.GasLimit)
			require.Equal(uint64(114550), header.GasUsed)
			require.Empty(header.Uncles)
			require.Equal(1, len(header.Transactions))
			require.Equal("0x0000000000000000000000000000000000000000000000000000000000000000", header.MixHash)

			transactionIndex := uint64(0)
			transactionHash := "0x3bcecde51408aa8e6793903a09f3e16d0947cc0f6c51e7a663607ab13c5575f9"
			transaction := block.Transactions[transactionIndex]
			transactionFrom := "0x141bbbde978d42217ceb85d087a3b425343fa1eb"
			transactionTo := "0xbcec0e5736614d8bd05502a240526836ba0bbfc5"
			s.app.Logger().Info("transaction:", zap.Reflect("transaction", transaction))
			require.Equal(transactionHash, transaction.Hash)
			require.Equal(fantomHash, transaction.BlockHash)
			require.Equal(transactionIndex, transaction.Index)
			require.Equal(transactionFrom, transaction.From)
			require.Equal(transactionTo, transaction.To)
			require.Equal(uint64(214411), transaction.Gas)
			require.Equal(uint64(217000000000), transaction.GasPrice)
			require.Equal("0", transaction.Value)
			require.Equal(uint64(0), transaction.Type)
			require.Nil(transaction.GetOptionalMaxFeePerGas())
			require.Nil(transaction.GetOptionalMaxPriorityFeePerGas())
			require.Nil(transaction.GetOptionalTransactionAccessList())

			transactionReceipt := transaction.Receipt
			require.NotNil(transactionReceipt)
			s.app.Logger().Info("transaction receipt:", zap.Reflect("transaction_receipt", transactionReceipt))
			require.Equal(transactionHash, transactionReceipt.TransactionHash)
			require.Equal(transactionIndex, transactionReceipt.TransactionIndex)
			require.Equal(fantomHash, transactionReceipt.BlockHash)
			require.Equal(fantomHeight, transactionReceipt.BlockNumber)
			require.Equal(transactionFrom, transactionReceipt.From)
			require.Equal(transactionTo, transactionReceipt.To)
			require.Equal(uint64(114550), transactionReceipt.CumulativeGasUsed)
			require.Equal(uint64(114550), transactionReceipt.GasUsed)
			require.Equal("0x00000020000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000080000000000000000001000000000000000000000008000002000000000000010000000040000000000000000000020000000000040000020800000000000000000001000010000000040180000000000000000000000000000000000000000000000000000000000810000000000000020000000000000000000000000000000000800000000000000040000002000000000000000000000000000000000000000000000000000020000010040000000000000000000000000000000000000000000000000000000010", transactionReceipt.LogsBloom)
			require.Equal(uint64(1), transactionReceipt.GetStatus())
			require.Equal(uint64(0), transactionReceipt.Type)
			require.Equal(uint64(217000000000), transactionReceipt.EffectiveGasPrice)
			require.Equal(4, len(transactionReceipt.Logs))

			// see https://ftmscan.com/tx/0x3bcecde51408aa8e6793903a09f3e16d0947cc0f6c51e7a663607ab13c5575f9
			transaction = block.Transactions[0]
			transactionHash = transaction.Hash
			transactionFlattenedTraces := transaction.FlattenedTraces
			require.Equal(5, len(transactionFlattenedTraces))
			s.app.Logger().Info("fantom transaction flattened traces:", zap.Reflect("transaction_flattened_traces", transactionFlattenedTraces))

			traceFrom := "0x141bbbde978d42217ceb85d087a3b425343fa1eb"
			traceTo := "0xbcec0e5736614d8bd05502a240526836ba0bbfc5"
			require.Equal("call", transactionFlattenedTraces[0].Type)
			require.Equal(traceFrom, transactionFlattenedTraces[0].From)
			require.Equal(traceTo, transactionFlattenedTraces[0].To)
			require.Equal("0", transactionFlattenedTraces[0].Value)
			require.Equal(uint64(4), transactionFlattenedTraces[0].Subtraces)
			require.Equal([]uint64{}, transactionFlattenedTraces[0].TraceAddress)
			require.Equal(fantomHeight, transactionFlattenedTraces[0].BlockNumber)
			require.Equal(fantomHash, transactionFlattenedTraces[0].BlockHash)
			require.Equal(transactionHash, transactionFlattenedTraces[0].TransactionHash)
			require.Equal(uint64(0), transactionFlattenedTraces[0].TransactionIndex)
			require.Equal("call", transactionFlattenedTraces[0].CallType)
			require.Equal("call", transactionFlattenedTraces[0].TraceType)
			require.Equal("call_0x3bcecde51408aa8e6793903a09f3e16d0947cc0f6c51e7a663607ab13c5575f9", transactionFlattenedTraces[0].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[0].Status)

			// TODO: Re-enable this block validation
			// FIXME: investigate why the ethereum validator algorithm is not working in fantom.
			err = s.parser.ValidateBlock(context.Background(), nativeBlock)
			require.ErrorIs(err, parser.ErrNotImplemented)
		})
	}
}

func (s *fantomIntegrationTestSuite) TestFantomGetBlock_NotFound() {
	const (
		heightNotFound = 99_999_999
		hashNotFound   = "0x0000000000000000000000000000000000000000000000000000000000000000"
	)

	require := testutil.Require(s.T())

	_, err := s.client.GetBlockByHeight(context.Background(), fantomTag, heightNotFound)
	require.Error(err)
	require.True(errors.Is(err, client.ErrBlockNotFound), err.Error())

	_, err = s.client.GetBlockByHash(context.Background(), fantomTag, heightNotFound, hashNotFound)
	require.Error(err)
	require.True(errors.Is(err, client.ErrBlockNotFound), err.Error())
}
