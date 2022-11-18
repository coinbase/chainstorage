package integration_test

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	ethereumTag              = uint32(1)
	ethereumHeight           = uint64(11322000)
	ethereumParentHeight     = uint64(11321999)
	ethereumBlockTimestamp   = "2020-11-24T16:07:21Z"
	ethereumHeight2          = uint64(11322001)
	ethereumHeightWithUncles = uint64(11058184)
	ethereumHash             = "0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b"
	ethereumHash2            = "0x5c96afd80ff548ebf21c12e36205b17a2eb1134588f704981c3ff167b6d80ae7"
	ethereumParentHash       = "0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575"
	ethereumNumTransactions  = 218
)

func TestIntegrationEthereumGetBlock(t *testing.T) {
	require := testutil.Require(t)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return deps.Client.GetBlockByHeight(context.Background(), ethereumTag, ethereumHeight)
			},
		},
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return deps.Client.GetBlockByHash(context.Background(), ethereumTag, ethereumHeight, ethereumHash)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app.Logger().Info("fetching block")
			rawBlock, err := test.getBlock()
			require.NoError(err)

			require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, rawBlock.Blockchain)
			require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, rawBlock.Network)
			require.Equal(ethereumTag, rawBlock.Metadata.Tag)
			require.Equal(ethereumHash, rawBlock.Metadata.Hash)
			require.Equal(ethereumParentHash, rawBlock.Metadata.ParentHash)
			require.Equal(ethereumHeight, rawBlock.Metadata.Height)
			require.Equal(ethereumParentHeight, rawBlock.Metadata.ParentHeight)
			require.False(rawBlock.Metadata.Skipped)
			require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), rawBlock.Metadata.Timestamp)

			nativeBlock, err := deps.Parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)
			require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, nativeBlock.Blockchain)
			require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, nativeBlock.Network)
			require.Equal(ethereumTag, nativeBlock.Tag)
			require.Equal(ethereumHash, nativeBlock.Hash)
			require.Equal(ethereumParentHash, nativeBlock.ParentHash)
			require.Equal(ethereumHeight, nativeBlock.Height)
			require.Equal(ethereumParentHeight, nativeBlock.ParentHeight)
			require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), nativeBlock.Timestamp)
			require.Equal(uint64(218), nativeBlock.NumTransactions)
			require.False(nativeBlock.Skipped)

			block := nativeBlock.GetEthereum()
			require.NotNil(block)

			header := block.Header
			require.NotNil(header)

			// See https://etherscan.io/block/11322000
			require.Equal(ethereumHash, header.Hash)
			require.Equal(ethereumParentHash, header.ParentHash)
			require.Equal(ethereumHeight, header.Number)
			require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), header.Timestamp)
			require.Equal("0x59f3d2da6c9f68ad81c7a31d682f5541f61c573ed0ed51fc2a9325456fd54fa1", header.Transactions[217])
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", header.Transactions[5])
			require.Equal("0xc83f6d8ab7e58888", header.Nonce)
			require.Equal("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", header.Sha3Uncles)
			require.Equal("0xfdf668a334802d0164a3e3cab8f79d6bab99b9800f565fa9dea9138b2192371768c4d8f83681bb0647e07d000807499cca14bcd05d1202c180e0e2a0f2777225f4990de285aa8086d82acd2c5653c46fe8943c0e50e6521a879a5144c14f57125c064c122e730c959509992ac09588e9c648da88a6eac64805fe9132d280772abd048d16428227c6c0d8c57a460c8281e0203f8791e402cdba21c5ea0430a282a2b3a7e593a2392a2523b7961b2fd0a06752631744001311b4a9ad111d20ec7d4c2d4e02892ed5023b12126442a219ac16400e40051900d250a3b7e6adc2e13053393130810561402181040301ca492fdb24320064c43a50c42c31ea259f4820", header.LogsBloom)
			require.Equal("0x8fcfe4d75266508496020675b9c3acfdf0074bf2d177c6366b40f669306310db", header.TransactionsRoot)
			require.Equal("0xf7135b656a6513846894dad825c7a2403ee2f93ea9e3fe0e8cd846ba0df2fd7d", header.StateRoot)
			require.Equal("0x18b4e30527b17d9e1e8f0dc129c828a28a2a32b43a651b4f9302a2686f7a5963", header.ReceiptsRoot)
			require.Equal("0xd224ca0c819e8e97ba0136b3b95ceff503b79f53", header.Miner)
			require.Equal(uint64(3512743988771745), header.Difficulty)
			require.Equal("18930033225567982479580", header.TotalDifficulty)
			require.Equal("0x7575706f6f6c2e636e2d3333", header.ExtraData)
			require.Equal(uint64(47340), header.Size)
			require.Equal(uint64(12463394), header.GasLimit)
			require.Equal(uint64(12461357), header.GasUsed)
			require.Empty(header.Uncles)
			require.Nil(header.GetOptionalBaseFeePerGas())

			// See https://etherscan.io/tx/0x59f3d2da6c9f68ad81c7a31d682f5541f61c573ed0ed51fc2a9325456fd54fa1
			require.Equal(ethereumNumTransactions, len(header.Transactions))
			transaction := block.Transactions[217]
			app.Logger().Info("transaction:", zap.Reflect("transaction", transaction))
			require.Equal("0x59f3d2da6c9f68ad81c7a31d682f5541f61c573ed0ed51fc2a9325456fd54fa1", transaction.Hash)
			require.Equal(ethereumHash, transaction.BlockHash)
			require.Equal(uint64(217), transaction.Index)
			require.Equal("0x23b2eac2a34fec26d92b4fe45a1c2e5c3562ba9d", transaction.From)
			require.Equal("0xfe4fd1b564b1df69b0cc5a28950ce308c12e9bb8", transaction.To)
			require.Equal(uint64(21000), transaction.Gas)
			require.Equal(uint64(96000000000), transaction.GasPrice)
			require.Equal("6473417250000000000", transaction.Value)
			require.Equal(uint64(0), transaction.Type)
			require.Equal(testutil.MustTimestamp(ethereumBlockTimestamp), transaction.BlockTimestamp)
			require.Nil(transaction.GetOptionalMaxFeePerGas())
			require.Nil(transaction.GetOptionalMaxPriorityFeePerGas())
			require.Nil(transaction.GetOptionalTransactionAccessList())

			// See https://etherscan.io/tx/0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61
			transactionReceipt := block.Transactions[5].Receipt
			require.NotNil(transactionReceipt)
			app.Logger().Info("transaction receipt:", zap.Reflect("transaction_receipt", transactionReceipt))
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionReceipt.TransactionHash)
			require.Equal(uint64(5), transactionReceipt.TransactionIndex)
			require.Equal(ethereumHash, transactionReceipt.BlockHash)
			require.Equal(ethereumHeight, transactionReceipt.BlockNumber)
			require.Equal("0x653457a6bb51aa79593bacb8edb5fd4fcc2645e3", transactionReceipt.From)
			require.Equal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", transactionReceipt.To)
			require.Equal(uint64(371563), transactionReceipt.CumulativeGasUsed)
			require.Equal(uint64(125139), transactionReceipt.GasUsed)
			require.Equal("0x00200000000000000000000080000800000000000000000000010000000020000000000000000000000000000000001002000000080000000000000000000000000000000000000000000008100000200000000000400000000000000000000008000000004000000000000000000000000000000000040000000010000000000000000000000000004000000000000000000000000000081000004000000000000004000000000000008000000000000000000000000000000000000000000000000002000040000000020000000000000000000000001000000002000020001000200000000000000000000000000000000000000000000000000000000000", transactionReceipt.LogsBloom)
			require.Equal(uint64(1), transactionReceipt.GetStatus())
			require.Equal(uint64(0), transactionReceipt.Type)
			require.Equal(uint64(133000000000), transactionReceipt.EffectiveGasPrice)

			// See https://etherscan.io/tx/0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61#eventlog
			require.Equal(5, len(transactionReceipt.Logs))
			eventLog := transactionReceipt.Logs[0]
			app.Logger().Info("event log:", zap.Reflect("event_log", eventLog))
			require.False(eventLog.Removed)
			require.Equal(uint64(9), eventLog.LogIndex)
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", eventLog.TransactionHash)
			require.Equal(uint64(5), eventLog.TransactionIndex)
			require.Equal(ethereumHash, eventLog.BlockHash)
			require.Equal(ethereumHeight, eventLog.BlockNumber)
			require.Equal("0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", eventLog.Address)
			require.Equal("0x0000000000000000000000000000000000000000000000e0f43d5f1b27380000", eventLog.Data)
			require.Equal([]string{
				"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
				"0x000000000000000000000000653457a6bb51aa79593bacb8edb5fd4fcc2645e3",
				"0x000000000000000000000000d3d2e2692501a5c9ca623199d38826e513033a17",
			}, eventLog.Topics)

			// See https://etherscan.io/tx/0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61/advanced#internal
			transactionFlattenedTraces := block.Transactions[5].FlattenedTraces
			require.Equal(10, len(transactionFlattenedTraces))
			app.Logger().Info("transaction flattened traces:", zap.Reflect("transaction_flattened_traces", transactionFlattenedTraces))

			require.Equal("CALL", transactionFlattenedTraces[0].Type)
			require.Equal("0x653457a6bb51aa79593bacb8edb5fd4fcc2645e3", transactionFlattenedTraces[0].From)
			require.Equal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", transactionFlattenedTraces[0].To)
			require.Equal("0", transactionFlattenedTraces[0].Value)
			require.Equal(uint64(5), transactionFlattenedTraces[0].Subtraces)
			require.Equal([]uint64{}, transactionFlattenedTraces[0].TraceAddress)
			require.Equal(uint64(11322000), transactionFlattenedTraces[0].BlockNumber)
			require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", transactionFlattenedTraces[0].BlockHash)
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionFlattenedTraces[0].TransactionHash)
			require.Equal(uint64(5), transactionFlattenedTraces[0].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[0].CallType)
			require.Equal("CALL", transactionFlattenedTraces[0].TraceType)
			require.Equal("CALL_0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionFlattenedTraces[0].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[0].Status)

			require.Equal("STATICCALL", transactionFlattenedTraces[1].Type)
			require.Equal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", transactionFlattenedTraces[1].From)
			require.Equal("0xd3d2e2692501a5c9ca623199d38826e513033a17", transactionFlattenedTraces[1].To)
			require.Equal("0", transactionFlattenedTraces[1].Value)
			require.Equal(uint64(0), transactionFlattenedTraces[1].Subtraces)
			require.Equal([]uint64{0}, transactionFlattenedTraces[1].TraceAddress)
			require.Equal(uint64(11322000), transactionFlattenedTraces[1].BlockNumber)
			require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", transactionFlattenedTraces[1].BlockHash)
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionFlattenedTraces[1].TransactionHash)
			require.Equal(uint64(5), transactionFlattenedTraces[1].TransactionIndex)
			require.Equal("STATICCALL", transactionFlattenedTraces[1].CallType)
			require.Equal("CALL", transactionFlattenedTraces[1].TraceType)
			require.Equal("CALL_0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61_0", transactionFlattenedTraces[1].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[1].Status)

			require.Equal("CALL", transactionFlattenedTraces[2].Type)
			require.Equal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", transactionFlattenedTraces[2].From)
			require.Equal("0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", transactionFlattenedTraces[2].To)
			require.Equal("0", transactionFlattenedTraces[2].Value)
			require.Equal(uint64(0), transactionFlattenedTraces[2].Subtraces)
			require.Equal([]uint64{1}, transactionFlattenedTraces[2].TraceAddress)
			require.Equal(uint64(11322000), transactionFlattenedTraces[2].BlockNumber)
			require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", transactionFlattenedTraces[2].BlockHash)
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionFlattenedTraces[2].TransactionHash)
			require.Equal(uint64(5), transactionFlattenedTraces[2].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[2].CallType)
			require.Equal("CALL", transactionFlattenedTraces[2].TraceType)
			require.Equal("CALL_0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61_1", transactionFlattenedTraces[2].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[2].Status)

			require.Equal("CALL", transactionFlattenedTraces[3].Type)
			require.Equal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", transactionFlattenedTraces[3].From)
			require.Equal("0xd3d2e2692501a5c9ca623199d38826e513033a17", transactionFlattenedTraces[3].To)
			require.Equal("0", transactionFlattenedTraces[3].Value)
			require.Equal(uint64(3), transactionFlattenedTraces[3].Subtraces)
			require.Equal([]uint64{2}, transactionFlattenedTraces[3].TraceAddress)
			require.Equal(uint64(11322000), transactionFlattenedTraces[3].BlockNumber)
			require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", transactionFlattenedTraces[3].BlockHash)
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionFlattenedTraces[3].TransactionHash)
			require.Equal(uint64(5), transactionFlattenedTraces[3].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[3].CallType)
			require.Equal("CALL", transactionFlattenedTraces[3].TraceType)
			require.Equal("CALL_0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61_2", transactionFlattenedTraces[3].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[3].Status)

			require.Equal("CALL", transactionFlattenedTraces[4].Type)
			require.Equal("0xd3d2e2692501a5c9ca623199d38826e513033a17", transactionFlattenedTraces[4].From)
			require.Equal("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", transactionFlattenedTraces[4].To)
			require.Equal("0", transactionFlattenedTraces[4].Value)
			require.Equal(uint64(0), transactionFlattenedTraces[4].Subtraces)
			require.Equal([]uint64{2, 0}, transactionFlattenedTraces[4].TraceAddress)
			require.Equal(uint64(11322000), transactionFlattenedTraces[4].BlockNumber)
			require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", transactionFlattenedTraces[4].BlockHash)
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionFlattenedTraces[4].TransactionHash)
			require.Equal(uint64(5), transactionFlattenedTraces[4].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[4].CallType)
			require.Equal("CALL", transactionFlattenedTraces[4].TraceType)
			require.Equal("CALL_0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61_2_0", transactionFlattenedTraces[4].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[4].Status)

			require.Equal("CALL", transactionFlattenedTraces[8].Type)
			require.Equal("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", transactionFlattenedTraces[8].From)
			require.Equal("0x7a250d5630b4cf539739df2c5dacb4c659f2488d", transactionFlattenedTraces[8].To)
			require.Equal("27737197430711507365", transactionFlattenedTraces[8].Value)
			require.Equal(uint64(0), transactionFlattenedTraces[8].Subtraces)
			require.Equal([]uint64{3, 0}, transactionFlattenedTraces[8].TraceAddress)
			require.Equal(uint64(11322000), transactionFlattenedTraces[8].BlockNumber)
			require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", transactionFlattenedTraces[8].BlockHash)
			require.Equal("0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61", transactionFlattenedTraces[8].TransactionHash)
			require.Equal(uint64(5), transactionFlattenedTraces[8].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[8].CallType)
			require.Equal("CALL", transactionFlattenedTraces[8].TraceType)
			require.Equal("CALL_0xf00ba2651120a60f5605cf81b793b3f22cc7c6a10437ecc5b47574e017d4cb61_3_0", transactionFlattenedTraces[8].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[8].Status)

			tokenTransfers := block.Transactions[4].TokenTransfers
			require.Equal(1, len(tokenTransfers))
			tokenTransfer := tokenTransfers[0]
			app.Logger().Info("token transfer:", zap.Reflect("token_transfer", tokenTransfer))
			require.Equal("0xdac17f958d2ee523a2206206994597c13d831ec7", tokenTransfer.TokenAddress)
			require.Equal("0x4977883b02ec6e8676a9c961d1ee0b4729931f48", tokenTransfer.FromAddress)
			require.Equal("0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2", tokenTransfer.ToAddress)
			require.Equal("252822900", tokenTransfer.Value)
			require.Equal("0x6bfc9a796f90256d840f78789963f99e764a099cb4ab79ee2b2a60671435ea8b", tokenTransfer.TransactionHash)
			require.Equal(uint64(4), tokenTransfer.TransactionIndex)
			require.Equal(uint64(8), tokenTransfer.LogIndex)
			require.Equal(ethereumHash, tokenTransfer.BlockHash)
			require.Equal(ethereumHeight, tokenTransfer.BlockNumber)
		})
	}
}

func TestIntegrationEthereumGetBlock_NotFound(t *testing.T) {
	const (
		heightNotFound = 99_999_999
		hashNotFound   = "0x0000000000000000000000000000000000000000000000000000000000000000"
	)

	require := testutil.Require(t)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	_, err := deps.Client.GetBlockByHeight(context.Background(), ethereumTag, heightNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())

	_, err = deps.Client.GetBlockByHash(context.Background(), ethereumTag, heightNotFound, hashNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
}

func TestIntegrationEthereumGetBlock_Goerli(t *testing.T) {
	const (
		tag              uint32 = 0
		blockNumber      uint64 = 5000000
		blockHash               = "0x0c795e5bb06f493cdf2b95f5b0280ed0f5109b595977feaf13dfa406df4279ac"
		blockParentHash         = "0x3a8a09de2b009bb811fbc1ab4721048e92642c8930f740e38100cb5cd66eec6b"
		blockTimestamp          = "2021-06-20T04:40:02Z"
		numTransactions         = 113
		transactionIndex uint64 = 111
		transactionHash         = "0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e"
		transactionFrom         = "0x69aeda7dc886550cb6938dc4f9fa7980097be0a7"
		transactionTo           = "0xc35f8c95f5f654ea26949f17db193ab614900a25"
	)

	require := testutil.Require(t)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_GOERLI),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return deps.Client.GetBlockByHeight(context.Background(), tag, blockNumber)
			},
		},
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return deps.Client.GetBlockByHash(context.Background(), tag, blockNumber, blockHash)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app.Logger().Info("fetching block")
			rawBlock, err := test.getBlock()
			require.NoError(err)

			require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, rawBlock.Blockchain)
			require.Equal(common.Network_NETWORK_ETHEREUM_GOERLI, rawBlock.Network)
			require.Equal(tag, rawBlock.Metadata.Tag)
			require.Equal(blockHash, rawBlock.Metadata.Hash)
			require.Equal(blockParentHash, rawBlock.Metadata.ParentHash)
			require.Equal(blockNumber, rawBlock.Metadata.Height)
			require.Equal(testutil.MustTimestamp(blockTimestamp), rawBlock.Metadata.Timestamp)

			nativeBlock, err := deps.Parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)

			block := nativeBlock.GetEthereum()
			require.NotNil(block)

			header := block.Header
			require.NotNil(header)

			// See https://goerli.etherscan.io/block/5000000
			require.Equal(blockHash, header.Hash)
			require.Equal(blockParentHash, header.ParentHash)
			require.Equal(blockNumber, header.Number)
			require.Equal(testutil.MustTimestamp(blockTimestamp), header.Timestamp)
			require.Equal(transactionHash, header.Transactions[transactionIndex])
			require.Equal("0x0000000000000000", header.Nonce)
			require.Equal("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", header.Sha3Uncles)
			require.Equal("0x7601801144282072ae1ca110010860040440080205050200108f0080320100a5421206412822e054028997661040942928810182025c0a8000024148a08569609681574084401bc814c05d080102d02803c00804100485400a1100581291b4800250ba551629f0328210001808a008a281004b07005e6100004020380a00408c004246002e60044400004088044e609f0022518160d09d818502468012060000680800942a9a4440a65944290c901983028500050c82760cca08a51ca03c042000068a32120020200282002320091511aac00086020058582100014ab3886805092a0bc801282296a411020c181b14b1e8e0030001880f046480188604a51444", header.LogsBloom)
			require.Equal("0xab5f8899970b00c8d86d05e2cf18a4a3c73020bab241b6c93af96618c331f61e", header.TransactionsRoot)
			require.Equal("0xe12f0adeb0b20d4dd9daca2a6d71245e9acc0c2a3b07e000204231eaa60f581e", header.StateRoot)
			require.Equal("0x3eb0dc7a748e850812404394a56d49a3b86caabd49b8133b298802609850a805", header.ReceiptsRoot)
			require.Equal("0x0000000000000000000000000000000000000000", header.Miner)
			require.Equal(uint64(0x2), header.Difficulty)
			require.Equal("7315117", header.TotalDifficulty)
			require.Equal("0x726f6e696e2d6b61697a656e0000000000000000000000000000000000000000c62d46426b3fe1864f5a7b6a09d3d51960e5f1c8c23e6cbe64ddb9d42061c35b11525a63f92f4047211d7d8e0e1ad6cbe04aff52b55f2e5d13b4f0e6251f03c801", header.ExtraData)
			require.Equal(uint64(0x7701), header.Size)
			require.Equal(uint64(0xb745f0), header.GasLimit)
			require.Equal(uint64(0xb73cd1), header.GasUsed)
			require.Empty(header.Uncles)
			require.Nil(header.GetOptionalBaseFeePerGas())

			// See https://goerli.etherscan.io/tx/0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e
			require.Equal(numTransactions, len(header.Transactions))
			transaction := block.Transactions[transactionIndex]
			app.Logger().Info("transaction:", zap.Reflect("transaction", transaction))
			require.Equal(transactionHash, transaction.Hash)
			require.Equal(blockHash, transaction.BlockHash)
			require.Equal(transactionIndex, transaction.Index)
			require.Equal(transactionFrom, transaction.From)
			require.Equal(transactionTo, transaction.To)
			require.Equal(uint64(0x15224), transaction.Gas)
			require.Equal(uint64(0x746a528800), transaction.GasPrice)
			require.Equal("0", transaction.Value)
			require.Equal(uint64(0), transaction.Type)
			require.Nil(transaction.GetOptionalMaxFeePerGas())
			require.Nil(transaction.GetOptionalMaxPriorityFeePerGas())
			require.Nil(transaction.GetOptionalTransactionAccessList())
			require.Equal(testutil.MustTimestamp("2021-06-20T04:40:02Z"), transaction.BlockTimestamp)

			transactionReceipt := transaction.Receipt
			require.NotNil(transactionReceipt)
			app.Logger().Info("transaction receipt:", zap.Reflect("transaction_receipt", transactionReceipt))
			require.Equal(transactionHash, transactionReceipt.TransactionHash)
			require.Equal(transactionIndex, transactionReceipt.TransactionIndex)
			require.Equal(blockHash, transactionReceipt.BlockHash)
			require.Equal(blockNumber, transactionReceipt.BlockNumber)
			require.Equal(transactionFrom, transactionReceipt.From)
			require.Equal(transactionTo, transactionReceipt.To)
			require.Equal(uint64(11941670), transactionReceipt.CumulativeGasUsed)
			require.Equal(uint64(86564), transactionReceipt.GasUsed)
			require.Equal("0x40000000000000000000000000000000000000020000000000000000000000000000000000000004000000000000000000000000000000000000000000002000000000000000000000000008000000000000000000008000000000000000100002000000002000000000000000000000000000000000000000000010020000000000000000000000000000000000008000000000000000000400008000000000000000000000000000000000000000000000000000000000800800000000000000008022000000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000400000000000000", transactionReceipt.LogsBloom)
			require.Equal(uint64(1), transactionReceipt.GetStatus())
			require.Equal(uint64(0), transactionReceipt.Type)
			require.Equal(uint64(500000000000), transactionReceipt.EffectiveGasPrice)

			// See https://goerli.etherscan.io/tx/0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e#eventlog
			require.Equal(2, len(transactionReceipt.Logs))
			eventLog := transactionReceipt.Logs[0]
			app.Logger().Info("event log:", zap.Reflect("event_log", eventLog))
			require.False(eventLog.Removed)
			require.Equal(uint64(162), eventLog.LogIndex)
			require.Equal(transactionHash, eventLog.TransactionHash)
			require.Equal(transactionIndex, eventLog.TransactionIndex)
			require.Equal(blockHash, eventLog.BlockHash)
			require.Equal(blockNumber, eventLog.BlockNumber)
			require.Equal("0x2ac3c1d3e24b45c6c310534bc2dd84b5ed576335", eventLog.Address)
			require.Equal("0x0000000000000000000000000000000000000000000000000000003e660b7800", eventLog.Data)
			require.Equal([]string{
				"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
				"0x000000000000000000000000c35f8c95f5f654ea26949f17db193ab614900a25",
				"0x000000000000000000000000748198c288f78fca410060fe83c342403c308816",
			}, eventLog.Topics)

			// See https://goerli.etherscan.io/tx/0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e#internal
			transactionFlattenedTraces := transaction.FlattenedTraces
			require.Equal(4, len(transactionFlattenedTraces))
			app.Logger().Info("goerli transaction flattened traces:", zap.Reflect("transaction_flattened_traces", transactionFlattenedTraces))

			require.Equal("CALL", transactionFlattenedTraces[0].Type)
			require.Equal("0x69aeda7dc886550cb6938dc4f9fa7980097be0a7", transactionFlattenedTraces[0].From)
			require.Equal("0xc35f8c95f5f654ea26949f17db193ab614900a25", transactionFlattenedTraces[0].To)
			require.Equal("0", transactionFlattenedTraces[0].Value)
			require.Equal(uint64(3), transactionFlattenedTraces[0].Subtraces)
			require.Equal([]uint64{}, transactionFlattenedTraces[0].TraceAddress)
			require.Equal(uint64(5000000), transactionFlattenedTraces[0].BlockNumber)
			require.Equal("0x0c795e5bb06f493cdf2b95f5b0280ed0f5109b595977feaf13dfa406df4279ac", transactionFlattenedTraces[0].BlockHash)
			require.Equal("0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e", transactionFlattenedTraces[0].TransactionHash)
			require.Equal(uint64(111), transactionFlattenedTraces[0].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[0].CallType)
			require.Equal("CALL", transactionFlattenedTraces[0].TraceType)
			require.Equal("CALL_0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e", transactionFlattenedTraces[0].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[0].Status)

			require.Equal("STATICCALL", transactionFlattenedTraces[1].Type)
			require.Equal("0xc35f8c95f5f654ea26949f17db193ab614900a25", transactionFlattenedTraces[1].From)
			require.Equal("0x0000000000000000000000000000000000000001", transactionFlattenedTraces[1].To)
			require.Equal("0", transactionFlattenedTraces[1].Value)
			require.Equal(uint64(0), transactionFlattenedTraces[1].Subtraces)
			require.Equal([]uint64{0}, transactionFlattenedTraces[1].TraceAddress)
			require.Equal(uint64(5000000), transactionFlattenedTraces[1].BlockNumber)
			require.Equal("0x0c795e5bb06f493cdf2b95f5b0280ed0f5109b595977feaf13dfa406df4279ac", transactionFlattenedTraces[1].BlockHash)
			require.Equal("0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e", transactionFlattenedTraces[1].TransactionHash)
			require.Equal(uint64(111), transactionFlattenedTraces[1].TransactionIndex)
			require.Equal("STATICCALL", transactionFlattenedTraces[1].CallType)
			require.Equal("CALL", transactionFlattenedTraces[1].TraceType)
			require.Equal("CALL_0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e_0", transactionFlattenedTraces[1].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[1].Status)

			require.Equal("STATICCALL", transactionFlattenedTraces[2].Type)
			require.Equal("0xc35f8c95f5f654ea26949f17db193ab614900a25", transactionFlattenedTraces[2].From)
			require.Equal("0x2ac3c1d3e24b45c6c310534bc2dd84b5ed576335", transactionFlattenedTraces[2].To)
			require.Equal("0", transactionFlattenedTraces[2].Value)
			require.Equal(uint64(0), transactionFlattenedTraces[2].Subtraces)
			require.Equal([]uint64{1}, transactionFlattenedTraces[2].TraceAddress)
			require.Equal(uint64(5000000), transactionFlattenedTraces[2].BlockNumber)
			require.Equal("0x0c795e5bb06f493cdf2b95f5b0280ed0f5109b595977feaf13dfa406df4279ac", transactionFlattenedTraces[2].BlockHash)
			require.Equal("0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e", transactionFlattenedTraces[2].TransactionHash)
			require.Equal(uint64(111), transactionFlattenedTraces[2].TransactionIndex)
			require.Equal("STATICCALL", transactionFlattenedTraces[2].CallType)
			require.Equal("CALL", transactionFlattenedTraces[2].TraceType)
			require.Equal("CALL_0x0715ef3bf9e8ee32d4755ac6901e7d1d392627a1bedfa1f8b81dd7ba6b5b081e_1", transactionFlattenedTraces[2].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[2].Status)

			tokenTransfers := transaction.TokenTransfers
			require.Equal(1, len(tokenTransfers))
			tokenTransfer := tokenTransfers[0]
			app.Logger().Info("token transfer:", zap.Reflect("token_transfer", tokenTransfer))
			require.Equal("0x2ac3c1d3e24b45c6c310534bc2dd84b5ed576335", tokenTransfer.TokenAddress)
			require.Equal(transactionTo, tokenTransfer.FromAddress)
			require.Equal("0x748198c288f78fca410060fe83c342403c308816", tokenTransfer.ToAddress)
			require.Equal("268000000000", tokenTransfer.Value)
			require.Equal(transactionHash, tokenTransfer.TransactionHash)
			require.Equal(transactionIndex, tokenTransfer.TransactionIndex)
			require.Equal(uint64(162), tokenTransfer.LogIndex)
			require.Equal(blockHash, tokenTransfer.BlockHash)
			require.Equal(blockNumber, tokenTransfer.BlockNumber)
		})
	}
}

// related to https://github.com/ethereum/go-ethereum/issues/23552
func TestIntegrationEthereumGetBlock_Goerli_DebugTraceFailures(t *testing.T) {
	require := testutil.Require(t)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_GOERLI),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	// know issue range: 5546892 to 5546957
	for _, height := range []uint64{5546891, 5546892, 5546957, 5546958, 5546959} {
		app.Logger().Info(fmt.Sprintf("fetching block: %v", height))
		rawBlock, err := deps.Client.GetBlockByHeight(context.Background(), ethereumTag, height)
		require.NoError(err)

		require.NotNil(rawBlock)
		require.Equal(height, rawBlock.GetMetadata().GetHeight())
	}
}

func TestIntegrationEthereumGetBlockByHeight_TraceTransactions(t *testing.T) {
	const (
		blockNumberWithMaliciousTraces = 2370721

		// The following transaction belongs to the Geth DoS Attach.
		// See https://etherscan.io/tx/0xb1e822c280cd9a3f8e58abeb1ef8ddd4642787c555cdc6da54d4ce46f959daf2/
		transactionHashWithMaliciousTraces = "0x700cef0fed7263158b410d61e9f95e15e5de5d6662025a25e63733d6d385ff24"
	)

	require := testutil.Require(t)

	var result struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	app.Logger().Info("fetching block with malicious transaction", zap.Uint64("height", blockNumberWithMaliciousTraces))
	rawBlock, err := result.Client.GetBlockByHeight(context.Background(), ethereumTag, blockNumberWithMaliciousTraces)
	require.NoError(err)

	nativeBlock, err := result.Parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)

	block := nativeBlock.GetEthereum()
	require.NotNil(block)

	for _, transaction := range block.Transactions {
		require.NotEmpty(transaction.FlattenedTraces)
		if transaction.Hash == transactionHashWithMaliciousTraces {
			require.Equal(1, len(transaction.FlattenedTraces))
			require.Equal("failed to trace transaction", transaction.FlattenedTraces[0].Error, transaction.Hash)
		} else {
			require.Empty(transaction.FlattenedTraces[0].Error, transaction.Hash)
		}
	}
}

func TestIntegrationEthereumGetBlockByHeight_Uncles(t *testing.T) {
	require := testutil.Require(t)

	var result struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	app.Logger().Info("fetching block")
	rawBlock, err := result.Client.GetBlockByHeight(context.Background(), ethereumTag, ethereumHeightWithUncles)
	require.NoError(err)

	require.Equal(ethereumHeightWithUncles, rawBlock.Metadata.Height)
	require.Equal("0x00e9b8435fd8897e2adb79963efdc5d305dd00096754d51c764bdb907e4104e2", rawBlock.Metadata.Hash)
	require.Equal(2, len(rawBlock.GetEthereum().GetUncles()))

	nativeBlock, err := result.Parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)

	block := nativeBlock.GetEthereum()
	require.NotNil(block)
	require.Equal(2, len(block.Header.Uncles))
	require.Equal(2, len(block.Uncles))
	app.Logger().Info("uncles:", zap.Reflect("uncles", block.Uncles))

	require.Equal(&api.EthereumHeader{
		Hash:             "0xc3dc6f1ac3d8eefca50d71c9f513051bf3d8eadf3a2dba858a3ad127d808f087",
		ParentHash:       "0xee0595fda331a8c1e89f0beb9f56c89299262cf4dc2c4e24c30a5e1ac96c4853",
		Number:           11058178,
		Timestamp:        &timestamppb.Timestamp{Seconds: 1602735388},
		Transactions:     []string{},
		Nonce:            "0x18fbeed82954a551",
		Sha3Uncles:       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		LogsBloom:        "0x38ad79062a400a840011c9509c408c22218ba8902d098b0284c934440080610500c0cb004c000370e80282002d7009b18eb2c1128b20a10425b48001b5652c294c0002090285120268188768312a83620685c04160d50201d2003977a242221c4a80495006508e087248695208248a1822203de992ca240100c00230002e91c2ea548802c048114052e04280280c010ae507134523349c68e0300169141028904706024a8028a94a32c9848121000403a206c043a68a4c6a42942378ecb240a1555c0d12728a414e10410a981a12004e08109302018d2075084b2437117168131018260a09108817003c460208a5a020102484966116a46122409c030fe20444",
		TransactionsRoot: "0xad8dac2add1d808676441d99792acadf7c90d59a879cb5518bb971f27dbccf2c",
		StateRoot:        "0xd466aea12abef782380b9404794e8a39a6bb1fe2f9b9b9a3be1eac744e4ad896",
		ReceiptsRoot:     "0xd178cc43839cdbb185547bbbc4a5c53bcb63ec3d98ba4dd7d122bd92e7f22507",
		Miner:            "0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c",
		Difficulty:       3397167725295764,
		TotalDifficulty:  "0",
		ExtraData:        "0x6574682d70726f2d687a692d74303031",
		Size:             537,
		GasLimit:         12438919,
		GasUsed:          12431453,
		Uncles:           []string{},
	}, block.Uncles[0])

	require.Equal(&api.EthereumHeader{
		Hash:             "0x4e8a96ef0efe0303945441ea667dd09f4344cb9d853295b14aa98469712b9c8d",
		ParentHash:       "0xe1fea8c84cf8a4c830a5c174a67ff502456714a2bfd33df8f3010738ca90250a",
		Number:           11058183,
		Timestamp:        &timestamppb.Timestamp{Seconds: 1602735459},
		Transactions:     []string{},
		Nonce:            "0x4cf0c00003b088d6",
		Sha3Uncles:       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		LogsBloom:        "0x362578044748481499a5c32880239c00608536d09c9848138c17f44b02a56191ba50e021461020d0f63184e8e35281109e34a1021a073181c942421055202c4d048621218a4154267b4c970a31a89af0a524205320423a8f9212f9fc9042730cfabad50e0b3a9400df2a0144c005889c00013461508b04594fa060710524af82983489a7401942856842532408d5196ea501d3c537bcae2a8c0a48639d135a94864f008119102ae27fde88a960141a22600b26000a48393350832340aa0b0e2f369d9126250240a01341659972625a8400309308010518385801209ba09b38081618744005049033ca1b84101030482551a44c0b61501042035ad00b83a50508",
		TransactionsRoot: "0xf65c489f18559ac309ad70f92924fab1641e0bdfb2ad379dda0e82b43dce0dee",
		StateRoot:        "0xb016e9d33315902adb7c1bdc8a6b297d28cb8f2ce70cee17149f58876feac4d6",
		ReceiptsRoot:     "0x5415f3eadd30a124d3eaabf124c286ced68f1c6061068b0bd9edf4b07cb0bbeb",
		Miner:            "0x829bd824b016326a401d083b33d092293333a830",
		Difficulty:       3395508953303117,
		TotalDifficulty:  "0",
		ExtraData:        "0x7070796520e4b883e5bda9e7a59ee4bb99e9b1bc010c",
		Size:             543,
		GasLimit:         12475359,
		GasUsed:          12460994,
		Uncles:           []string{},
	}, block.Uncles[1])
}

func TestIntegrationEthereumGetLatestHeight(t *testing.T) {
	require := testutil.Require(t)

	var result struct {
		fx.In
		Client client.Client `name:"slave"`
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	height, err := result.Client.GetLatestHeight(context.Background())
	require.NoError(err)

	app.Logger().Info("latest height", zap.Uint64("height", height))
	require.Greater(height, uint64(11000000))
}

func TestIntegrationEthereumBatchGetBlockMetadata(t *testing.T) {
	require := testutil.Require(t)

	var result struct {
		fx.In
		Client client.Client `name:"slave"`
	}
	app := testapp.New(
		t,
		testapp.WithFunctional(),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	app.Logger().Info("fetching block")
	blockMetadatas, err := result.Client.BatchGetBlockMetadata(context.Background(), ethereumTag, ethereumHeight, ethereumHeight2+1)
	require.NoError(err)
	require.Equal(2, len(blockMetadatas))

	require.Equal(ethereumTag, blockMetadatas[0].Tag)
	require.Equal(ethereumHeight, blockMetadatas[0].Height)
	require.Equal(ethereumHash, blockMetadatas[0].Hash)
	require.Equal("0xb91edf64c8c47f199398050a1d18efc3b00725d866b875e340198f563a000575", blockMetadatas[0].ParentHash)
	require.Equal(ethereumTag, blockMetadatas[1].Tag)
	require.Equal(ethereumHeight2, blockMetadatas[1].Height)
	require.Equal(ethereumHash2, blockMetadatas[1].Hash)
	require.Equal("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b", blockMetadatas[1].ParentHash)
}
