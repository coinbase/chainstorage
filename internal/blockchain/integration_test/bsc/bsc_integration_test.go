package bsc_test

import (
	"context"
	"testing"

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

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

const (
	bscTag             = uint32(1)
	bscHeight          = uint64(12419969)
	bscParentHeight    = uint64(12419968)
	bscHash            = "0xb94ccd031e76c41214e924ef21bc5aec4c1264272f6fa1c15a03ed8f29433ca5"
	bscParentHash      = "0x7641e9a1220486bdd8554fa4b24fc1afb6a0e0e9cba6d356da7d91b942595173"
	bscBlockTimestamp  = "2021-11-06T15:19:42Z"
	bscNumTransactions = 2
)

func TestIntegrationBscGetBlock(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_BSC),
		config.WithNetwork(common.Network_NETWORK_BSC_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	app := testapp.New(
		t,
		testapp.WithConfig(cfg),
		testapp.WithFunctional(),
		jsonrpc.Module,
		restapi.Module,
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
				return deps.Client.GetBlockByHeight(context.Background(), bscTag, bscHeight)
			},
		},
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return deps.Client.GetBlockByHash(context.Background(), bscTag, bscHeight, bscHash)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app.Logger().Info("fetching block")
			rawBlock, err := test.getBlock()
			require.NoError(err)

			require.Equal(common.Blockchain_BLOCKCHAIN_BSC, rawBlock.Blockchain)
			require.Equal(common.Network_NETWORK_BSC_MAINNET, rawBlock.Network)
			require.Equal(bscTag, rawBlock.Metadata.Tag)
			require.Equal(bscHash, rawBlock.Metadata.Hash)
			require.Equal(bscParentHash, rawBlock.Metadata.ParentHash)
			require.Equal(bscHeight, rawBlock.Metadata.Height)
			require.Equal(bscParentHeight, rawBlock.Metadata.ParentHeight)
			require.False(rawBlock.Metadata.Skipped)
			require.Equal(testutil.MustTimestamp(bscBlockTimestamp), rawBlock.Metadata.Timestamp)

			nativeBlock, err := deps.Parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)
			require.Equal(common.Blockchain_BLOCKCHAIN_BSC, nativeBlock.Blockchain)
			require.Equal(common.Network_NETWORK_BSC_MAINNET, nativeBlock.Network)
			require.Equal(bscTag, nativeBlock.Tag)
			require.Equal(bscHash, nativeBlock.Hash)
			require.Equal(bscParentHash, nativeBlock.ParentHash)
			require.Equal(bscHeight, nativeBlock.Height)
			require.Equal(bscParentHeight, nativeBlock.ParentHeight)
			require.Equal(testutil.MustTimestamp(bscBlockTimestamp), nativeBlock.Timestamp)
			require.Equal(uint64(2), nativeBlock.NumTransactions)
			require.False(nativeBlock.Skipped)

			block := nativeBlock.GetEthereum()
			require.NotNil(block)

			header := block.Header
			require.NotNil(header)

			// See https://www.bscscan.com/block/12419969
			app.Logger().Info("header:", zap.Reflect("header", header))
			require.Equal(bscHash, header.Hash)
			require.Equal(bscParentHash, header.ParentHash)
			require.Equal(bscHeight, header.Number)
			require.Equal(testutil.MustTimestamp(bscBlockTimestamp), header.Timestamp)
			require.Equal("0xd3035674621c53539a92f5700a386fdc262cf6a5b49bf876f459a71ee481fb0f", header.Transactions[1])
			require.Equal("0x33dd44fb8b489f7effe4a434829543e2f5eaaf24a8bac425acac4907af69b1f5", header.Transactions[0])
			require.Equal("0x0000000000000000", header.Nonce)
			require.Equal("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", header.Sha3Uncles)
			require.Equal("0x00200210000000102000000080000000000000000000000000000000000000000000000000000000010000000000000000000000000000000020000000200000000000000000040000000008000000202010000000000000800400000000080000001020000200400100000000000000000000808000000000200010200000001000000000000000000080080000000000240400000000080000004000000800020000000000000000000400020040000100000000000000008000000000000000400002020000020000000000001000000000000000001000000000000080000010000000000000010000000000000000000000000000000000000000000000", header.LogsBloom)
			require.Equal("0x8ab4f30177282819611263d5632a5b30eab9a8a9b98629d2ab70847433a1abdd", header.TransactionsRoot)
			require.Equal("0x4c48c93f5c4a61698e29a6a59e907f01c6085643c2a610f50839831d35ad5244", header.StateRoot)
			require.Equal("0x39da63c347d67388f6c78530c4e40517138b43f2034b0c1fe8d7701cf9fdda6a", header.ReceiptsRoot)
			require.Equal("0x29a97c6effb8a411dabc6adeefaa84f5067c8bbe", header.Miner)
			require.Equal(uint64(2), header.Difficulty)
			require.Equal("24710698", header.TotalDifficulty)
			require.Equal("0xd883010103846765746888676f312e31362e39856c696e7578000000fc3ca6b71b10d399072823b07603fd5d4dad7ea3d43effe92b71604d7b64c1c6390ce3bc4ff2e5d186ab64d89188317b7495e9546708ec8d06195f977b79c59970fbf70301", header.ExtraData)
			require.Equal(uint64(1164), header.Size)
			require.Equal(uint64(96790025), header.GasLimit)
			require.Equal(uint64(240050), header.GasUsed)
			require.Empty(header.Uncles)
			require.Nil(header.GetOptionalBaseFeePerGas())
			require.Equal("0x0000000000000000000000000000000000000000000000000000000000000000", header.MixHash)

			// See https://www.bscscan.com/tx/0xd3035674621c53539a92f5700a386fdc262cf6a5b49bf876f459a71ee481fb0f
			require.Equal(bscNumTransactions, len(header.Transactions))
			transaction := block.Transactions[1]
			app.Logger().Info("transaction:", zap.Reflect("transaction", transaction))
			require.Equal("0xd3035674621c53539a92f5700a386fdc262cf6a5b49bf876f459a71ee481fb0f", transaction.Hash)
			require.Equal("0xb94ccd031e76c41214e924ef21bc5aec4c1264272f6fa1c15a03ed8f29433ca5", transaction.BlockHash)
			require.Equal(uint64(1), transaction.Index)
			require.Equal("0x29a97c6effb8a411dabc6adeefaa84f5067c8bbe", transaction.From)
			require.Equal("0x0000000000000000000000000000000000001000", transaction.To)
			require.Equal(uint64(9223372036854775807), transaction.Gas)
			require.Equal(uint64(0), transaction.GasPrice)
			require.Equal("22290000000000000", transaction.Value)
			require.Equal(uint64(0), transaction.Type)
			require.Equal(testutil.MustTimestamp(bscBlockTimestamp), transaction.BlockTimestamp)
			require.Nil(transaction.GetOptionalMaxFeePerGas())
			require.Nil(transaction.GetOptionalMaxPriorityFeePerGas())
			require.Nil(transaction.GetOptionalTransactionAccessList())

			// Receipt.
			transactionReceipt := transaction.Receipt
			require.NotNil(transactionReceipt)
			app.Logger().Info("transaction receipt:", zap.Reflect("transaction_receipt", transactionReceipt))
			require.Equal("0xd3035674621c53539a92f5700a386fdc262cf6a5b49bf876f459a71ee481fb0f", transactionReceipt.TransactionHash)
			require.Equal(uint64(1), transactionReceipt.TransactionIndex)
			require.Equal("0xb94ccd031e76c41214e924ef21bc5aec4c1264272f6fa1c15a03ed8f29433ca5", transactionReceipt.BlockHash)
			require.Equal(bscHeight, transactionReceipt.BlockNumber)
			require.Equal("0x29a97c6effb8a411dabc6adeefaa84f5067c8bbe", transactionReceipt.From)
			require.Equal("0x0000000000000000000000000000000000001000", transactionReceipt.To)
			require.Equal(uint64(240050), transactionReceipt.CumulativeGasUsed)
			require.Equal(uint64(17150), transactionReceipt.GasUsed)
			require.Equal("0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000000000000000002010000000000000000000000000000000000020000200000000000000000000000000000000000000000000000000001000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000", transactionReceipt.LogsBloom)
			require.Equal(uint64(1), transactionReceipt.GetStatus())
			require.Equal(uint64(0), transactionReceipt.Type)
			require.Equal(uint64(0), transactionReceipt.EffectiveGasPrice)

			// Receipt logs.
			require.Equal(1, len(transactionReceipt.Logs))
			eventLog := transactionReceipt.Logs[0]
			app.Logger().Info("event log:", zap.Reflect("event_log", eventLog))
			require.False(eventLog.Removed)
			require.Equal(uint64(9), eventLog.LogIndex)
			require.Equal("0xd3035674621c53539a92f5700a386fdc262cf6a5b49bf876f459a71ee481fb0f", eventLog.TransactionHash)
			require.Equal(uint64(1), eventLog.TransactionIndex)
			require.Equal("0xb94ccd031e76c41214e924ef21bc5aec4c1264272f6fa1c15a03ed8f29433ca5", eventLog.BlockHash)
			require.Equal(bscHeight, eventLog.BlockNumber)
			require.Equal("0x0000000000000000000000000000000000001000", eventLog.Address)
			require.Equal("0x000000000000000000000000000000000000000000000000004f30a30c0b2000", eventLog.Data)
			require.Equal([]string{
				"0x93a090ecc682c002995fad3c85b30c5651d7fd29b0be5da9d784a3302aedc055",
				"0x00000000000000000000000029a97c6effb8a411dabc6adeefaa84f5067c8bbe",
			}, eventLog.Topics)

			// Flattened traces.
			transactionFlattenedTraces := transaction.FlattenedTraces
			require.Equal(1, len(transactionFlattenedTraces))
			app.Logger().Info("goerli transaction flattened traces:", zap.Reflect("transaction_flattened_traces", transactionFlattenedTraces))

			require.Equal("CALL", transactionFlattenedTraces[0].Type)
			require.Equal("0x29a97c6effb8a411dabc6adeefaa84f5067c8bbe", transactionFlattenedTraces[0].From)
			require.Equal("0x0000000000000000000000000000000000001000", transactionFlattenedTraces[0].To)
			require.Equal("22290000000000000", transactionFlattenedTraces[0].Value)
			require.Equal(uint64(0), transactionFlattenedTraces[0].Subtraces)
			require.Equal([]uint64{}, transactionFlattenedTraces[0].TraceAddress)
			require.Equal(bscHeight, transactionFlattenedTraces[0].BlockNumber)
			require.Equal("0xb94ccd031e76c41214e924ef21bc5aec4c1264272f6fa1c15a03ed8f29433ca5", transactionFlattenedTraces[0].BlockHash)
			require.Equal("0xd3035674621c53539a92f5700a386fdc262cf6a5b49bf876f459a71ee481fb0f", transactionFlattenedTraces[0].TransactionHash)
			require.Equal(uint64(1), transactionFlattenedTraces[0].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[0].CallType)
			require.Equal("CALL", transactionFlattenedTraces[0].TraceType)
			require.Equal("CALL_0xd3035674621c53539a92f5700a386fdc262cf6a5b49bf876f459a71ee481fb0f", transactionFlattenedTraces[0].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[0].Status)

			// Token transfers.
			tokenTransfers := block.Transactions[0].TokenTransfers
			require.Equal(4, len(tokenTransfers))
			tokenTransfer := tokenTransfers[0]
			app.Logger().Info("token transfer:", zap.Reflect("token_transfer", tokenTransfer))
			require.Equal("0x9d12cc56d133fc5c60e9385b7a92f35a682da0bd", tokenTransfer.TokenAddress)
			require.Equal("0x38d9860af557e1cbf622c6513dc6876ffbf5a965", tokenTransfer.FromAddress)
			require.Equal("0x9d12cc56d133fc5c60e9385b7a92f35a682da0bd", tokenTransfer.ToAddress)
			require.Equal("1680000000000000000000", tokenTransfer.Value)
			require.Equal("0x33dd44fb8b489f7effe4a434829543e2f5eaaf24a8bac425acac4907af69b1f5", tokenTransfer.TransactionHash)
			require.Equal(uint64(0), tokenTransfer.TransactionIndex)
			require.Equal(uint64(0), tokenTransfer.LogIndex)
			require.Equal(bscHash, tokenTransfer.BlockHash)
			require.Equal(bscHeight, tokenTransfer.BlockNumber)

			tokenTransfer = tokenTransfers[1]
			app.Logger().Info("token transfer:", zap.Reflect("token_transfer", tokenTransfer))
			require.Equal("0x9d12cc56d133fc5c60e9385b7a92f35a682da0bd", tokenTransfer.TokenAddress)
			require.Equal("0x38d9860af557e1cbf622c6513dc6876ffbf5a965", tokenTransfer.FromAddress)
			require.Equal("0x204c71820301bedf4a43ecd0c2ae299f04710afd", tokenTransfer.ToAddress)
			require.Equal("26320000000000000000000", tokenTransfer.Value)
			require.Equal("0x33dd44fb8b489f7effe4a434829543e2f5eaaf24a8bac425acac4907af69b1f5", tokenTransfer.TransactionHash)
			require.Equal(uint64(0), tokenTransfer.TransactionIndex)
			require.Equal(uint64(1), tokenTransfer.LogIndex)
			require.Equal(bscHash, tokenTransfer.BlockHash)
			require.Equal(bscHeight, tokenTransfer.BlockNumber)

			tokenTransfer = tokenTransfers[2]
			app.Logger().Info("token transfer:", zap.Reflect("token_transfer", tokenTransfer))
			require.Equal("0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", tokenTransfer.TokenAddress)
			require.Equal("0x204c71820301bedf4a43ecd0c2ae299f04710afd", tokenTransfer.FromAddress)
			require.Equal("0x58f876857a02d6762e0101bb5c46a8c1ed44dc16", tokenTransfer.ToAddress)
			require.Equal("537583199823844034", tokenTransfer.Value)
			require.Equal("0x33dd44fb8b489f7effe4a434829543e2f5eaaf24a8bac425acac4907af69b1f5", tokenTransfer.TransactionHash)
			require.Equal(uint64(0), tokenTransfer.TransactionIndex)
			require.Equal(uint64(3), tokenTransfer.LogIndex)
			require.Equal(bscHash, tokenTransfer.BlockHash)
			require.Equal(bscHeight, tokenTransfer.BlockNumber)

			tokenTransfer = tokenTransfers[3]
			app.Logger().Info("token transfer:", zap.Reflect("token_transfer", tokenTransfer))
			require.Equal("0xe9e7cea3dedca5984780bafc599bd69add087d56", tokenTransfer.TokenAddress)
			require.Equal("0x58f876857a02d6762e0101bb5c46a8c1ed44dc16", tokenTransfer.FromAddress)
			require.Equal("0x38d9860af557e1cbf622c6513dc6876ffbf5a965", tokenTransfer.ToAddress)
			require.Equal("327852074385057875345", tokenTransfer.Value)
			require.Equal("0x33dd44fb8b489f7effe4a434829543e2f5eaaf24a8bac425acac4907af69b1f5", tokenTransfer.TransactionHash)
			require.Equal(uint64(0), tokenTransfer.TransactionIndex)
			require.Equal(uint64(6), tokenTransfer.LogIndex)
			require.Equal(bscHash, tokenTransfer.BlockHash)
			require.Equal(bscHeight, tokenTransfer.BlockNumber)

			// TODO: Re-enable this block validation
			err = deps.Parser.ValidateBlock(context.Background(), nativeBlock)
			require.ErrorIs(err, parser.ErrNotImplemented)
		})
	}
}

func TestIntegrationBscGetBlock_NotFound(t *testing.T) {
	const (
		heightNotFound = 99_999_999
		hashNotFound   = "0x0000000000000000000000000000000000000000000000000000000000000000"
	)

	require := testutil.Require(t)

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_BSC),
		config.WithNetwork(common.Network_NETWORK_BSC_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	app := testapp.New(
		t,
		testapp.WithConfig(cfg),
		testapp.WithFunctional(),
		jsonrpc.Module,
		restapi.Module,
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
				return deps.Client.GetBlockByHeight(context.Background(), bscTag, heightNotFound)
			},
		},
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return deps.Client.GetBlockByHash(context.Background(), bscTag, heightNotFound, hashNotFound)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.getBlock()
			require.Error(err)
			require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
		})
	}
}
