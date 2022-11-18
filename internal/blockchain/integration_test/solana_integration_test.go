package integration_test

import (
	"context"
	"testing"

	"github.com/btcsuite/btcutil/base58"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

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
	solanaTag                 = uint32(1)
	solanaHeight              = uint64(100_000_000)
	solanaParentHeight        = uint64(99_999_999)
	solanaBlockTimestamp      = "2021-10-06T07:18:25Z"
	solanaBlockNumberSkipped  = uint64(28_425_585)
	solanaBlockNumberNotFound = uint64(999_999_999)
	solanaHash                = "GdY1gj7F8vq1nCy4dgCZK42WV19bkfQ4cp2e9evK18ry"
	solanaParentHash          = "7KpgQJdgXdPhzj69gCnyvyBiw9s6DZ5gmfNrhQr3XW1t"
)

type solanaTestSuite struct {
	suite.Suite

	app           testapp.TestApp
	logger        *zap.Logger
	client        client.Client
	jsonrpcClient jsonrpc.Client
	parser        parser.Parser
}

func TestIntegrationSolanaTestSuite(t *testing.T) {
	suite.Run(t, new(solanaTestSuite))
}

func (s *solanaTestSuite) SetupTest() {
	var deps struct {
		fx.In
		Client        client.Client  `name:"slave"`
		JsonrpcClient jsonrpc.Client `name:"slave"`
		Parser        parser.Parser
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)

	s.logger = s.app.Logger()
	s.client = deps.Client
	s.jsonrpcClient = deps.JsonrpcClient
	s.parser = deps.Parser
	s.Require().NotNil(s.client)
	s.Require().NotNil(s.parser)
}

func (s *solanaTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *solanaTestSuite) TestGetLatestHeight() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	height, err := s.client.GetLatestHeight(ctx)
	require.NoError(err)
	s.logger.Info("solana height", zap.Uint64("height", height))
	require.Greater(height, uint64(100_000_000))
}

func (s *solanaTestSuite) TestBatchGetBlockMetadata() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	metadatas, err := s.client.BatchGetBlockMetadata(ctx, solanaTag, solanaHeight, solanaHeight+3)
	require.NoError(err)
	s.logger.Info("solana metadatas", zap.Reflect("metadatas", metadatas))
	require.Equal(3, len(metadatas))

	metadata := metadatas[0]
	require.Equal(solanaTag, metadata.Tag)
	require.Equal(solanaHeight, metadata.Height)
	require.Equal(solanaParentHeight, metadata.ParentHeight)
	require.Equal(solanaHash, metadata.Hash)
	require.Equal(solanaParentHash, metadata.ParentHash)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(solanaBlockTimestamp), metadata.Timestamp)

	metadata = metadatas[1]
	require.Equal(solanaTag, metadata.Tag)
	require.Equal(solanaHeight+1, metadata.Height)
	require.Equal(solanaHeight, metadata.ParentHeight)
	require.False(metadata.Skipped)

	metadata = metadatas[2]
	require.Equal(solanaTag, metadata.Tag)
	require.Equal(solanaHeight+2, metadata.Height)
	require.Equal(solanaHeight+1, metadata.ParentHeight)
	require.False(metadata.Skipped)
}

func (s *solanaTestSuite) TestBatchGetBlockMetadata_Skipped() {
	const (
		startHeight = uint64(28_425_584)
		endHeight   = uint64(28_425_590)
		numBlocks   = int(endHeight - startHeight)
	)

	require := testutil.Require(s.T())

	ctx := context.Background()
	metadatas, err := s.client.BatchGetBlockMetadata(ctx, solanaTag, startHeight, endHeight)
	require.NoError(err)
	s.logger.Info("solana metadatas", zap.Reflect("metadatas", metadatas))
	require.Equal(numBlocks, len(metadatas))

	for i := 0; i < numBlocks; i++ {
		metadata := metadatas[i]
		height := startHeight + uint64(i)
		require.Equal(solanaTag, metadata.Tag)
		require.Equal(height, metadata.Height)

		isSkipped := height != startHeight && height != endHeight-1
		if isSkipped {
			require.True(metadata.Skipped)
			require.Empty(metadata.Hash)
			require.Empty(metadata.ParentHash)
		} else {
			require.False(metadata.Skipped)
			require.NotEmpty(metadata.Hash)
			require.NotEmpty(metadata.ParentHash)
		}
	}
}

func (s *solanaTestSuite) TestGetBlockByHeight_Native() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	blockTimestamp := testutil.MustTimestamp(solanaBlockTimestamp)
	numTransactions := 2888

	block, err := s.client.GetBlockByHeight(ctx, solanaTag, solanaHeight)
	require.NoError(err)
	s.logger.Info("solana block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(solanaTag, block.Metadata.Tag)
	require.Equal(solanaHeight, block.Metadata.Height)
	require.Equal(solanaParentHeight, block.Metadata.ParentHeight)
	require.Equal(solanaHash, block.Metadata.Hash)
	require.Equal(solanaParentHash, block.Metadata.ParentHash)
	require.Less(0, len(block.GetSolana().GetHeader()))
	require.False(block.Metadata.Skipped)
	require.Equal(blockTimestamp, block.Metadata.Timestamp)

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, nativeBlock.Network)
	require.Equal(solanaTag, nativeBlock.Tag)
	require.Equal(solanaHeight, nativeBlock.Height)
	require.Equal(solanaParentHeight, nativeBlock.ParentHeight)
	require.Equal(solanaHash, nativeBlock.Hash)
	require.Equal(solanaParentHash, nativeBlock.ParentHash)
	require.Equal(blockTimestamp, nativeBlock.Timestamp, nativeBlock.Timestamp.AsTime().String())
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))
	require.False(nativeBlock.Skipped)

	solanaBlock := nativeBlock.GetSolana()
	require.NotNil(solanaBlock)
	require.Equal(&api.SolanaHeader{
		BlockHash:         "GdY1gj7F8vq1nCy4dgCZK42WV19bkfQ4cp2e9evK18ry",
		PreviousBlockHash: "7KpgQJdgXdPhzj69gCnyvyBiw9s6DZ5gmfNrhQr3XW1t",
		Slot:              solanaHeight,
		ParentSlot:        solanaParentHeight,
		BlockTime:         blockTimestamp,
		BlockHeight:       89586871,
	}, solanaBlock.GetHeader())
	require.Equal(numTransactions, len(solanaBlock.GetTransactions()))

	// See https://explorer.solana.com/tx/21KCZeaBuvdwNeUqZqgzS5Pix5bTVD2hGxDmWguExuV33aA1QypbTkTAE1AHvgBZ5sBfcbj9JSJxforonQhmnWNe
	transaction := solanaBlock.GetTransactions()[0]
	require.Equal("2xRnwfAMxAvv5z2eiWC1YCR6bdcPj7ebPRTKFiFZuBHvWNc8QjM33W4Ev71T8C18g3yARJcHtMzC3VWTdASDybkU", transaction.TransactionId)
	require.Equal(&api.SolanaTransactionMeta{
		Err: "",
		Fee: 5000,
		PreBalances: []uint64{
			64706316963,
			15886107809,
			1,
			1,
			1,
		},
		PostBalances: []uint64{
			64706311963,
			15886107809,
			1,
			1,
			1,
		},
		PreTokenBalances:  []*api.SolanaTokenBalance{},
		PostTokenBalances: []*api.SolanaTokenBalance{},
		LogMessages: []string{
			"Program Vote111111111111111111111111111111111111111 invoke [1]",
			"Program Vote111111111111111111111111111111111111111 success",
		},
		Rewards: []*api.SolanaReward{},
	}, transaction.GetMeta())

	require.Equal(&api.SolanaTransactionPayload{
		Signatures: []string{"2xRnwfAMxAvv5z2eiWC1YCR6bdcPj7ebPRTKFiFZuBHvWNc8QjM33W4Ev71T8C18g3yARJcHtMzC3VWTdASDybkU"},
		Message: &api.SolanaMessage{
			Header: &api.SolanaMessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlySignedAccounts:   0,
				NumReadonlyUnsignedAccounts: 3,
			},
			Accounts: []*api.SolanaAccount{
				{
					PublicKey: "7PwCuKPmGF3ZqWHgn8zXPtsWJ7Ud2q1DFggRkzctwJnJ",
					Signer:    true,
					Writable:  true,
				},
				{
					PublicKey: "B2pPLcxHFAkrYYAEMMkpUb4QtSR46FJ5u6bYWapwW9Fj",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "SysvarS1otHashes111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				},
				{
					PublicKey: "SysvarC1ock11111111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				}, {
					PublicKey: "Vote111111111111111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				},
			},
			RecentBlockHash: "5MP52sdK3oeGihqmSKHU3xLTTcyf1MCk6yakZw2Q8VLQ",
			Instructions: []*api.SolanaInstruction{
				{
					ProgramIdIndex: 4,
					ProgramId:      "Vote111111111111111111111111111111111111111",
					Accounts:       []uint64{1, 2, 3, 0},
					AccountKeys: []string{
						"B2pPLcxHFAkrYYAEMMkpUb4QtSR46FJ5u6bYWapwW9Fj",
						"SysvarS1otHashes111111111111111111111111111",
						"SysvarC1ock11111111111111111111111111111111",
						"7PwCuKPmGF3ZqWHgn8zXPtsWJ7Ud2q1DFggRkzctwJnJ",
					},
					Data: base58.Decode("rTDbDtm67JPw9WoJj4WBK7rtYAXWYpfVXvLZiQp4VMFa6K7uMey6XXShyR5brxw9mem4MpE25ftvcNEHaVt7akAaEXnx5nMkQatPhB6w"),
				},
			},
		},
	}, transaction.GetPayload())

	// See https://explorer.solana.com/tx/s28hELYcWfbRFKScS4Ysdh8CSyhFoCM3SkxU1VS6GZcWNVxHjYyBA81QQ4WNTMMnkmk68AWLB9ZsyzxAL6Tb1fc
	transaction = solanaBlock.GetTransactions()[6]
	require.Equal("s28hELYcWfbRFKScS4Ysdh8CSyhFoCM3SkxU1VS6GZcWNVxHjYyBA81QQ4WNTMMnkmk68AWLB9ZsyzxAL6Tb1fc", transaction.TransactionId)
	require.Equal(&api.SolanaTransactionMeta{
		Err: "",
		Fee: 5000,
		PreBalances: []uint64{
			294904284599248,
			3591360,
			23357760,
			5428800,
			7299063360,
			457104960,
			457104960,
			2039280,
			2039280,
			2039280,
			1089991680,
			1,
			2039280,
			1141440,
		},
		PostBalances: []uint64{
			294904284594248,
			3591360,
			23357760,
			5428800,
			7299063360,
			457104960,
			457104960,
			2039280,
			2039280,
			2039280,
			1089991680,
			1,
			2039280,
			1141440,
		},
		PreTokenBalances: []*api.SolanaTokenBalance{
			{
				AccountIndex: 7,
				Mint:         "z3dn17yLaGMKffVogeFHQ9zWVcXgqgf3PQnDsNs2g6M",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "10510779000000",
					Decimals:       6,
					UiAmountString: "10510779",
				},
			},
			{
				AccountIndex: 8,
				Mint:         "z3dn17yLaGMKffVogeFHQ9zWVcXgqgf3PQnDsNs2g6M",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "20253297000000",
					Decimals:       6,
					UiAmountString: "20253297",
				},
			},
			{
				AccountIndex: 9,
				Mint:         "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "160658729080",
					Decimals:       6,
					UiAmountString: "160658.72908",
				},
			},
			{
				AccountIndex: 12,
				Mint:         "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "29825083642",
					Decimals:       6,
					UiAmountString: "29825.083642",
				},
			},
		},
		PostTokenBalances: []*api.SolanaTokenBalance{
			{
				AccountIndex: 7,
				Mint:         "z3dn17yLaGMKffVogeFHQ9zWVcXgqgf3PQnDsNs2g6M",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "10509537000000",
					Decimals:       6,
					UiAmountString: "10509537",
				},
			},
			{
				AccountIndex: 8,
				Mint:         "z3dn17yLaGMKffVogeFHQ9zWVcXgqgf3PQnDsNs2g6M",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "20254539000000",
					Decimals:       6,
					UiAmountString: "20254539",
				},
			},
			{
				AccountIndex: 9,
				Mint:         "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "160658729080",
					Decimals:       6,
					UiAmountString: "160658.72908",
				},
			},
			{
				AccountIndex: 12,
				Mint:         "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "29825083642",
					Decimals:       6,
					UiAmountString: "29825.083642",
				},
			},
		},
		InnerInstructions: []*api.SolanaInnerInstruction{
			{
				Index: 0,
				Instructions: []*api.SolanaInstruction{
					{
						ProgramIdIndex: 10,
						ProgramId:      "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
						Accounts:       []uint64{7, 8, 0},
						AccountKeys: []string{
							"AMD3D21NmYeeohviSpyc1TmfHU1Zz4KoHbEsMYBSasu2",
							"AhQLbtvmca4VUZBCpjEeSWwrNBTE6ZskjrFSTUqWJwDp",
							"CuieVDEDtLo7FypA9SbLM9saXFdb1dsshEkyErMqkRQq",
						},
						Data: base58.Decode("3awLk33zPqEF"),
					},
				},
			},
		},
		LogMessages: []string{
			"Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [1]",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]",
			"Program log: Instruction: Transfer",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 3121 of 186541 compute units",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success",
			"Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin consumed 17783 of 200000 compute units",
			"Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success",
		},
		Rewards: []*api.SolanaReward{},
	}, transaction.GetMeta())

	require.Equal(&api.SolanaTransactionPayload{
		Signatures: []string{"s28hELYcWfbRFKScS4Ysdh8CSyhFoCM3SkxU1VS6GZcWNVxHjYyBA81QQ4WNTMMnkmk68AWLB9ZsyzxAL6Tb1fc"},
		Message: &api.SolanaMessage{
			Header: &api.SolanaMessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlySignedAccounts:   0,
				NumReadonlyUnsignedAccounts: 4,
			},
			Accounts: []*api.SolanaAccount{
				{
					PublicKey: "CuieVDEDtLo7FypA9SbLM9saXFdb1dsshEkyErMqkRQq",
					Signer:    true,
					Writable:  true,
				},
				{
					PublicKey: "GKLev6UHeX1KSDCyo2bzyG6wqhByEzDBkmYTxEdmYJgB",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "SvQ3U4fnRNj5CyGS4hewVEcZSnEv4DpqMA1JszwaTNY",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "He1jvmXwu88eHbwkFXz22vy1WUzyV38pwxmpopDSxRwW",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "G1BY1b3qBqRjdAznMGHoti7XS6E13YQYW8kTxNStk516",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "4fVcEBb1fR6k3ssMTdRdTuaHgdstwRyiWGKYe6ALLKaw",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "E9yZnNjakLF4FUWzJAYn7P9Tsv2ag6QddgvHLvAbfbXB",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "AMD3D21NmYeeohviSpyc1TmfHU1Zz4KoHbEsMYBSasu2",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "AhQLbtvmca4VUZBCpjEeSWwrNBTE6ZskjrFSTUqWJwDp",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "D7fucyQzUwPr2JgnnR9SyV3B8n3yrjkqGGizqoX3EN3G",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
					Signer:    false,
					Writable:  false,
				},
				{
					PublicKey: "SysvarRent111111111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				},
				{
					PublicKey: "9oR7c4swDSoTz588cU3vSG7p3zqU9RCZxfAUuKKeztaD",
					Signer:    false,
					Writable:  false,
				},
				{
					PublicKey: "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
					Signer:    false,
					Writable:  false,
				},
			},
			RecentBlockHash: "JBtnR68eL5dCC7wwvKueCrL4D5suM3qNbKsunmfGtdxs",
			Instructions: []*api.SolanaInstruction{
				{
					ProgramIdIndex: uint64(13),
					ProgramId:      "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
					Accounts:       []uint64{1, 2, 3, 4, 5, 6, 7, 0, 8, 9, 10, 11, 12},
					AccountKeys: []string{
						"GKLev6UHeX1KSDCyo2bzyG6wqhByEzDBkmYTxEdmYJgB",
						"SvQ3U4fnRNj5CyGS4hewVEcZSnEv4DpqMA1JszwaTNY",
						"He1jvmXwu88eHbwkFXz22vy1WUzyV38pwxmpopDSxRwW",
						"G1BY1b3qBqRjdAznMGHoti7XS6E13YQYW8kTxNStk516",
						"4fVcEBb1fR6k3ssMTdRdTuaHgdstwRyiWGKYe6ALLKaw",
						"E9yZnNjakLF4FUWzJAYn7P9Tsv2ag6QddgvHLvAbfbXB",
						"AMD3D21NmYeeohviSpyc1TmfHU1Zz4KoHbEsMYBSasu2",
						"CuieVDEDtLo7FypA9SbLM9saXFdb1dsshEkyErMqkRQq",
						"AhQLbtvmca4VUZBCpjEeSWwrNBTE6ZskjrFSTUqWJwDp",
						"D7fucyQzUwPr2JgnnR9SyV3B8n3yrjkqGGizqoX3EN3G",
						"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
						"SysvarRent111111111111111111111111111111111",
						"9oR7c4swDSoTz588cU3vSG7p3zqU9RCZxfAUuKKeztaD",
					},
					Data: base58.Decode("189VEfQJy2YS9hmaN8A9KEn2mtV8du2qbhJEEFqn2yzArea6BFRXe1vuU8ZeqyGavG57C"),
				},
			},
		},
	}, transaction.GetPayload())

	// See https://explorer.solana.com/tx/uhHy7XE5bWFR1JpC1kYYqHpUMhBoQTkjBUCM7M4KcVAUwrxM1bx8Dpx95zVQT35XbrDJad7XJiqspaaYQS2jiHa
	transaction = solanaBlock.GetTransactions()[7]
	require.Equal("uhHy7XE5bWFR1JpC1kYYqHpUMhBoQTkjBUCM7M4KcVAUwrxM1bx8Dpx95zVQT35XbrDJad7XJiqspaaYQS2jiHa", transaction.TransactionId)
	require.Equal(&api.SolanaTransactionMeta{
		Fee: 5000,
		PreBalances: []uint64{
			664650165100,
			23357760,
			3591360,
			1825496640,
			1141440,
			1,
		},
		PostBalances: []uint64{
			664650160100,
			23357760,
			3591360,
			1825496640,
			1141440,
			1,
		},
		PreTokenBalances:  []*api.SolanaTokenBalance{},
		PostTokenBalances: []*api.SolanaTokenBalance{},
		InnerInstructions: []*api.SolanaInnerInstruction{},
		LogMessages: []string{
			"Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin invoke [1]",
			"Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin consumed 2654 of 200000 compute units",
			"Program 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin success",
			"Program 11111111111111111111111111111111 invoke [1]",
			"Program 11111111111111111111111111111111 success",
		},
		Rewards: []*api.SolanaReward{},
	}, transaction.GetMeta())

	require.Equal(&api.SolanaTransactionPayload{
		Signatures: []string{"uhHy7XE5bWFR1JpC1kYYqHpUMhBoQTkjBUCM7M4KcVAUwrxM1bx8Dpx95zVQT35XbrDJad7XJiqspaaYQS2jiHa"},
		Message: &api.SolanaMessage{
			Header: &api.SolanaMessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlySignedAccounts:   0,
				NumReadonlyUnsignedAccounts: 2,
			},
			Accounts: []*api.SolanaAccount{
				{
					PublicKey: "7ivguYMpnUBMboByJbKc7z31fJMg2pXYQ4nNPziWLchZ",
					Signer:    true,
					Writable:  true,
				},
				{
					PublicKey: "J662wqRVdQhBUm8ANJoHQf6uA99ssj7pcp4rv7VbW96H",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "8GufnKq7YnXKhnB3WNhgy5PzU9uvHbaaRrZWQK6ixPxW",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "ExbLY71YpFaAGKuHjJKXSsWLA8hf1hGLoUYHNtzvbpGJ",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
					Signer:    false,
					Writable:  false,
				},
				{
					PublicKey: "11111111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				},
			},
			RecentBlockHash: "7BYK2UTP9YP71kSLDJigvf1rrhLVw25tiMXJaTnK54G1",
			Instructions: []*api.SolanaInstruction{
				{
					ProgramIdIndex: 4,
					ProgramId:      "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
					Accounts:       []uint64{1, 2, 3, 2, 2},
					AccountKeys: []string{
						"J662wqRVdQhBUm8ANJoHQf6uA99ssj7pcp4rv7VbW96H",
						"8GufnKq7YnXKhnB3WNhgy5PzU9uvHbaaRrZWQK6ixPxW",
						"ExbLY71YpFaAGKuHjJKXSsWLA8hf1hGLoUYHNtzvbpGJ",
						"8GufnKq7YnXKhnB3WNhgy5PzU9uvHbaaRrZWQK6ixPxW",
						"8GufnKq7YnXKhnB3WNhgy5PzU9uvHbaaRrZWQK6ixPxW",
					},
					Data: base58.Decode("12VeXEUfH"),
				},
				{
					ProgramIdIndex: 5,
					ProgramId:      "11111111111111111111111111111111",
					Accounts:       []uint64{0, 0},
					AccountKeys: []string{
						"7ivguYMpnUBMboByJbKc7z31fJMg2pXYQ4nNPziWLchZ",
						"7ivguYMpnUBMboByJbKc7z31fJMg2pXYQ4nNPziWLchZ",
					},
					Data: base58.Decode("3Bxs4BkGKMXmbfNP"),
				},
			},
		},
	}, transaction.GetPayload())

	// See https://explorer.solana.com/tx/xjUw3f94FvbbGkhkokevTUn6aY4bHPcbnkF7ybe3MnM6a9DEEbkrURT5DP4JtnFJtpE7nLeX4qF27inBxXxnqi6
	transaction = solanaBlock.GetTransactions()[8]
	require.Equal("xjUw3f94FvbbGkhkokevTUn6aY4bHPcbnkF7ybe3MnM6a9DEEbkrURT5DP4JtnFJtpE7nLeX4qF27inBxXxnqi6", transaction.TransactionId)
	require.Equal(&api.SolanaTransactionMeta{
		Err: "InstructionError",
		Fee: 5000,
		PreBalances: []uint64{
			1972300160,
			26858640,
			1,
			1,
			1,
		},
		PostBalances: []uint64{
			1972295160,
			26858640,
			1,
			1,
			1,
		},
		PreTokenBalances:  []*api.SolanaTokenBalance{},
		PostTokenBalances: []*api.SolanaTokenBalance{},
		InnerInstructions: []*api.SolanaInnerInstruction{},
		LogMessages: []string{
			"Program Vote111111111111111111111111111111111111111 invoke [1]",
			"Program Vote111111111111111111111111111111111111111 failed: custom program error: 0x0",
		},
		Rewards: []*api.SolanaReward{},
	}, transaction.GetMeta())

	require.Equal(&api.SolanaTransactionPayload{
		Signatures: []string{"xjUw3f94FvbbGkhkokevTUn6aY4bHPcbnkF7ybe3MnM6a9DEEbkrURT5DP4JtnFJtpE7nLeX4qF27inBxXxnqi6"},
		Message: &api.SolanaMessage{
			Header: &api.SolanaMessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlySignedAccounts:   0,
				NumReadonlyUnsignedAccounts: 3,
			},
			Accounts: []*api.SolanaAccount{
				{
					PublicKey: "2ZjcDzwmkptGyD43siZDf4wCjM3NL7pQksmwbsKvYF1N",
					Signer:    true,
					Writable:  true,
				},
				{
					PublicKey: "E6M4cSa1fjvx1jHL3LTK16ev8qdqRPeG1rWvKdw5V7ps",
					Signer:    false,
					Writable:  true,
				},
				{
					PublicKey: "SysvarS1otHashes111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				},
				{
					PublicKey: "SysvarC1ock11111111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				},
				{
					PublicKey: "Vote111111111111111111111111111111111111111",
					Signer:    false,
					Writable:  false,
				},
			},
			RecentBlockHash: "7HyZdPQcrvsmC9VhoT3w4pm48AoByMMgMUH8HgVTdd3w",
			Instructions: []*api.SolanaInstruction{
				{
					ProgramIdIndex: 4,
					ProgramId:      "Vote111111111111111111111111111111111111111",
					Accounts:       []uint64{1, 2, 3, 0},
					AccountKeys: []string{
						"E6M4cSa1fjvx1jHL3LTK16ev8qdqRPeG1rWvKdw5V7ps",
						"SysvarS1otHashes111111111111111111111111111",
						"SysvarC1ock11111111111111111111111111111111",
						"2ZjcDzwmkptGyD43siZDf4wCjM3NL7pQksmwbsKvYF1N",
					},
					Data: base58.Decode("TxGzmQCqCug1wpnkL36ue1XGjDUL8z25jJHrbPGZGRW12oXiuLEcPFF6g7xxPhoMaxFvdw8HdF6oqQRnECPsJaceNtqauPUUYvW9sWzPYq4XeF539cwAjSJwWVykwR"),
				},
			},
		},
	}, transaction.GetPayload())

	rewards := solanaBlock.GetRewards()
	require.Equal(1, len(rewards))
	reward := rewards[0]
	require.Equal(&api.SolanaReward{
		Pubkey:             base58.Decode("DDnAqxJVFo2GVTujibHt5cjevHMSE9bo8HJaydHoshdp"),
		Lamports:           7247500,
		PostBalance:        37188329304,
		RewardType:         "Fee",
		OptionalCommission: nil,
	}, reward)
}

func (s *solanaTestSuite) TestGetBlockByHeight_Skipped() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	block, err := s.client.GetBlockByHeight(ctx, solanaTag, solanaBlockNumberSkipped)
	require.NoError(err)
	s.logger.Info("solana block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(solanaTag, block.Metadata.Tag)
	require.Equal(solanaBlockNumberSkipped, block.Metadata.Height)
	require.Equal("", block.Metadata.Hash)
	require.Equal("", block.Metadata.ParentHash)
	require.True(block.Metadata.Skipped)
	require.Equal(0, len(block.GetSolana().GetHeader()))
}

func (s *solanaTestSuite) TestGetBlockByHeight_NotFound() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	_, err := s.client.GetBlockByHeight(ctx, solanaTag, solanaBlockNumberNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
}

func (s *solanaTestSuite) TestGetBlockByHeight_TransactionErr() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	rawBlock, err := s.client.GetBlockByHeight(ctx, solanaTag, 133488056)
	require.NoError(err)

	block, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)

	// SolanaTransactionError:
	// "err": {
	//   "InstructionError": [
	//     0,
	//     {
	//       "Custom": 0
	//     }
	//   ]
	// }
	transaction := block.GetSolana().GetTransactions()[0]
	require.Equal("InstructionError", transaction.Meta.Err)

	// SolanaTransactionError:
	// "err": null
	transaction = block.GetSolana().GetTransactions()[4]
	require.Equal("", transaction.Meta.Err)

	// SolanaTransactionError:
	// "err": "InvalidRentPayingAccount"
	transaction = block.GetSolana().GetTransactions()[145]
	require.Equal("InvalidRentPayingAccount", transaction.Meta.Err)
}
