package solana_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
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
		restapi.Module,
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
	const numBlocks = 25

	require := testutil.Require(s.T())

	ctx := context.Background()
	metadatas, err := s.client.BatchGetBlockMetadata(ctx, solanaTag, solanaHeight, solanaHeight+numBlocks)
	require.NoError(err)
	s.logger.Info("solana metadatas", zap.Reflect("metadatas", metadatas))
	require.Equal(numBlocks, len(metadatas))

	metadata := metadatas[0]
	require.Equal(solanaTag, metadata.Tag)
	require.Equal(solanaHeight, metadata.Height)
	require.Equal(solanaParentHeight, metadata.ParentHeight)
	require.Equal(solanaHash, metadata.Hash)
	require.Equal(solanaParentHash, metadata.ParentHash)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(solanaBlockTimestamp), metadata.Timestamp)

	for i := uint64(1); i < numBlocks; i++ {
		metadata = metadatas[i]
		require.Equal(solanaTag, metadata.Tag)
		require.Equal(solanaHeight+i, metadata.Height)
		require.Equal(solanaHeight+i-1, metadata.ParentHeight)
		require.False(metadata.Skipped)
	}
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

func (s *solanaTestSuite) TestGetBlockByHeight_NativeV2() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	blockTimestamp := testutil.MustTimestamp("2023-05-23T16:06:58Z")
	numTransactions := 1446

	block, err := s.client.GetBlockByHeight(ctx, 2, 195545750)
	require.NoError(err)
	s.logger.Info("solana block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
	require.Equal(uint32(0x2), block.Metadata.Tag)
	require.Equal(uint64(195545750), block.Metadata.Height)
	require.Equal(uint64(195545749), block.Metadata.ParentHeight)
	require.Equal("E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd", block.Metadata.Hash)
	require.Equal("DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS", block.Metadata.ParentHash)
	require.Less(0, len(block.GetSolana().GetHeader()))
	require.False(block.Metadata.Skipped)
	require.Equal(blockTimestamp, block.Metadata.Timestamp)

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, nativeBlock.Network)
	require.Equal(uint32(0x2), nativeBlock.Tag)
	require.Equal(uint64(195545750), nativeBlock.Height)
	require.Equal(uint64(195545749), nativeBlock.ParentHeight)
	require.Equal("E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd", nativeBlock.Hash)
	require.Equal("DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS", nativeBlock.ParentHash)
	require.Equal(blockTimestamp, nativeBlock.Timestamp, nativeBlock.Timestamp.AsTime().String())
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))
	require.False(nativeBlock.Skipped)

	solanaBlock := nativeBlock.GetSolanaV2()
	require.NotNil(solanaBlock)
	require.Equal(&api.SolanaHeader{
		BlockHash:         "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd",
		PreviousBlockHash: "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
		Slot:              195545750,
		ParentSlot:        195545749,
		BlockTime:         blockTimestamp,
		BlockHeight:       178337950,
	}, solanaBlock.GetHeader())
	require.Equal(numTransactions, len(solanaBlock.GetTransactions()))

	// See https://explorer.solana.com/tx/2TpKeGQZg4f2YnsbFYJrG2x7hLNFoRFwCkbMPn7Ad2Yvv3dBhNi5DtmHyb57S1TD4m6Wrsxctsi4i1jj2BXnbGcU
	transaction := solanaBlock.GetTransactions()[0]
	require.Equal("2TpKeGQZg4f2YnsbFYJrG2x7hLNFoRFwCkbMPn7Ad2Yvv3dBhNi5DtmHyb57S1TD4m6Wrsxctsi4i1jj2BXnbGcU", transaction.TransactionId)
	require.Equal(&api.SolanaTransactionMetaV2{
		Err: "",
		Fee: 5000,
		PreBalances: []uint64{
			5355534599,
			10623757770,
			1169280,
			143487360,
			1,
		},
		PostBalances: []uint64{
			5355529599,
			10623757770,
			1169280,
			143487360,
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

	require.Equal(&api.SolanaTransactionPayloadV2{
		Signatures: []string{"2TpKeGQZg4f2YnsbFYJrG2x7hLNFoRFwCkbMPn7Ad2Yvv3dBhNi5DtmHyb57S1TD4m6Wrsxctsi4i1jj2BXnbGcU"},
		Message: &api.SolanaMessageV2{
			AccountKeys: []*api.AccountKey{
				{
					Pubkey:   "ErnUUo8z4fjEteEBmBp952x41TMLUMsFcrUmtpBiXx2e",
					Signer:   true,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "491AojvAJnRFsXMmhpxebFinaAUwC9h31DREX429ggWs",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "SysvarC1ock11111111111111111111111111111111",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "SysvarS1otHashes111111111111111111111111111",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "Vote111111111111111111111111111111111111111",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
			},
			AddressTableLookups: nil,
			Instructions: []*api.SolanaInstructionV2{
				{
					ProgramId: "Vote111111111111111111111111111111111111111",
					Program:   api.SolanaProgram_VOTE,
					ProgramData: &api.SolanaInstructionV2_VoteProgram{
						VoteProgram: &api.SolanaVoteProgram{
							InstructionType: api.SolanaVoteProgram_VOTE,
							Instruction: &api.SolanaVoteProgram_Vote{
								Vote: &api.SolanaVoteVoteInstruction{
									VoteAccount:      "491AojvAJnRFsXMmhpxebFinaAUwC9h31DREX429ggWs",
									SlotHashesSysvar: "SysvarS1otHashes111111111111111111111111111",
									ClockSysvar:      "SysvarC1ock11111111111111111111111111111111",
									VoteAuthority:    "ErnUUo8z4fjEteEBmBp952x41TMLUMsFcrUmtpBiXx2e",
									Vote: &api.SolanaVoteVoteInstruction_Vote{
										Slots: []uint64{195545748},
										Hash:  "3AR1etHpWwTraxBBCumNiYqvGC67bu7Qb4mM2QT2efaR",
										Timestamp: &timestamp.Timestamp{
											Seconds: 1684858017,
										},
									},
								},
							},
						},
					},
				},
			},
			RecentBlockHash: "5NC3cZiTsuYjxz1ApSaA5XCZu8taC2PW8hCiTdyQLUcU",
		},
	}, transaction.GetPayload())

	// See https://explorer.solana.com/tx/59AZH71gLaQnUuoBbz3TWbswNJWnptH4KwN49uJR5yQQuG7gNqK9CGH2MieCBMC2to4NsxM7db4h79mQVZDY27Gz
	transaction = solanaBlock.GetTransactions()[42]
	require.Equal("59AZH71gLaQnUuoBbz3TWbswNJWnptH4KwN49uJR5yQQuG7gNqK9CGH2MieCBMC2to4NsxM7db4h79mQVZDY27Gz", transaction.TransactionId)
	require.Equal(&api.SolanaTransactionMetaV2{
		Err: "",
		Fee: 5001,
		InnerInstructions: []*api.SolanaInnerInstructionV2{
			{
				Index: 2,
				Instructions: []*api.SolanaInstructionV2{
					{
						Program:   api.SolanaProgram_SPL_TOKEN,
						ProgramId: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
						ProgramData: &api.SolanaInstructionV2_SplTokenProgram{
							SplTokenProgram: &api.SolanaSplTokenProgram{
								InstructionType: api.SolanaSplTokenProgram_TRANSFER,
								Instruction: &api.SolanaSplTokenProgram_Transfer{
									Transfer: &api.SolanaSplTokenTransferInstruction{
										Source:      "Gg1HXc1DXpN4HkzWmzSsPfHEKmUTk9iXwxE8G5bvc6Wt",
										Destination: "8aMhFVDw1yVpAhZyS8oXptXKk4ZWhgdhdG41WWrywXSj",
										Authority:   "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
										Amount:      "12500",
									},
								},
							},
						},
					},

					{
						Program:   api.SolanaProgram_SPL_TOKEN,
						ProgramId: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
						ProgramData: &api.SolanaInstructionV2_SplTokenProgram{
							SplTokenProgram: &api.SolanaSplTokenProgram{
								InstructionType: api.SolanaSplTokenProgram_TRANSFER,
								Instruction: &api.SolanaSplTokenProgram_Transfer{
									Transfer: &api.SolanaSplTokenTransferInstruction{
										Source:      "Gg1HXc1DXpN4HkzWmzSsPfHEKmUTk9iXwxE8G5bvc6Wt",
										Destination: "56pzXb1q2t4ZwgHC77pTELid2NYw8upu57McFK5j8Mj3",
										Authority:   "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
										Amount:      "12500",
									},
								},
							},
						},
					},
				},
			},
		},

		PreBalances: []uint64{
			78932183540,
			3480000,
			1731313240,
			4043760,
			1738696819,
			5317440,
			5317440,
			1739873418,
			27693840,
			0,
			5317440,
			1737282289,
			434239280,
			5317440,
			9723120,
			1,
			8741760,
			3480000,
			165152881365,
			1141440,
			934087680,
		},
		PostBalances: []uint64{
			78932178539,
			3480000,
			1731313240,
			4043760,
			1738709319,
			5317440,
			5317440,
			1739885918,
			27693840,
			0,
			5317440,
			1737282289,
			434214280,
			5317440,
			9723120,
			1,
			8741760,
			3480000,
			165152881365,
			1141440,
			934087680,
		},
		PreTokenBalances: []*api.SolanaTokenBalance{
			{
				AccountIndex: 2,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1729273960",
					Decimals:       9,
					UiAmountString: "1.72927396",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 4,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1736657539",
					Decimals:       9,
					UiAmountString: "1.736657539",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 7,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1737834138",
					Decimals:       9,
					UiAmountString: "1.737834138",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 11,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1735243009",
					Decimals:       9,
					UiAmountString: "1.735243009",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 12,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "432200000",
					Decimals:       9,
					UiAmountString: "0.4322",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
		},
		PostTokenBalances: []*api.SolanaTokenBalance{
			{
				AccountIndex: 2,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1729273960",
					Decimals:       9,
					UiAmountString: "1.72927396",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 4,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1736670039",
					Decimals:       9,
					UiAmountString: "1.736670039",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 7,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1737846638",
					Decimals:       9,
					UiAmountString: "1.737846638",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 11,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "1735243009",
					Decimals:       9,
					UiAmountString: "1.735243009",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
			{
				AccountIndex: 12,
				Mint:         "So11111111111111111111111111111111111111112",
				TokenAmount: &api.SolanaTokenAmount{
					Amount:         "432175000",
					Decimals:       9,
					UiAmountString: "0.432175",
				},
				Owner: "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
			},
		},
		LogMessages: []string{
			"Program ComputeBudget111111111111111111111111111111 invoke [1]",
			"Program ComputeBudget111111111111111111111111111111 success",
			"Program ComputeBudget111111111111111111111111111111 invoke [1]",
			"Program ComputeBudget111111111111111111111111111111 success",
			"Program SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f invoke [1]",
			"Program log: Instruction: AggregatorSaveResult",
			"Program data: Dk7x7N2nVamqiD+qOdX7POYtSwhzJnoNW0XVHeABvurymwylfzTTc5bKpwsAAAAAouRsZAAAAAAAAAAAAAAAAA==",
			"Program data: A5o8/ZicmX6qiD+qOdX7POYtSwhzJnoNW0XVHeABvurymwylfzTTc+x0LS4AAAAAAAAAAAAAAAAKAAAAlsqnCwAAAACi5GxkAAAAAE2qog13MYE1ZZpGh3EEOXXIKSTGSD2rwhqba8fT6jF0AAAAAA==",
			"Program log: P1 CUgoqwiQ4wCt6Tthkrgx5saAEpLBjPCdHshVa4Pbfcx2",
			"Program log: MODE_ROUND",
			"Program data: m/42h1I1hpSqiD+qOdX7POYtSwhzJnoNW0XVHeABvurymwylfzTTczL6gQs835TTqwPGGZqgS6etNi+tvi8vHAhHmTqeQ8m051I8SQQkijUQGaZKTJDK4re7MrcPszyAYcympKHEcWEpQ4FtEevv1sepvf/xfCkvVr6CLBiM6bj9sZxTRnXz9QAAAAAAAAAAkcqnCwAAAACi5GxkAAAAAA==",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]",
			"Program log: Instruction: Transfer",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 4736 of 179386 compute units",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success",
			"Program data: lYLi/gDS5xCqiD+qOdX7POYtSwhzJnoNW0XVHeABvurymwylfzTTczL6gQs835TTqwPGGZqgS6etNi+tvi8vHAhHmTqeQ8m0WD5Meu5OSSR2YVbZEM3EetYaaglKqikDfv44BILzO8ZwjV+5Rs+YsmOnM7wm+uXbB0YrWOgcRkGx4VcP7FaDXNQwAAAAAAAAkcqnCwAAAACi5GxkAAAAAA==",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]",
			"Program log: Instruction: Transfer",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 4736 of 165852 compute units",
			"Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success",
			"Program data: lYLi/gDS5xCqiD+qOdX7POYtSwhzJnoNW0XVHeABvurymwylfzTTczL6gQs835TTqwPGGZqgS6etNi+tvi8vHAhHmTqeQ8m0TaqiDXcxgTVlmkaHcQQ5dcgpJMZIPavCGptrx9PqMXQ87OqTRCbRVcD4ohkYT/Y3A0NaaQyaTY7hp5rXGUVJDtQwAAAAAAAAkcqnCwAAAACi5GxkAAAAAA==",
			"Program data: m/42h1I1hpSqiD+qOdX7POYtSwhzJnoNW0XVHeABvurymwylfzTTczL6gQs835TTqwPGGZqgS6etNi+tvi8vHAhHmTqeQ8m03yRRJs0nMOQivltwHIv1oisAh1U72d2Cl+97+eL2J1/hcJ5RwgrItP3XFm7p7C9MQM+GI5ftzwVzpELKzq7eYQAAAAAAAAAAkcqnCwAAAACi5GxkAAAAAA==",
			"Program data: cB8z6WFkK/WqiD+qOdX7POYtSwhzJnoNW0XVHeABvurymwylfzTTc+x0LS4AAAAAAAAAAAAAAAAKAAAAlsqnCwAAAACi5GxkAAAAAAIAAABYPkx67k5JJHZhVtkQzcR61hpqCUqqKQN+/jgEgvM7xk2qog13MYE1ZZpGh3EEOXXIKSTGSD2rwhqba8fT6jF0AgAAAOx0LS4AAAAAAAAAAAAAAAAKAAAA7HQtLgAAAAAAAAAAAAAAAAoAAAA=",
			"Program SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f consumed 101756 of 250000 compute units",
			"Program SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f success",
		},
		Rewards: []*api.SolanaReward{},
	}, transaction.GetMeta())

	require.Equal(&api.SolanaTransactionPayloadV2{
		Signatures: []string{"59AZH71gLaQnUuoBbz3TWbswNJWnptH4KwN49uJR5yQQuG7gNqK9CGH2MieCBMC2to4NsxM7db4h79mQVZDY27Gz"},
		Message: &api.SolanaMessageV2{
			AccountKeys: []*api.AccountKey{
				{
					Pubkey:   "31Sof5r1xi7dfcaz4x9Kuwm8J9ueAdDduMcme59sP8gc",
					Signer:   true,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "2ba9WNyfVCwjZasLbdviHpxkhV2Q6gaoQg19RiiubZTt",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "3n5REjpMUw11sgL6TxTktT2gZu45PBAeUSJ2vWgXhTvC",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "4Rzw48RJeCQkRaXtZmddWAQTkdZ8jXgw85JjBfXbZW27",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "56pzXb1q2t4ZwgHC77pTELid2NYw8upu57McFK5j8Mj3",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "6EBJV2LPV4NDoysJPzabd5SAwtBSvwQxDs4CPaC1GXpX",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "6wTyY1JKzcTKfTVN7M7rQcBo15FkDEZA4eoWk3rBGVdP",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "8aMhFVDw1yVpAhZyS8oXptXKk4ZWhgdhdG41WWrywXSj",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "CUgoqwiQ4wCt6Tthkrgx5saAEpLBjPCdHshVa4Pbfcx2",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "ExrpUcYozgBLFDN7gTUXVHNcwGDm214ShAKqFfuio2bE",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "G2434YvZY4KWojTKqhL8EZJMxsRLtbhUe6RfA1KziGGe",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "GB2LGttWydxXt3P3uie9x6mBvFHRmWx2SgLc6T3PRRf6",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "Gg1HXc1DXpN4HkzWmzSsPfHEKmUTk9iXwxE8G5bvc6Wt",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "GZyuePbLkoNzMVT3dMoU8SfbnH4hA2RCv5mum6PWiPcc",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "3HBb2DQqDfuMdzWxNk1Eo9RTMkFYmuEAd32RiLKn9pAn",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "ComputeBudget111111111111111111111111111111",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "DAhLsQmss76jr1iP9jxnBv5hQxgybcdSXHSJZpmX2fMw",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "So11111111111111111111111111111111111111112",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
				{
					Pubkey:   "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
					Signer:   false,
					Source:   "transaction",
					Writable: false,
				},
			},
			AddressTableLookups: nil,
			Instructions: []*api.SolanaInstructionV2{
				{
					ProgramId: "ComputeBudget111111111111111111111111111111",
					ProgramData: &api.SolanaInstructionV2_RawInstruction{
						RawInstruction: &api.SolanaRawInstruction{
							Data: base58.Decode("3DdGGhkhJbjm"),
						},
					},
				},
				{
					ProgramId: "ComputeBudget111111111111111111111111111111",
					ProgramData: &api.SolanaInstructionV2_RawInstruction{
						RawInstruction: &api.SolanaRawInstruction{
							Data: base58.Decode("HnkkG7"),
						},
					},
				},
				{
					ProgramId: "SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f",
					ProgramData: &api.SolanaInstructionV2_RawInstruction{
						RawInstruction: &api.SolanaRawInstruction{
							Accounts: []string{
								"CUgoqwiQ4wCt6Tthkrgx5saAEpLBjPCdHshVa4Pbfcx2",
								"6EBJV2LPV4NDoysJPzabd5SAwtBSvwQxDs4CPaC1GXpX",
								"31Sof5r1xi7dfcaz4x9Kuwm8J9ueAdDduMcme59sP8gc",
								"3HBb2DQqDfuMdzWxNk1Eo9RTMkFYmuEAd32RiLKn9pAn",
								"31Sof5r1xi7dfcaz4x9Kuwm8J9ueAdDduMcme59sP8gc",
								"2ba9WNyfVCwjZasLbdviHpxkhV2Q6gaoQg19RiiubZTt",
								"DAhLsQmss76jr1iP9jxnBv5hQxgybcdSXHSJZpmX2fMw",
								"4Rzw48RJeCQkRaXtZmddWAQTkdZ8jXgw85JjBfXbZW27",
								"Gg1HXc1DXpN4HkzWmzSsPfHEKmUTk9iXwxE8G5bvc6Wt",
								"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
								"CyZuD7RPDcrqCGbNvLCyqk6Py9cEZTKmNKujfPi3ynDd",
								"CUgoqwiQ4wCt6Tthkrgx5saAEpLBjPCdHshVa4Pbfcx2",
								"So11111111111111111111111111111111111111112",
								"GZyuePbLkoNzMVT3dMoU8SfbnH4hA2RCv5mum6PWiPcc",
								"6wTyY1JKzcTKfTVN7M7rQcBo15FkDEZA4eoWk3rBGVdP",
								"6EBJV2LPV4NDoysJPzabd5SAwtBSvwQxDs4CPaC1GXpX",
								"G2434YvZY4KWojTKqhL8EZJMxsRLtbhUe6RfA1KziGGe",
								"3n5REjpMUw11sgL6TxTktT2gZu45PBAeUSJ2vWgXhTvC",
								"8aMhFVDw1yVpAhZyS8oXptXKk4ZWhgdhdG41WWrywXSj",
								"56pzXb1q2t4ZwgHC77pTELid2NYw8upu57McFK5j8Mj3",
								"GB2LGttWydxXt3P3uie9x6mBvFHRmWx2SgLc6T3PRRf6",
								"ExrpUcYozgBLFDN7gTUXVHNcwGDm214ShAKqFfuio2bE",
							},
							Data: base58.Decode("3hAw8ppjnbTHztHcRrr9JtVZhbjVW3sirrU5No4Bsjf23mCRTQeui9DYoWWrvbGRXN7XBW3QauPT5ZiiWibqqozxZfRiapffHsT71tiqUZkhhz9QDm4TdsXnyhKwEB3opUdPpAezJTvWcVdEFU3GL"),
						},
					},
				},
			},
			RecentBlockHash: "9q2KbfPU2VycCoeSEDaufnfjZUp7UQyrkrLjyyWHCYFZ",
		},
	}, transaction.GetPayload())
}

func (s *solanaTestSuite) TestGetBlockByHeight_Rosetta() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tests := []struct {
		blockNumber           uint64
		numNativeTransactions int
		numVoteTransactions   int
	}{
		{
			blockNumber:           195545750,
			numNativeTransactions: 1446,
			numVoteTransactions:   1317,
		},
		{
			blockNumber:           200279539,
			numNativeTransactions: 1296,
			numVoteTransactions:   1067,
		},
		{
			blockNumber:           80553352,
			numNativeTransactions: 923,
			numVoteTransactions:   845,
		},
		{
			blockNumber:           217003034,
			numNativeTransactions: 2874,
			numVoteTransactions:   2787,
		},
		{
			blockNumber:           200003351,
			numNativeTransactions: 0,
			numVoteTransactions:   0,
		},
		{
			blockNumber:           200947008,
			numNativeTransactions: 2372,
			numVoteTransactions:   2053,
		},
	}

	for _, test := range tests {
		block, err := s.client.GetBlockByHeight(ctx, 2, test.blockNumber)
		require.NoError(err)
		s.logger.Info("solana block", zap.Reflect("block", block.Metadata))
		require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, block.Blockchain)
		require.Equal(common.Network_NETWORK_SOLANA_MAINNET, block.Network)
		require.Equal(uint32(0x2), block.Metadata.Tag)
		require.Less(0, len(block.GetSolana().GetHeader()))
		require.False(block.Metadata.Skipped)

		nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
		require.NoError(err)
		require.NotNil(nativeBlock)
		nativeTxs := nativeBlock.GetSolanaV2().GetTransactions()
		require.Len(nativeTxs, test.numNativeTransactions)

		rosettaBlock, err := s.parser.ParseRosettaBlock(context.Background(), block)
		require.NoError(err)
		require.NotNil(rosettaBlock)
		rosettaTxs := rosettaBlock.GetBlock().GetTransactions()
		require.Equal(test.numVoteTransactions, testutil.NumberOfSolanaVoteTransactions(require, rosettaTxs))

		err = s.parser.ValidateRosettaBlock(ctx, &api.ValidateRosettaBlockRequest{NativeBlock: nativeBlock}, rosettaBlock)
		require.NoError(err)
	}
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

func (s *solanaTestSuite) TestGetBlockByHeight_GenerateTransactionList_Success() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	block, err := s.client.GetBlockByHeight(ctx, solanaTag, solanaHeight)
	require.NoError(err)

	txnMetadata := block.GetTransactionMetadata()
	require.NotNil(txnMetadata)
	transactions := txnMetadata.GetTransactions()
	require.NotNil(transactions)
	require.Equal(2888, len(transactions))
	require.Equal("2xRnwfAMxAvv5z2eiWC1YCR6bdcPj7ebPRTKFiFZuBHvWNc8QjM33W4Ev71T8C18g3yARJcHtMzC3VWTdASDybkU", transactions[0])
	require.Equal("BJHDYmjhDktMSc1kVWiP7WVnncXknRBsJdPUS8LvFqEpkvv8JZEYYKFE2Zy6MnFz7b4Msiy9z2hE7ZQuS1349qs", transactions[1000])
	require.Equal("5N9tAE8WAig6NYgALH4mqnWyHGoVhsoP9eFBT2FJz7MpzaZcAXiAXFt1fzVxuiLna132mvV9F8RzhhMFFVaaBa63", transactions[2000])
	require.Equal("5ReoZL45AytWtzdx4vNVgn4kHk73JHL5i49hTzQDCWRZv3CBMEFGBMUeTvoqubhg6dbNjcdABaneLkNZEwaxtRjV", transactions[2887])
}

func mustDumpBlock(block proto.Message, filename string) {
	data, err := json.MarshalIndent(block, "", "  ")
	if err != nil {
		panic(err)
	}

	err = ioutil.WriteFile(filename, data, 0644)
	if err != nil {
		panic(err)
	}
}
