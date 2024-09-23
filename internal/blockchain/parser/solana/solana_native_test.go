package solana

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type solanaNativeParserTestSuite struct {
	suite.Suite
	app    testapp.TestApp
	parser internal.NativeParser
}

func TestSolanaNativeParserTestSuite(t *testing.T) {
	suite.Run(t, new(solanaNativeParserTestSuite))
}

func (s *solanaNativeParserTestSuite) SetupTest() {
	s.app = testapp.New(s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(NewSolanaNativeParser),
		fx.Populate(&s.parser),
	)
	s.NotNil(s.parser)
}

func (s *solanaNativeParserTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *solanaNativeParserTestSuite) TestParseBlockV2() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Hash:         "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd",
			ParentHash:   "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
			Height:       195545750,
			ParentHeight: 195545749,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_195545749_v2.json"),
			},
		},
	}
	blockTimestamp := testutil.MustTimestamp("2023-05-23T16:06:58Z")
	numTransactions := 1446

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
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
	require.Equal(SolanaLegacyVersion, transaction.Version)
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
	require.Equal(SolanaLegacyVersion, transaction.Version)
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
							Data: internal.DecodeBase58("3DdGGhkhJbjm"),
						},
					},
				},
				{
					ProgramId: "ComputeBudget111111111111111111111111111111",
					ProgramData: &api.SolanaInstructionV2_RawInstruction{
						RawInstruction: &api.SolanaRawInstruction{
							Data: internal.DecodeBase58("HnkkG7"),
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
							Data: internal.DecodeBase58("3hAw8ppjnbTHztHcRrr9JtVZhbjVW3sirrU5No4Bsjf23mCRTQeui9DYoWWrvbGRXN7XBW3QauPT5ZiiWibqqozxZfRiapffHsT71tiqUZkhhz9QDm4TdsXnyhKwEB3opUdPpAezJTvWcVdEFU3GL"),
						},
					},
				},
			},
			RecentBlockHash: "9q2KbfPU2VycCoeSEDaufnfjZUp7UQyrkrLjyyWHCYFZ",
		},
	}, transaction.GetPayload())
}

// This block had a failed transaction (Instruction Error - ProgramFailedToComplete). Test that we can parse it
// without error.
func (s *solanaNativeParserTestSuite) TestParseBlockV2_Slot_241043141() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Hash:         "7UVhKXDoFXfQWHRRMNaXiEXiQsabDvU7oz4TRLHFuzd8",
			ParentHash:   "8KrXYfWrGMBJg6owJ5U5X1c6rxh3iVxbybvSK5hbTZtA",
			Height:       241043141,
			ParentHeight: 241043140,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_241043141_v2.json"),
			},
		},
	}
	_, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
}

func (s *solanaNativeParserTestSuite) TestParseBlockV2_Slot_217003034() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Hash:         "7CRnkUB7s6deSZ5Pro4xDcebU8vZj3eXBTqeCDMzBBW4",
			ParentHash:   "TgZ5L5YBSKVPm6kEncCeGxxn19QNKBJqL2yhPcLcYAo",
			Height:       217003034,
			ParentHeight: 217003033,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_217003034_v2.json"),
			},
		},
	}
	//Mon, 11 Sep 2023 23:42:36 GMT
	blockTimestamp := testutil.MustTimestamp("2023-09-11T22:30:21Z")
	numTransactions := 2874
	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_SOLANA, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_SOLANA_MAINNET, nativeBlock.Network)
	require.Equal(uint32(0x2), nativeBlock.Tag)
	require.Equal(uint64(217003034), nativeBlock.Height)
	require.Equal(uint64(217003033), nativeBlock.ParentHeight)
	require.Equal("7CRnkUB7s6deSZ5Pro4xDcebU8vZj3eXBTqeCDMzBBW4", nativeBlock.Hash)
	require.Equal("TgZ5L5YBSKVPm6kEncCeGxxn19QNKBJqL2yhPcLcYAo", nativeBlock.ParentHash)
	require.Equal(blockTimestamp, nativeBlock.Timestamp, nativeBlock.Timestamp.AsTime().String())
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))
	require.False(nativeBlock.Skipped)

	solanaBlock := nativeBlock.GetSolanaV2()
	require.NotNil(solanaBlock)
	require.Equal(&api.SolanaHeader{
		BlockHash:         "7CRnkUB7s6deSZ5Pro4xDcebU8vZj3eXBTqeCDMzBBW4",
		PreviousBlockHash: "TgZ5L5YBSKVPm6kEncCeGxxn19QNKBJqL2yhPcLcYAo",
		Slot:              217003034,
		ParentSlot:        217003033,
		BlockTime:         blockTimestamp,
		BlockHeight:       199312437,
	}, solanaBlock.GetHeader())
	require.Equal(numTransactions, len(solanaBlock.GetTransactions()))

	// see https://explorer.solana.com/tx/5Mwn1x3QaXgKeAVxFiQk3LmYC5YZnsYUgx1Riy9VU3o7ZuTVVTfDCpbsJenX7KAAWzSQQz8iesW45977tYN1Moh2
	transaction := solanaBlock.GetTransactions()[3]
	require.Equal("5Mwn1x3QaXgKeAVxFiQk3LmYC5YZnsYUgx1Riy9VU3o7ZuTVVTfDCpbsJenX7KAAWzSQQz8iesW45977tYN1Moh2", transaction.TransactionId)
	require.Equal(&api.SolanaTransactionPayloadV2{
		Signatures: []string{"5Mwn1x3QaXgKeAVxFiQk3LmYC5YZnsYUgx1Riy9VU3o7ZuTVVTfDCpbsJenX7KAAWzSQQz8iesW45977tYN1Moh2"},
		Message: &api.SolanaMessageV2{
			AccountKeys: []*api.AccountKey{
				{
					Pubkey:   "cZCvgqgrdX2LCScVku8CprMnAUZtSicBDJ8eiowfonn",
					Signer:   true,
					Source:   "transaction",
					Writable: true,
				},
				{
					Pubkey:   "8BbpzqTUCUa4oR71AMgcBiTQvd6Gta3xJHCG4xK9mKBp",
					Signer:   false,
					Source:   "transaction",
					Writable: true,
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
							InstructionType: api.SolanaVoteProgram_COMPACT_UPDATE_VOTE_STATE,
							Instruction: &api.SolanaVoteProgram_CompactUpdateVoteState{
								CompactUpdateVoteState: &api.SolanaVoteCompactUpdateVoteStateInstruction{
									VoteAccount:   "8BbpzqTUCUa4oR71AMgcBiTQvd6Gta3xJHCG4xK9mKBp",
									VoteAuthority: "cZCvgqgrdX2LCScVku8CprMnAUZtSicBDJ8eiowfonn",
									VoteStateUpdate: &api.SolanaVoteCompactUpdateVoteStateInstruction_VoteStateUpdate{
										Hash: "D1GjsFs4a6h7rBH6YHNbyxNnXjtf93JRKNK174gXait9",
										Lockouts: []*api.SolanaVoteCompactUpdateVoteStateInstruction_Lockout{
											{
												ConfirmationCount: 31,
												Slot:              217003002,
											},
											{
												ConfirmationCount: 30,
												Slot:              217003003,
											},
											{
												ConfirmationCount: 29,
												Slot:              217003004,
											},
											{
												ConfirmationCount: 28,
												Slot:              217003005,
											},
											{
												ConfirmationCount: 27,
												Slot:              217003006,
											},
											{
												ConfirmationCount: 26,
												Slot:              217003007,
											},
											{
												ConfirmationCount: 25,
												Slot:              217003008,
											},
											{
												ConfirmationCount: 24,
												Slot:              217003009,
											},
											{
												ConfirmationCount: 23,
												Slot:              217003010,
											},
											{
												ConfirmationCount: 22,
												Slot:              217003011,
											},
											{
												ConfirmationCount: 21,
												Slot:              217003012,
											},
											{
												ConfirmationCount: 20,
												Slot:              217003013,
											},
											{
												ConfirmationCount: 19,
												Slot:              217003014,
											},
											{
												ConfirmationCount: 18,
												Slot:              217003015,
											},
											{
												ConfirmationCount: 17,
												Slot:              217003016,
											},
											{
												ConfirmationCount: 16,
												Slot:              217003017,
											},
											{
												ConfirmationCount: 15,
												Slot:              217003018,
											},
											{
												ConfirmationCount: 14,
												Slot:              217003019,
											},
											{
												ConfirmationCount: 13,
												Slot:              217003020,
											},
											{
												ConfirmationCount: 12,
												Slot:              217003021,
											},
											{
												ConfirmationCount: 11,
												Slot:              217003022,
											},
											{
												ConfirmationCount: 10,
												Slot:              217003023,
											},
											{
												ConfirmationCount: 9,
												Slot:              217003024,
											},
											{
												ConfirmationCount: 8,
												Slot:              217003025,
											},
											{
												ConfirmationCount: 7,
												Slot:              217003026,
											},
											{
												ConfirmationCount: 6,
												Slot:              217003027,
											},
											{
												ConfirmationCount: 5,
												Slot:              217003028,
											},
											{
												ConfirmationCount: 4,
												Slot:              217003029,
											},
											{
												ConfirmationCount: 3,
												Slot:              217003030,
											},
											{
												ConfirmationCount: 2,
												Slot:              217003031,
											},
											{
												ConfirmationCount: 1,
												Slot:              217003032,
											},
										},
										Root: 217003001,
										Timestamp: &timestamp.Timestamp{
											Seconds: 1694471421,
										},
									},
								},
							},
						},
					},
				},
			},
			RecentBlockHash: "7FKKrL8jexBbmSr9qkHeWs6crje56viSBB6X6jUt8srq",
		},
	}, transaction.GetPayload())
}

func TestParseSolanaTransactionVersion(t *testing.T) {
	type Envelope struct {
		Version *SolanaTransactionVersion
	}

	tests := []struct {
		name     string
		expected int32
		input    string
	}{
		{
			name:     "empty",
			expected: -1,
			input:    `{"version": ""}`,
		},
		{
			name:     "legacy",
			expected: -1,
			input:    `{"version": "legacy"}`,
		},
		{
			name:     "zero",
			expected: 0,
			input:    `{"version": 0}`,
		},
		{
			name:     "one",
			expected: 1,
			input:    `{"version": 1}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			var envelope Envelope
			err := json.Unmarshal([]byte(test.input), &envelope)
			require.NoError(err)
			require.Equal(test.expected, envelope.Version.Value())
		})
	}
}
