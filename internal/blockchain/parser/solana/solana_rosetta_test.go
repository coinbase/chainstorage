package solana

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

type solanaRosettaParserTestSuite struct {
	suite.Suite
	app          testapp.TestApp
	parser       internal.RosettaParser
	nativeParser internal.NativeParser
}

const (
	unknownCurrencySymbol   = "UNKNOWN_CURRENCY"
	nativeSymbol            = "SOL"
	contractAddressMetadata = "contract_address"
)

func TestSolanaRosettaParserTestSuite(t *testing.T) {
	suite.Run(t, new(solanaRosettaParserTestSuite))
}

func (s *solanaRosettaParserTestSuite) SetupTest() {
	s.app = testapp.New(s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(NewSolanaRosettaParser),
		fx.Provide(NewSolanaNativeParser),
		fx.Populate(&s.parser),
		fx.Populate(&s.nativeParser),
	)
	s.NotNil(s.parser)
}

func (s *solanaRosettaParserTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *solanaRosettaParserTestSuite) TestParseBlock() {
	require := testutil.Require(s.T())

	blockHash := "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd"
	blockHeight := uint64(195545750)
	blockTimestamp := testutil.MustTimestamp("2023-05-23T16:06:58Z")
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Hash:         blockHash,
			ParentHash:   "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
			Height:       blockHeight,
			ParentHeight: blockHeight - 1,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_195545749_v2.json"),
			},
		},
	}

	nativeBlock, err := s.nativeParser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(nativeBlock)
	nativeTxs := nativeBlock.GetSolanaV2().GetTransactions()
	numNativeTransactions := len(nativeTxs)

	parsed, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(parsed)
	rosettaBlock := parsed.Block
	require.Equal(&rosetta.BlockIdentifier{
		Index: int64(blockHeight),
		Hash:  blockHash,
	}, rosettaBlock.GetBlockIdentifier())
	require.Equal(&rosetta.BlockIdentifier{
		Index: int64(blockHeight - 1),
		Hash:  "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
	}, rosettaBlock.GetParentBlockIdentifier())
	require.Equal(blockTimestamp, rosettaBlock.Timestamp)
	// block reward has 1 additional transaction
	rosettaTxs := rosettaBlock.GetTransactions()
	require.Len(rosettaTxs, numNativeTransactions+1)

	require.NotEmpty(rosettaTxs)
	transaction := rosettaTxs[0]
	require.Equal("2TpKeGQZg4f2YnsbFYJrG2x7hLNFoRFwCkbMPn7Ad2Yvv3dBhNi5DtmHyb57S1TD4m6Wrsxctsi4i1jj2BXnbGcU", transaction.GetTransactionIdentifier().GetHash())
	require.Len(transaction.Operations, 1)
	operation := transaction.GetOperations()[0]
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(0),
		},
		Type:   "FEE",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "ErnUUo8z4fjEteEBmBp952x41TMLUMsFcrUmtpBiXx2e",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-5000).String(),
			Currency: &nativeRosettaCurrency,
		},
	}, operation)

	// tx_id: 4H6uGfHviqT87Au64fv9t7AQHYd75SZPa7NMsX2fZixdcwZgpPB11b4cQq65JVM4EzppLmNPt2d8cGeFY9ys1dBH
	transaction = rosettaTxs[25]
	require.Equal("4H6uGfHviqT87Au64fv9t7AQHYd75SZPa7NMsX2fZixdcwZgpPB11b4cQq65JVM4EzppLmNPt2d8cGeFY9ys1dBH", transaction.GetTransactionIdentifier().GetHash())
	require.Len(transaction.GetOperations(), 6)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(0),
		},
		Type:   "FEE",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-16200).String(),
			Currency: &nativeRosettaCurrency,
		},
	}, transaction.GetOperations()[0])
	metadata, err := rosetta.FromSDKMetadata(map[string]any{
		"instruction_type": "UNKNOWN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(1),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(129112223).String(),
			Currency: &nativeRosettaCurrency,
		},
		Metadata: metadata,
	}, transaction.GetOperations()[1])
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(2),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "6kxCwpuJbDbYqYqw3958ZXCZiwkRQEubsFN6jCsPUSMN",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-129112223).String(),
			Currency: &nativeRosettaCurrency,
		},
		Metadata: metadata,
	}, transaction.GetOperations()[2])
	amountMetadata, err := rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "So11111111111111111111111111111111111111112",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(3),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(-129112223).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 9,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[3])
	amountMetadata, err = rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(4),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(-50000000).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 6,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[4])
	amountMetadata, err = rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(5),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(50000000).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 6,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[5])

	transaction = rosettaTxs[1348]
	require.Equal("66Er8hksXa2tDyX3cCGPJXFUfQtinLWMQY6t4RFeQ4Y71gc3zneDBfmFdtVEAv31ViFyaYC9zSeRSdCB87Vdd4Q7", transaction.GetTransactionIdentifier().GetHash())
	require.Len(transaction.GetOperations(), 3)
	metadata, err = rosetta.FromSDKMetadata(map[string]any{
		"instruction_type": "TRANSFER",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(1),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "RBHdGVfDfMjfU6iUfCb1LczMJcQLx7hGnxbzRsoDNvx",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-11488300000).String(),
			Currency: &nativeRosettaCurrency,
		},
		Metadata: metadata,
	}, transaction.GetOperations()[1])

	// validate block reward transaction
	transaction = rosettaTxs[numNativeTransactions]
	ops := transaction.GetOperations()
	rewards := nativeBlock.GetSolanaV2().GetRewards()
	opIndex := 0
	for _, reward := range rewards {
		lamports := reward.Lamports
		if lamports == 0 {
			continue
		}

		op := ops[opIndex]
		require.Equal(internal.EncodeBase58(reward.GetPubkey()), op.GetAccount().GetAddress())
		expectedAmount := new(big.Int).SetInt64(lamports)
		if lamports > 0 {
			require.Equal("REWARD", op.Type)

		} else {
			require.Equal("FEE", ops[opIndex].Type)
		}
		actualAmount, ok := new(big.Int).SetString(op.GetAmount().GetValue(), 10)
		require.True(ok)
		require.Equal(expectedAmount, actualAmount)
		opIndex++
	}

	require.Equal("-2439", ops[0].GetAmount().GetValue())
	require.Equal("CEkiXAgowjS5LH5m6KBQmaSWnuLgxWzfZJGgyqEEqQwk", ops[0].GetAccount().Address)
	require.Equal("REWARD", ops[2].Type)
	require.Equal("3704791", ops[2].GetAmount().GetValue())
	require.Equal("2av6anjHUwvpzhbCa9vAUEEybEHs9MSU6qw3pB3o6cC7", ops[2].GetAccount().Address)

	// check vote transactions count
	require.Equal(1317, testutil.NumberOfSolanaVoteTransactions(require, rosettaTxs))

	// validate the txs, excluding block reward transaction
	for i := 0; i < len(rosettaTxs)-1; i++ {
		s.validateSolanaRosettaTransactionOperations(require, nativeTxs[i], rosettaTxs[i])
	}
}

func (s *solanaRosettaParserTestSuite) validateSolanaRosettaTransactionOperations(
	require *testutil.Assertions,
	nativeTx *api.SolanaTransactionV2,
	rosettaTx *rosetta.Transaction,
) {
	require.Equal(nativeTx.TransactionId, rosettaTx.GetTransactionIdentifier().Hash)
	ops := rosettaTx.GetOperations()
	accountKeys := nativeTx.GetPayload().GetMessage().GetAccountKeys()
	preBalances := nativeTx.GetMeta().GetPreBalances()
	postBalances := nativeTx.GetMeta().GetPostBalances()
	preTokenBalances := nativeTx.GetMeta().GetPreTokenBalances()
	postTokenBalances := nativeTx.GetMeta().GetPostTokenBalances()

	require.NotEmpty(accountKeys)
	require.Equal(len(preBalances), len(postBalances))
	require.Equal(len(postBalances), len(accountKeys))
	accountKeyIndexMap := make(map[string]int)
	for i, account := range accountKeys {
		accountKeyIndexMap[account.Pubkey] = i
	}

	expectedPostBalances := make([]*big.Int, len(accountKeys))
	actualPostBalances := make([]*big.Int, len(accountKeys))
	for i := range accountKeys {
		expectedPostBalances[i] = new(big.Int).SetUint64(postBalances[i])
		actualPostBalances[i] = new(big.Int).SetUint64(preBalances[i])
	}

	actualPostTokenBalances, err := GetTokenBalanceAmountMap(preTokenBalances, accountKeys)
	require.NoError(err)
	expectedPostTokenBalances, err := GetTokenBalanceAmountMap(postTokenBalances, accountKeys)
	require.NoError(err)

	for i, op := range ops {
		require.Equal(int64(i), op.OperationIdentifier.Index)
		if op.Status != "SUCCESS" {
			continue
		}
		symbol := op.Amount.Currency.Symbol
		account := op.Account.Address
		require.NotEmpty(account)
		amount, ok := new(big.Int).SetString(op.Amount.Value, 10)
		require.True(ok)
		if symbol == nativeSymbol {
			accountIndex, ok := accountKeyIndexMap[account]
			require.True(ok)
			actualPostBalances[accountIndex].Add(actualPostBalances[accountIndex], amount)
		} else if symbol == unknownCurrencySymbol {
			metadata, err := rosetta.ToSDKMetadata(op.Amount.Currency.Metadata)
			require.NoError(err)
			contractAddress, ok := metadata[contractAddressMetadata]
			require.True(ok)
			key := GetTokenBalanceMapKey(contractAddress.(string), account)
			val, ok := actualPostTokenBalances[key]
			if !ok {
				val = big.NewInt(0)
			}
			actualPostTokenBalances[key] = val.Add(val, amount)
		} else {
			panic(xerrors.Errorf("unknown symbol=%s", symbol))
		}
	}

	for i := range accountKeys {
		require.Equal(0, expectedPostBalances[i].Cmp(actualPostBalances[i]))
	}

	for key, expected := range expectedPostTokenBalances {
		actual, ok := actualPostTokenBalances[key]
		require.True(ok)
		require.Equal(0, expected.Cmp(actual), fmt.Sprintf("actualPostTokenBalances=%+v, expectedPostTokeBalances=%+v", actualPostTokenBalances, expectedPostTokenBalances))
	}

	for key, actual := range actualPostTokenBalances {
		expected, ok := expectedPostTokenBalances[key]
		if !ok {
			require.Equal(0, actual.Cmp(big.NewInt(0)))
		} else {
			require.Equal(0, expected.Cmp(actual))
		}
	}
}

func (s *solanaRosettaParserTestSuite) TestParseBlock_NewTokenAccount() {
	require := testutil.Require(s.T())

	blockHash := "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd"
	blockHeight := uint64(195545750)
	blockTimestamp := testutil.MustTimestamp("2023-05-23T16:06:58Z")
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Hash:         blockHash,
			ParentHash:   "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
			Height:       blockHeight,
			ParentHeight: blockHeight - 1,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_new_token_account.json"),
			},
		},
	}

	nativeBlock, err := s.nativeParser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(nativeBlock)
	nativeTxs := nativeBlock.GetSolanaV2().GetTransactions()
	numNativeTransactions := len(nativeTxs)

	parsed, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(parsed)
	rosettaBlock := parsed.Block
	require.Equal(&rosetta.BlockIdentifier{
		Index: int64(blockHeight),
		Hash:  blockHash,
	}, rosettaBlock.GetBlockIdentifier())
	require.Equal(&rosetta.BlockIdentifier{
		Index: int64(blockHeight - 1),
		Hash:  "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
	}, rosettaBlock.GetParentBlockIdentifier())
	require.Equal(blockTimestamp, rosettaBlock.Timestamp)
	// block reward has 1 additional transaction
	rosettaTxs := rosettaBlock.GetTransactions()
	require.Len(rosettaTxs, numNativeTransactions+1)
	require.NotEmpty(rosettaTxs)

	// tx_id: 4H6uGfHviqT87Au64fv9t7AQHYd75SZPa7NMsX2fZixdcwZgpPB11b4cQq65JVM4EzppLmNPt2d8cGeFY9ys1dBH
	transaction := rosettaTxs[0]
	require.Equal("4H6uGfHviqT87Au64fv9t7AQHYd75SZPa7NMsX2fZixdcwZgpPB11b4cQq65JVM4EzppLmNPt2d8cGeFY9ys1dBH", transaction.GetTransactionIdentifier().GetHash())
	require.Len(transaction.GetOperations(), 6)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(0),
		},
		Type:   "FEE",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-16200).String(),
			Currency: &nativeRosettaCurrency,
		},
	}, transaction.GetOperations()[0])
	metadata, err := rosetta.FromSDKMetadata(map[string]any{
		"instruction_type": "UNKNOWN",
	})
	require.NoError(err)

	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(1),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(129112223).String(),
			Currency: &nativeRosettaCurrency,
		},
		Metadata: metadata,
	}, transaction.GetOperations()[1])
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(2),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "6kxCwpuJbDbYqYqw3958ZXCZiwkRQEubsFN6jCsPUSMN",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-129112223).String(),
			Currency: &nativeRosettaCurrency,
		},
		Metadata: metadata,
	}, transaction.GetOperations()[2])
	amountMetadata, err := rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "So11111111111111111111111111111111111111112",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(3),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(-129112223).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 9,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[3])
	amountMetadata, err = rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(4),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(50000000).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 6,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[4])
	amountMetadata, err = rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(5),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(3000).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 6,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[5])

	// validate block reward transaction
	transaction = rosettaTxs[numNativeTransactions]
	ops := transaction.GetOperations()
	rewards := nativeBlock.GetSolanaV2().GetRewards()
	require.Equal(len(rewards), len(ops))
	opIndex := 0
	for _, reward := range rewards {
		lamports := reward.Lamports
		if lamports == 0 {
			continue
		}

		op := ops[opIndex]
		require.Equal(internal.EncodeBase58(reward.GetPubkey()), op.GetAccount().GetAddress())
		expectedAmount := new(big.Int).SetInt64(lamports)
		if lamports > 0 {
			require.Equal("REWARD", op.Type)

		} else {
			require.Equal("FEE", ops[opIndex].Type)
		}
		actualAmount, ok := new(big.Int).SetString(op.GetAmount().GetValue(), 10)
		require.True(ok)
		require.Equal(expectedAmount, actualAmount)
		opIndex++
	}

	// validate the txs, excluding block reward transaction
	for i := 0; i < len(rosettaTxs)-1; i++ {
		s.validateSolanaRosettaTransactionOperations(require, nativeTxs[i], rosettaTxs[i])
	}
}

func (s *solanaRosettaParserTestSuite) TestParseBlock_AccountDeletion() {
	require := testutil.Require(s.T())

	blockHash := "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd"
	blockHeight := uint64(195545750)
	blockTimestamp := testutil.MustTimestamp("2023-05-23T16:06:58Z")
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Hash:         blockHash,
			ParentHash:   "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
			Height:       blockHeight,
			ParentHeight: blockHeight - 1,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_account_deletion.json"),
			},
		},
	}

	nativeBlock, err := s.nativeParser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(nativeBlock)
	nativeTxs := nativeBlock.GetSolanaV2().GetTransactions()
	numNativeTransactions := len(nativeTxs)

	parsed, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(parsed)
	rosettaBlock := parsed.Block
	require.Equal(&rosetta.BlockIdentifier{
		Index: int64(blockHeight),
		Hash:  blockHash,
	}, rosettaBlock.GetBlockIdentifier())
	require.Equal(&rosetta.BlockIdentifier{
		Index: int64(blockHeight - 1),
		Hash:  "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
	}, rosettaBlock.GetParentBlockIdentifier())
	require.Equal(blockTimestamp, rosettaBlock.Timestamp)
	// block reward has 1 additional transaction
	rosettaTxs := rosettaBlock.GetTransactions()
	require.Len(rosettaTxs, numNativeTransactions+1)
	require.NotEmpty(rosettaTxs)

	// tx_id: 4H6uGfHviqT87Au64fv9t7AQHYd75SZPa7NMsX2fZixdcwZgpPB11b4cQq65JVM4EzppLmNPt2d8cGeFY9ys1dBH
	transaction := rosettaTxs[0]
	require.Equal("4H6uGfHviqT87Au64fv9t7AQHYd75SZPa7NMsX2fZixdcwZgpPB11b4cQq65JVM4EzppLmNPt2d8cGeFY9ys1dBH", transaction.GetTransactionIdentifier().GetHash())
	require.Len(transaction.GetOperations(), 6)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(0),
		},
		Type:   "FEE",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-16200).String(),
			Currency: &nativeRosettaCurrency,
		},
	}, transaction.GetOperations()[0])
	metadata, err := rosetta.FromSDKMetadata(map[string]any{
		"instruction_type": "UNKNOWN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(1),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(129112223).String(),
			Currency: &nativeRosettaCurrency,
		},
		Metadata: metadata,
	}, transaction.GetOperations()[1])
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(2),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "6kxCwpuJbDbYqYqw3958ZXCZiwkRQEubsFN6jCsPUSMN",
		},
		Amount: &rosetta.Amount{
			Value:    big.NewInt(-129112223).String(),
			Currency: &nativeRosettaCurrency,
		},
		Metadata: metadata,
	}, transaction.GetOperations()[2])
	amountMetadata, err := rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "So11111111111111111111111111111111111111112",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(3),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(-129112223).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 9,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[3])
	amountMetadata, err = rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(4),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(-50000000).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 6,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[4])
	amountMetadata, err = rosetta.FromSDKMetadata(map[string]any{
		"contract_address": "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
	})
	require.NoError(err)
	require.Equal(&rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(5),
		},
		Type:   "TRANSFER",
		Status: "SUCCESS",
		Account: &rosetta.AccountIdentifier{
			Address: "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
		},
		Amount: &rosetta.Amount{
			Value: big.NewInt(50000000).String(),
			Currency: &rosetta.Currency{
				Symbol:   unknownCurrencySymbol,
				Decimals: 6,
				Metadata: amountMetadata,
			},
		},
		Metadata: metadata,
	}, transaction.GetOperations()[5])

	// validate block reward transaction
	transaction = rosettaTxs[numNativeTransactions]
	ops := transaction.GetOperations()
	rewards := nativeBlock.GetSolanaV2().GetRewards()
	require.Equal(len(rewards), len(ops))
	opIndex := 0
	for _, reward := range rewards {
		lamports := reward.Lamports
		if lamports == 0 {
			continue
		}

		op := ops[opIndex]
		require.Equal(internal.EncodeBase58(reward.GetPubkey()), op.GetAccount().GetAddress())
		expectedAmount := new(big.Int).SetInt64(lamports)
		if lamports > 0 {
			require.Equal("REWARD", op.Type)

		} else {
			require.Equal("FEE", ops[opIndex].Type)
		}
		actualAmount, ok := new(big.Int).SetString(op.GetAmount().GetValue(), 10)
		require.True(ok)
		require.Equal(expectedAmount, actualAmount)
		opIndex++
	}

	// validate the txs, excluding block reward transaction
	for i := 0; i < len(rosettaTxs)-1; i++ {
		s.validateSolanaRosettaTransactionOperations(require, nativeTxs[i], rosettaTxs[i])
	}
}

func (s *solanaRosettaParserTestSuite) TestParsedBlock_OldTag() {
	require := testutil.Require(s.T())

	blockHash := "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd"
	blockHeight := uint64(195545750)
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          1,
			Hash:         blockHash,
			ParentHash:   "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
			Height:       blockHeight,
			ParentHeight: blockHeight - 1,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_195545749_v2.json"),
			},
		},
	}

	parsed, err := s.parser.ParseBlock(context.Background(), block)
	require.Error(err)
	require.Nil(parsed)
	require.ErrorIs(err, internal.ErrNotImplemented)
}

func (s *solanaRosettaParserTestSuite) TestParsedBlock_SkippedBlock() {
	require := testutil.Require(s.T())

	blockHeight := uint64(195545750)
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:     2,
			Skipped: true,
			Height:  blockHeight,
		},
	}

	parsed, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(&api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Index: int64(blockHeight),
			},
		},
	}, parsed)
}

func (s *solanaRosettaParserTestSuite) TestParsedBlock_NoPreBalances() {
	require := testutil.Require(s.T())

	blockHash := "7gjbq5pn7Pt7qFEaU8xwqetCXnQkN4jJX4SDmcgxDRFS"
	blockHeight := uint64(108931)
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Hash:         blockHash,
			ParentHash:   "6ePKqrZHpPNfS3Q18ykDDd6YT79fYPhAo1Y3eSsSGDgP",
			Height:       blockHeight,
			ParentHeight: blockHeight - 1,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: fixtures.MustReadFile("parser/solana/block_no_preBalances.json"),
			},
		},
	}

	nativeBlock, err := s.nativeParser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(nativeBlock)
	nativeTxs := nativeBlock.GetSolanaV2().GetTransactions()
	require.Len(nativeTxs, 1)

	parsed, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(parsed)
	rosettaBlock := parsed.Block
	rosettaTxs := rosettaBlock.GetTransactions()
	require.Len(rosettaTxs, 1)
	require.NotEmpty(rosettaTxs)
	require.Empty(rosettaTxs[0].Operations)
	require.Equal("5UdthFFkqxNe9zwBBszrkQzY2TninhkqYkokmwC2LmCQQfc2NzA11Ue92aucFfDFEFoth6N9eHGg6mNayYRomwJK", rosettaTxs[0].TransactionIdentifier.Hash)
}
