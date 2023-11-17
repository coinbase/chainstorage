package solana

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestNewSolanaRosettaChecker_Success(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		fx.Populate(&parser),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
	)
	defer app.Close()
	require.NotNil(parser)

	ctx := context.Background()

	tests := []struct {
		height     uint64
		hash       string
		parentHash string
		timestamp  string
	}{
		{
			height:     195545749,
			hash:       "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd",
			parentHash: "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
		},
		{
			height:     217003034,
			hash:       "7CRnkUB7s6deSZ5Pro4xDcebU8vZj3eXBTqeCDMzBBW4",
			parentHash: "TgZ5L5YBSKVPm6kEncCeGxxn19QNKBJqL2yhPcLcYAo",
		},
		{
			height:     220114808,
			hash:       "FgqEnHGWvY7gNH5oL7a4G1TajF5jkQHfa7fueWiwqBT",
			parentHash: "5SPTxbnMVUVtPVnG5ck4EdVYXY2YP1FZgSaKr6jVV9wR",
		},
	}
	for _, test := range tests {
		height := test.height
		block := &api.Block{
			Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
			Network:    common.Network_NETWORK_SOLANA_MAINNET,
			Metadata: &api.BlockMetadata{
				Tag:          2,
				Hash:         test.hash,
				ParentHash:   test.parentHash,
				Height:       height,
				ParentHeight: height - 1,
			},
			Blobdata: &api.Block_Solana{
				Solana: &api.SolanaBlobdata{
					Header: fixtures.MustReadFile(fmt.Sprintf("parser/solana/block_%d_v2.json", height)),
				},
			},
		}

		nativeBlock, err := parser.ParseNativeBlock(ctx, block)
		require.NoError(err)
		require.NotNil(nativeBlock)
		rosettaBlock, err := parser.ParseRosettaBlock(ctx, block)
		require.NoError(err)
		require.NotNil(rosettaBlock)

		err = parser.ValidateRosettaBlock(ctx, &api.ValidateRosettaBlockRequest{
			NativeBlock: nativeBlock,
		}, rosettaBlock)
		require.NoError(err)
	}
}

func TestGetTokenBalanceAmountMap(t *testing.T) {
	require := testutil.Require(t)
	balances := []*api.SolanaTokenBalance{
		{
			AccountIndex: 4,
			Mint:         "So11111111111111111111111111111111111111112",
			Owner:        "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
			TokenAmount: &api.SolanaTokenAmount{
				Amount:         "138024920000",
				Decimals:       9,
				UiAmountString: "138.02492",
			},
		},
		{
			AccountIndex: 5,
			Mint:         "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
			Owner:        "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
			TokenAmount: &api.SolanaTokenAmount{
				Amount:         "50000000",
				Decimals:       6,
				UiAmountString: "50",
			},
		},
		{
			AccountIndex: 5,
			Mint:         "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
			Owner:        "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
			TokenAmount: &api.SolanaTokenAmount{
				Amount:         "60000000",
				Decimals:       6,
				UiAmountString: "60",
			},
		},
		{
			AccountIndex: 6,
			Mint:         "Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN",
			Owner:        "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
			TokenAmount: &api.SolanaTokenAmount{
				Amount:         "67856549196",
				Decimals:       6,
				UiAmountString: "67856.549196",
			},
		},
	}
	accountKeys := []*api.AccountKey{
		{
			Pubkey:   "DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf",
			Signer:   true,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "8t6JV7jwgwyvezcsvZf4CZCDCUf1xD9QKFo9sDx8qjaK",
			Signer:   false,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "B2U7PdW43XWb5jEGZzj3rvo8qshsNqVJhHR7wY8bgQGC",
			Signer:   false,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H",
			Signer:   false,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "6kxCwpuJbDbYqYqw3958ZXCZiwkRQEubsFN6jCsPUSMN",
			Signer:   false,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "DBVq4zJqEiWyU12rgW5pQhczZ3gokcYt7vuw7DwAHPEM",
			Signer:   false,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "DpYtvzAh5xf26YNiuAaZPqeHKbWi6PeBG5eEiwpe84BR",
			Signer:   false,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "5tkja5oMTd5GXrhrXL2ox3LcgkrmTfMqpQ78QaUZTTmT",
			Signer:   false,
			Source:   "transaction",
			Writable: true,
		},
		{
			Pubkey:   "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
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
			Pubkey:   "11111111111111111111111111111111",
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
		{
			Pubkey:   "JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo",
			Signer:   false,
			Source:   "transaction",
			Writable: false,
		},
		{
			Pubkey:   "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
			Signer:   false,
			Source:   "transaction",
			Writable: false,
		},
		{
			Pubkey:   "8h37dWzr7h9SDq6PAhjNtL5cpcPbcgL2YpQ7f8MXhFQR",
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
	}

	expectedBalanceMap := map[string]*big.Int{
		"So11111111111111111111111111111111111111112#13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H":  big.NewInt(138024920000),
		"Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN#DxCsN4jatqeTW2pj6fR55TrNqoCcoo8UNAy8xosheeVf": big.NewInt(110000000),
		"Fishy64jCaa3ooqXw7BHtKvYD8BTkSyAPh6RNE3xZpcN#13ztuDnY8HvZCkBK7XwRv5jT5fd9KS7bgdXp1mL1Gd3H": big.NewInt(67856549196),
	}
	actualBalanceMap, err := GetTokenBalanceAmountMap(balances, accountKeys)
	require.NoError(err)
	require.Equal(expectedBalanceMap, actualBalanceMap)
}
