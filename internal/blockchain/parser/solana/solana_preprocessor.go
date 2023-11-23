package solana

import (
	"context"
	"fmt"
	"math/big"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

type (
	solanaPreProcessor   struct{}
	solanaRosettaChecker struct{}
)

const (
	timestampGapSeconds = 5
)

func NewSolanaChecker(params internal.ParserParams) (internal.Checker, error) {
	return internal.NewChecker(params, &solanaPreProcessor{}, &solanaRosettaChecker{})
}

func (p *solanaPreProcessor) PreProcessNativeBlock(expected, actual *api.NativeBlock) error {
	return nil
}

func (c *solanaRosettaChecker) ValidateRosettaBlock(ctx context.Context, req *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	nativeBlock := req.GetNativeBlock()
	if nativeBlock == nil {
		return xerrors.New("native block not set")
	}

	if nativeBlock.Tag < 2 {
		return internal.ErrNotImplemented
	}

	// validate block metadata
	if nativeBlock.Skipped {
		return nil
	}

	if nativeBlock.Hash != actualRosettaBlock.Block.BlockIdentifier.Hash {
		return xerrors.Errorf("block hash mismatch, expected=%s, actual=%s", nativeBlock.Hash, actualRosettaBlock.Block.BlockIdentifier.Hash)
	}

	if nativeBlock.Height != uint64(actualRosettaBlock.Block.BlockIdentifier.Index) {
		return xerrors.Errorf("block height mismatch, expected=%d, actual=%d", nativeBlock.Height, actualRosettaBlock.Block.BlockIdentifier.Index)
	}

	if nativeBlock.ParentHash != actualRosettaBlock.Block.ParentBlockIdentifier.Hash {
		return xerrors.Errorf("block parent hash mismatch, expected=%s, actual=%s", nativeBlock.ParentHash, actualRosettaBlock.Block.ParentBlockIdentifier.Hash)
	}

	if nativeBlock.ParentHeight != uint64(actualRosettaBlock.Block.ParentBlockIdentifier.Index) {
		return xerrors.Errorf("block parent height mismatch, expected=%d, actual=%d", nativeBlock.ParentHeight, actualRosettaBlock.Block.ParentBlockIdentifier.Index)
	}

	if !nativeBlock.Timestamp.AsTime().Equal(actualRosettaBlock.Block.Timestamp.AsTime()) {
		return xerrors.Errorf("block timestamp mismatch, expected=%s, actual=%s", nativeBlock.Timestamp.AsTime(), actualRosettaBlock.Block.Timestamp.AsTime())
	}

	// validate transactions
	rosettaTxs := actualRosettaBlock.GetBlock().GetTransactions()
	nativeTxs := nativeBlock.GetSolanaV2().GetTransactions()

	rewards := nativeBlock.GetSolanaV2().GetRewards()
	var expectedNumberOfTransactions int
	if len(rewards) == 0 {
		expectedNumberOfTransactions = len(nativeTxs)
	} else {
		expectedNumberOfTransactions = len(nativeTxs) + 1
	}

	if len(rosettaTxs) != expectedNumberOfTransactions {
		return xerrors.Errorf("block mismatching number of transactions, expected=%d, actual=%d", expectedNumberOfTransactions, len(rosettaTxs))
	}

	for i := 0; i < len(nativeTxs); i++ {
		if err := c.validateRosettaTransaction(nativeTxs[i], rosettaTxs[i]); err != nil {
			return xerrors.Errorf("failed to validate rosetta transaction(tx=%s): %w", nativeTxs[i].TransactionId, err)
		}
	}

	if len(rewards) == 0 {
		return nil
	}

	// check the last transaction is a reward transaction
	rewardTx := rosettaTxs[len(rosettaTxs)-1]
	if rewardTx.TransactionIdentifier.Hash != req.GetNativeBlock().GetHash() {
		return xerrors.Errorf("invalid reward transaction hash=%s, expected=%s", rewardTx.TransactionIdentifier.Hash, req.GetNativeBlock().GetHash())
	}

	if err := c.validateRewardTransaction(rewardTx, nativeBlock.GetSolanaV2().GetRewards()); err != nil {
		return xerrors.Errorf("failed to validate reward transaction: %w", err)
	}

	return nil
}

func (c *solanaRosettaChecker) validateRosettaTransaction(
	nativeTx *api.SolanaTransactionV2,
	rosettaTx *rosetta.Transaction,
) error {
	ops := rosettaTx.GetOperations()
	accountKeys := nativeTx.GetPayload().GetMessage().GetAccountKeys()
	preBalances := nativeTx.GetMeta().GetPreBalances()
	postBalances := nativeTx.GetMeta().GetPostBalances()
	preTokenBalances := nativeTx.GetMeta().GetPreTokenBalances()
	postTokenBalances := nativeTx.GetMeta().GetPostTokenBalances()

	if len(accountKeys) == 0 {
		return xerrors.New("native block has invalid account keys")
	}

	if len(preBalances) != len(postBalances) || len(postBalances) != len(accountKeys) {
		return xerrors.New("native block has mismatching pre/post balances")
	}

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
	if err != nil {
		return xerrors.Errorf("failed to get token balance amount map: %w", err)
	}

	expectedPostTokenBalances, err := GetTokenBalanceAmountMap(postTokenBalances, accountKeys)
	if err != nil {
		return xerrors.Errorf("failed to get token balance amount map: %w", err)
	}

	for i, op := range ops {
		if op.OperationIdentifier.Index != int64(i) {
			return xerrors.Errorf("invalid transaction operation index, expected=%d, actual=%d", i, op.OperationIdentifier.Index)
		}

		if op.Status != OpStatusSuccess {
			continue
		}

		symbol := op.Amount.Currency.Symbol
		account := op.Account.Address
		if len(account) == 0 {
			return xerrors.New("invalid account address")
		}

		amount, ok := new(big.Int).SetString(op.Amount.Value, 10)
		if !ok {
			return xerrors.Errorf("invalid amount=%s", op.Amount.Value)
		}

		switch symbol {
		case NativeSymbol:
			accountIndex, ok := accountKeyIndexMap[account]
			if !ok {
				return xerrors.Errorf("unknown account=%s", account)
			}
			actualPostBalances[accountIndex].Add(actualPostBalances[accountIndex], amount)
		case UnknownCurrencySymbol:
			metadata, err := rosetta.ToSDKMetadata(op.Amount.Currency.Metadata)
			if err != nil {
				return xerrors.Errorf("invalid amount currency metadata: %w", err)
			}
			contractAddress, ok := metadata[ContractAddressAmountMetadataKey]
			if !ok {
				return xerrors.New("contract address not found in amount currency metadata")
			}

			key := GetTokenBalanceMapKey(contractAddress.(string), account)
			val, ok := actualPostTokenBalances[key]
			if !ok {
				val = big.NewInt(0)
			}
			actualPostTokenBalances[key] = val.Add(val, amount)
		default:
			return xerrors.Errorf("unknown currency symbol=%s", symbol)
		}
	}

	for i := range accountKeys {
		if expectedPostBalances[i].Cmp(actualPostBalances[i]) != 0 {
			return xerrors.Errorf("balance mismatch for account=%s, expected=%d, actual=%d", accountKeys[i].Pubkey, expectedPostBalances[i], actualPostBalances[i])
		}
	}

	for key, expected := range expectedPostTokenBalances {
		actual, ok := actualPostTokenBalances[key]
		if !ok || expected.Cmp(actual) != 0 {
			return xerrors.Errorf("token balance mismatch for key=%s, expected=%d, actual=%d", key, expected, actual)
		}
	}

	for key, actual := range actualPostTokenBalances {
		expected, ok := expectedPostTokenBalances[key]
		if !ok {
			if actual.Cmp(big.NewInt(0)) != 0 {
				return xerrors.Errorf("token balance mismatch for key=%s, expected=%d, actual=%d", key, 0, actual)
			}
		} else {
			if expected.Cmp(actual) != 0 {
				return xerrors.Errorf("token balance mismatch for key=%s, expected=%d, actual=%d", key, expected, actual)
			}
		}
	}

	return nil
}

func (c *solanaRosettaChecker) validateRewardTransaction(rewardTx *rosetta.Transaction, nativeRewards []*api.SolanaReward) error {
	opIndex := 0
	ops := rewardTx.GetOperations()
	for _, reward := range nativeRewards {
		lamports := reward.Lamports
		if lamports == 0 {
			continue
		}

		op := ops[opIndex]
		if op.Status != OpStatusSuccess {
			return xerrors.Errorf("invalid operation status=%s", op.Status)
		}

		if internal.EncodeBase58(reward.GetPubkey()) != op.GetAccount().GetAddress() {
			return xerrors.Errorf("reward operation account mismatch, expected=%s, actual=%s", internal.EncodeBase58(reward.GetPubkey()), op.GetAccount().GetAddress())
		}

		expectedAmount := new(big.Int).SetInt64(lamports)
		if (lamports > 0 && op.Type != OpTypeReward) || (lamports < 0 && op.Type != OpTypeFee) {
			return xerrors.Errorf("invalid operation type in reward op=%+v", op)
		}

		actualAmount, ok := new(big.Int).SetString(op.GetAmount().GetValue(), 10)
		if !ok {
			return xerrors.Errorf("invalid amount=%s", op.GetAmount().GetValue())
		}

		if expectedAmount.Cmp(actualAmount) != 0 {
			return xerrors.Errorf("operation amount mismatch, expected=%d, actual=%d", expectedAmount, actualAmount)
		}
		opIndex++
	}
	return nil
}

func GetTokenBalanceAmountMap(balances []*api.SolanaTokenBalance, accountKeys []*api.AccountKey) (map[string]*big.Int, error) {
	balanceMap := make(map[string]*big.Int)
	for _, balance := range balances {
		amount, ok := new(big.Int).SetString(balance.TokenAmount.Amount, 10)
		if !ok {
			return nil, xerrors.Errorf("invalid amount=%s", balance.TokenAmount.Amount)
		}

		var address string
		if len(balance.Owner) != 0 {
			address = balance.Owner
		} else {
			address = accountKeys[balance.AccountIndex].GetPubkey()
		}

		if len(address) == 0 {
			return nil, xerrors.New("invalid account address")
		}

		key := GetTokenBalanceMapKey(balance.Mint, address)
		val, ok := balanceMap[key]
		if !ok {
			val = big.NewInt(0)
		}
		balanceMap[key] = val.Add(val, amount)
	}
	return balanceMap, nil
}

func GetTokenBalanceMapKey(mint string, address string) string {
	return fmt.Sprintf("%s#%s", mint, address)
}
