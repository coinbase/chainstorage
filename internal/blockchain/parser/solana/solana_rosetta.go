package solana

import (
	"context"
	"fmt"
	"math/big"

	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

const (
	OpStatusFailure = "FAILURE"
	OpStatusSuccess = "SUCCESS"

	OpTypeFee      = "FEE"
	OpTypeTransfer = "TRANSFER"
	OpTypeReward   = "REWARD"

	NativeSymbol          = "SOL"
	UnknownCurrencySymbol = "UNKNOWN_CURRENCY"

	instructionTypeUnknown = "UNKNOWN"

	// TODO: align with other rosetta implementation
	ContractAddressAmountMetadataKey = "contract_address"
	instructionTypeMetadataKey       = "instruction_type"
	isVoteTransactionMetadataKey     = "is_solana_vote_transaction"
)

type (
	solanaRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser internal.NativeParser
	}

	// parseInstructionResult:
	// containsThirdPartyPrograms and isVoteTransaction are mutual exclusive
	// as a transaction that only contains one VoteVote/VoteCompactUpdateVoteState instruction cannot contain third party programs
	parseInstructionResult struct {
		ops                        []*rosetta.Operation
		containsThirdPartyPrograms bool // whether the transaction contains third party programs
		isVoteTransaction          bool // whether the transaction contains only one VoteVote/VoteCompactUpdateVoteState instruction
	}
)

var (
	nativeRosettaCurrency = rosetta.Currency{
		Symbol:   NativeSymbol,
		Decimals: 9, // 1 sol = 10^9 Lamports
	}
)

func NewSolanaRosettaParser(
	params internal.ParserParams,
	nativeParser internal.NativeParser,
	opts ...internal.ParserFactoryOption,
) (internal.RosettaParser, error) {
	return &solanaRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

func (p *solanaRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, xerrors.New("metadata not found")
	}

	if metadata.Skipped {
		return &api.RosettaBlock{
			Block: &rosetta.Block{
				BlockIdentifier: &rosetta.BlockIdentifier{
					Index: int64(rawBlock.GetMetadata().GetHeight()),
				},
			},
		}, nil
	}

	if metadata.Tag < 2 {
		return nil, internal.ErrNotImplemented
	}

	nativeBlock, err := p.nativeParser.ParseBlock(ctx, rawBlock)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block into native format: %w", err)
	}

	block := nativeBlock.GetSolanaV2()
	if block == nil {
		return nil, xerrors.New("failed to find solana block")
	}

	blockIdentifier := &rosetta.BlockIdentifier{
		Index: int64(rawBlock.GetMetadata().GetHeight()),
		Hash:  rawBlock.GetMetadata().GetHash(),
	}

	parentBlockIdentifier := &rosetta.BlockIdentifier{
		Index: int64(rawBlock.GetMetadata().GetParentHeight()),
		Hash:  rawBlock.GetMetadata().GetParentHash(),
	}

	transactions, err := p.getRosettaTransactions(block)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block transactions: %w", err)
	}

	rewardOps, err := p.parseRewards(block.GetRewards(), OpStatusSuccess, 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block rewards: %w", err)
	}

	if len(rewardOps) != 0 {
		// as block rewards is not a real transaction, use the block hash as the identifier.
		transactions = append(transactions, &rosetta.Transaction{
			TransactionIdentifier: &rosetta.TransactionIdentifier{
				Hash: blockIdentifier.Hash,
			},
			Operations: rewardOps,
		})

	}

	return &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier:       blockIdentifier,
			ParentBlockIdentifier: parentBlockIdentifier,
			Timestamp:             block.GetHeader().BlockTime,
			Transactions:          transactions,
			Metadata:              nil,
		},
	}, nil
}

func (p *solanaRosettaParserImpl) getRosettaTransactions(block *api.SolanaBlockV2) ([]*rosetta.Transaction, error) {
	rosettaTransactions := make([]*rosetta.Transaction, len(block.Transactions))
	for i, tx := range block.GetTransactions() {
		var ops []*rosetta.Operation
		status := OpStatusSuccess
		if len(tx.GetMeta().GetErr()) != 0 {
			status = OpStatusFailure
		}

		feeOps, err := p.feeOps(tx, len(ops))
		if err != nil {
			return nil, xerrors.Errorf("failed to parse fee operations: %w", err)
		}
		ops = append(ops, feeOps...)

		// parse tx level rewards
		rewards := tx.GetMeta().GetRewards()
		var rewardOps []*rosetta.Operation
		if len(rewards) > 0 {
			rewardOps, err = p.parseRewards(rewards, status, len(ops))
			if err != nil {
				return nil, xerrors.Errorf("failed to parse rewards for tx=%v: %w", tx.GetTransactionId(), err)
			}
			ops = append(ops, rewardOps...)
		}

		parseResult, err := p.parseInstructionsForTransaction(tx, status, len(ops))
		if err != nil {
			return nil, xerrors.Errorf("failed to parse instructions: %w", err)
		}

		if parseResult.containsThirdPartyPrograms {
			thirdPartyOps, err := p.processThirdPartyTransaction(tx, status, ops)
			if err != nil {
				return nil, xerrors.Errorf("failed to process third party transaction: %w", err)
			}

			// ops entirely derived from processThirdPartyTransaction
			ops = thirdPartyOps
		} else {
			ops = append(ops, parseResult.ops...)
		}

		var metadata map[string]*anypb.Any
		if parseResult.isVoteTransaction {
			metadata, err = rosetta.FromSDKMetadata(map[string]any{
				isVoteTransactionMetadataKey: true,
			})

			if err != nil {
				return nil, xerrors.Errorf("failed to convert metadata: %w", err)
			}
		}

		rosettaTransactions[i] = &rosetta.Transaction{
			TransactionIdentifier: &rosetta.TransactionIdentifier{
				Hash: tx.GetTransactionId(),
			},
			Operations:          ops,
			RelatedTransactions: nil,
			Metadata:            metadata,
		}
	}

	return rosettaTransactions, nil
}

func (p *solanaRosettaParserImpl) parseRewards(rewards []*api.SolanaReward, status string, startIndex int) ([]*rosetta.Operation, error) {
	ops := make([]*rosetta.Operation, 0, len(rewards))
	for _, reward := range rewards {
		lamports := reward.GetLamports()
		if lamports == 0 {
			continue
		} else {
			opType := OpTypeReward
			if lamports < 0 {
				// lamports can be negative.
				// negative value usually means rent collected. treat it as a fee op
				opType = OpTypeFee
			}
			fromAccount := internal.EncodeBase58(reward.GetPubkey())
			ops = append(ops, &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(startIndex + len(ops)),
				},
				Type:   opType,
				Status: status,
				Account: &rosetta.AccountIdentifier{
					Address: fromAccount,
				},
				Amount: &rosetta.Amount{
					Value:    big.NewInt(lamports).String(),
					Currency: &nativeRosettaCurrency,
				},
				Metadata: map[string]*anypb.Any{},
			})
		}
	}

	return ops, nil
}

func (p *solanaRosettaParserImpl) parseInstructionsForTransaction(tx *api.SolanaTransactionV2, status string, startIndex int) (*parseInstructionResult, error) {
	var ops []*rosetta.Operation
	instructions := tx.GetPayload().GetMessage().GetInstructions()
	innerInstructions := tx.GetMeta().GetInnerInstructions()

	isVoteTransaction := false
	if len(instructions) == 1 && len(innerInstructions) == 0 {
		instruction := instructions[0]
		instructionType := instruction.GetVoteProgram().GetInstructionType()
		if instruction.GetProgram() == api.SolanaProgram_VOTE && (instructionType == api.SolanaVoteProgram_VOTE ||
			instructionType == api.SolanaVoteProgram_COMPACT_UPDATE_VOTE_STATE) {
			isVoteTransaction = true
		}
	}

	for _, instruction := range instructions {
		parseResult, err := p.parseInstruction(instruction, tx, status, startIndex+len(ops))
		if err != nil {
			return nil, xerrors.Errorf("failed to parse instruction=%+v for tx=%s: %w", instruction, tx.TransactionId, err)
		}

		if parseResult.containsThirdPartyPrograms {
			return &parseInstructionResult{
				ops:                        nil,
				containsThirdPartyPrograms: true,
			}, nil
		}
		ops = append(ops, parseResult.ops...)
	}

	// parse inner instructions
	for _, innerInstruction := range innerInstructions {
		for _, instruction := range innerInstruction.GetInstructions() {
			parseResult, err := p.parseInstruction(instruction, tx, status, startIndex+len(ops))
			if err != nil {
				return nil, xerrors.Errorf("failed to parse innerInstruction=%v for tx=%s: %w", instruction, tx.TransactionId, err)
			}

			if parseResult.containsThirdPartyPrograms {
				return &parseInstructionResult{
					ops:                        nil,
					containsThirdPartyPrograms: true,
				}, nil
			}
			ops = append(ops, parseResult.ops...)
		}
	}

	return &parseInstructionResult{
		ops:                        ops,
		containsThirdPartyPrograms: false,
		isVoteTransaction:          isVoteTransaction,
	}, nil
}

func (p *solanaRosettaParserImpl) processThirdPartyTransaction(
	tx *api.SolanaTransactionV2,
	status string,
	ops []*rosetta.Operation,
) ([]*rosetta.Operation, error) {
	ops, err := p.processBalancesForThirdPartyProgram(tx, status, ops)
	if err != nil {
		return nil, xerrors.Errorf("failed to process balance transfer: %w", err)
	}

	ops, err = p.processTokenBalancesForThirdPartyProgram(tx, status, ops)
	if err != nil {
		return nil, xerrors.Errorf("failed to process token balance transfer: %w", err)
	}
	return ops, nil
}

func (p *solanaRosettaParserImpl) processBalancesForThirdPartyProgram(
	tx *api.SolanaTransactionV2,
	status string,
	ops []*rosetta.Operation,
) ([]*rosetta.Operation, error) {
	accountKeys := tx.GetPayload().GetMessage().GetAccountKeys()
	preBalances := tx.GetMeta().GetPreBalances()
	postBalances := tx.GetMeta().GetPostBalances()
	if len(preBalances) == 0 && len(postBalances) == 0 {
		return nil, nil
	}

	if len(accountKeys) != len(preBalances) || len(accountKeys) != len(postBalances) {
		return nil, xerrors.Errorf("unexpected lengths of accountKeys in tx=%v", tx.GetTransactionId())
	}

	// convert to bigInt for easy comparison between amount value and pre/postBalances
	observedBalances := make([]*big.Int, len(preBalances))
	expectedBalances := make([]*big.Int, len(postBalances))
	for i := range preBalances {
		observedBalances[i] = new(big.Int).SetUint64(preBalances[i])
		expectedBalances[i] = new(big.Int).SetUint64(postBalances[i])
	}

	accountKeyIndexMap := make(map[string]int)
	for i, account := range accountKeys {
		accountKeyIndexMap[account.Pubkey] = i
	}

	// calculate observedBalances to take existing ops into account
	for _, op := range ops {
		if op.Amount == nil || len(op.Amount.Value) == 0 {
			// Skip as it's not a balance changing operation
			continue
		}

		account := op.Account.Address
		accountIndex, ok := accountKeyIndexMap[account]
		if !ok {
			return nil, xerrors.Errorf("address=%s not found in account map", account)
		}

		amount, ok := new(big.Int).SetString(op.Amount.Value, 10)
		if !ok {
			return nil, xerrors.Errorf("failed to convert amount from string=%s", op.Amount.Value)
		}
		observedBalances[accountIndex].Add(observedBalances[accountIndex], amount)
	}

	for i := range observedBalances {
		observed := observedBalances[i]
		expected := expectedBalances[i]
		c := observed.Cmp(expected)
		if c == 0 {
			// observed matches expected, no implicit balance-change operation
			continue
		}

		account := accountKeys[i].GetPubkey()
		metadata, err := rosetta.FromSDKMetadata(map[string]any{
			instructionTypeMetadataKey: instructionTypeUnknown,
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to convert metadata: %w", err)
		}

		op := &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: int64(len(ops)),
			},
			Type: OpTypeTransfer,
			Account: &rosetta.AccountIdentifier{
				Address: account,
			},
			Amount: &rosetta.Amount{
				Value:    new(big.Int).Sub(expected, observed).String(),
				Currency: &nativeRosettaCurrency,
			},
			Status:   status,
			Metadata: metadata,
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func (p *solanaRosettaParserImpl) processTokenBalancesForThirdPartyProgram(
	tx *api.SolanaTransactionV2,
	status string,
	ops []*rosetta.Operation,
) ([]*rosetta.Operation, error) {
	accountKeys := tx.GetPayload().GetMessage().GetAccountKeys()
	preTokenBalances := tx.GetMeta().GetPreTokenBalances()
	postTokenBalances := tx.GetMeta().GetPostTokenBalances()

	postTokenBalanceMap := getTokenBalanceLookUpMap(postTokenBalances)
	for _, preTokenBalance := range preTokenBalances {
		preAmount, ok := new(big.Int).SetString(preTokenBalance.TokenAmount.Amount, 10)
		if !ok {
			return nil, xerrors.Errorf("failed to parse balance=%+v", preTokenBalance)
		}

		var postAmount *big.Int
		postTokenBalance, ok := postTokenBalanceMap[makeTokenBalanceLookUpKey(preTokenBalance)]
		if !ok {
			// account closed
			postAmount = big.NewInt(0)
		} else {
			// transfer
			postAmount, ok = new(big.Int).SetString(postTokenBalance.TokenAmount.Amount, 10)
			if !ok {
				return nil, xerrors.Errorf("failed to parse TokenAmount=%+v", postTokenBalance.TokenAmount.Amount)
			}
		}

		c := preAmount.Cmp(postAmount)
		if c == 0 {
			// no balance change
			continue
		}

		if postTokenBalance != nil && postTokenBalance.TokenAmount.Decimals != preTokenBalance.TokenAmount.Decimals {
			return nil, xerrors.Errorf("token amount decimals do not match. preTokenBalance.Decimals=%d, postTokenBalance.Decimals=%d",
				preTokenBalance.TokenAmount.Decimals,
				postTokenBalance.TokenAmount.Decimals,
			)
		}

		transferOp, err := p.tokenTransferOp(preTokenBalance, accountKeys, new(big.Int).Sub(postAmount, preAmount), status, len(ops))
		if err != nil {
			return nil, xerrors.Errorf("failed to generate token transfer op for balance=%+v: %w", preTokenBalance, err)
		}
		ops = append(ops, transferOp)
	}

	// handle new account creation, where there is no preTokenBalances for the new account
	preTokenBalanceMap := getTokenBalanceLookUpMap(preTokenBalances)
	for _, postTokenBalance := range postTokenBalances {
		postAmount, ok := new(big.Int).SetString(postTokenBalance.TokenAmount.Amount, 10)
		if !ok {
			return nil, xerrors.Errorf("failed to parse balance=%+v", postTokenBalance)
		}

		_, ok = preTokenBalanceMap[makeTokenBalanceLookUpKey(postTokenBalance)]
		if ok {
			// skip accounts that are already handled in previous steps
			continue
		}

		transferOp, err := p.tokenTransferOp(postTokenBalance, accountKeys, postAmount, status, len(ops))
		if err != nil {
			return nil, xerrors.Errorf("failed to generate token transfer op for balance=%+v: %w", postTokenBalance, err)
		}
		ops = append(ops, transferOp)
	}

	return ops, nil
}

func (p *solanaRosettaParserImpl) parseInstruction(
	instruction *api.SolanaInstructionV2,
	tx *api.SolanaTransactionV2,
	status string,
	startIndex int,
) (*parseInstructionResult, error) {
	var err error
	var parseResult *parseInstructionResult
	switch instruction.GetProgram() {
	case api.SolanaProgram_VOTE:
		parseResult, err = p.parseVoteProgram(instruction, status, startIndex)
	case api.SolanaProgram_STAKE:
		parseResult, err = p.parseStakeProgram(instruction, status, startIndex, tx)
	case api.SolanaProgram_SYSTEM:
		parseResult, err = p.parseSystemProgram(instruction, status, startIndex)
	default:
		return &parseInstructionResult{
			containsThirdPartyPrograms: true,
		}, nil
	}

	if err != nil {
		return nil, xerrors.Errorf("failed to parse instruction=%+v: %w", instruction, err)
	}

	if parseResult.containsThirdPartyPrograms {
		return &parseInstructionResult{
			containsThirdPartyPrograms: true,
		}, nil
	}

	return &parseInstructionResult{
		ops: parseResult.ops,
	}, nil
}

func (p *solanaRosettaParserImpl) parseStakeProgram(
	instruction *api.SolanaInstructionV2,
	status string,
	startIndex int,
	transaction *api.SolanaTransactionV2,
) (*parseInstructionResult, error) {
	var ops []*rosetta.Operation
	var err error
	instructionType := instruction.GetStakeProgram().GetInstructionType()
	var source string
	var destination string
	var lamports uint64
	switch instructionType {
	case api.SolanaStakeProgram_INITIALIZE, api.SolanaStakeProgram_DELEGATE, api.SolanaStakeProgram_DEACTIVATE:
		return &parseInstructionResult{}, nil
	case api.SolanaStakeProgram_SPLIT:
		split := instruction.GetStakeProgram().GetSplit()
		source, destination, lamports = split.GetStakeAccount(), split.GetNewSplitAccount(), split.GetLamports()
	case api.SolanaStakeProgram_WITHDRAW:
		withdraw := instruction.GetStakeProgram().GetWithdraw()
		source, destination, lamports = withdraw.GetStakeAccount(), withdraw.GetDestination(), withdraw.GetLamports()
	case api.SolanaStakeProgram_MERGE:
		// SolanaStakeProgram_MERGE is NOT parsed here,
		// because lamports need to be extracted from both preBalances and existing ops prior to this instruction.
		// defer to use third party program parsing if there is a stake merge instruction.
	}

	if len(source) == 0 || len(destination) == 0 {
		return &parseInstructionResult{
			containsThirdPartyPrograms: true,
		}, nil
	}

	ops, err = p.transferOps(source, destination, status, lamports, startIndex, instructionType.String())
	if err != nil {
		return nil, xerrors.Errorf("failed to parse instruction=%v: %w", instructionType, err)
	}

	return &parseInstructionResult{
		ops: ops,
	}, nil
}

func (p *solanaRosettaParserImpl) parseVoteProgram(
	instruction *api.SolanaInstructionV2,
	status string,
	startIndex int,
) (*parseInstructionResult, error) {
	var ops []*rosetta.Operation
	var err error
	instructionType := instruction.GetVoteProgram().GetInstructionType()
	var source string
	var destination string
	var lamports uint64
	switch instructionType {
	case api.SolanaVoteProgram_INITIALIZE, api.SolanaVoteProgram_VOTE, api.SolanaVoteProgram_COMPACT_UPDATE_VOTE_STATE:
		return &parseInstructionResult{}, nil
	case api.SolanaVoteProgram_WITHDRAW:
		withdraw := instruction.GetVoteProgram().GetWithdraw()
		source, destination, lamports = withdraw.GetVoteAccount(), withdraw.GetDestination(), withdraw.Lamports
	}

	if len(source) == 0 || len(destination) == 0 {
		return &parseInstructionResult{
			containsThirdPartyPrograms: true,
		}, nil
	}

	ops, err = p.transferOps(source, destination, status, lamports, startIndex, instructionType.String())
	if err != nil {
		return nil, xerrors.Errorf("failed to parse instruction=%v: %w", instructionType, err)
	}

	return &parseInstructionResult{
		ops: ops,
	}, nil
}

func (p *solanaRosettaParserImpl) parseSystemProgram(
	instruction *api.SolanaInstructionV2,
	status string,
	startIndex int,
) (*parseInstructionResult, error) {
	var ops []*rosetta.Operation
	var err error
	instructionType := instruction.GetSystemProgram().GetInstructionType()
	var source string
	var destination string
	var lamports uint64
	switch instructionType {
	case api.SolanaSystemProgram_CREATE_ACCOUNT:
		create := instruction.GetSystemProgram().GetCreateAccount()
		source, destination, lamports = create.GetSource(), create.GetNewAccount(), create.GetLamports()
	case api.SolanaSystemProgram_TRANSFER:
		transfer := instruction.GetSystemProgram().GetTransfer()
		source, destination, lamports = transfer.GetSource(), transfer.GetDestination(), transfer.GetLamports()
	case api.SolanaSystemProgram_CREATE_ACCOUNT_WITH_SEED:
		create := instruction.GetSystemProgram().GetCreateAccountWithSeed()
		source, destination, lamports = create.GetSource(), create.GetNewAccount(), create.GetLamports()
	case api.SolanaSystemProgram_TRANSFER_WITH_SEED:
		transfer := instruction.GetSystemProgram().GetTransferWithSeed()
		source, destination, lamports = transfer.GetSource(), transfer.GetDestination(), transfer.GetLamports()
	}

	if len(source) == 0 || len(destination) == 0 {
		return &parseInstructionResult{
			containsThirdPartyPrograms: true,
		}, nil
	}

	ops, err = p.transferOps(source, destination, status, lamports, startIndex, instructionType.String())
	if err != nil {
		return nil, xerrors.Errorf("failed to parse instruction=%v: %w", instructionType, err)
	}
	return &parseInstructionResult{ops: ops}, nil
}

func (p *solanaRosettaParserImpl) feeOps(transaction *api.SolanaTransactionV2, startIndex int) ([]*rosetta.Operation, error) {
	if transaction.GetMeta().GetFee() <= 0 {
		return nil, nil
	}

	accounts := transaction.GetPayload().GetMessage().GetAccountKeys()
	if len(accounts) == 0 {
		return nil, xerrors.Errorf("accountKeys are empty in transactionId=%v", transaction.TransactionId)
	}

	// the first account always pays the fee
	// no matter whether the transaction is successful or not
	fromAccount := accounts[0].GetPubkey()
	fee := big.NewInt(int64(transaction.GetMeta().GetFee()))
	return []*rosetta.Operation{
		{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: int64(startIndex),
			},
			Type:   OpTypeFee,
			Status: OpStatusSuccess,
			Account: &rosetta.AccountIdentifier{
				Address: fromAccount,
			},
			Amount: &rosetta.Amount{
				Value:    new(big.Int).Neg(fee).String(),
				Currency: &nativeRosettaCurrency,
			},
			Metadata: map[string]*anypb.Any{},
		},
	}, nil
}

func (p *solanaRosettaParserImpl) transferOps(fromAccount string, toAccount string, status string, lamports uint64, startIndex int, instructionType string) ([]*rosetta.Operation, error) {
	amount := big.NewInt(int64(lamports))
	metadata, err := rosetta.FromSDKMetadata(map[string]any{
		instructionTypeMetadataKey: instructionType,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal metadata for instructionType %v: %w", instructionType, err)
	}

	return []*rosetta.Operation{
		{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: int64(startIndex),
			},
			Type:   OpTypeTransfer,
			Status: status,
			Account: &rosetta.AccountIdentifier{
				Address: fromAccount,
			},
			Amount: &rosetta.Amount{
				Value:    new(big.Int).Neg(amount).String(),
				Currency: &nativeRosettaCurrency,
			},
			Metadata: metadata,
		},
		{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: int64(startIndex + 1),
			},
			Type:   OpTypeTransfer,
			Status: status,
			Account: &rosetta.AccountIdentifier{
				Address: toAccount,
			},
			Amount: &rosetta.Amount{
				Value:    amount.String(),
				Currency: &nativeRosettaCurrency,
			},
			Metadata: metadata,
		},
	}, nil
}

func (p *solanaRosettaParserImpl) tokenTransferOp(
	balance *api.SolanaTokenBalance,
	accountKeys []*api.AccountKey,
	amount *big.Int,
	status string,
	startIndex int,
) (*rosetta.Operation, error) {
	address, err := p.getTokenTransferOpAddress(balance, accountKeys)
	if err != nil {
		return nil, xerrors.Errorf("failed to get address for token transfer: %w", err)
	}

	amountMetadata, err := rosetta.FromSDKMetadata(map[string]any{
		ContractAddressAmountMetadataKey: balance.Mint,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal currency metadata for contractAddress %v: %w", balance.Mint, err)
	}

	metadata, err := rosetta.FromSDKMetadata(map[string]any{
		instructionTypeMetadataKey: instructionTypeUnknown,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal metadata for instructionType %v: %w", instructionTypeUnknown, err)
	}

	return &rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: int64(startIndex),
		},
		Type: OpTypeTransfer,
		Account: &rosetta.AccountIdentifier{
			Address: address,
		},
		Amount: &rosetta.Amount{
			Value: amount.String(),
			Currency: &rosetta.Currency{
				Symbol:   UnknownCurrencySymbol,
				Decimals: int32(balance.TokenAmount.Decimals),
				Metadata: amountMetadata,
			},
		},
		Status:   status,
		Metadata: metadata,
	}, nil
}

func (p *solanaRosettaParserImpl) getAccountIndex(address string, accountKeys []*api.AccountKey) (int, error) {
	for i, account := range accountKeys {
		if account.Pubkey == address {
			return i, nil
		}
	}

	return -1, fmt.Errorf("address not found in accountKeys array: %s", address)
}

func (p *solanaRosettaParserImpl) getTokenTransferOpAddress(balance *api.SolanaTokenBalance, accountKeys []*api.AccountKey) (string, error) {
	// address should be token owner. if owner is not present, use the token account
	if len(balance.Owner) != 0 {
		return balance.Owner, nil
	}

	accountIndex := balance.AccountIndex
	if accountIndex >= uint64(len(accountKeys)) || accountIndex < 0 {
		return "", xerrors.Errorf("invalid account index in balance=%+v, len(accountKeys)=%d", balance, len(accountKeys))
	}

	tokenAccount := accountKeys[accountIndex].GetPubkey()
	if len(tokenAccount) == 0 {
		return "", xerrors.New("both owner and token account are empty")
	}

	return tokenAccount, nil
}

func getTokenBalanceLookUpMap(balances []*api.SolanaTokenBalance) map[string]*api.SolanaTokenBalance {
	result := make(map[string]*api.SolanaTokenBalance)
	for _, balance := range balances {
		result[makeTokenBalanceLookUpKey(balance)] = balance
	}
	return result
}

func makeTokenBalanceLookUpKey(balance *api.SolanaTokenBalance) string {
	return fmt.Sprintf("%d#%s#%s", balance.AccountIndex, balance.Mint, balance.Owner)
}
