package types

import (
	"golang.org/x/xerrors"

	sdk "github.com/coinbase/rosetta-sdk-go/types"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

var (
	// FromSDKDirectionMap maps from rosetta sdk Direction to proto RelatedTransaction_Direction
	FromSDKDirectionMap = map[sdk.Direction]RelatedTransaction_Direction{
		sdk.Backward: RelatedTransaction_BACKWARD,
		sdk.Forward:  RelatedTransaction_FORWARD,
	}

	// FromSDKCoinActionMap maps from rosetta sdk CoinAction to proto CoinChange_CoinAction
	FromSDKCoinActionMap = map[sdk.CoinAction]CoinChange_CoinAction{
		sdk.CoinCreated: CoinChange_COIN_CREATED,
		sdk.CoinSpent:   CoinChange_COIN_SPENT,
	}

	// ToSDKCoinActionMap maps from proto CoinChange_CoinAction to rosetta sdk CoinAction
	ToSDKCoinActionMap = map[CoinChange_CoinAction]sdk.CoinAction{
		CoinChange_COIN_CREATED: sdk.CoinCreated,
		CoinChange_COIN_SPENT:   sdk.CoinSpent,
	}

	// ToSDKDirectionMap maps from proto RelatedTransaction_Direction to rosetta sdk Direction
	ToSDKDirectionMap = map[RelatedTransaction_Direction]sdk.Direction{
		RelatedTransaction_BACKWARD: sdk.Backward,
		RelatedTransaction_FORWARD:  sdk.Forward,
	}
)

// FromSDKBlock converts Block from rosetta sdk type to proto type
func FromSDKBlock(block *sdk.Block) (*Block, error) {
	if block == nil {
		return nil, xerrors.Errorf("block is nil")
	}

	if block.BlockIdentifier == nil || block.ParentBlockIdentifier == nil {
		return nil, xerrors.Errorf("missing required fields BlockIdentifier or ParentBlockIdentifier")
	}

	blockIdentifier := FromSDKBlockIdentifier(block.BlockIdentifier)
	parentBlockIdentifier := FromSDKBlockIdentifier(block.ParentBlockIdentifier)

	blockIndex := blockIdentifier.Index

	transactions, err := FromSDKTransactions(block.Transactions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transactions for block %v: %w", blockIndex, err)
	}

	blockMetadata, err := FromSDKMetadata(block.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block metadata for block %v: %w", blockIndex, err)
	}

	return &Block{
		BlockIdentifier:       blockIdentifier,
		ParentBlockIdentifier: parentBlockIdentifier,
		Timestamp:             ConvertMillisecondsToTimestamp(block.Timestamp),
		Transactions:          transactions,
		Metadata:              blockMetadata,
	}, nil
}

// FromSDKBlocks converts blocks from Rosetta sdk type to proto type
func FromSDKBlocks(blocks []*sdk.Block) ([]*Block, error) {
	var result []*Block
	for _, block := range blocks {
		protoBlock, err := FromSDKBlock(block)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse block: %w", err)
		}
		result = append(result, protoBlock)
	}
	return result, nil
}

// FromSDKBlockIdentifier converts BlockIdentifier from rosetta sdk type to proto type
func FromSDKBlockIdentifier(blockIdentifier *sdk.BlockIdentifier) *BlockIdentifier {
	if blockIdentifier == nil {
		return nil
	}

	return &BlockIdentifier{
		Index: blockIdentifier.Index,
		Hash:  blockIdentifier.Hash,
	}
}

// FromSDKTransaction converts Transaction from rosetta sdk type to proto type
func FromSDKTransaction(transaction *sdk.Transaction) (*Transaction, error) {
	if transaction == nil {
		return nil, nil
	}

	if transaction.TransactionIdentifier == nil {
		return nil, xerrors.Errorf("missing transaction identifier")
	}

	transactionHash := transaction.TransactionIdentifier.Hash

	operations, err := FromSDKOperations(transaction.Operations)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transaction operations transactionHash=%v: %w", transactionHash, err)
	}

	relatedTransactions, err := FromSDKRelatedTransactions(transaction.RelatedTransactions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transaction relatedTransactions transactionHash=%v: %w", transactionHash, err)
	}

	metadata, err := FromSDKMetadata(transaction.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transaction metadata transactionHash=%v: %w", transactionHash, err)
	}

	return &Transaction{
		TransactionIdentifier: &TransactionIdentifier{
			Hash: transactionHash,
		},
		Operations:          operations,
		RelatedTransactions: relatedTransactions,
		Metadata:            metadata,
	}, nil
}

// FromSDKTransactions converts transactions from rosetta sdk type to proto type
func FromSDKTransactions(transactions []*sdk.Transaction) ([]*Transaction, error) {
	result := make([]*Transaction, len(transactions))
	for i, transaction := range transactions {
		tx, err := FromSDKTransaction(transaction)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction: %w", err)
		}
		result[i] = tx
	}
	return result, nil
}

// FromSDKRelatedTransaction converts RelatedTransaction from rosetta sdk type to proto type
func FromSDKRelatedTransaction(relatedTransaction *sdk.RelatedTransaction) (*RelatedTransaction, error) {
	if relatedTransaction == nil {
		return nil, nil
	}

	if relatedTransaction.TransactionIdentifier == nil {
		return nil, xerrors.Errorf("missing TransactionIdentifier of related transaction")
	}

	networkIdentifier, err := FromSDKNetworkIdentifier(relatedTransaction.NetworkIdentifier)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse networkIdentifier: %w", err)
	}

	return &RelatedTransaction{
		TransactionIdentifier: &TransactionIdentifier{
			Hash: relatedTransaction.TransactionIdentifier.Hash,
		},
		Direction:         FromSDKDirectionMap[relatedTransaction.Direction],
		NetworkIdentifier: networkIdentifier,
	}, nil
}

// FromSDKRelatedTransactions converts relatedTransactions from rosetta sdk type to proto type
func FromSDKRelatedTransactions(relatedTransactions []*sdk.RelatedTransaction) ([]*RelatedTransaction, error) {
	result := make([]*RelatedTransaction, len(relatedTransactions))
	for i, transaction := range relatedTransactions {
		tx, err := FromSDKRelatedTransaction(transaction)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse related transaction: %w", err)
		}
		result[i] = tx
	}

	return result, nil
}

// FromSDKSubNetworkIdentifier converts SubNetworkIdentifier from rosetta sdk type to proto type
func FromSDKSubNetworkIdentifier(subNetworkIdentifier *sdk.SubNetworkIdentifier) (*SubNetworkIdentifier, error) {
	if subNetworkIdentifier == nil {
		return nil, nil
	}

	subNetworkMetadata, err := FromSDKMetadata(subNetworkIdentifier.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse subNetworkIdentifier metadata: %w", err)
	}

	return &SubNetworkIdentifier{
		Network:  subNetworkIdentifier.Network,
		Metadata: subNetworkMetadata,
	}, nil
}

// FromSDKNetworkIdentifier converts NetworkIdentifier from rosetta sdk type to proto type
func FromSDKNetworkIdentifier(networkIdentifier *sdk.NetworkIdentifier) (*NetworkIdentifier, error) {
	if networkIdentifier == nil {
		return nil, nil
	}

	subNetworkIdentifier, err := FromSDKSubNetworkIdentifier(networkIdentifier.SubNetworkIdentifier)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse subNetworkIdentifier: %w", err)
	}

	return &NetworkIdentifier{
		Blockchain:           networkIdentifier.Blockchain,
		Network:              networkIdentifier.Network,
		SubNetworkIdentifier: subNetworkIdentifier,
	}, nil
}

// FromSDKOperation converts Operation from rosetta sdk type to proto type
func FromSDKOperation(operation *sdk.Operation) (*Operation, error) {
	if operation == nil {
		return nil, nil
	}

	identifier := FromSDKOperationIdentifier(operation.OperationIdentifier)
	relatedOps := FromSDKOperationIdentifiers(operation.RelatedOperations)

	account, err := FromSDKAccountIdentifier(operation.Account)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse operation account: %w", err)
	}

	amount, err := FromSDKAmount(operation.Amount)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse operation amount: %w", err)
	}

	coinChange := FromSDKCoinChange(operation.CoinChange)

	metadata, err := FromSDKMetadata(operation.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse operation metadata: %w", err)
	}

	return &Operation{
		OperationIdentifier: identifier,
		RelatedOperations:   relatedOps,
		Type:                operation.Type,
		Status:              StringDeref(operation.Status),
		Account:             account,
		Amount:              amount,
		CoinChange:          coinChange,
		Metadata:            metadata,
	}, nil
}

// FromSDKOperations converts operations from rosetta sdk type to proto type
func FromSDKOperations(operations []*sdk.Operation) ([]*Operation, error) {
	result := make([]*Operation, len(operations))
	for i, operation := range operations {
		op, err := FromSDKOperation(operation)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse operation: %w", err)
		}
		result[i] = op
	}
	return result, nil
}

// FromSDKOperationIdentifier converts OperationIdentifier from rosetta sdk type to proto type
func FromSDKOperationIdentifier(oi *sdk.OperationIdentifier) *OperationIdentifier {
	if oi == nil {
		return nil
	}

	return &OperationIdentifier{
		Index:        oi.Index,
		NetworkIndex: Int64Deref(oi.NetworkIndex),
	}
}

// FromSDKOperationIdentifiers converts operationIdentifiers from rosetta sdk type to proto type
func FromSDKOperationIdentifiers(ops []*sdk.OperationIdentifier) []*OperationIdentifier {
	result := make([]*OperationIdentifier, len(ops))
	for i, op := range ops {
		result[i] = FromSDKOperationIdentifier(op)
	}
	return result
}

// FromSDKAccountIdentifier converts AccountIdentifier from rosetta sdk type to proto type
func FromSDKAccountIdentifier(accountIdentifier *sdk.AccountIdentifier) (*AccountIdentifier, error) {
	if accountIdentifier == nil {
		return nil, nil
	}

	accountMetadata, err := FromSDKMetadata(accountIdentifier.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse account metadata: %w", err)
	}

	subAccount, err := FromSDKSubAccountIdentifier(accountIdentifier.SubAccount)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse subAccount: %w", err)
	}

	return &AccountIdentifier{
		Address:    accountIdentifier.Address,
		Metadata:   accountMetadata,
		SubAccount: subAccount,
	}, nil
}

// FromSDKSubAccountIdentifier converts SubAccountIdentifier from rosetta sdk type to proto type
func FromSDKSubAccountIdentifier(subAccountIdentifier *sdk.SubAccountIdentifier) (*SubAccountIdentifier, error) {
	if subAccountIdentifier == nil {
		return nil, nil
	}

	subAccountMetadata, err := FromSDKMetadata(subAccountIdentifier.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse subAccount metadata: %w", err)
	}
	return &SubAccountIdentifier{
		Address:  subAccountIdentifier.Address,
		Metadata: subAccountMetadata,
	}, nil
}

// FromSDKAmount converts Amount from rosetta sdk type to proto type
func FromSDKAmount(amount *sdk.Amount) (*Amount, error) {
	if amount == nil {
		return nil, nil
	}

	amountMetadata, err := FromSDKMetadata(amount.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse amount metadata: %w", err)
	}

	currency, err := FromSDKCurrency(amount.Currency)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse amount currency: %w", err)
	}

	return &Amount{
		Value:    amount.Value,
		Metadata: amountMetadata,
		Currency: currency,
	}, nil
}

// FromSDKCurrency converts Currency from rosetta sdk type to proto type
func FromSDKCurrency(currency *sdk.Currency) (*Currency, error) {
	if currency == nil {
		return nil, nil
	}

	metadata, err := FromSDKMetadata(currency.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse currency metadata: %w", err)
	}

	return &Currency{
		Symbol:   currency.Symbol,
		Decimals: currency.Decimals,
		Metadata: metadata,
	}, nil
}

// FromSDKCoinChange converts CoinChange from rosetta sdk type to proto type
func FromSDKCoinChange(coinChange *sdk.CoinChange) *CoinChange {
	if coinChange == nil {
		return nil
	}

	coinIdentifier := FromSDKCoinIdentifier(coinChange.CoinIdentifier)

	return &CoinChange{
		CoinAction:     FromSDKCoinActionMap[coinChange.CoinAction],
		CoinIdentifier: coinIdentifier,
	}
}

// FromSDKCoinIdentifier converts CoinIdentifier from rosetta sdk type to proto type
func FromSDKCoinIdentifier(coinIdentifier *sdk.CoinIdentifier) *CoinIdentifier {
	if coinIdentifier == nil {
		return nil
	}

	return &CoinIdentifier{
		Identifier: coinIdentifier.Identifier,
	}
}

// FromSDKMetadata converts metadata from rosetta sdk type to proto type
func FromSDKMetadata(metadata map[string]interface{}) (map[string]*anypb.Any, error) {
	result := make(map[string]*anypb.Any, len(metadata))
	for k, v := range metadata {
		value, err := MarshalToAny(v)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal %v to Any: %w", v, err)
		}
		result[k] = value
	}
	return result, nil
}

// ToSDKTransaction converts Transaction from proto type to sdk type
func ToSDKTransaction(transaction *Transaction) (*sdk.Transaction, error) {
	if transaction == nil {
		return nil, nil
	}

	if transaction.TransactionIdentifier == nil {
		return nil, xerrors.Errorf("missing transaction identifier")
	}

	transactionHash := transaction.GetTransactionIdentifier().GetHash()
	operations, err := ToSDKOperations(transaction.Operations)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transaction operations to sdk type transactionHash=%v: %w", transactionHash, err)
	}

	relatedTransactions, err := ToSDKRelatedTransactions(transaction.RelatedTransactions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse related transactions to sdk type transactionHash=%v: %w", transactionHash, err)
	}

	metadata, err := ToSDKMetadata(transaction.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transaction metadata to sdk type transactionHash=%v: %w", transactionHash, err)
	}

	return &sdk.Transaction{
		TransactionIdentifier: &sdk.TransactionIdentifier{
			Hash: transactionHash,
		},
		Operations:          operations,
		RelatedTransactions: relatedTransactions,
		Metadata:            metadata,
	}, nil
}

// ToSDKOperations converts Operation from proto type to sdk type
func ToSDKOperations(operations []*Operation) ([]*sdk.Operation, error) {
	result := make([]*sdk.Operation, len(operations))
	for i, operation := range operations {
		op, err := ToSDKOperation(operation)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse operation to sdk type: %w", err)
		}
		result[i] = op
	}
	return result, nil
}

// ToSDKRelatedTransactions converts list of RelatedTransaction from proto type to sdk type
func ToSDKRelatedTransactions(relatedTransactions []*RelatedTransaction) ([]*sdk.RelatedTransaction, error) {
	result := make([]*sdk.RelatedTransaction, len(relatedTransactions))
	for i, transaction := range relatedTransactions {
		tx, err := ToSDKRelatedTransaction(transaction)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse related transaction to sdk type: %w", err)
		}
		result[i] = tx
	}
	return result, nil
}

// ToSDKRelatedTransaction converts RelatedTransaction from proto type to sdk type
func ToSDKRelatedTransaction(relatedTransaction *RelatedTransaction) (*sdk.RelatedTransaction, error) {
	if relatedTransaction == nil {
		return nil, nil
	}

	if relatedTransaction.TransactionIdentifier == nil {
		return nil, xerrors.Errorf("missing TransactionIdentifier of related transaction")
	}

	networkIdentifier, err := ToSDKNetworkIdentifier(relatedTransaction.NetworkIdentifier)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse networkIdentifier to sdk type: %w", err)
	}

	return &sdk.RelatedTransaction{
		TransactionIdentifier: &sdk.TransactionIdentifier{
			Hash: relatedTransaction.GetTransactionIdentifier().GetHash(),
		},
		NetworkIdentifier: networkIdentifier,
		Direction:         ToSDKDirectionMap[relatedTransaction.Direction],
	}, nil
}

// ToSDKSubNetworkIdentifier converts SubNetworkIdentifier from proto type to sdk type
func ToSDKSubNetworkIdentifier(subNetworkIdentifier *SubNetworkIdentifier) (*sdk.SubNetworkIdentifier, error) {
	if subNetworkIdentifier == nil {
		return nil, nil
	}

	subNetworkMetadata, err := ToSDKMetadata(subNetworkIdentifier.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse subNetworkIdentifier metadata to sdk type: %w", err)
	}

	return &sdk.SubNetworkIdentifier{
		Network:  subNetworkIdentifier.Network,
		Metadata: subNetworkMetadata,
	}, nil
}

// ToSDKNetworkIdentifier converts NetworkIdentifier from proto type to sdk type
func ToSDKNetworkIdentifier(networkIdentifier *NetworkIdentifier) (*sdk.NetworkIdentifier, error) {
	if networkIdentifier == nil {
		return nil, nil
	}

	subNetworkIdentifier, err := ToSDKSubNetworkIdentifier(networkIdentifier.SubNetworkIdentifier)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse subNetworkIdentifier to sdk type: %w", err)
	}

	return &sdk.NetworkIdentifier{
		Blockchain:           networkIdentifier.Blockchain,
		Network:              networkIdentifier.Network,
		SubNetworkIdentifier: subNetworkIdentifier,
	}, nil
}

// ToSDKCoinChange converts CoinChange from proto type to sdk type
func ToSDKCoinChange(coinChange *CoinChange) *sdk.CoinChange {
	if coinChange == nil {
		return nil
	}
	result := &sdk.CoinChange{
		CoinAction: ToSDKCoinActionMap[coinChange.CoinAction],
	}
	if coinChange.CoinIdentifier != nil {
		result.CoinIdentifier = &sdk.CoinIdentifier{
			Identifier: coinChange.CoinIdentifier.Identifier,
		}
	}
	return result
}

// ToSDKOperationIdentifiers converts OperationIdentifier list from proto type to sdk type
func ToSDKOperationIdentifiers(ops []*OperationIdentifier) []*sdk.OperationIdentifier {
	result := make([]*sdk.OperationIdentifier, len(ops))
	for i, op := range ops {
		result[i] = &sdk.OperationIdentifier{
			Index:        op.Index,
			NetworkIndex: Int64Ref(op.NetworkIndex),
		}
	}
	return result
}

// ToSDKOperation converts Operation from proto type to sdk type
func ToSDKOperation(rosettaOp *Operation) (*sdk.Operation, error) {
	if rosettaOp == nil {
		return nil, nil
	}

	if rosettaOp.OperationIdentifier == nil {
		return nil, xerrors.Errorf("missing OperationIdentifier")
	}

	account, err := ToSDKAccountIdentifier(rosettaOp.Account)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse account to sdk type: %w", err)
	}
	amount, err := ToSDKAmount(rosettaOp.Amount)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse amount to sdk type: %w", err)
	}
	metadata, err := ToSDKMetadata(rosettaOp.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse operation metadata to sdk type: %w", err)
	}

	result := &sdk.Operation{
		OperationIdentifier: &sdk.OperationIdentifier{
			Index:        rosettaOp.OperationIdentifier.Index,
			NetworkIndex: &rosettaOp.OperationIdentifier.NetworkIndex,
		},
		RelatedOperations: ToSDKOperationIdentifiers(rosettaOp.RelatedOperations),
		Type:              rosettaOp.Type,
		Status:            &rosettaOp.Status,
		Account:           account,
		Amount:            amount,
		CoinChange:        ToSDKCoinChange(rosettaOp.CoinChange),
		Metadata:          metadata,
	}

	return result, nil
}

// ToSDKAccountIdentifier converts AccountIdentifier from proto type to sdk type
func ToSDKAccountIdentifier(account *AccountIdentifier) (*sdk.AccountIdentifier, error) {
	if account == nil {
		return nil, nil
	}
	accountMetadata, err := ToSDKMetadata(account.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse account metadata %+v: %w", account.Metadata, err)
	}
	sdkAccount := &sdk.AccountIdentifier{
		Address:  account.Address,
		Metadata: accountMetadata,
	}
	subAccount := account.SubAccount
	if subAccount != nil {
		subAccountMetadata, err := ToSDKMetadata(subAccount.Metadata)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse subAccount metadata %+v: %w", subAccount.Metadata, err)
		}
		sdkAccount.SubAccount = &sdk.SubAccountIdentifier{
			Address:  subAccount.Address,
			Metadata: subAccountMetadata,
		}
	}
	return sdkAccount, nil
}

// ToSDKAmount converts Amount from proto type to rosetta sdk type
func ToSDKAmount(amount *Amount) (*sdk.Amount, error) {
	if amount == nil {
		return nil, nil
	}
	amountMetadata, err := ToSDKMetadata(amount.Metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse amount metadata %+v: %w", amount.Metadata, err)
	}

	sdkAmount := &sdk.Amount{
		Value:    amount.Value,
		Metadata: amountMetadata,
	}
	amountCurrency := amount.Currency
	if amountCurrency != nil {
		currencyMetadata, err := ToSDKMetadata(amountCurrency.Metadata)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse amount currency metadata %+v: %w", amountCurrency.Metadata, err)
		}
		sdkAmount.Currency = &sdk.Currency{
			Symbol:   amountCurrency.Symbol,
			Decimals: amountCurrency.Decimals,
			Metadata: currencyMetadata,
		}
	}
	return sdkAmount, nil
}

// ToSDKMetadata converts metadata from proto type to rosetta sdk type
func ToSDKMetadata(metadata map[string]*anypb.Any) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(metadata))
	for k, v := range metadata {
		value, err := UnmarshalToInterface(v)
		if err != nil {
			return nil, xerrors.Errorf("unable to unmarshal proto value %+v to a generic Go interface", v)
		}
		result[k] = value
	}
	return result, nil
}
