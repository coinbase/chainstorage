package model

import (
	"fmt"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
)

const (
	TransactionPidKeyName  = "transaction_pid"
	TransactionSortKeyName = "transaction_rid"
	PartitionKeyFormat     = "%d#%v"
)

type (
	TransactionDDBEntry struct {
		TransactionPid string `dynamodbav:"transaction_pid"`
		TransactionRid string `dynamodbav:"transaction_rid"`
		Hash           string `dynamodbav:"hash"`
		BlockHash      string `dynamodbav:"block_hash"`
		BlockNumber    uint64 `dynamodbav:"block_number"`
		BlockTag       uint32 `dynamodbav:"block_tag"`
	}
)

func NewTransactionDDBEntry(transaction *model.Transaction) *TransactionDDBEntry {
	return &TransactionDDBEntry{
		TransactionPid: MakeTransactionPartitionKey(transaction.BlockTag, transaction.Hash),
		TransactionRid: transaction.BlockHash,
		Hash:           transaction.Hash,
		BlockHash:      transaction.BlockHash,
		BlockNumber:    transaction.BlockNumber,
		BlockTag:       transaction.BlockTag,
	}
}

func MakeTransactionPartitionKey(tag uint32, txnHash string) string {
	return fmt.Sprintf(PartitionKeyFormat, tag, txnHash)
}

func TransformToTransaction(entry *TransactionDDBEntry) (*model.Transaction, error) {
	if entry.Hash == "" {
		return nil, xerrors.Errorf("transaction hash is empty for returned ddb item(%+v)", entry)
	}
	if entry.BlockHash == "" {
		return nil, xerrors.Errorf("block hash is empty for returned ddb item(%+v)", entry)
	}

	return &model.Transaction{
		Hash:        entry.Hash,
		BlockNumber: entry.BlockNumber,
		BlockHash:   entry.BlockHash,
		BlockTag:    entry.BlockTag,
	}, nil
}
