package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
)

type (
	transactionStorageImpl struct {
		db *sql.DB
	}
)

func newTransactionStorage(db *sql.DB, params Params) (internal.TransactionStorage, error) {
	accessor := &transactionStorageImpl{
		db: db,
	}
	return accessor, nil
}

func (t *transactionStorageImpl) AddTransactions(ctx context.Context, transaction []*model.Transaction, parallelism int) error {
	// TODO: Implement transaction insertion
	return errors.New("not implemented")
}

func (t *transactionStorageImpl) GetTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*model.Transaction, error) {
	// TODO: Implement get transaction
	return nil, errors.New("not implemented")
}
