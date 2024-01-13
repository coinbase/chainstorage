package firestore

import (
	"context"

	"cloud.google.com/go/firestore"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
)

type (
	transactionStorageImpl struct {
		client                           *firestore.Client
		instrumentAddOrUpdateTransaction instrument.Instrument
		instrumentGetTransaction         instrument.InstrumentWithResult[[]*model.Transaction]
	}
)

var _ internal.TransactionStorage = (*transactionStorageImpl)(nil)

func newTransactionStorage(params Params, client *firestore.Client) (internal.TransactionStorage, error) {
	metrics := params.Metrics.SubScope("transaction_storage").Tagged(map[string]string{
		"storage_type": "firestore",
	})
	return &transactionStorageImpl{
		client:                           client,
		instrumentAddOrUpdateTransaction: instrument.New(metrics, "add_transactions"),
		instrumentGetTransaction:         instrument.NewWithResult[[]*model.Transaction](metrics, "get_transaction"),
	}, nil
}

// AddTransactions implements internal.TransactionStorage.
func (*transactionStorageImpl) AddTransactions(ctx context.Context, transaction []*model.Transaction, parallelism int) error {
	panic("unimplemented")
}

// GetTransaction implements internal.TransactionStorage.
func (*transactionStorageImpl) GetTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*model.Transaction, error) {
	panic("unimplemented")
}
