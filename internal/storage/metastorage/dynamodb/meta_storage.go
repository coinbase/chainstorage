package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	metaStorageImpl struct {
		internal.BlockStorage
		internal.EventStorage
		internal.TransactionStorage
	}

	Params struct {
		fx.In
		fxparams.Params
		Session *session.Session
	}

	metaStorageFactory struct {
		params Params
	}
)

func NewMetaStorage(params Params) (internal.Result, error) {
	blockStorage, err := newBlockStorage(params)
	if err != nil {
		return internal.Result{}, fmt.Errorf("failed create new BlockStorage: %w", err)
	}

	eventStorage, err := newEventStorage(params)
	if err != nil {
		return internal.Result{}, fmt.Errorf("failed create new EventStorage: %w", err)
	}

	transactionStorage, err := newTransactionStorage(params)
	if err != nil {
		return internal.Result{}, fmt.Errorf("failed create new TransactionStorage: %w", err)
	}

	metaStorage := &metaStorageImpl{
		BlockStorage:       blockStorage,
		EventStorage:       eventStorage,
		TransactionStorage: transactionStorage,
	}

	return internal.Result{
		BlockStorage: blockStorage,
		EventStorage: eventStorage,
		MetaStorage:  metaStorage,
	}, nil
}

// Create implements internal.MetaStorageFactory.
func (f *metaStorageFactory) Create() (internal.Result, error) {
	return NewMetaStorage(f.params)
}

func NewFactory(params Params) internal.MetaStorageFactory {
	return &metaStorageFactory{params}
}
