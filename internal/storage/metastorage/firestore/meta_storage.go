package firestore

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
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
	}

	metaStorageFactory struct {
		params Params
	}
)

func NewMetaStorage(params Params) (internal.Result, error) {
	ctx := context.Background()
	config := params.Config.GCP
	if config == nil {
		return internal.Result{}, fmt.Errorf("failed to create firestore meta storage: missing GCP config")
	}

	client, err := firestore.NewClient(ctx, config.Project)
	if err != nil {
		return internal.Result{}, fmt.Errorf("failed to create firestore client: %w", err)
	}

	blockStorage, err := newBlockStorage(params, client)
	if err != nil {
		return internal.Result{}, fmt.Errorf("failed create new BlockStorage: %w", err)
	}

	eventStorage, err := newEventStorage(params, client)
	if err != nil {
		return internal.Result{}, fmt.Errorf("failed create new EventStorage: %w", err)
	}

	transactionStorage, err := newTransactionStorage(params, client)
	if err != nil {
		return internal.Result{}, fmt.Errorf("failed create new TransactionStorage: %w", err)
	}

	metaStorage := &metaStorageImpl{
		BlockStorage:       blockStorage,
		EventStorage:       eventStorage,
		TransactionStorage: transactionStorage,
	}

	return internal.Result{
		BlockStorage:       blockStorage,
		EventStorage:       eventStorage,
		TransactionStorage: transactionStorage,
		MetaStorage:        metaStorage,
	}, nil
}

// Create implements internal.MetaStorageFactory.
func (f *metaStorageFactory) Create() (internal.Result, error) {
	return NewMetaStorage(f.params)
}

func NewFactory(params Params) internal.MetaStorageFactory {
	return &metaStorageFactory{params}
}
