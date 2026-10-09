package postgres

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

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
	// Use shared connection pool instead of creating new connections
	pool, err := GetConnectionPool(context.Background(), params.Config.AWS.Postgres)
	if err != nil {
		return internal.Result{}, err
	}

	db := pool.DB()
	if db == nil {
		return internal.Result{}, xerrors.New("connection pool returned nil database connection")
	}
	// Create storage implementations with database connection
	blockStorage, err := newBlockStorage(db, params)
	if err != nil {
		return internal.Result{}, err
	}

	eventStorage, err := newEventStorage(db, params)
	if err != nil {
		return internal.Result{}, err
	}

	transactionStorage, err := newTransactionStorage(db, params)
	if err != nil {
		return internal.Result{}, err
	}

	// Combine into meta storage
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

func (m *metaStorageImpl) AcquireSingleBlockUploadGuard(
	ctx context.Context,
	tag uint32,
	height uint64,
	hash string,
) (internal.SingleBlockUploadGuard, error) {
	storage, ok := m.BlockStorage.(internal.SingleBlockUploadGuardStorage)
	if !ok {
		return nil, xerrors.New("postgres block storage does not support single-block upload guards")
	}
	return storage.AcquireSingleBlockUploadGuard(ctx, tag, height, hash)
}

func (f *metaStorageFactory) Create() (internal.Result, error) {
	return NewMetaStorage(f.params)
}

func NewFactory(params Params) internal.MetaStorageFactory {
	return &metaStorageFactory{params}
}
