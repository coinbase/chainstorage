package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/xerrors"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

func (m *metaStorageImpl) AcquireSingleBlockUploadGuard(
	ctx context.Context,
	tag uint32,
	height uint64,
	hash string,
) (internal.SingleBlockUploadGuard, error) {
	// Durable retirement is Postgres-only, so DynamoDB has no retirement fence.
	return internal.NewUnfencedSingleBlockUploadGuard(), nil
}

type (
	metaStorageImpl struct {
		internal.BlockStorage
		internal.EventStorage
		internal.TransactionStorage
	}

	Params struct {
		fx.In
		fxparams.Params
		AWSConfig aws.Config
	}

	metaStorageFactory struct {
		params Params
	}
)

func NewMetaStorage(params Params) (internal.Result, error) {
	blockStorage, err := newBlockStorage(params)
	if err != nil {
		return internal.Result{}, xerrors.Errorf("failed create new BlockStorage: %w", err)
	}

	eventStorage, err := newEventStorage(params)
	if err != nil {
		return internal.Result{}, xerrors.Errorf("failed create new EventStorage: %w", err)
	}

	transactionStorage, err := newTransactionStorage(params)
	if err != nil {
		return internal.Result{}, xerrors.Errorf("failed create new TransactionStorage: %w", err)
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
