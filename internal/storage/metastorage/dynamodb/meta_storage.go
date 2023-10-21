package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"golang.org/x/xerrors"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	metaStorageImpl struct {
		internal.BlockStorage
		internal.EventStorage
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
		return internal.Result{}, xerrors.Errorf("failed create new BlockStorage: %w", err)
	}

	eventStorage, err := newEventStorage(params)
	if err != nil {
		return internal.Result{}, xerrors.Errorf("failed create new EventStorage: %w", err)
	}

	metaStorage := &metaStorageImpl{
		BlockStorage: blockStorage,
		EventStorage: eventStorage,
	}

	return internal.Result{
		MetaStorage:  metaStorage,
		BlockStorage: blockStorage,
		EventStorage: eventStorage,
	}, nil
}

// Create implements internal.MetaStorageFactory.
func (f *metaStorageFactory) Create() (internal.Result, error) {
	return NewMetaStorage(f.params)
}

func NewFactory(params Params) internal.MetaStorageFactory {
	return &metaStorageFactory{params}
}
