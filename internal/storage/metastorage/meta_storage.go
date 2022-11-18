package metastorage

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"golang.org/x/xerrors"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	MetaStorage interface {
		BlockStorage
		EventStorage
	}

	metaStorageImpl struct {
		BlockStorage
		EventStorage
	}

	Params struct {
		fx.In
		fxparams.Params
		Session *session.Session
	}

	Result struct {
		fx.Out
		BlockStorage BlockStorage
		EventStorage EventStorage
		MetaStorage  MetaStorage
	}
)

func NewMetaStorage(params Params) (Result, error) {
	blockStorage, err := newBlockStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed create new BlockStorage: %w", err)
	}

	eventStorage, err := newEventStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed create new EventStorage: %w", err)
	}

	metaStorage := &metaStorageImpl{
		BlockStorage: blockStorage,
		EventStorage: eventStorage,
	}

	return Result{
		BlockStorage: blockStorage,
		EventStorage: eventStorage,
		MetaStorage:  metaStorage,
	}, nil
}
