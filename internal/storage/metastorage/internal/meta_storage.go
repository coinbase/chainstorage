package internal

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlockStorage interface {
		PersistBlockMetas(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error
		GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error)
		GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error)
		GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error)
		GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error)
	}

	EventStorage interface {
		AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error
		GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error)
		GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error)
		GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error)
		GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error)
		SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error
		GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error)
		GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error)
	}

	MetaStorage interface {
		BlockStorage
		EventStorage
	}

	Result struct {
		fx.Out
		BlockStorage BlockStorage
		EventStorage EventStorage
		MetaStorage  MetaStorage
	}

	MetaStorageFactory interface {
		Create() (Result, error)
	}

	MetaStorageFactoryParams struct {
		fx.In
		fxparams.Params
		DynamoDB MetaStorageFactory `name:"metastorage/dynamodb"`
	}
)

func WithMetaStorageFactory(params MetaStorageFactoryParams) (Result, error) {
	var factory MetaStorageFactory
	storageType := params.Config.StorageType.MetaStorageType
	switch storageType {
	case config.MetaStorageType_UNSPECIFIED, config.MetaStorageType_DYNAMODB:
		factory = params.DynamoDB
	}
	if factory == nil {
		return Result{}, xerrors.Errorf("meta storage type is not implemented: %v", storageType)
	}
	result, err := factory.Create()
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create meta storage of type %v, error: %w", storageType, err)
	}
	return result, nil
}
