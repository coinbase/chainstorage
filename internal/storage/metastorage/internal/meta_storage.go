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
		// GetBlocksByHeights gets blocks by heights. Results is an ordered array that matches the order in `heights` array
		// i.e. if the heights is [100,2,3], it will return the metadata in order: [block 100, block 2, block 3]
		GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error)
	}

	EventStorage interface {
		AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error
		AddEventEntries(ctx context.Context, eventTag uint32, eventEntries []*model.EventEntry) error
		GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error)
		GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error)
		GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error)
		GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error)
		SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error
		GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error)
		GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error)
	}

	TransactionStorage interface {
		// AddTransactions adds or updates a transaction to block number and hash mapping to the storage.
		AddTransactions(ctx context.Context, transaction []*model.Transaction, parallelism int) error

		// GetTransaction returns a slice of model.Transaction objects, representing the blocks'
		// information for the specific transaction.
		//
		// There are two reasons that a transaction could map to multiple blocks:
		// 1. blockchain reorgs
		// 2. protocol by design, e.g. NEAR
		GetTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*model.Transaction, error)
	}

	MetaStorage interface {
		BlockStorage
		EventStorage
		TransactionStorage
	}

	Result struct {
		fx.Out
		BlockStorage       BlockStorage
		EventStorage       EventStorage
		MetaStorage        MetaStorage
		TransactionStorage TransactionStorage
	}

	MetaStorageFactory interface {
		Create() (Result, error)
	}

	MetaStorageFactoryParams struct {
		fx.In
		fxparams.Params
		DynamoDB  MetaStorageFactory `name:"metastorage/dynamodb"`
		Firestore MetaStorageFactory `name:"metastorage/firestore"`
	}
)

func WithMetaStorageFactory(params MetaStorageFactoryParams) (Result, error) {
	var factory MetaStorageFactory
	storageType := params.Config.StorageType.MetaStorageType
	switch storageType {
	case config.MetaStorageType_UNSPECIFIED, config.MetaStorageType_DYNAMODB:
		factory = params.DynamoDB
	case config.MetaStorageType_FIRESTORE:
		factory = params.Firestore
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
