package metastorage

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlockEvent           = model.BlockEvent
	EventEntry           = model.EventEntry
	MetaStorage          = internal.MetaStorage
	BlockStorage         = internal.BlockStorage
	EventStorage         = internal.EventStorage
	EventsToChainAdaptor = internal.EventsToChainAdaptor
)

const (
	EventIdStartValue = internal.EventIdStartValue
	EventIdDeleted    = internal.EventIdDeleted
)

func NewBlockEventWithBlockMeta(eventType api.BlockchainEvent_Type, block *api.BlockMetadata) *BlockEvent {
	return internal.NewBlockEventWithBlockMeta(eventType, block)
}

func NewBlockEvent(eventType api.BlockchainEvent_Type, blockHash string, parentHash string, blockHeight uint64, tag uint32, skipped bool, blockTimestamp int64) *BlockEvent {
	return internal.NewBlockEvent(eventType, blockHash, parentHash, blockHeight, tag, skipped, blockTimestamp)
}

func NewEventEntry(eventTag uint32, eventId int64, inputEvent *BlockEvent) *EventEntry {
	return internal.NewEventEntry(eventTag, eventId, inputEvent)
}

func NewEventsToChainAdaptor() *EventsToChainAdaptor {
	return internal.NewEventsToChainAdaptor()
}

func WithMetaStorageFactory(params internal.MetaStorageFactoryParams) (internal.Result, error) {
	var factory internal.MetaStorageFactory
	switch params.Config.StorageType.MetaStorageType {
	case config.MetaStorageType_UNSPECIFIED, config.MetaStorageType_DYNAMODB:
		factory = params.DynamoDB
	}
	if factory == nil {
		return internal.Result{}, xerrors.Errorf(
			"meta storage type is not implemented: %v",
			params.Config.StorageType.MetaStorageType)
	}
	result, err := factory.Create()
	if err != nil {
		return internal.Result{}, xerrors.Errorf("failed to create meta storage, error: %w", err)
	}
	return result, nil
}

var Module = fx.Options(
	dynamodb.Module,
	fx.Provide(WithMetaStorageFactory),
)
